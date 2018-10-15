package dcos_metadata

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/processors"

	"github.com/dcos/dcos-go/dcos/http/transport"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/agent"
	"github.com/mesos/mesos-go/api/v1/lib/agent/calls"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli/httpagent"
)

const dcosMetricsPrefix = "DCOS_METRICS_"
dcosMetricsLabelsWhitelist := [...]string{"DCOS_SERVICE_NAME"}

type DCOSMetadata struct {
	MesosAgentUrl     string
	Timeout           internal.Duration
	RateLimit         internal.Duration
	CaCertificatePath string
	IamConfigPath     string
	containers        map[string]containerInfo
	mu                sync.Mutex
	once              Once
	client            *httpcli.Client
}

// containerInfo is a tuple of metadata which we use to map a container ID to
// information about the task, executor and framework.
type containerInfo struct {
	containerID   string
	taskName      string
	executorName  string
	frameworkName string
	taskLabels    map[string]string
}

const sampleConfig = `
	## The URL of the local mesos agent
	mesos_agent_url = "http://$NODE_PRIVATE_IP:5051"
	## The period after which requests to mesos agent should time out
	timeout = "10s"
	## The minimum period between requests to the mesos agent
	rate_limit = "5s"
  ## Optional IAM configuration
  # ca_certificate_path = "/run/dcos/pki/CA/ca-bundle.crt"
  # iam_config_path = "/run/dcos/etc/dcos-telegraf/service_account.json"
`

// SampleConfig returns the default configuration
func (dm *DCOSMetadata) SampleConfig() string {
	return sampleConfig
}

// Description returns a one-sentence description of dcos_metadata
func (dm *DCOSMetadata) Description() string {
	return "Plugin for adding metadata to dcos-specific metrics"
}

// Apply the filter to the given metrics
func (dm *DCOSMetadata) Apply(in ...telegraf.Metric) []telegraf.Metric {
	// stale tracks whether our container cache is stale
	stale := false

	// track unrecognised container ids
	unknown := map[string]bool{}

	for _, metric := range in {
		// Ignore metrics without container_id tag
		if cid, ok := metric.Tags()["container_id"]; ok {
			if c, ok := dm.containers[cid]; ok {
				// Data for this container was cached
				for k, v := range c.taskLabels {
					metric.AddTag(k, v)
				}
				metric.AddTag("service_name", c.frameworkName)
				if c.executorName != "" {
					metric.AddTag("executor_name", c.executorName)
				}
				metric.AddTag("task_name", c.taskName)
			} else {
				unknown[cid] = true
				stale = true
			}
		}
	}

	if stale {
		cids := []string{}
		for cid, _ := range unknown {
			cids = append(cids, cid)
		}
		go dm.refresh(cids...)
	}

	return in
}

// refresh triggers a call to Mesos state. Calls to refresh are throttled by
// the rate_limit option in configuration. Optionally, the container IDs which
// caused the refresh may be passed in to be logged.
func (dm *DCOSMetadata) refresh(cids ...string) {
	dm.once.Do(func() {
		// Subsequent calls to refresh() will be ignored until the RateLimit period
		// has expired
		go func() {
			time.Sleep(dm.RateLimit.Duration)
			dm.once.Reset()
		}()

		for _, cid := range cids {
			log.Printf("I! Metadata for container %q was not found in cache", cid)
		}

		client, err := dm.getClient()
		if err != nil {
			log.Printf("E! %s", err)
			return
		}

		cli := httpagent.NewSender(client.Send)
		ctx, cancel := context.WithTimeout(context.Background(), dm.Timeout.Duration)
		defer cancel()

		state, err := dm.getState(ctx, cli)
		if err != nil {
			log.Printf("E! %s", err)
			return
		}
		err = dm.cache(state)
		if err != nil {
			log.Printf("E! %s", err)
		}
	})
}

// getState requests state from the operator API
func (dm *DCOSMetadata) getState(ctx context.Context, cli calls.Sender) (*agent.Response_GetState, error) {
	resp, err := cli.Send(ctx, calls.NonStreaming(calls.GetState()))
	if err != nil {
		return nil, err
	}
	r, err := processResponse(resp, agent.Response_GET_STATE)
	if err != nil {
		return nil, err
	}

	gs := r.GetGetState()
	if gs == nil {
		return nil, errors.New("the getState response from the mesos agent was empty")
	}
	return gs, nil
}

// cache caches container info from state
func (dm *DCOSMetadata) cache(gs *agent.Response_GetState) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	containers := map[string]containerInfo{}

	gt := gs.GetGetTasks()
	if gt == nil { // no tasks are running on the cluster
		dm.containers = containers
		return nil
	}

	// map frameworks and executors in advance to avoid iterating
	// over both for each container
	frameworkNames := mapFrameworkNames(gs.GetGetFrameworks())
	executorNames := mapExecutorNames(gs.GetGetExecutors())

	for _, t := range gt.GetLaunchedTasks() {
		cid := getContainerID(t.GetStatuses())
		eName := ""
		// ExecutorID is _not_ guaranteed not to be nil (FrameworkID is)
		if eid := t.GetExecutorID(); eid != nil {
			eName = executorNames[eid.Value]
		}

		// If container ID could not be found, don't add a nil entry
		if cid != "" {
			containers[cid] = containerInfo{
				containerID:   cid,
				taskName:      t.GetName(),
				executorName:  eName,
				frameworkName: frameworkNames[t.GetFrameworkID().Value],
				taskLabels:    mapTaskLabels(t.GetLabels()),
			}
		}
	}

	dm.containers = containers
	return nil
}

// getClient returns an httpcli client configured with the available levels of
// TLS and IAM according to flags set in the config
func (dm *DCOSMetadata) getClient() (*httpcli.Client, error) {
	if dm.client != nil {
		return dm.client, nil
	}
	uri := dm.MesosAgentUrl + "/api/v1"
	client := httpcli.New(httpcli.Endpoint(uri))
	cfgOpts := []httpcli.ConfigOpt{}
	opts := []httpcli.Opt{}

	var tr *http.Transport
	var rt http.RoundTripper
	var err error

	if dm.CaCertificatePath != "" {
		if tr, err = getTransport(dm.CaCertificatePath); err != nil {
			return client, err
		}
	}

	if dm.IamConfigPath != "" {
		if rt, err = transport.NewRoundTripper(
			tr,
			transport.OptionReadIAMConfig(dm.IamConfigPath)); err != nil {
			return client, err
		}
		cfgOpts = append(cfgOpts, httpcli.RoundTripper(rt))
	}
	opts = append(opts, httpcli.Do(httpcli.With(cfgOpts...)))
	client.With(opts...)

	dm.client = client
	return client, nil
}

// getContainerID retrieves the container ID linked to this task. Task can have
// multiple statuses. Each status can have multiple container IDs. In DC/OS,
// there is a one-to-one mapping between tasks and containers; however it is
// possible to have nested containers. Therefore we use the first status, and
// return its parent container ID if possible, and if not, then its ID.
func getContainerID(statuses []mesos.TaskStatus) string {
	// Container ID is held in task status
	for _, s := range statuses {
		if cs := s.GetContainerStatus(); cs != nil {
			// TODO (philipnrmn) account for deeply-nested containers
			if cid := cs.GetContainerID(); cid != nil {
				return cid.GetValue()
			}
		}
	}
	return ""
}

// mapFrameworkNames returns a map of framework ids and names
func mapFrameworkNames(gf *agent.Response_GetFrameworks) map[string]string {
	results := map[string]string{}
	if gf != nil {
		for _, f := range gf.GetFrameworks() {
			fi := f.GetFrameworkInfo()
			id := fi.GetID().Value
			results[id] = fi.GetName()
		}
	}
	return results
}

// mapExecutorNames returns a map of executor ids and names
func mapExecutorNames(ge *agent.Response_GetExecutors) map[string]string {
	results := map[string]string{}
	if ge != nil {
		for _, e := range ge.GetExecutors() {
			ei := e.GetExecutorInfo()
			id := ei.GetExecutorID().Value
			results[id] = ei.GetName()
		}
	}
	return results
}

// mapTaskLabels returns a map of all task labels prefixed DCOS_METRICS_
func mapTaskLabels(labels *mesos.Labels) map[string]string {
	results := map[string]string{}
	if labels != nil {
		var selected bool
		for _, l := range labels.GetLabels() {
			selected = false

			k := l.GetKey()

			if strings.HasPrefix(k, dcosMetricsPrefix) {
				k = strings.TrimPrefix(k, dcosMetricsPrefix)
				if len(k) > 0 {
					selected = true
				}
				for _, whitelistLabel := range dcosMetricsLabelsWhitelist {
					if k == whitelistLabel {
						selected = true
						break
					}
				}
			}
			if selected {
				results[k] = l.GetValue()
			}
		}
	}
	return results
}

// processResponse reads the response from a triggered request, verifies its
// type, and returns an agent response
func processResponse(resp mesos.Response, t agent.Response_Type) (agent.Response, error) {
	var r agent.Response
	defer func() {
		if resp != nil {
			resp.Close()
		}
	}()
	for {
		if err := resp.Decode(&r); err != nil {
			if err == io.EOF {
				break
			}
			return r, err
		}
	}
	if r.GetType() == t {
		return r, nil
	} else {
		return r, fmt.Errorf("processResponse expected type %q, got %q", t, r.GetType())
	}
}

// init is called once when telegraf starts
func init() {
	processors.Add("dcos_metadata", func() telegraf.Processor {
		return &DCOSMetadata{
			Timeout:   internal.Duration{Duration: 10 * time.Second},
			RateLimit: internal.Duration{Duration: 5 * time.Second},
		}
	})
}

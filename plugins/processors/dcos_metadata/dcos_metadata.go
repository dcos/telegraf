package dcos_metadata

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/processors"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/agent"
	"github.com/mesos/mesos-go/api/v1/lib/agent/calls"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli/httpagent"
)

type DCOSMetadata struct {
	MesosAgentUrl string
	Timeout       internal.Duration
	RateLimit     internal.Duration
	containers    map[string]containerInfo
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
				log.Printf("I! Information for container %q was not found in cache", cid)
				stale = true
			}
		}
	}

	if stale {
		go dm.refresh()
	}

	return in
}

func (dm *DCOSMetadata) refresh() {
		uri := dm.MesosAgentUrl + "/api/v1"
		cli := httpagent.NewSender(httpcli.New(httpcli.Endpoint(uri)).Send)
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

// getContainerID retrieves the container ID linked to this task. Task can have
// multiple statuses. Each status can have multiple container IDs. In DC/OS,
// there is a one-to-one mapping between tasks and containers; however it is
// possible to have nested containers. Therefore we use the first status, and
// return its parent container ID if possible, and if not, then its ID.
func getContainerID(statuses []mesos.TaskStatus) string {
	// Container ID is held in task status
	for _, s := range statuses {
		if cs := s.GetContainerStatus(); cs != nil {
			if cid := cs.GetContainerID(); cid != nil {
				// TODO (philipnrmn) account for deeply-nested containers
				if p := cid.GetParent(); p != nil {
					return p.GetValue()
				}
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
		for _, l := range labels.GetLabels() {
			k := l.GetKey()
			if len(k) > 13 {
				if k[:13] == "DCOS_METRICS_" {
					results[k[13:]] = l.GetValue()
				}
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
			Timeout:   internal.Duration{10 * time.Second},
			RateLimit: internal.Duration{5 * time.Second},
		}
	})
}

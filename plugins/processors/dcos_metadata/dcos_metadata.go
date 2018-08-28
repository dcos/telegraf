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
				metric.AddTag("service_name", c.frameworkName)
					metric.AddTag("executor_name", c.executorName)
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

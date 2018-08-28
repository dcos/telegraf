package dcos_metadata

import (
	"log"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/processors"
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
	for _, metric := range in {
		// Ignore metrics without container_id tag
		if cid, ok := metric.Tags()["container_id"]; ok {
			if c, ok := dm.containers[cid]; ok {
				metric.AddTag("service_name", c.frameworkName)
					metric.AddTag("executor_name", c.executorName)
				metric.AddTag("task_name", c.taskName)
			} else {
				log.Printf("I! Information for container %q was not found in cache", cid)
			}
		}
	}

	return in
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

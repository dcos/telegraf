package dcos_containers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/agent"
	"github.com/mesos/mesos-go/api/v1/lib/agent/calls"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli/httpagent"
)

const sampleConfig = `
## The URL of the local mesos agent
mesos_agent_url = "http://$NODE_PRIVATE_IP:5051"
## The period after which requests to mesos agent should time out
timeout = 10s
`

// DCOSContainers describes the options available to this plugin
type DCOSContainers struct {
	MesosAgentUrl string
	Timeout       time.Duration
}

// measurement is a combination of fields and tags specific to those fields
type measurement struct {
	name   string
	fields map[string]interface{}
	tags   map[string]string
}

// combineTags combines this measurement's tags with some other tags. In the
// event of a collision, this measurement's tags take priority.
func (m *measurement) combineTags(newTags map[string]string) map[string]string {
	results := make(map[string]string)
	for k, v := range newTags {
		results[k] = v
	}
	for k, v := range m.tags {
		results[k] = v
	}
	return results
}

// newMeasurement is a convenience method for instantiating new measurements
func newMeasurement(name string) measurement {
	return measurement{
		name:   name,
		fields: make(map[string]interface{}),
		tags:   make(map[string]string),
	}
}

// SampleConfig returns the default configuration
func (dc *DCOSContainers) SampleConfig() string {
	return sampleConfig
}

// Description returns a one-sentence description of dcos_containers
func (dc *DCOSContainers) Description() string {
	return "Plugin for monitoring mesos container resource consumption"
}

// Gather takes in an accumulator and adds the metrics that the plugin gathers.
// It is invoked on a schedule (default every 10s) by the telegraf runtime.
func (dc *DCOSContainers) Gather(acc telegraf.Accumulator) error {
	uri := dc.MesosAgentUrl + "/api/v1"
	cli := httpagent.NewSender(httpcli.New(httpcli.Endpoint(uri)).Send)
	ctx, _ := context.WithTimeout(context.Background(), dc.Timeout)

	gc, err := dc.getContainers(ctx, cli)
	if err != nil {
		return err
	}

	for _, c := range gc.Containers {
		ts := cTS(c)
		tags := cTags(c)
		for _, m := range cMeasurements(c) {
			if len(m.fields) > 0 {
				acc.AddFields(m.name, m.fields, m.combineTags(tags), ts)
			}
		}
	}

	return nil
}

// getContainers requests a list of containers from the operator API
func (dc *DCOSContainers) getContainers(ctx context.Context, cli calls.Sender) (*agent.Response_GetContainers, error) {
	resp, err := cli.Send(ctx, calls.NonStreaming(calls.GetContainers()))
	if err != nil {
		return nil, err
	}
	r, err := processResponse(resp, agent.Response_GET_CONTAINERS)
	if err != nil {
		return nil, err
	}

	gc := r.GetGetContainers()
	if gc == nil {
		return gc, errors.New("the getContainers response from the mesos agent was empty")
	}

	return gc, nil
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

// cMeasurements flattens a Container object into a slice of measurements with
// fields and tags
func cMeasurements(c agent.Response_GetContainers_Container) []measurement {
	container := newMeasurement("container")
	cpus := newMeasurement("cpus")
	mem := newMeasurement("mem")
	disk := newMeasurement("disk")
	net := newMeasurement("net")

	results := []measurement{
		container, cpus, mem, disk, net,
	}

	rs := c.ResourceStatistics

	// These items are not in alphabetical order; instead we preserve the order
	// in the source of the ResourceStatistics struct to make it easy to update.
	warnIfNotSet(setIfNotNil(container.fields, "processes", rs.GetProcesses))
	warnIfNotSet(setIfNotNil(container.fields, "threads", rs.GetThreads))

	warnIfNotSet(setIfNotNil(cpus.fields, "user_time_secs", rs.GetCPUsUserTimeSecs))
	warnIfNotSet(setIfNotNil(cpus.fields, "system_time_secs", rs.GetCPUsSystemTimeSecs))
	warnIfNotSet(setIfNotNil(cpus.fields, "limit", rs.GetCPUsLimit))
	warnIfNotSet(setIfNotNil(cpus.fields, "nr_periods", rs.GetCPUsNrPeriods))
	warnIfNotSet(setIfNotNil(cpus.fields, "nr_throttled", rs.GetCPUsNrThrottled))
	warnIfNotSet(setIfNotNil(cpus.fields, "throttled_time_secs", rs.GetCPUsThrottledTimeSecs))

	warnIfNotSet(setIfNotNil(mem.fields, "total_bytes", rs.GetMemTotalBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "total_memsw_bytes", rs.GetMemTotalMemswBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "limit_bytes", rs.GetMemLimitBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "soft_limit_bytes", rs.GetMemSoftLimitBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "file_bytes", rs.GetMemFileBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "anon_bytes", rs.GetMemAnonBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "cache_bytes", rs.GetMemCacheBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "rss_bytes", rs.GetMemRSSBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "mapped_file_bytes", rs.GetMemMappedFileBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "swap_bytes", rs.GetMemSwapBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "unevictable_bytes", rs.GetMemUnevictableBytes))
	warnIfNotSet(setIfNotNil(mem.fields, "low_pressure_counter", rs.GetMemLowPressureCounter))
	warnIfNotSet(setIfNotNil(mem.fields, "medium_pressure_counter", rs.GetMemMediumPressureCounter))
	warnIfNotSet(setIfNotNil(mem.fields, "critical_pressure_counter", rs.GetMemCriticalPressureCounter))

	warnIfNotSet(setIfNotNil(disk.fields, "limit_bytes", rs.GetDiskLimitBytes))
	warnIfNotSet(setIfNotNil(disk.fields, "used_bytes", rs.GetDiskUsedBytes))

	// TODO (philipnrmn) *rs.DiskStatistics for per-volume stats

	// TODO (philipnrmn) *rs.Perf for perf stats

	warnIfNotSet(setIfNotNil(net.fields, "rx_packets", rs.GetNetRxPackets))
	warnIfNotSet(setIfNotNil(net.fields, "rx_bytes", rs.GetNetRxBytes))
	warnIfNotSet(setIfNotNil(net.fields, "rx_errors", rs.GetNetRxErrors))
	warnIfNotSet(setIfNotNil(net.fields, "rx_dropped", rs.GetNetRxDropped))
	warnIfNotSet(setIfNotNil(net.fields, "tx_packets", rs.GetNetTxPackets))
	warnIfNotSet(setIfNotNil(net.fields, "tx_bytes", rs.GetNetTxBytes))
	warnIfNotSet(setIfNotNil(net.fields, "tx_errors", rs.GetNetTxErrors))
	warnIfNotSet(setIfNotNil(net.fields, "tx_dropped", rs.GetNetTxDropped))
	warnIfNotSet(setIfNotNil(net.fields, "tcp_rtt_microsecs_p50", rs.GetNetTCPRttMicrosecsP50))
	warnIfNotSet(setIfNotNil(net.fields, "tcp_rtt_microsecs_p90", rs.GetNetTCPRttMicrosecsP90))
	warnIfNotSet(setIfNotNil(net.fields, "tcp_rtt_microsecs_p95", rs.GetNetTCPRttMicrosecsP95))
	warnIfNotSet(setIfNotNil(net.fields, "tcp_rtt_microsecs_p99", rs.GetNetTCPRttMicrosecsP99))
	warnIfNotSet(setIfNotNil(net.fields, "tcp_active_connections", rs.GetNetTCPActiveConnections))
	warnIfNotSet(setIfNotNil(net.fields, "tcp_time_wait_connections", rs.GetNetTCPTimeWaitConnections))
	// TODO (philipnrmn) *rs.NetTrafficControlStatistics  for net traffic control statistics
	// TODO (philipnrmn) *rs.NetSNMPStatistics for net snmp statistics

	return results
}


// cTags extracts relevant metadata from a Container object as a map of tags
func cTags(c agent.Response_GetContainers_Container) map[string]string {
	return map[string]string{"container_id": c.ContainerID.Value}
}

// cTS retrieves the timestamp from a Container object as a time rounded to the
// nearest second
func cTS(c agent.Response_GetContainers_Container) time.Time {
	return time.Unix(int64(math.Trunc(c.ResourceStatistics.Timestamp)), 0)
}

// setIfNotNil runs get() and adds its value to a map, if not nil
func setIfNotNil(target map[string]interface{}, key string, get interface{}) error {
	var val interface{}
	var zero interface{}

	switch get.(type) {
	case func() uint32:
		val = get.(func() uint32)()
		zero = uint32(0)
		break
	case func() uint64:
		val = get.(func() uint64)()
		zero = uint64(0)
		break
	case func() float64:
		val = get.(func() float64)()
		zero = float64(0)
		break
	default:
		return fmt.Errorf("get function for key %s was not of a recognized type", key)
	}
	// Zero is nil for numeric types
	if val != zero {
		target[key] = val
	}
	return nil
}

// warnIfNotSet is a convenience method to log a warning whenever setIfNotNil
// did not succesfully complete
func warnIfNotSet(err error) {
	if err != nil {
		log.Printf("Warning: %s", err)
	}
}

// init is called once when telegraf starts
func init() {
	inputs.Add("dcos_containers", func() telegraf.Input {
		return &DCOSContainers{
			Timeout: 10 * time.Second,
		}
	})
}

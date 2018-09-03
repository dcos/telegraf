package dcos_metrics

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dcos/dcos-metrics/producers"

	"github.com/influxdata/telegraf"
)

// producerTranslator converts telegraf.Metric to producers.MetricsMessage.
type producerTranslator struct {
	MesosID           string
	DCOSNodeRole      string
	DCOSClusterID     string
	DCOSNodePrivateIP string
}

// Translate returns a producers.MetricsMessage created from metric. ok is false if a MetricsMessage could not be
// created.
func (t *producerTranslator) Translate(metric telegraf.Metric) (msg producers.MetricsMessage, ok bool, err error) {
	nameSuffix := metricNameSuffix(metric.Name())
	tags := metric.Tags()
	metricType := metric.Type()

	ok = true
	switch {
	// Container metrics
	// We assume any metric with a container_id tag but without a metric_type tag is a container metric from the
	// dcos_containers input.
	case hasAllKeys(tags, []string{"container_id"}) && !hasAnyKeys(tags, []string{"metric_type"}):
		msg = t.containerMetricsMessage(metric)

	// App metrics
	// We assume any metric with both a container_id tag and a metric_type tag is an app metric from the dcos_statsd
	// input.
	case hasAllKeys(tags, []string{"container_id", "metric_type"}):
		msg = t.appMetricsMessage(metric)

	// Node metrics
	// CPU metrics may be reported for individual cores or total CPU, and as a count (time) or gauge (percentage).
	// We want the gauge for total CPU.
	case nameSuffix == "cpu" && metricType == telegraf.Gauge && tags["cpu"] == "cpu-total":
		msg, err = t.cpuMetricsMessage(metric)

	// Check tags to filter out disk metrics from the dcos_containers input.
	case nameSuffix == "disk" && !hasAnyKeys(tags, []string{"container_id"}):
		msg = t.diskMetricsMessage(metric)

	// Check tags to filter out mem metrics from the dcos_containers input.
	case nameSuffix == "mem" && !hasAnyKeys(tags, []string{"container_id"}):
		msg = t.memMetricsMessage(metric)

	// Swap metrics may be reported as a gauge of usage/capacity or a counter of swaps in/out. We want the gauge.
	case nameSuffix == "swap" && metricType == telegraf.Gauge:
		msg = t.swapMetricsMessage(metric)

	// Check tags to filter out net metrics from the dcos_containers input.
	case nameSuffix == "net" && !hasAnyKeys(tags, []string{"container_id"}):
		msg = t.netMetricsMessage(metric)

	case nameSuffix == "processes":
		msg = t.processesMetricsMessage(metric)

	case nameSuffix == "system":
		msg = t.systemMetricsMessage(metric)

	default:
		// We aren't able to create a MetricsMessage for this metric.
		ok = false
	}

	return
}

// containerMetricsMessage returns a producers.MetricsMessage built from the container metric m.
func (t *producerTranslator) containerMetricsMessage(m telegraf.Metric) producers.MetricsMessage {
	tags := m.Tags()
	// Delete the tags we retrieve so they aren't duplicated. All remaining tags are assumed to be task labels.
	containerID := getAndDelete(tags, "container_id")
	frameworkName := getAndDelete(tags, "service_name") // DC/OS services are Mesos frameworks.
	taskName := getAndDelete(tags, "task_name")
	executorName := getAndDelete(tags, "executor_name")

	dpTags := map[string]string{"container_id": containerID}
	if executorName != "" {
		dpTags["executor_name"] = executorName
	}

	return producers.MetricsMessage{
		Name:       producers.ContainerMetricPrefix,
		Datapoints: datapointsFromMetric(m, producers.ContainerMetricPrefix, dpTags),
		Dimensions: producers.Dimensions{
			MesosID:       t.MesosID,
			ClusterID:     t.DCOSClusterID,
			Hostname:      t.DCOSNodePrivateIP,
			ContainerID:   containerID,
			FrameworkName: frameworkName,
			TaskName:      taskName,
			Labels:        tags,
		},
	}
}

// appMetricsMessage returns a producers.MetricsMessage built from the app metric m.
func (t *producerTranslator) appMetricsMessage(m telegraf.Metric) producers.MetricsMessage {
	tags := m.Tags()
	// Delete the tags we retrieve so they aren't duplicated. All remaining tags are assumed to be task labels.
	containerID := getAndDelete(tags, "container_id")
	frameworkName := getAndDelete(tags, "service_name") // DC/OS services are Mesos frameworks.
	taskName := getAndDelete(tags, "task_name")
	// We don't use metric_type.
	delete(tags, "metric_type")

	fieldNamePrefix := producers.AppMetricPrefix + "." + metricNameSuffix(m.Name())

	return producers.MetricsMessage{
		Name:       fieldNamePrefix,
		Datapoints: datapointsFromMetric(m, fieldNamePrefix, tags),
		Dimensions: producers.Dimensions{
			MesosID:       t.MesosID,
			ClusterID:     t.DCOSClusterID,
			Hostname:      t.DCOSNodePrivateIP,
			ContainerID:   containerID,
			FrameworkName: frameworkName,
			TaskName:      taskName,
			Labels:        nil,
		},
	}
}

// cpuMetricsMessage returns a producers.MetricsMessage built from the cpu metric m.
func (t *producerTranslator) cpuMetricsMessage(m telegraf.Metric) (producers.MetricsMessage, error) {
	fields := m.Fields()
	timestamp := timestampFromMetric(m)

	// Infer usage_total from usage_idle.
	usage_idle, ok := fields["usage_idle"].(float64)
	if !ok {
		return producers.MetricsMessage{}, errors.New(fmt.Sprintf("Non-float64 value for usage_idle: %s", fields["usage_idle"]))
	}
	usage_total := 100.0 - usage_idle

	return producers.MetricsMessage{
		Name: producers.NodeMetricPrefix,
		Datapoints: []producers.Datapoint{
			// Number of CPU cores isn't available. See https://github.com/influxdata/telegraf/issues/2020.
			producers.Datapoint{
				Name:      "cpu.total",
				Unit:      "percent",
				Value:     usage_total,
				Timestamp: timestamp,
			},
			producers.Datapoint{
				Name:      "cpu.user",
				Unit:      "percent",
				Value:     fields["usage_user"],
				Timestamp: timestamp,
			},
			producers.Datapoint{
				Name:      "cpu.system",
				Unit:      "percent",
				Value:     fields["usage_system"],
				Timestamp: timestamp,
			},
			producers.Datapoint{
				Name:      "cpu.idle",
				Unit:      "percent",
				Value:     usage_idle,
				Timestamp: timestamp,
			},
			producers.Datapoint{
				Name:      "cpu.wait",
				Unit:      "percent",
				Value:     fields["usage_iowait"],
				Timestamp: timestamp,
			},
		},
		Dimensions: producers.Dimensions{
			MesosID:   t.MesosID,
			ClusterID: t.DCOSClusterID,
			Hostname:  t.DCOSNodePrivateIP,
		},
	}, nil
}

// diskMetricsMessage returns a producers.MetricsMessage built from the disk metric m.
func (t *producerTranslator) diskMetricsMessage(m telegraf.Metric) producers.MetricsMessage {
	fields := m.Fields()
	timestamp := timestampFromMetric(m)
	tags := map[string]string{"path": m.Tags()["path"]}
	return producers.MetricsMessage{
		Name: producers.NodeMetricPrefix,
		Datapoints: []producers.Datapoint{
			producers.Datapoint{
				Name:      "filesystem.capacity.total",
				Unit:      "bytes",
				Value:     fields["total"],
				Timestamp: timestamp,
				Tags:      tags,
			},
			producers.Datapoint{
				Name:      "filesystem.capacity.used",
				Unit:      "bytes",
				Value:     fields["used"],
				Timestamp: timestamp,
				Tags:      tags,
			},
			producers.Datapoint{
				Name:      "filesystem.capacity.free",
				Unit:      "bytes",
				Value:     fields["free"],
				Timestamp: timestamp,
				Tags:      tags,
			},
			producers.Datapoint{
				Name:      "filesystem.inode.total",
				Unit:      "count",
				Value:     fields["inodes_total"],
				Timestamp: timestamp,
				Tags:      tags,
			},
			producers.Datapoint{
				Name:      "filesystem.inode.used",
				Unit:      "count",
				Value:     fields["inodes_used"],
				Timestamp: timestamp,
				Tags:      tags,
			},
			producers.Datapoint{
				Name:      "filesystem.inode.free",
				Unit:      "count",
				Value:     fields["inodes_free"],
				Timestamp: timestamp,
				Tags:      tags,
			},
		},
		Dimensions: producers.Dimensions{
			MesosID:   t.MesosID,
			ClusterID: t.DCOSClusterID,
			Hostname:  t.DCOSNodePrivateIP,
		},
	}
}

// memMetricsMessage returns a producers.MetricsMessage built from the mem metric m.
func (t *producerTranslator) memMetricsMessage(m telegraf.Metric) producers.MetricsMessage {
	fields := m.Fields()
	timestamp := timestampFromMetric(m)
	return producers.MetricsMessage{
		Name: producers.NodeMetricPrefix,
		Datapoints: []producers.Datapoint{
			producers.Datapoint{
				Name:      "memory.total",
				Unit:      "bytes",
				Value:     fields["total"],
				Timestamp: timestamp,
			},
			producers.Datapoint{
				Name:      "memory.free",
				Unit:      "bytes",
				Value:     fields["free"],
				Timestamp: timestamp,
			},
			producers.Datapoint{
				Name:      "memory.buffers",
				Unit:      "bytes",
				Value:     fields["buffered"],
				Timestamp: timestamp,
			},
			producers.Datapoint{
				Name:      "memory.cached",
				Unit:      "bytes",
				Value:     fields["cached"],
				Timestamp: timestamp,
			},
		},
		Dimensions: producers.Dimensions{
			MesosID:   t.MesosID,
			ClusterID: t.DCOSClusterID,
			Hostname:  t.DCOSNodePrivateIP,
		},
	}
}

// swapMetricsMessage returns a producers.MetricsMessage built from the swap metric m.
func (t *producerTranslator) swapMetricsMessage(m telegraf.Metric) producers.MetricsMessage {
	fields := m.Fields()
	timestamp := timestampFromMetric(m)
	return producers.MetricsMessage{
		Name: producers.NodeMetricPrefix,
		Datapoints: []producers.Datapoint{
			producers.Datapoint{
				Name:      "swap.total",
				Unit:      "bytes",
				Value:     fields["total"],
				Timestamp: timestamp,
			},
			producers.Datapoint{
				Name:      "swap.free",
				Unit:      "bytes",
				Value:     fields["free"],
				Timestamp: timestamp,
			},
			producers.Datapoint{
				Name:      "swap.used",
				Unit:      "bytes",
				Value:     fields["used"],
				Timestamp: timestamp,
			},
		},
		Dimensions: producers.Dimensions{
			MesosID:   t.MesosID,
			ClusterID: t.DCOSClusterID,
			Hostname:  t.DCOSNodePrivateIP,
		},
	}
}

// netMetricsMessage returns a producers.MetricsMessage built from the net metric m.
func (t *producerTranslator) netMetricsMessage(m telegraf.Metric) producers.MetricsMessage {
	fields := m.Fields()
	timestamp := timestampFromMetric(m)
	tags := map[string]string{"interface": m.Tags()["interface"]}
	return producers.MetricsMessage{
		Name: producers.NodeMetricPrefix,
		Datapoints: []producers.Datapoint{
			producers.Datapoint{
				Name:      "network.in",
				Unit:      "bytes",
				Value:     fields["bytes_recv"],
				Timestamp: timestamp,
				Tags:      tags,
			},
			producers.Datapoint{
				Name:      "network.out",
				Unit:      "bytes",
				Value:     fields["bytes_sent"],
				Timestamp: timestamp,
				Tags:      tags,
			},
			producers.Datapoint{
				Name:      "network.in.packets",
				Unit:      "count",
				Value:     fields["packets_recv"],
				Timestamp: timestamp,
				Tags:      tags,
			},
			producers.Datapoint{
				Name:      "network.out.packets",
				Unit:      "count",
				Value:     fields["packets_sent"],
				Timestamp: timestamp,
				Tags:      tags,
			},
			producers.Datapoint{
				Name:      "network.in.dropped",
				Unit:      "count",
				Value:     fields["drop_in"],
				Timestamp: timestamp,
				Tags:      tags,
			},
			producers.Datapoint{
				Name:      "network.out.dropped",
				Unit:      "count",
				Value:     fields["drop_out"],
				Timestamp: timestamp,
				Tags:      tags,
			},
			producers.Datapoint{
				Name:      "network.in.errors",
				Unit:      "count",
				Value:     fields["err_in"],
				Timestamp: timestamp,
				Tags:      tags,
			},
			producers.Datapoint{
				Name:      "network.out.errors",
				Unit:      "count",
				Value:     fields["err_out"],
				Timestamp: timestamp,
				Tags:      tags,
			},
		},
		Dimensions: producers.Dimensions{
			MesosID:   t.MesosID,
			ClusterID: t.DCOSClusterID,
			Hostname:  t.DCOSNodePrivateIP,
		},
	}
}

// processesMetricsMessage returns a producers.MetricsMessage built from the processes metric m.
func (t *producerTranslator) processesMetricsMessage(m telegraf.Metric) producers.MetricsMessage {
	return producers.MetricsMessage{
		Name: producers.NodeMetricPrefix,
		Datapoints: []producers.Datapoint{
			producers.Datapoint{
				Name:      "process.count",
				Unit:      "count",
				Value:     m.Fields()["total"],
				Timestamp: timestampFromMetric(m),
			},
		},
		Dimensions: producers.Dimensions{
			MesosID:   t.MesosID,
			ClusterID: t.DCOSClusterID,
			Hostname:  t.DCOSNodePrivateIP,
		},
	}
}

// systemMetricsMessage returns a producers.MetricsMessage built from the system metric m.
func (t *producerTranslator) systemMetricsMessage(m telegraf.Metric) producers.MetricsMessage {
	fields := m.Fields()
	timestamp := timestampFromMetric(m)
	return producers.MetricsMessage{
		Name: producers.NodeMetricPrefix,
		Datapoints: []producers.Datapoint{
			producers.Datapoint{
				Name:      "load.1min",
				Unit:      "count",
				Value:     fields["load1"],
				Timestamp: timestamp,
			},
			producers.Datapoint{
				Name:      "load.5min",
				Unit:      "count",
				Value:     fields["load5"],
				Timestamp: timestamp,
			},
			producers.Datapoint{
				Name:      "load.15min",
				Unit:      "count",
				Value:     fields["load15"],
				Timestamp: timestamp,
			},
			producers.Datapoint{
				Name:      "system.uptime",
				Unit:      "count",
				Value:     fields["uptime"],
				Timestamp: timestamp,
			},
		},
		Dimensions: producers.Dimensions{
			MesosID:   t.MesosID,
			ClusterID: t.DCOSClusterID,
			Hostname:  t.DCOSNodePrivateIP,
		},
	}
}

// datapointsFromMetric returns a []producers.Datapoint for the fields in m.
// Tags are applied to each Datapoint, and each Datapoint name is prefixed with namePrefix.
// Datapoints are sorted by name for stability.
func datapointsFromMetric(m telegraf.Metric, namePrefix string, tags map[string]string) []producers.Datapoint {
	fields := m.Fields()
	timestamp := timestampFromMetric(m)

	// Sort datapoints by name for stability.
	fns := make([]string, len(fields))
	i := 0
	for fn := range fields {
		fns[i] = fn
		i += 1
	}
	sort.Strings(fns)

	datapoints := make([]producers.Datapoint, len(fns))
	for i, fn := range fns {
		// If we have a single metric field whose name is value, omit it from the complete field name.
		var name string
		if len(fns) == 1 && fn == "value" {
			name = namePrefix
		} else {
			name = namePrefix + "." + fn
		}

		datapoints[i] = producers.Datapoint{
			Name:      name,
			Value:     fields[fn],
			Timestamp: timestamp,
			Tags:      tags,
		}
	}

	return datapoints
}

// timestampFromMetric returns a string representation of m's timestamp formatted according to RFC 3339.
func timestampFromMetric(m telegraf.Metric) string {
	return m.Time().Format(time.RFC3339)
}

// metricNameSuffix returns the last part of a dot-separated metric name.
// If name doesn't contain ".", name is returned.
func metricNameSuffix(name string) string {
	parts := strings.Split(name, ".")
	return parts[len(parts)-1]
}

// getAndDelete returns m[k] after deleting k from m.
func getAndDelete(m map[string]string, k string) string {
	v := m[k]
	delete(m, k)
	return v
}

// hasAllKeys returns true if m contains all provided keys, otherwise false.
func hasAllKeys(m map[string]string, keys []string) bool {
	for _, k := range keys {
		if _, ok := m[k]; !ok {
			return false
		}
	}
	return true
}

// hasAnyKeys returns true if m contains any provided key, otherwise false.
func hasAnyKeys(m map[string]string, keys []string) bool {
	for _, k := range keys {
		if _, ok := m[k]; ok {
			return true
		}
	}
	return false
}

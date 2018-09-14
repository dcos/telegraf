package dcos_metrics

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/dcos/dcos-metrics/producers"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
)

var (
	translator = producerTranslator{
		MesosID:           "mesos_id",
		DCOSNodeRole:      "master",
		DCOSClusterID:     "cluster_id",
		DCOSNodePrivateIP: "10.0.0.1",
	}
	tm        = time.Unix(0, 0)
	timestamp = tm.Format(time.RFC3339)
)

type metricParams struct {
	name   string
	tags   map[string]string
	fields map[string]interface{}
	tm     time.Time
	tp     telegraf.ValueType
}

func (mp *metricParams) NewMetric(t *testing.T) telegraf.Metric {
	m, err := metric.New(
		mp.name,
		mp.tags,
		mp.fields,
		mp.tm,
		mp.tp,
	)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestTranslate(t *testing.T) {
	type testCase struct {
		name   string
		input  metricParams
		output producers.MetricsMessage
	}

	testCases := []testCase{
		testCase{
			name: "cpu metric",
			input: metricParams{
				name: "prefix.cpu",
				tags: map[string]string{"cpu": "cpu-total"},
				fields: map[string]interface{}{
					"usage_idle":   70.0,
					"usage_user":   20.0,
					"usage_system": 6.0,
					"usage_iowait": 4.0,
				},
				tm: tm,
				tp: telegraf.Gauge,
			},
			output: producers.MetricsMessage{
				Name: "dcos.metrics.node",
				Dimensions: producers.Dimensions{
					MesosID:   translator.MesosID,
					ClusterID: translator.DCOSClusterID,
					Hostname:  translator.DCOSNodePrivateIP,
				},
				Datapoints: []producers.Datapoint{
					producers.Datapoint{
						Name:      "cpu.total",
						Unit:      "percent",
						Value:     30.0,
						Timestamp: timestamp,
					},
					producers.Datapoint{
						Name:      "cpu.user",
						Unit:      "percent",
						Value:     20.0,
						Timestamp: timestamp,
					},
					producers.Datapoint{
						Name:      "cpu.system",
						Unit:      "percent",
						Value:     6.0,
						Timestamp: timestamp,
					},
					producers.Datapoint{
						Name:      "cpu.idle",
						Unit:      "percent",
						Value:     70.0,
						Timestamp: timestamp,
					},
					producers.Datapoint{
						Name:      "cpu.wait",
						Unit:      "percent",
						Value:     4.0,
						Timestamp: timestamp,
					},
				},
			},
		},

		testCase{
			name: "disk metric",
			input: metricParams{
				name: "prefix.disk",
				tags: map[string]string{"path": "/"},
				fields: map[string]interface{}{
					"total":        uint64(1000),
					"used":         uint64(600),
					"free":         uint64(400),
					"inodes_total": uint64(2000),
					"inodes_used":  uint64(1200),
					"inodes_free":  uint64(800),
				},
				tm: tm,
				tp: telegraf.Gauge,
			},
			output: producers.MetricsMessage{
				Name: "dcos.metrics.node",
				Dimensions: producers.Dimensions{
					MesosID:   translator.MesosID,
					ClusterID: translator.DCOSClusterID,
					Hostname:  translator.DCOSNodePrivateIP,
				},
				Datapoints: []producers.Datapoint{
					producers.Datapoint{
						Name:      "filesystem.capacity.total",
						Unit:      "bytes",
						Value:     uint64(1000),
						Timestamp: timestamp,
						Tags:      map[string]string{"path": "/"},
					},
					producers.Datapoint{
						Name:      "filesystem.capacity.used",
						Unit:      "bytes",
						Value:     uint64(600),
						Timestamp: timestamp,
						Tags:      map[string]string{"path": "/"},
					},
					producers.Datapoint{
						Name:      "filesystem.capacity.free",
						Unit:      "bytes",
						Value:     uint64(400),
						Timestamp: timestamp,
						Tags:      map[string]string{"path": "/"},
					},
					producers.Datapoint{
						Name:      "filesystem.inode.total",
						Unit:      "count",
						Value:     uint64(2000),
						Timestamp: timestamp,
						Tags:      map[string]string{"path": "/"},
					},
					producers.Datapoint{
						Name:      "filesystem.inode.used",
						Unit:      "count",
						Value:     uint64(1200),
						Timestamp: timestamp,
						Tags:      map[string]string{"path": "/"},
					},
					producers.Datapoint{
						Name:      "filesystem.inode.free",
						Unit:      "count",
						Value:     uint64(800),
						Timestamp: timestamp,
						Tags:      map[string]string{"path": "/"},
					},
				},
			},
		},

		testCase{
			name: "memory metric",
			input: metricParams{
				name: "prefix.mem",
				fields: map[string]interface{}{
					"total":    uint64(1024),
					"free":     uint64(512),
					"buffered": uint64(258),
					"cached":   uint64(254),
				},
				tm: tm,
				tp: telegraf.Gauge,
			},
			output: producers.MetricsMessage{
				Name: "dcos.metrics.node",
				Dimensions: producers.Dimensions{
					MesosID:   translator.MesosID,
					ClusterID: translator.DCOSClusterID,
					Hostname:  translator.DCOSNodePrivateIP,
				},
				Datapoints: []producers.Datapoint{
					producers.Datapoint{
						Name:      "memory.total",
						Unit:      "bytes",
						Value:     uint64(1024),
						Timestamp: timestamp,
					},
					producers.Datapoint{
						Name:      "memory.free",
						Unit:      "bytes",
						Value:     uint64(512),
						Timestamp: timestamp,
					},
					producers.Datapoint{
						Name:      "memory.buffers",
						Unit:      "bytes",
						Value:     uint64(258),
						Timestamp: timestamp,
					},
					producers.Datapoint{
						Name:      "memory.cached",
						Unit:      "bytes",
						Value:     uint64(254),
						Timestamp: timestamp,
					},
				},
			},
		},

		testCase{
			name: "swap metric",
			input: metricParams{
				name: "prefix.swap",
				fields: map[string]interface{}{
					"total": uint64(1024),
					"free":  uint64(514),
					"used":  uint64(510),
				},
				tm: tm,
				tp: telegraf.Gauge,
			},
			output: producers.MetricsMessage{
				Name: "dcos.metrics.node",
				Dimensions: producers.Dimensions{
					MesosID:   translator.MesosID,
					ClusterID: translator.DCOSClusterID,
					Hostname:  translator.DCOSNodePrivateIP,
				},
				Datapoints: []producers.Datapoint{
					producers.Datapoint{
						Name:      "swap.total",
						Unit:      "bytes",
						Value:     uint64(1024),
						Timestamp: timestamp,
					},
					producers.Datapoint{
						Name:      "swap.free",
						Unit:      "bytes",
						Value:     uint64(514),
						Timestamp: timestamp,
					},
					producers.Datapoint{
						Name:      "swap.used",
						Unit:      "bytes",
						Value:     uint64(510),
						Timestamp: timestamp,
					},
				},
			},
		},

		testCase{
			name: "network metric",
			input: metricParams{
				name: "prefix.net",
				tags: map[string]string{"interface": "eth0"},
				fields: map[string]interface{}{
					"bytes_recv":   uint64(2048),
					"bytes_sent":   uint64(256),
					"packets_recv": uint64(1000),
					"packets_sent": uint64(500),
					"drop_in":      uint64(10),
					"drop_out":     uint64(5),
					"err_in":       uint64(2),
					"err_out":      uint64(1),
				},
				tm: tm,
				tp: telegraf.Gauge,
			},
			output: producers.MetricsMessage{
				Name: "dcos.metrics.node",
				Dimensions: producers.Dimensions{
					MesosID:   translator.MesosID,
					ClusterID: translator.DCOSClusterID,
					Hostname:  translator.DCOSNodePrivateIP,
				},
				Datapoints: []producers.Datapoint{
					producers.Datapoint{
						Name:      "network.in",
						Unit:      "bytes",
						Value:     uint64(2048),
						Timestamp: timestamp,
						Tags:      map[string]string{"interface": "eth0"},
					},
					producers.Datapoint{
						Name:      "network.out",
						Unit:      "bytes",
						Value:     uint64(256),
						Timestamp: timestamp,
						Tags:      map[string]string{"interface": "eth0"},
					},
					producers.Datapoint{
						Name:      "network.in.packets",
						Unit:      "count",
						Value:     uint64(1000),
						Timestamp: timestamp,
						Tags:      map[string]string{"interface": "eth0"},
					},
					producers.Datapoint{
						Name:      "network.out.packets",
						Unit:      "count",
						Value:     uint64(500),
						Timestamp: timestamp,
						Tags:      map[string]string{"interface": "eth0"},
					},
					producers.Datapoint{
						Name:      "network.in.dropped",
						Unit:      "count",
						Value:     uint64(10),
						Timestamp: timestamp,
						Tags:      map[string]string{"interface": "eth0"},
					},
					producers.Datapoint{
						Name:      "network.out.dropped",
						Unit:      "count",
						Value:     uint64(5),
						Timestamp: timestamp,
						Tags:      map[string]string{"interface": "eth0"},
					},
					producers.Datapoint{
						Name:      "network.in.errors",
						Unit:      "count",
						Value:     uint64(2),
						Timestamp: timestamp,
						Tags:      map[string]string{"interface": "eth0"},
					},
					producers.Datapoint{
						Name:      "network.out.errors",
						Unit:      "count",
						Value:     uint64(1),
						Timestamp: timestamp,
						Tags:      map[string]string{"interface": "eth0"},
					},
				},
			},
		},

		testCase{
			name: "processes metric",
			input: metricParams{
				name: "prefix.processes",
				fields: map[string]interface{}{
					"total": uint64(22),
				},
				tm: tm,
				tp: telegraf.Gauge,
			},
			output: producers.MetricsMessage{
				Name: "dcos.metrics.node",
				Dimensions: producers.Dimensions{
					MesosID:   translator.MesosID,
					ClusterID: translator.DCOSClusterID,
					Hostname:  translator.DCOSNodePrivateIP,
				},
				Datapoints: []producers.Datapoint{
					producers.Datapoint{
						Name:      "process.count",
						Unit:      "count",
						Value:     uint64(22),
						Timestamp: timestamp,
					},
				},
			},
		},

		testCase{
			name: "system metric",
			input: metricParams{
				name: "prefix.system",
				fields: map[string]interface{}{
					"load1":  1.0,
					"load5":  2.0,
					"load15": 3.0,
					"uptime": uint64(1000),
				},
				tm: tm,
				tp: telegraf.Gauge,
			},
			output: producers.MetricsMessage{
				Name: "dcos.metrics.node",
				Dimensions: producers.Dimensions{
					MesosID:   translator.MesosID,
					ClusterID: translator.DCOSClusterID,
					Hostname:  translator.DCOSNodePrivateIP,
				},
				Datapoints: []producers.Datapoint{
					producers.Datapoint{
						Name:      "load.1min",
						Unit:      "count",
						Value:     1.0,
						Timestamp: timestamp,
					},
					producers.Datapoint{
						Name:      "load.5min",
						Unit:      "count",
						Value:     2.0,
						Timestamp: timestamp,
					},
					producers.Datapoint{
						Name:      "load.15min",
						Unit:      "count",
						Value:     3.0,
						Timestamp: timestamp,
					},
					producers.Datapoint{
						Name:      "system.uptime",
						Unit:      "count",
						Value:     uint64(1000),
						Timestamp: timestamp,
					},
				},
			},
		},

		testCase{
			name: "container metric",
			input: metricParams{
				name: "prefix.foo",
				tags: map[string]string{
					"container_id":  "cid",
					"service_name":  "sname",
					"task_name":     "tname",
					"executor_name": "ename",
					"label_name":    "label_value",
				},
				fields: map[string]interface{}{
					"metric1": uint64(0),
					"metric2": uint64(1),
				},
				tm: tm,
				tp: telegraf.Untyped,
			},
			output: producers.MetricsMessage{
				Name: "dcos.metrics.container",
				Dimensions: producers.Dimensions{
					MesosID:       translator.MesosID,
					ClusterID:     translator.DCOSClusterID,
					Hostname:      translator.DCOSNodePrivateIP,
					ContainerID:   "cid",
					FrameworkName: "sname",
					TaskName:      "tname",
					Labels:        map[string]string{"label_name": "label_value"},
				},
				Datapoints: []producers.Datapoint{
					producers.Datapoint{
						Name:      "prefix.foo.metric1",
						Value:     uint64(0),
						Timestamp: timestamp,
						Tags: map[string]string{
							"container_id":  "cid",
							"executor_name": "ename",
						},
					},
					producers.Datapoint{
						Name:      "prefix.foo.metric2",
						Value:     uint64(1),
						Timestamp: timestamp,
						Tags: map[string]string{
							"container_id":  "cid",
							"executor_name": "ename",
						},
					},
				},
			},
		},

		testCase{
			name: "container metric with empty executor_name",
			input: metricParams{
				name: "prefix.foo",
				tags: map[string]string{
					"container_id":  "cid",
					"service_name":  "sname",
					"task_name":     "tname",
					"executor_name": "",
					"label_name":    "label_value",
				},
				fields: map[string]interface{}{
					"metric1": uint64(0),
					"metric2": uint64(1),
				},
				tm: tm,
				tp: telegraf.Untyped,
			},
			output: producers.MetricsMessage{
				Name: "dcos.metrics.container",
				Dimensions: producers.Dimensions{
					MesosID:       translator.MesosID,
					ClusterID:     translator.DCOSClusterID,
					Hostname:      translator.DCOSNodePrivateIP,
					ContainerID:   "cid",
					FrameworkName: "sname",
					TaskName:      "tname",
					Labels:        map[string]string{"label_name": "label_value"},
				},
				Datapoints: []producers.Datapoint{
					producers.Datapoint{
						Name:      "prefix.foo.metric1",
						Value:     uint64(0),
						Timestamp: timestamp,
						Tags: map[string]string{
							"container_id": "cid",
						},
					},
					producers.Datapoint{
						Name:      "prefix.foo.metric2",
						Value:     uint64(1),
						Timestamp: timestamp,
						Tags: map[string]string{
							"container_id": "cid",
						},
					},
				},
			},
		},

		testCase{
			name: "app metric",
			input: metricParams{
				name: "prefix.foo",
				tags: map[string]string{
					"container_id": "cid",
					"service_name": "sname",
					"task_name":    "tname",
					"metric_type":  "mtype",
					"label_name":   "label_value",
				},
				fields: map[string]interface{}{
					"metric1": uint64(0),
					"metric2": uint64(1),
				},
				tm: tm,
				tp: telegraf.Untyped,
			},
			output: producers.MetricsMessage{
				Name: "dcos.metrics.app",
				Dimensions: producers.Dimensions{
					MesosID:       translator.MesosID,
					ClusterID:     translator.DCOSClusterID,
					Hostname:      translator.DCOSNodePrivateIP,
					ContainerID:   "cid",
					FrameworkName: "sname",
					TaskName:      "tname",
				},
				Datapoints: []producers.Datapoint{
					producers.Datapoint{
						Name:      "prefix.foo.metric1",
						Value:     uint64(0),
						Timestamp: timestamp,
						Tags:      map[string]string{"label_name": "label_value"},
					},
					producers.Datapoint{
						Name:      "prefix.foo.metric2",
						Value:     uint64(1),
						Timestamp: timestamp,
						Tags:      map[string]string{"label_name": "label_value"},
					},
				},
			},
		},

		// App metrics are assumed to come from statsd, which may provide NaN values. These values should be converted
		// to an empty string.
		testCase{
			name: "app metric with NaN value",
			input: metricParams{
				name: "prefix.foo",
				tags: map[string]string{
					"container_id": "cid",
					"service_name": "sname",
					"task_name":    "tname",
					"metric_type":  "mtype",
					"label_name":   "label_value",
				},
				fields: map[string]interface{}{
					"metric1": math.NaN(),
				},
				tm: tm,
				tp: telegraf.Untyped,
			},
			output: producers.MetricsMessage{
				Name: "dcos.metrics.app",
				Dimensions: producers.Dimensions{
					MesosID:       translator.MesosID,
					ClusterID:     translator.DCOSClusterID,
					Hostname:      translator.DCOSNodePrivateIP,
					ContainerID:   "cid",
					FrameworkName: "sname",
					TaskName:      "tname",
				},
				Datapoints: []producers.Datapoint{
					producers.Datapoint{
						Name:      "prefix.foo.metric1",
						Value:     "",
						Timestamp: timestamp,
						Tags:      map[string]string{"label_name": "label_value"},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			msg, ok, err := translator.Translate(tc.input.NewMetric(t))
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("translation failed to produce a MetricsMessage")
			}
			if !reflect.DeepEqual(msg, tc.output) {
				t.Log("expected:", tc.output)
				t.Log("actually:", msg)
				t.Fatal("translation returned an unexpected MetricsMessage")
			}
		})
	}
}

func TestTranslateFail(t *testing.T) {
	type testCase struct {
		name  string
		input metricParams
	}

	testCases := []testCase{
		testCase{
			name: "non-node metric without container_id tag",
			input: metricParams{
				name: "prefix.foo",
				fields: map[string]interface{}{
					"metric1": uint64(0),
					"metric2": uint64(1),
				},
				tm: tm,
				tp: telegraf.Untyped,
			},
		},

		testCase{
			name: "cpu metric for individual cpu",
			input: metricParams{
				name: "prefix.cpu",
				tags: map[string]string{"cpu": "1"},
				fields: map[string]interface{}{
					"usage_idle":   70.0,
					"usage_user":   20.0,
					"usage_system": 6.0,
					"usage_iowait": 4.0,
				},
				tm: tm,
				tp: telegraf.Gauge,
			},
		},

		testCase{
			name: "swap metric for swaps in/out",
			input: metricParams{
				name: "prefix.swap",
				fields: map[string]interface{}{
					"in":  uint64(1000),
					"out": uint64(600),
				},
				tm: tm,
				tp: telegraf.Counter,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, ok, err := translator.Translate(tc.input.NewMetric(t))
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatal("translation unexpectedly succeeded")
			}
		})
	}
}

func TestTranslateError(t *testing.T) {
	type testCase struct {
		name  string
		input metricParams
	}

	testCases := []testCase{
		testCase{
			name: "cpu metric with non-float64 values",
			input: metricParams{
				name: "prefix.cpu",
				tags: map[string]string{"cpu": "cpu-total"},
				fields: map[string]interface{}{
					"usage_idle":   70,
					"usage_user":   20,
					"usage_system": 6,
					"usage_iowait": 4,
				},
				tm: tm,
				tp: telegraf.Gauge,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := translator.Translate(tc.input.NewMetric(t))
			if err == nil {
				t.Fatal("translation unexpectedly did not return an error")
			}
		})
	}
}

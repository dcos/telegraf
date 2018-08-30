package dcos_metrics

import (
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/dcos/dcos-metrics/producers"
	httpProducer "github.com/dcos/dcos-metrics/producers/http"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/outputs"
)

type DCOSMetrics struct {
	Listen            string
	CacheExpiry       internal.Duration `toml:"cache_expiry"`
	MesosID           string            `toml:"mesos_id"`
	DCOSNodeRole      string            `toml:"dcos_node_role"`
	DCOSClusterID     string            `toml:"dcos_cluster_id"`
	DCOSNodePrivateIP string            `toml:"dcos_node_private_ip"`

	translator producerTranslator
	metricChan chan producers.MetricsMessage
}

func (d *DCOSMetrics) Description() string {
	return "Configuration for the DC/OS Metrics API output plugin"
}

func (d *DCOSMetrics) SampleConfig() string {
	return `
  # Address to listen on. Leave unset to listen on a systemd-provided socket.
  listen = ":8080"

  # Duration to cache metrics in memory.
  cache_expiry = "2m"

  # DC/OS node's role (master or agent).
  dcos_node_role = "agent"

  # DC/OS node's private IP, as reported by /opt/mesosphere/bin/detect_ip.
  dcos_node_private_ip = "10.10.0.1"

  # Local Mesos instance's ID.
  mesos_id = "ABCDEF1234"
`
}

func (d *DCOSMetrics) Start() error {
	d.translator = producerTranslator{
		MesosID:           d.MesosID,
		DCOSNodeRole:      d.DCOSNodeRole,
		DCOSClusterID:     d.DCOSClusterID,
		DCOSNodePrivateIP: d.DCOSNodePrivateIP,
	}

	config, err := d.producerConfig()
	if err != nil {
		return err
	}

	producer, producerChan := httpProducer.New(config)
	d.metricChan = producerChan
	go producer.Run()

	return nil
}

// dcos-metrics producers don't offer a mechanism to stop them, and there's nothing to clean up.
func (d *DCOSMetrics) Stop() {}

// This output doesn't need to connect to anything.
func (d *DCOSMetrics) Connect() error { return nil }

// This output doesn't create any connections, so there's nothing to close.
func (d *DCOSMetrics) Close() error { return nil }

func (d *DCOSMetrics) Write(metrics []telegraf.Metric) error {
	for _, metric := range metrics {
		message, ok, err := d.translator.Translate(metric)
		if err != nil {
			return errors.New(fmt.Sprintf("error translating metric %s: %s", metric.Name(), err))
		}
		if ok {
			d.metricChan <- message
		}
	}
	return nil
}

// producerConfig returns a httpProducer.Config configured from d.
func (d *DCOSMetrics) producerConfig() (httpProducer.Config, error) {
	var (
		err        error
		listenHost string
		listenPort int
	)

	if d.Listen != "" {
		listenHost, listenPort, err = splitHostPort(d.Listen)
		if err != nil {
			return httpProducer.Config{}, errors.New(fmt.Sprintf("error reading listen: %s", err))
		}
	}

	switch d.DCOSNodeRole {
	case "master", "agent":
	default:
		return httpProducer.Config{}, errors.New("error reading dcos_node_role: must be one of master or agent")
	}

	return httpProducer.Config{
		IP:          listenHost,
		Port:        listenPort,
		CacheExpiry: d.CacheExpiry.Duration,
		DCOSRole:    d.DCOSNodeRole,
	}, nil
}

// splitHostPort splits a string of the format "host:port" and returns the host and port.
func splitHostPort(hostPort string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(hostPort)
	if err != nil {
		return "", 0, err
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, errors.New(fmt.Sprintf("error reading port: %s", err))
	}

	return host, port, nil
}

func init() {
	outputs.Add("dcos_metrics", func() telegraf.Output { return &DCOSMetrics{} })
}

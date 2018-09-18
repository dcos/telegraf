package dcos_metrics

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/dcos/dcos-metrics/producers"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/metric"
)

func TestSplitHostPort(t *testing.T) {
	type testCase struct {
		hostPort string
		host     string
		port     int
	}

	testCases := []testCase{
		testCase{
			hostPort: "localhost:8000",
			host:     "localhost",
			port:     8000,
		},
		testCase{
			hostPort: "10.10.0.1:10",
			host:     "10.10.0.1",
			port:     10,
		},
		testCase{
			hostPort: "host.name.com:60000",
			host:     "host.name.com",
			port:     60000,
		},
		testCase{
			hostPort: ":80",
			host:     "",
			port:     80,
		},
	}

	for _, tc := range testCases {
		host, port, err := splitHostPort(tc.hostPort)
		if err != nil {
			t.Fatal(err)
		}

		if host != tc.host {
			t.Fatalf("expected host %s for hostport %s, got %s", tc.host, tc.hostPort, host)
		}
		if port != tc.port {
			t.Fatalf("expected port %d for hostport %s, got %d", tc.port, tc.hostPort, port)
		}
	}

	badPortCases := []string{
		"localhost:foo",
		"localhost:",
	}
	for _, hostPort := range badPortCases {
		_, _, err := splitHostPort(hostPort)
		if err == nil {
			t.Fatalf("expected error for hostport %s", hostPort)
		}
	}
}

func TestDCOSMetricsNaNValue(t *testing.T) {
	// Assert that the server returns a 200 status for container app metrics after the HTTP producer receives a NaN value.
	containerID := "cid"

	dcosMetrics, url, err := setupDCOSMetrics()
	if err != nil {
		t.Fatal(err)
	}
	defer dcosMetrics.Stop()

	m, err := metric.New(
		"prefix.foo",
		map[string]string{
			"container_id":  containerID,
			"service_name":  "sname",
			"task_name":     "tname",
			"executor_name": "ename",
			"label_name":    "label_value",
			"metric_type":   "gauge",
		},
		map[string]interface{}{
			"metric1": math.NaN(),
		},
		time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}

	err = dcosMetrics.Write([]telegraf.Metric{m})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(url + "/v0/containers/" + containerID + "/app")
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != 200 {
		t.Fatalf("expected status code 200, got %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	var metrics producers.MetricsMessage
	json.Unmarshal(body, &metrics)

	found := false
	for _, dp := range metrics.Datapoints {
		if dp.Name == "prefix.foo.metric1" {
			if dp.Value != "" {
				t.Fatalf("expected datapoint value to be empty string, got %v", dp.Value)
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("datapoint missing in response")
	}
}

func TestDCOSMetricsNilValue(t *testing.T) {
	// Assert that the server returns node metrics, even when supplied with multiple metrics messages,
	// some of which have missing fields

	dcosMetrics, url, err := setupDCOSMetrics()
	if err != nil {
		t.Fatal(err)
	}
	defer dcosMetrics.Stop()

	m1, err := metric.New(
		"dcos.metrics.node.system",
		map[string]string{},
		map[string]interface{}{"load1": uint64(123), "load5": uint64(1234), "load15": uint64(12345)},
		time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}

	m2, err := metric.New(
		"dcos.metrics.node.system",
		map[string]string{"mesos_id": "fake mesos id"},
		map[string]interface{}{"uptime": uint64(12345)},
		time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}

	err = dcosMetrics.Write([]telegraf.Metric{m1, m2})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(url + "/v0/node")
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != 200 {
		t.Fatalf("expected status code 200, got %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	var metrics producers.MetricsMessage
	json.Unmarshal(body, &metrics)

	results := map[string]interface{}{}
	for _, dp := range metrics.Datapoints {
		results[dp.Name] = dp.Value
	}
	if len(results) != 4 {
		t.Fatal("datapoint missing in response")
	}
}

func setupDCOSMetrics() (DCOSMetrics, string, error) {
	serverHostPort := fmt.Sprintf("localhost:%d", findFreePort())
	serverURL := fmt.Sprintf("http://%s", serverHostPort)

	dm := DCOSMetrics{
		Listen:            serverHostPort,
		CacheExpiry:       internal.Duration{Duration: time.Second},
		MesosID:           "fake-mesos-id",
		DCOSNodeRole:      "agent",
		DCOSClusterID:     "fake-cluster-id",
		DCOSNodePrivateIP: "10.0.0.1",
	}

	return dm, serverURL, dm.Start()
}

// findFreePort momentarily listens on :0, then closes the connection and
// returns the port assigned
func findFreePort() int {
	ln, _ := net.Listen("tcp", ":0")
	ln.Close()

	addr := ln.Addr().(*net.TCPAddr)
	return addr.Port
}

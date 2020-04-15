package prometheus

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sampleTextFormat = `# HELP go_gc_duration_seconds A summary of the GC invocation durations.
# TYPE go_gc_duration_seconds summary
go_gc_duration_seconds{quantile="0"} 0.00010425500000000001
go_gc_duration_seconds{quantile="0.25"} 0.000139108
go_gc_duration_seconds{quantile="0.5"} 0.00015749400000000002
go_gc_duration_seconds{quantile="0.75"} 0.000331463
go_gc_duration_seconds{quantile="1"} 0.000667154
go_gc_duration_seconds_sum 0.0018183950000000002
go_gc_duration_seconds_count 7
# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines 15
# HELP test_metric An untyped metric with a timestamp
# TYPE test_metric untyped
test_metric{label="value"} 1.0 1490802350000
`

func TestPrometheusGeneratesMetrics(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, sampleTextFormat)
	}))
	defer ts.Close()

	p := &Prometheus{
		URLs: []string{ts.URL},
	}

	var acc testutil.Accumulator

	err := acc.GatherError(p.Gather)
	require.NoError(t, err)

	assert.True(t, acc.HasFloatField("go_gc_duration_seconds", "count"))
	assert.True(t, acc.HasFloatField("go_goroutines", "gauge"))
	assert.True(t, acc.HasFloatField("test_metric", "value"))
	assert.True(t, acc.HasTimestamp("test_metric", time.Unix(1490802350, 0)))
	assert.False(t, acc.HasTag("test_metric", "address"))
	assert.True(t, acc.TagValue("test_metric", "url") == ts.URL+"/metrics")
}

func TestPrometheusGeneratesMetricsWithHostNameTag(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, sampleTextFormat)
	}))
	defer ts.Close()

	p := &Prometheus{
		KubernetesServices: []string{ts.URL},
	}
	u, _ := url.Parse(ts.URL)
	tsAddress := u.Hostname()

	var acc testutil.Accumulator

	err := acc.GatherError(p.Gather)
	require.NoError(t, err)

	assert.True(t, acc.HasFloatField("go_gc_duration_seconds", "count"))
	assert.True(t, acc.HasFloatField("go_goroutines", "gauge"))
	assert.True(t, acc.HasFloatField("test_metric", "value"))
	assert.True(t, acc.HasTimestamp("test_metric", time.Unix(1490802350, 0)))
	assert.True(t, acc.TagValue("test_metric", "address") == tsAddress)
	assert.True(t, acc.TagValue("test_metric", "url") == ts.URL)
}

func TestPrometheusGeneratesMetricsAlthoughFirstDNSFails(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, sampleTextFormat)
	}))
	defer ts.Close()

	p := &Prometheus{
		URLs:               []string{ts.URL},
		KubernetesServices: []string{"http://random.telegraf.local:88/metrics"},
	}

	var acc testutil.Accumulator

	err := acc.GatherError(p.Gather)
	require.NoError(t, err)

	assert.True(t, acc.HasFloatField("go_gc_duration_seconds", "count"))
	assert.True(t, acc.HasFloatField("go_goroutines", "gauge"))
	assert.True(t, acc.HasFloatField("test_metric", "value"))
	assert.True(t, acc.HasTimestamp("test_metric", time.Unix(1490802350, 0)))
}

func TestPrometheusGathersMesosMetrics(t *testing.T) {
	testCases := map[string]map[string]URLAndAddress{
		"empty":                    {},
		"malformedTaskLabelIndex":  {},
		"taskLabelIndexOutOfRange": {},
		"wrongPortName":            {},
		"portlabel": {
			"http://127.0.0.1:12345/metrics": {
				URL:         unsafelyParse("http://127.0.0.1:12345/metrics"),
				OriginalURL: unsafelyParse("http://127.0.0.1:12345/metrics"),
				Tags:        map[string]string{"container_id": "abc-123"},
			},
			"http://127.0.0.1:12345/federate": {
				URL:         unsafelyParse("http://127.0.0.1:12345/federate"),
				OriginalURL: unsafelyParse("http://127.0.0.1:12345/federate"),
				Tags:        map[string]string{"container_id": "xyz-123"},
			},
		},
		"tasklabelViaIndex": {
			"http://127.0.0.1:12345/metrics": {
				URL:         unsafelyParse("http://127.0.0.1:12345/metrics"),
				OriginalURL: unsafelyParse("http://127.0.0.1:12345/metrics"),
				Tags:        map[string]string{"container_id": "abc-123"},
			},
		},
		"tasklabelViaName": {
			"http://127.0.0.1:12345/metrics": {
				URL:         unsafelyParse("http://127.0.0.1:12345/metrics"),
				OriginalURL: unsafelyParse("http://127.0.0.1:12345/metrics"),
				Tags:        map[string]string{"container_id": "abc-123"},
			},
		},
		"tasklabelIndexPriority": {
			"http://127.0.0.1:12345/metrics": {
				URL:         unsafelyParse("http://127.0.0.1:12345/metrics"),
				OriginalURL: unsafelyParse("http://127.0.0.1:12345/metrics"),
				Tags:        map[string]string{"container_id": "abc-123"},
			},
		},
		"tasklabelAlternatePath": {
			"http://127.0.0.1:12345/federate": {
				URL:         unsafelyParse("http://127.0.0.1:12345/federate"),
				OriginalURL: unsafelyParse("http://127.0.0.1:12345/federate"),
				Tags:        map[string]string{"container_id": "abc-123"},
			},
		},
		"networkModes": {
			"http://127.0.0.1:85254/metrics": {
				URL:         unsafelyParse("http://127.0.0.1:85254/metrics"),
				OriginalURL: unsafelyParse("http://127.0.0.1:85254/metrics"),
				Tags:        map[string]string{"container_id": "mapped-pod-labeled-container-id"},
			},
			"http://127.0.0.1:25854/metrics": {
				URL:         unsafelyParse("http://127.0.0.1:25854/metrics"),
				OriginalURL: unsafelyParse("http://127.0.0.1:25854/metrics"),
				Tags:        map[string]string{"container_id": "mapped-pod-container-id"},
			},
			// Bridge mode should use hostname:mapped-port (not hostname:container-port)
			"http://127.0.0.1:10388/metrics": {
				URL:         unsafelyParse("http://127.0.0.1:10388/metrics"),
				OriginalURL: unsafelyParse("http://127.0.0.1:10388/metrics"),
				Tags:        map[string]string{"container_id": "host-docker-container-id"},
			},
			"http://127.0.0.1:8486/metrics": {
				URL:         unsafelyParse("http://127.0.0.1:8486/metrics"),
				OriginalURL: unsafelyParse("http://127.0.0.1:8486/metrics"),
				Tags:        map[string]string{"container_id": "host-mesos-container-id"},
			},
			"http://9.0.4.4:85254/metrics": {
				URL:         unsafelyParse("http://9.0.4.4:85254/metrics"),
				OriginalURL: unsafelyParse("http://9.0.4.4:85254/metrics"),
				Tags:        map[string]string{"container_id": "mixed-pod-labeled-container-id"},
			},
			"http://9.0.4.3:85254/metrics": {
				URL:         unsafelyParse("http://9.0.4.3:85254/metrics"),
				OriginalURL: unsafelyParse("http://9.0.4.3:85254/metrics"),
				Tags:        map[string]string{"container_id": "mixed-pod-container-id"},
			},
			"http://9.0.3.9:85254/metrics": {
				URL:         unsafelyParse("http://9.0.3.9:85254/metrics"),
				OriginalURL: unsafelyParse("http://9.0.3.9:85254/metrics"),
				Tags:        map[string]string{"container_id": "mixed-mesos-container-id"},
			},
			"http://9.0.4.9:85254/metrics": {
				URL:         unsafelyParse("http://9.0.4.9:85254/metrics"),
				OriginalURL: unsafelyParse("http://9.0.4.9:85254/metrics"),
				Tags:        map[string]string{"container_id": "mixed-mesos-labeled-container-id"},
			},
			"http://9.0.2.3:85254/metrics": {
				URL:         unsafelyParse("http://9.0.2.3:85254/metrics"),
				OriginalURL: unsafelyParse("http://9.0.2.3:85254/metrics"),
				Tags:        map[string]string{"container_id": "container-pod-container-id"},
			},
			"http://9.0.2.4:85254/metrics": {
				URL:         unsafelyParse("http://9.0.2.4:85254/metrics"),
				OriginalURL: unsafelyParse("http://9.0.2.4:85254/metrics"),
				Tags:        map[string]string{"container_id": "container-pod-labeled-container-id"},
			},
			"http://9.0.0.0:85254/metrics": {
				URL:         unsafelyParse("http://9.0.0.0:85254/metrics"),
				OriginalURL: unsafelyParse("http://9.0.0.0:85254/metrics"),
				Tags:        map[string]string{"container_id": "container-mesos-container-id"},
			},
			"http://9.0.0.1:85254/metrics": {
				URL:         unsafelyParse("http://9.0.0.1:85254/metrics"),
				OriginalURL: unsafelyParse("http://9.0.0.1:85254/metrics"),
				Tags:        map[string]string{"container_id": "container-mesos-labeled-container-id"},
			},
		},
	}
	for scenario, expected := range testCases {
		t.Run(scenario, func(t *testing.T) {
			server := startTestServer(t, scenario)
			defer server.Close()

			p := &Prometheus{
				MesosTimeout:  internal.Duration{Duration: 500 * time.Millisecond},
				MesosAgentUrl: server.URL,
				// mesosHostname is assigned in Start()
				mesosHostname: "127.0.0.1",
			}

			urls, err := p.GetAllURLs()
			assert.Nil(t, err)
			assert.Equal(t, expected, urls)

		})
	}
}

func TestGetMesosHostname(t *testing.T) {
	goodUrls := map[string]string{
		"http://localhost":                       "localhost",
		"http://localhost:9090":                  "localhost",
		"http://192.168.2.2":                     "192.168.2.2",
		"http://192.168.2.2:9090":                "192.168.2.2",
		"https://192.168.2.2":                    "192.168.2.2",
		"http://some-agent.testing.example.com/": "some-agent.testing.example.com",
	}
	badUrls := []string{
		"$UNPARSED_ENVIRONMENT_VARIABLE",
		"",
	}
	for input, expected := range goodUrls {
		output, err := getMesosHostname(input)
		assert.Nil(t, err)
		assert.Equal(t, expected, output)
	}
	for _, input := range badUrls {
		_, err := getMesosHostname(input)
		assert.NotNil(t, err)
	}
}

// unsafelyParse is a utility method that ignores errors from url.Parse
func unsafelyParse(u string) *url.URL {
	result, _ := url.Parse(u)
	return result
}

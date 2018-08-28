package dcos_metadata

import (
	"testing"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/metric"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	fixture  string
	inputs   []telegraf.Metric
	expected []telegraf.Metric
	// cachedContainers prepopulates the plugin with container info
	cachedContainers map[string]containerInfo
	// containers is how the dm.containers map should look after
	// metrics are retrieved
	containers map[string]containerInfo
}

var (
	TEST_CASES = []testCase{
		// No metrics, no state; nothing to do
		testCase{
			fixture:  "empty",
			inputs:   []telegraf.Metric{},
			expected: []telegraf.Metric{},
		},
		// One metric, cached state; tags are added
		testCase{
			fixture: "normal",
			inputs: []telegraf.Metric{
				newMetric("test",
					map[string]string{"container_id": "abc123"},
					map[string]interface{}{"value": int64(1)},
					time.Now(),
				),
			},
		// One metric, no cached state; no tags are added but state is updated
		testCase{
			fixture: "fresh",
			inputs: []telegraf.Metric{
				newMetric("test",
					map[string]string{"container_id": "abc123"},
					map[string]interface{}{"value": int64(1)},
					time.Now(),
				),
			},
			expected: []telegraf.Metric{
				newMetric("test",
					// We don't expect tags, since no cache exists
					map[string]string{"container_id": "abc123"},
					map[string]interface{}{"value": int64(1)},
					time.Now(),
				),
			},
			cachedContainers: map[string]containerInfo{},
			// We do expect the cache to be updated when apply is done
			containers: map[string]containerInfo{
				"abc123": containerInfo{"abc123", "task", "executor", "framework",
					// Ensure that the tags are picked up from state
					map[string]string{"FOO": "bar", "BAZ": "qux"}},
			},
		},
		// One metric without a container ID; nothing to do
		testCase{
			fixture: "unrelated",
			inputs: []telegraf.Metric{
				newMetric("test",
					map[string]string{}, // no container_id tag
					map[string]interface{}{"value": int64(1)},
					time.Now(),
				),
			},
			expected: []telegraf.Metric{
				newMetric("test",
					map[string]string{},
					map[string]interface{}{"value": int64(1)},
					time.Now(),
				),
			},
			cachedContainers: map[string]containerInfo{},
			// We do not expect the cache to be updated
			containers: map[string]containerInfo{},
		},
		// No executor;
		testCase{
			fixture: "noexecutor",
			inputs: []telegraf.Metric{
				newMetric("test",
					map[string]string{"container_id": "abc123"},
					map[string]interface{}{"value": int64(1)},
					time.Now(),
				),
			},
			expected: []telegraf.Metric{
				newMetric("test",
					map[string]string{
						"container_id": "abc123",
						"service_name": "framework",
						// no executor tag at all
						"task_name": "task",
					},
					map[string]interface{}{"value": int64(1)},
					time.Now(),
				),
			},
			cachedContainers: map[string]containerInfo{
				"abc123": containerInfo{"abc123", "task", "", "framework",
					map[string]string{}},
			},
			containers: map[string]containerInfo{
				"abc123": containerInfo{"abc123", "task", "", "framework",
					map[string]string{}},
			},
		},
	}
)

func TestApply(t *testing.T) {
	for _, tc := range TEST_CASES {
		t.Run(tc.fixture, func(t *testing.T) {
			server, teardown := startTestServer(t, tc.fixture)
			defer teardown()

			dm := DCOSMetadata{
				MesosAgentUrl: server.URL,
				Timeout:       internal.Duration{100 * time.Millisecond},
				RateLimit:     internal.Duration{50 * time.Millisecond},
				containers:    tc.cachedContainers,
			}

			outputs := dm.Apply(tc.inputs...)

			// No metrics were dropped
			assert.Equal(t, len(tc.expected), len(outputs))
			// Tags were added as expected
			for i, actual := range outputs {
				expected := tc.expected[i]
				assert.Equal(t, expected.Name(), actual.Name())
				assert.Equal(t, expected.Tags(), actual.Tags())
			}

			assert.Equal(t, tc.containers, dm.containers)
		})
	}
}

// newMetric is a convenience method which allows us to define test cases at
// package level without doing error handling
func newMetric(name string, tags map[string]string, fields map[string]interface{}, tm time.Time) telegraf.Metric {
	m, err := metric.New(name, tags, fields, tm)
	if err != nil {
		panic(err)
	}
	return m
}

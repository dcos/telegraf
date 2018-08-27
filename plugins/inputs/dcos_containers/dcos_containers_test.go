package dcos_containers

import (
	"testing"
	"time"

	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	fixture      string
	measurements map[string]map[string]interface{}
	tags         map[string]string
	ts           int64
}

var (
	TEST_CASES = []testCase{
		testCase{
			fixture:      "empty",
			measurements: map[string]map[string]interface{}{},
			tags:         map[string]string{},
			ts:           0,
		},
		testCase{
			fixture: "normal",
			measurements: map[string]map[string]interface{}{
				"cpus": map[string]interface{}{
					"limit":               8.25,
					"nr_periods":          uint32(769021),
					"nr_throttled":        uint32(1046),
					"system_time_secs":    34501.45,
					"throttled_time_secs": 352.597023453,
					"user_time_secs":      96348.84,
				},
				"mem": map[string]interface{}{
					"anon_bytes":        uint64(4845449216),
					"file_bytes":        uint64(260165632),
					"limit_bytes":       uint64(7650410496),
					"mapped_file_bytes": uint64(7159808),
					"rss_bytes":         uint64(5105614848),
				},
			},
			tags: map[string]string{
				"container_id": "abc123",
			},
			ts: 1388534400,
		},
		testCase{
			fixture: "blkio",
			measurements: map[string]map[string]interface{}{
				"blkio": map[string]interface{}{
					"io_serviced":      uint64(1),
					"io_service_bytes": uint64(2),
					"io_service_time":  uint64(3),
					"io_wait_time":     uint64(4),
					"io_merged":        uint64(5),
					"io_queued":        uint64(6),
				},
			},
			tags: map[string]string{
				"container_id": "abc123",
				"device":       "default",
				"policy":       "cfq",
			},
			ts: 1388534400,
		},
	}
)

func TestGather(t *testing.T) {
	for _, tc := range TEST_CASES {
		t.Run(tc.fixture, func(t *testing.T) {
			var acc testutil.Accumulator

			server := startTestServer(t, tc.fixture)
			defer server.Close()

			dc := DCOSContainers{
				MesosAgentUrl: server.URL,
				Timeout:       internal.Duration{Duration: 10 * time.Millisecond},
			}

			err := acc.GatherError(dc.Gather)
			assert.Nil(t, err)
			if len(tc.measurements) > 0 {
				for m, fields := range tc.measurements {
					// all expected fields are present
					acc.AssertContainsFields(t, m, fields)
					// all expected tags are present
					acc.AssertContainsTaggedFields(t, m, fields, tc.tags)
					// the expected timestamp is present
					assertHasTimestamp(t, acc, m, tc.ts)
				}
			} else {
				acc.AssertDoesNotContainMeasurement(t, "containers")
				acc.AssertDoesNotContainMeasurement(t, "cpus")
				acc.AssertDoesNotContainMeasurement(t, "mem")
				acc.AssertDoesNotContainMeasurement(t, "disk")
				acc.AssertDoesNotContainMeasurement(t, "net")
			}
		})
	}
}

func TestSetIfNotNil(t *testing.T) {
	t.Run("Legal set methods which return concrete values", func(t *testing.T) {
		mmap := make(map[string]interface{})
		methods := map[string]interface{}{
			"a": func() uint32 { return 1 },
			"b": func() uint64 { return 1 },
			"c": func() float64 { return 1 },
		}
		expected := map[string]interface{}{
			"a": uint32(1),
			"b": uint64(1),
			"c": float64(1),
		}
		for key, set := range methods {
			err := setIfNotNil(mmap, key, set)
			assert.Nil(t, err)
		}
		assert.Equal(t, mmap, expected)
	})
	t.Run("Legal set methods which return nil", func(t *testing.T) {
		mmap := make(map[string]interface{})
		methods := map[string]interface{}{
			"a": func() uint32 { return 0 },
			"b": func() uint64 { return 0 },
			"c": func() float64 { return 0 },
		}
		expected := map[string]interface{}{}
		for key, set := range methods {
			err := setIfNotNil(mmap, key, set)
			assert.Nil(t, err)
		}
		assert.Equal(t, mmap, expected)
	})
	t.Run("Illegal set methods", func(t *testing.T) {
		mmap := make(map[string]interface{})
		methods := map[string]interface{}{
			"a": func() string { return "foo" },
			"b": func() interface{} { return 1 },
			"c": func() {},
		}
		expected := map[string]interface{}{}
		for key, set := range methods {
			err := setIfNotNil(mmap, key, set)
			assert.NotNil(t, err)
		}
		assert.Equal(t, mmap, expected)
	})
}

// assertHasTimestamp checks that the specified measurement has the expected ts
func assertHasTimestamp(t *testing.T, acc testutil.Accumulator, measurement string, ts int64) {
	expected := time.Unix(ts, 0)
	if acc.HasTimestamp(measurement, expected) {
		return
	}
	if m, ok := acc.Get(measurement); ok {
		actual := m.Time
		t.Errorf("%s had a bad timestamp: expected %q; got %q", measurement, expected, actual)
		return
	}
	t.Errorf("%s could not be retrieved while attempting to assert it had timestamp", measurement)
}

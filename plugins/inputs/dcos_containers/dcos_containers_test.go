package dcos_containers

import (
	"testing"
	"time"

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
	}
)

func TestGather(t *testing.T) {
	for _, tc := range TEST_CASES {
		t.Run(tc.fixture, func(t *testing.T) {
			var acc testutil.Accumulator

			server, teardown := startTestServer(t, tc.fixture)
			defer teardown()

			dc := DCOSContainers{
				MesosAgentUrl: server.URL,
				Timeout:       10 * time.Millisecond,
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

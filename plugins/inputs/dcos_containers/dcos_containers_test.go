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
		})
	}
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

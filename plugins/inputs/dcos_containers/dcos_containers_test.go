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

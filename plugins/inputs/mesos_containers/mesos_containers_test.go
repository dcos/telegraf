package mesos_containers

import (
	"testing"
	"time"

	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	name         string
	fixture      string
	measurements map[string]map[string]interface{}
	tags         map[string]string
	ts           int64
}

var (
	TEST_CASES = []testCase{
		{
			name:         "empty",
			fixture:      "empty",
			measurements: map[string]map[string]interface{}{},
			tags:         map[string]string{},
			ts:           0,
		},
		{
			name:    "normal",
			fixture: "normal",
			measurements: map[string]map[string]interface{}{
				"cpus": {
					"limit":               8.25,
					"nr_periods":          uint32(769021),
					"nr_throttled":        uint32(1046),
					"system_time_secs":    34501.45,
					"throttled_time_secs": 352.597023453,
					"user_time_secs":      96348.84,
				},
				"mem": {
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
		{
			name:    "blkio cfq",
			fixture: "blkio_cfq",
			measurements: map[string]map[string]interface{}{
				"blkio": {
					"io_serviced_total":      uint64(1),
					"io_service_bytes_total": uint64(2),
					"io_service_time_total":  uint64(3),
					"io_wait_time_total":     uint64(4),
					"io_merged_total":        uint64(5),
					"io_queued_total":        uint64(6),
				},
			},
			tags: map[string]string{
				"container_id": "abc123",
				"device":       "default",
				"policy":       "cfq",
			},
			ts: 1388534400,
		},
		{
			name:    "blkio cfq recursive",
			fixture: "blkio_cfq_recursive",
			measurements: map[string]map[string]interface{}{
				"blkio": {
					"io_serviced_total":      uint64(1),
					"io_service_bytes_total": uint64(2),
					"io_service_time_total":  uint64(3),
					"io_wait_time_total":     uint64(4),
					"io_merged_total":        uint64(5),
					"io_queued_total":        uint64(6),
				},
			},
			tags: map[string]string{
				"container_id": "abc123",
				"device":       "default",
				"policy":       "cfq_recursive",
			},
			ts: 1388534400,
		},
		{
			name:    "blkio throttling",
			fixture: "blkio_throttling",
			measurements: map[string]map[string]interface{}{
				"blkio": {
					"io_serviced_total":      uint64(1),
					"io_serviced_read":       uint64(11),
					"io_service_bytes_total": uint64(2),
					"io_service_bytes_async": uint64(22),
				},
			},
			tags: map[string]string{
				"container_id": "abc123",
				"device":       "111.22",
				"policy":       "throttling",
			},
			ts: 1388534400,
		},
		{
			name:    "blkio throttling",
			fixture: "blkio_throttling",
			measurements: map[string]map[string]interface{}{
				"blkio": {
					"io_serviced_total":      uint64(1),
					"io_serviced_read":       uint64(11),
					"io_service_bytes_total": uint64(2),
					"io_service_bytes_async": uint64(22),
				},
			},
			tags: map[string]string{
				"container_id": "abc123",
				"device":       "333.44",
				"policy":       "throttling",
			},
			ts: 1388534400,
		},
		{
			name:    "blkio throttling",
			fixture: "blkio_throttling",
			measurements: map[string]map[string]interface{}{
				"blkio": {
					"io_serviced_total":      uint64(1),
					"io_serviced_read":       uint64(11),
					"io_service_bytes_total": uint64(2),
					"io_service_bytes_async": uint64(22),
				},
			},
			tags: map[string]string{
				"container_id": "abc123",
				"device":       "222.33",
				"policy":       "throttling",
			},
			ts: 1388534400,
		},
		{
			name:    "disk statistics",
			fixture: "blkio_cfq",
			measurements: map[string]map[string]interface{}{
				"disk": {
					"limit_bytes": uint64(1073741824),
					"used_bytes":  uint64(4096),
				},
			},
			tags: map[string]string{
				"container_id":                 "abc123",
				"volume_persistence_id":        "blkio#vol#7fac4205-a714-11e8-a05e-fa1a5b5940b8",
				"volume_persistence_principal": "dcos_marathon",
			},
			ts: 1388534400,
		},
		{
			name:    "perf",
			fixture: "blkio_cfq",
			measurements: map[string]map[string]interface{}{
				"perf": {
					"timestamp": 1535056712.427976,
				},
			},
			tags: map[string]string{
				"container_id": "abc123",
			},
			ts: 1388534400,
		},
		{
			name:    "net traffic control",
			fixture: "net_traffic_control",
			measurements: map[string]map[string]interface{}{
				"net": {
					"tx_backlog":     uint64(1),
					"tx_bytes":       uint64(2),
					"tx_dropped":     uint64(3),
					"tx_over_limits": uint64(4),
					"tx_packets":     uint64(5),
					"tx_qlen":        uint64(6),
					"tx_rate_bps":    uint64(7),
					"tx_rate_pps":    uint64(8),
					"tx_requeues":    uint64(9),
				},
			},
			tags: map[string]string{
				"container_id": "abc123",
				"id":           "tx_bw_cap",
			},
			ts: 1388534400,
		},
		{
			name:    "net snmp",
			fixture: "net_snmp",
			measurements: map[string]map[string]interface{}{
				"net": {
					"ip_forwarding":       int64(1),
					"ip_default_ttl":      int64(2),
					"ip_in_receives":      int64(3),
					"icmp_in_msgs":        int64(1),
					"icmp_in_errors":      int64(2),
					"icmp_in_csum_errors": int64(3),
					"tcp_rto_algorithm":   int64(1),
					"tcp_rto_min":         int64(2),
					"tcp_rto_max":         int64(3),
					"udp_in_datagrams":    int64(1),
					"udp_no_ports":        int64(2),
					"udp_in_errors":       int64(3),
				},
			},
			tags: map[string]string{
				"container_id": "abc123",
			},
			ts: 1388534400,
		},
	}
)

func TestGather(t *testing.T) {
	for _, tc := range TEST_CASES {
		t.Run(tc.name, func(t *testing.T) {
			var acc testutil.Accumulator

			server := startTestServer(t, tc.fixture)
			defer server.Close()

			mc := MesosContainers{
				MesosAgentUrl: server.URL,
				Timeout:       internal.Duration{Duration: 100 * time.Millisecond},
			}

			err := acc.GatherError(mc.Gather)
			assert.Nil(t, err)
			if len(tc.measurements) > 0 {
				for m, fields := range tc.measurements {
					// all expected fields are present
					acc.AssertContainsFields(t, m, fields)
					// all expected tags are present
					acc.AssertContainsTaggedFields(t, m, fields, tc.tags)
					// the expected timestamp is present
					assertHasTimestamp(t, &acc, m, tc.ts)
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

func TestGetClient(t *testing.T) {
	mc := MesosContainers{}
	client1 := mc.getClient()
	client2 := mc.getClient()
	assert.Equal(t, client1, client2)
}

// assertHasTimestamp checks that the specified measurement has the expected ts
func assertHasTimestamp(t *testing.T, acc *testutil.Accumulator, measurement string, ts int64) {
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

// +build udp
// Tests which involve UDP packets do not pass due to networking issues. This
// This file therefore holds valuable tests which can't pass on CI. They can be
// invoked via `go test -tags udp .`

package mesos_statsd

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/assert"
)

func TestGatherUDP(t *testing.T) {
	var acc testutil.Accumulator
	dir, err := ioutil.TempDir("", "containers")
	if err != nil {
		assert.Fail(t, fmt.Sprintf("Could not create temp dir: %s", err))
	}
	defer os.RemoveAll(dir)
	ds := MesosStatsd{StatsdHost: "127.0.0.1", ContainersDir: dir}

	addr := startTestServer(t, &ds)
	defer ds.Stop()

	t.Log("A container on a random port")
	abcjson := `{"container_id": "abc123"}`
	resp, err := http.Post(addr+"/container", "application/json", bytes.NewBuffer([]byte(abcjson)))
	assert.Nil(t, err)
	abc := parseContainer(t, resp.Body)
	assert.Equal(t, "abc123", abc.Id)
	assert.NotEmpty(t, abc.StatsdHost)
	assert.NotEmpty(t, abc.StatsdPort)

	t.Log("A container on a known port")
	xyzport := findFreePort()
	xyzjson := fmt.Sprintf(`{"container_id":"xyz123","statsd_host":"127.0.0.1","statsd_port":%d}`, xyzport)
	resp, err = http.Post(addr+"/container", "application/json", bytes.NewBuffer([]byte(xyzjson)))
	assert.Nil(t, err)
	xyz := parseContainer(t, resp.Body)
	assert.Equal(t, "xyz123", xyz.Id)
	assert.Equal(t, "127.0.0.1", xyz.StatsdHost)
	assert.Equal(t, xyzport, xyz.StatsdPort)

	t.Log("Sending statsd to containers")
	abcconn := dialUDPPort(t, abc.StatsdPort)
	xyzconn := dialUDPPort(t, xyz.StatsdPort)

	// Send each count ten times to each server
	for i := 0; i < 10; i++ {
		abcconn.Write([]byte("foo.bar:123|c"))
		xyzconn.Write([]byte("foo.bar:123|c"))
	}

	abcconn.Close()
	xyzconn.Close()

	err = acc.GatherError(ds.Gather)
	assert.Nil(t, err)

	// Wait for at least one of the sent values to arrive and be tagged
	err = waitFor(func() bool {
		acc.Lock()
		defer acc.Unlock()
		for _, p := range acc.Metrics {
			var cid string
			var rawVal interface{}
			var val int64
			var ok bool
			if p.Measurement != "foo.bar" {
				continue
			}
			if cid, ok = p.Tags["container_id"]; !ok {
				continue
			}
			if rawVal, ok = p.Fields["value"]; !ok {
				continue
			}
			if val, ok = rawVal.(int64); !ok {
				continue
			}
			if (cid == "abc123" || cid == "xyz123") && val >= 123 {
				return true
			}
		}
		return false
	})

	assert.Nil(t, err)
}

package dcos_statsd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"testing"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs/dcos_statsd/containers"
	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/assert"
)

func TestStart(t *testing.T) {
	t.Run("Server with no saved state", func(t *testing.T) {
		ds := DCOSStatsd{}
		// startTestServer runs a /health request test
		addr := startTestServer(t, &ds)
		defer ds.Stop()

		// Check that no containers were created
		resp, err := http.Get(addr + "/containers")
		assertResponseWas(t, resp, err, "[]")
	})

	t.Run("Server with a single container saved", func(t *testing.T) {
		// Create a temp dir:
		dir, err := ioutil.TempDir("", "containers")
		if err != nil {
			assert.Fail(t, fmt.Sprintf("Could not create temp dir: %s", err))
		}
		defer os.RemoveAll(dir)

		// Create JSON in memory:
		ctrport := findFreePort()
		ctrjson := fmt.Sprintf(
			`{"container_id":"abc123","statsd_host":"127.0.0.1","statsd_port":%d}`,
			ctrport)

		// Write JSON to disk:
		err = ioutil.WriteFile(dir+"/abc123", []byte(ctrjson), 0666)
		if err != nil {
			assert.Fail(t, fmt.Sprintf("Could not write container state: %s", err))
		}

		// Finally run DCOSStatsd.Start():
		ds := DCOSStatsd{ContainersDir: dir}
		addr := startTestServer(t, &ds)
		defer ds.Stop()

		// Ensure that container shows up in output:
		resp, err := http.Get(addr + "/containers")
		// encoding/json respects alphabetical order, so this is safe
		assertResponseWas(t, resp, err, fmt.Sprintf("[%s]", ctrjson))
	})

}

func TestStop(t *testing.T) {
	t.Run("Server with no containers", func(t *testing.T) {
		ds := DCOSStatsd{}
		addr := startTestServer(t, &ds)
		ds.Stop()

		// Test that the server has stopped
		resp, err := http.Get(addr + "/health")
		assert.NotNil(t, err)
		assert.Nil(t, resp)
	})

	t.Run("Server with a container", func(t *testing.T) {
		ds := DCOSStatsd{}
		addr := startTestServer(t, &ds)

		port := findFreePort()
		ctrjson := fmt.Sprintf(`{"container_id": "abc123","statsd_host": "127.0.0.1","statsd_port":%d}`, port)
		http.Post(addr+"/container", "application/json", bytes.NewBuffer([]byte(ctrjson)))
		ds.Stop()

		// Test that the server has stopped
		resp, err := http.Get(addr + "/health")
		assert.NotNil(t, err)
		assert.Nil(t, resp)

		// Test that the statsd server has stopped by listening on the same port
		statsdAddr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
		ln, err := net.ListenUDP("udp", statsdAddr)
		assert.Nil(t, err)
		if err == nil { // this test is necesasry to avoid segfaults
			ln.Close()
		}
	})
}

func TestGather(t *testing.T) {
	var acc testutil.Accumulator
	ds := DCOSStatsd{StatsdHost: "127.0.0.1"}

	addr := startTestServer(t, &ds)
	defer ds.Stop()

	// Test that the command API works as expected:

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

	t.Log("A container with an ID that already exists")
	resp, err = http.Post(addr+"/container", "application/json", bytes.NewBuffer([]byte(abcjson)))
	assert.Nil(t, err)
	abc2 := parseContainer(t, resp.Body)
	// Should have been redirected to the original abc123
	assert.Equal(t, abc, abc2)
	// no new containers should have been created
	assert.Equal(t, 2, len(ds.containers))

	t.Log("A container on an occupied port")
	qqqjson := fmt.Sprintf(`{"container_id":"qqq123","statsd_host":"127.0.0.1","statsd_port":%d}`, xyzport)
	_, err = http.Post(addr+"/container", "application/json", bytes.NewBuffer([]byte(qqqjson)))
	assert.Nil(t, err)
	// no new containers should have been created
	assert.Equal(t, 2, len(ds.containers))

	t.Log("Sending statsd to containers")
	abcconn := dialUDPPort(t, abc.StatsdPort)
	xyzconn := dialUDPPort(t, xyz.StatsdPort)
	defer abcconn.Close()
	defer xyzconn.Close()
	abcconn.Write([]byte("foo:123|c"))
	xyzconn.Write([]byte("foo:123|c"))

	err = acc.GatherError(ds.Gather)
	assert.Nil(t, err)

	acc.AssertContainsFields(t, "foo", map[string]interface{}{"value": int64(123)})
	acc.AssertContainsTaggedFields(t, "foo",
		map[string]interface{}{"value": int64(123)},
		map[string]string{"container_id": "abc123"})
}

// startTestServer starts a server on the specified DCOSStatsd on a randomly
// selected port and returns the address on which it will be served. It also
// runs a test against the /health endpoint to ensure that the command API is
// ready.
func startTestServer(t *testing.T, ds *DCOSStatsd) string {
	port := findFreePort()
	ds.Listen = fmt.Sprintf(":%d", port)
	addr := fmt.Sprintf("http://localhost:%d", port)

	var acc telegraf.Accumulator
	acc = &testutil.Accumulator{}

	err := ds.Start(acc)
	assert.Nil(t, err)

	// Ensure that the command API is ready
	_, err = http.Get(addr + "/health")
	assert.Nil(t, err)

	return addr
}

// findFreePort momentarily listens on :0, then closes the connection and
// returns the port assigned
func findFreePort() int {
	ln, _ := net.Listen("tcp", ":0")
	ln.Close()

	addr := ln.Addr().(*net.TCPAddr)
	return addr.Port
}

// assertResponseWas is a convenience method for testing http request responses
func assertResponseWas(t *testing.T, r *http.Response, err error, expected string) {
	assert.Nil(t, err)
	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	assert.Nil(t, err)
	assert.Equal(t, expected, string(body))
}

func parseContainer(t *testing.T, body io.Reader) containers.Container {
	var ctr containers.Container
	decoder := json.NewDecoder(body)
	if err := decoder.Decode(&ctr); err != nil {
		assert.Fail(t, "JSON could not be decoded to container: %q", err)
	}
	return ctr
}

// dialUDPPort dials localhost:port and returns the connection
func dialUDPPort(t *testing.T, port int) net.Conn {
	straddr := fmt.Sprintf(":%d", port)
	addr, err := net.ResolveUDPAddr("udp", straddr)
	if err != nil {
		assert.Fail(t, "Could not resolve address ", straddr)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		assert.Fail(t, "Could not dial UDP ", straddr)
	}
	return conn
}

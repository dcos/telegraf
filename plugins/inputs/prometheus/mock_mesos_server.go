package prometheus

// NOTE: this file relies on protobuf fixtures. These are binary files and
// cannot readily be changed. We therefore provide the go generate step below
// which serializes the contents of json files in the testdata directory to
// protobuf.
//
// You should run 'go generate' every time you change one of the json files in
// the testdata directory, and commit both the changed json file and the
// changed binary file.
//go:generate go run cmd/gen.go

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

// raw protobuf request types:
var (
	GET_CONTAINERS = []byte{8, 10}
	GET_STATE      = []byte{8, 9}
	GET_TASKS      = []byte{8, 13}
)

// startTestServer starts a server and serves the specified fixture's content
// at /api/v1
func startTestServer(t *testing.T, fixture string) *httptest.Server {
	router := http.NewServeMux()
	router.HandleFunc("/api/v1", func(w http.ResponseWriter, r *http.Request) {
		body, _ := ioutil.ReadAll(r.Body)

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		if bytes.Equal(body, GET_CONTAINERS) {
			containers := loadFixture(t, filepath.Join(fixture, "containers.bin"))
			w.Write(containers)
			return
		}
		if bytes.Equal(body, GET_STATE) {
			state := loadFixture(t, filepath.Join(fixture, "state.bin"))
			w.Write(state)
			return
		}
		if bytes.Equal(body, GET_TASKS) {
			tasks := loadFixture(t, filepath.Join(fixture, "tasks.bin"))
			w.Write(tasks)
			return
		}
		t.Fatal("Body contained an unknown request: ", body)
		return
	})
	return httptest.NewServer(router)
}

// loadFixture retrieves data from a file in ./testdata
func loadFixture(t *testing.T, filename string) []byte {
	path := filepath.Join("testdata", filename)
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return bytes
}

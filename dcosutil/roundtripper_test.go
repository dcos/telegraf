package dcosutil

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/telegraf/internal"
)

func TestRoundTripper(t *testing.T) {
	// Default User-Agent header
	expected := fmt.Sprintf("%s/%s", defaultUserAgent, internal.Version())
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headerVal := r.Header.Get("User-Agent")
		if headerVal != expected {
			t.Fatalf(fmt.Sprintf("Expected request header: `User-Agent: %s`. Got: %s", expected, headerVal))
		}
	}))
	defer testServer.Close()

	rt := NewRoundTripper(&http.Transport{}, "")
	c := http.Client{
		Transport: rt,
	}

	resp, err := c.Get(testServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
}

func TestRoundTripperConfigured(t *testing.T) {
	// Configured User-Agent header
	userAgent := "Telegraf-mesos"
	expected := fmt.Sprintf("%s/%s", userAgent, internal.Version())
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headerVal := r.Header.Get("User-Agent")
		if headerVal != expected {
			t.Fatalf(fmt.Sprintf("Expected request header: `User-Agent: %s`. Got: %s", expected, headerVal))
		}
	}))
	defer testServer.Close()

	rt := NewRoundTripper(&http.Transport{}, userAgent)
	c := http.Client{
		Transport: rt,
	}

	resp, err := c.Get(testServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
}

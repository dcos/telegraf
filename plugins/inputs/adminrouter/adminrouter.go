package adminrouter

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/internal/tls"
	"github.com/influxdata/telegraf/plugins/inputs"
)

const acceptHeader = `application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited;q=0.7,text/plain;version=0.0.4;q=0.3`

type AdminRouter struct {
	// URL strings of Prometheus endpoints
	PrometheusEndpoints []string `toml:"prometheus_endpoints"`

	ResponseTimeout internal.Duration `toml:"response_timeout"`

	tls.ClientConfig

	client *http.Client
}

var sampleConfig = `
  ## An array of urls to scrape metrics from.
  prometheus_endpoints = ["http://localhost:9100/metrics"]

  ## Specify timeout duration for slower prometheus clients (default is 3s)
  # response_timeout = "3s"

  ## Optional TLS Config
  # tls_ca = /path/to/cafile
  # tls_cert = /path/to/certfile
  # tls_key = /path/to/keyfile
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
`

func (p *AdminRouter) SampleConfig() string {
	return sampleConfig
}

func (p *AdminRouter) Description() string {
	return "Read metrics from one or many prometheus clients"
}

var ErrProtocolError = errors.New("prometheus protocol error")

func (a *AdminRouter) AddressToURL(u *url.URL, address string) *url.URL {
	host := address
	if u.Port() != "" {
		host = address + ":" + u.Port()
	}
	reconstructedURL := &url.URL{
		Scheme:     u.Scheme,
		Opaque:     u.Opaque,
		User:       u.User,
		Path:       u.Path,
		RawPath:    u.RawPath,
		ForceQuery: u.ForceQuery,
		RawQuery:   u.RawQuery,
		Fragment:   u.Fragment,
		Host:       host,
	}
	return reconstructedURL
}

func (a *AdminRouter) PrometheusEndpointURLs() ([]*url.URL, error) {
	promURLs := make([]*url.URL, 0)
	for _, u := range a.PrometheusEndpoints {
		u, err := url.Parse(u)
		if err != nil {
			log.Printf("prometheus: Could not parse %s, skipping it. Error: %s", u, err.Error())
			continue
		}
		promURLs = append(promURLs, u)
	}
	return promURLs, nil
}

func (a *AdminRouter) Gather(acc telegraf.Accumulator) error {
	if a.client == nil {
		client, err := a.createHTTPClient()
		if err != nil {
			return err
		}
		a.client = client
	}
	var wg sync.WaitGroup
	promURLs, err := a.PrometheusEndpointURLs()
	if err != nil {
		return err
	}
	for _, u := range promURLs {
		wg.Add(1)
		go func(u *url.URL) {
			defer wg.Done()
			acc.AddError(a.scrapePrometheus(u, acc))
		}(u)
	}
	wg.Wait()
	return nil
}

func (p *AdminRouter) createHTTPClient() (*http.Client, error) {
	tlsCfg, err := p.ClientConfig.TLSConfig()
	if err != nil {
		return nil, err
	}
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   tlsCfg,
			DisableKeepAlives: true,
		},
		Timeout: p.ResponseTimeout.Duration,
	}
	return client, nil
}

func (p *AdminRouter) scrapePrometheus(u *url.URL, acc telegraf.Accumulator) error {
	var req *http.Request
	var err error
	var uClient *http.Client
	if u.Scheme == "unix" {
		path := u.Query().Get("path")
		if path == "" {
			path = "/metrics"
		}
		req, err = http.NewRequest("GET", "http://localhost"+path, nil)

		// ignore error because it's been handled before getting here
		tlsCfg, _ := p.ClientConfig.TLSConfig()
		uClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig:   tlsCfg,
				DisableKeepAlives: true,
				Dial: func(network, addr string) (net.Conn, error) {
					c, err := net.Dial("unix", u.Path)
					return c, err
				},
			},
			Timeout: p.ResponseTimeout.Duration,
		}
	} else {
		if u.Path == "" {
			u.Path = "/metrics"
		}
		req, err = http.NewRequest("GET", u.String(), nil)
	}
	req.Header.Add("Accept", acceptHeader)
	var resp *http.Response
	if u.Scheme != "unix" {
		resp, err = p.client.Do(req)
	} else {
		resp, err = uClient.Do(req)
	}
	if err != nil {
		return fmt.Errorf("error making HTTP request to %s: %s", u, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned HTTP status %s", u, resp.Status)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading body: %s", err)
	}
	metrics, err := Parse(body, resp.Header)
	if err != nil {
		return fmt.Errorf("error reading metrics for %s: %s",
			u, err)
	}
	for _, metric := range metrics {
		switch metric.Type() {
		case telegraf.Counter:
			acc.AddCounter(metric.Name(), metric.Fields(), metric.Tags(), metric.Time())
		case telegraf.Gauge:
			acc.AddGauge(metric.Name(), metric.Fields(), metric.Tags(), metric.Time())
		case telegraf.Summary:
			acc.AddSummary(metric.Name(), metric.Fields(), metric.Tags(), metric.Time())
		case telegraf.Histogram:
			acc.AddHistogram(metric.Name(), metric.Fields(), metric.Tags(), metric.Time())
		default:
			acc.AddFields(metric.Name(), metric.Fields(), metric.Tags(), metric.Time())
		}
	}
	return nil
}

func init() {
	inputs.Add("adminrouter", func() telegraf.Input {
		return &AdminRouter{
			ResponseTimeout: internal.Duration{Duration: time.Second * 3},
		}
	})
}

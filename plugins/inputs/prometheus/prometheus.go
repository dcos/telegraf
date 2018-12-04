package prometheus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/dcosutil"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/internal/tls"
	"github.com/influxdata/telegraf/plugins/inputs"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/agent"
	"github.com/mesos/mesos-go/api/v1/lib/agent/calls"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli/httpagent"
)

const acceptHeader = `application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited;q=0.7,text/plain;version=0.0.4;q=0.3`

type Prometheus struct {
	// An array of urls to scrape metrics from.
	URLs []string `toml:"urls"`

	// An array of Kubernetes services to scrape metrics from.
	KubernetesServices []string

	// The URL of the local mesos agent
	MesosAgentUrl string
	MesosTimeout  internal.Duration
	dcosutil.DCOSConfig

	// Bearer Token authorization file path
	BearerToken string `toml:"bearer_token"`

	ResponseTimeout internal.Duration `toml:"response_timeout"`

	tls.ClientConfig

	client      *http.Client
	mesosClient *httpcli.Client
}

var sampleConfig = `
  ## An array of urls to scrape metrics from.
  urls = ["http://localhost:9100/metrics"]

  ## An array of Kubernetes services to scrape metrics from.
  # kubernetes_services = ["http://my-service-dns.my-namespace:9100/metrics"]

  ## The URL of the local mesos agent
  mesos_agent_url = "http://$NODE_PRIVATE_IP:5051"
	## The period after which requests to mesos agent should time out
	mesos_timeout = "10s"

  ## The user agent to send with requests
  user_agent = "telegraf-prometheus"
  ## Optional IAM configuration
  # ca_certificate_path = "/run/dcos/pki/CA/ca-bundle.crt"
  # iam_config_path = "/run/dcos/etc/dcos-telegraf/service_account.json"

  ## Use bearer token for authorization
  # bearer_token = /path/to/bearer/token

  ## Specify timeout duration for slower prometheus clients (default is 3s)
  # response_timeout = "3s"

  ## Optional TLS Config
  # tls_ca = /path/to/cafile
  # tls_cert = /path/to/certfile
  # tls_key = /path/to/keyfile
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
`

func (p *Prometheus) SampleConfig() string {
	return sampleConfig
}

func (p *Prometheus) Description() string {
	return "Read metrics from one or many prometheus clients"
}

var ErrProtocolError = errors.New("prometheus protocol error")

func (p *Prometheus) AddressToURL(u *url.URL, address string) *url.URL {
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

type URLAndAddress struct {
	OriginalURL *url.URL
	URL         *url.URL
	Address     string
}

func (p *Prometheus) GetAllURLs() ([]URLAndAddress, error) {
	allURLs := make([]URLAndAddress, 0)
	for _, u := range p.URLs {
		URL, err := url.Parse(u)
		if err != nil {
			log.Printf("prometheus: Could not parse %s, skipping it. Error: %s", u, err)
			continue
		}

		allURLs = append(allURLs, URLAndAddress{URL: URL, OriginalURL: URL})
	}
	// Kubernetes service discovery
	for _, service := range p.KubernetesServices {
		URL, err := url.Parse(service)
		if err != nil {
			return nil, err
		}
		resolvedAddresses, err := net.LookupHost(URL.Hostname())
		if err != nil {
			log.Printf("prometheus: Could not resolve %s, skipping it. Error: %s", URL.Host, err)
			continue
		}
		for _, resolved := range resolvedAddresses {
			serviceURL := p.AddressToURL(URL, resolved)
			allURLs = append(allURLs, URLAndAddress{URL: serviceURL, Address: resolved, OriginalURL: URL})
		}
	}
	// Mesos service discovery
	if p.MesosAgentUrl != "" {
		client, err := p.getMesosClient()
		if err != nil {
			log.Printf("E! %s", err)
			return allURLs, err
		}

		cli := httpagent.NewSender(client.Send)
		ctx, cancel := context.WithTimeout(context.Background(), p.MesosTimeout.Duration)
		defer cancel()

		tasks, err := p.getTasks(ctx, cli)
		if err != nil {
			log.Printf("E! %s", err)
			return allURLs, err
		}

		for _, url := range getMesosTaskPrometheusURLs(tasks) {
			allURLs = append(allURLs, URLAndAddress{URL: url, OriginalURL: url})
		}
	}
	return allURLs, nil
}

// Reads stats from all configured servers accumulates stats.
// Returns one of the errors encountered while gather stats (if any).
func (p *Prometheus) Gather(acc telegraf.Accumulator) error {
	if p.client == nil {
		client, err := p.createHttpClient()
		if err != nil {
			return err
		}
		p.client = client
	}

	var wg sync.WaitGroup

	allURLs, err := p.GetAllURLs()
	if err != nil {
		return err
	}
	for _, URL := range allURLs {
		wg.Add(1)
		go func(serviceURL URLAndAddress) {
			defer wg.Done()
			acc.AddError(p.gatherURL(serviceURL, acc))
		}(URL)
	}

	wg.Wait()

	return nil
}

var tr = &http.Transport{
	ResponseHeaderTimeout: time.Duration(3 * time.Second),
}

var client = &http.Client{
	Transport: tr,
	Timeout:   time.Duration(4 * time.Second),
}

func (p *Prometheus) createHttpClient() (*http.Client, error) {
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

func (p *Prometheus) gatherURL(u URLAndAddress, acc telegraf.Accumulator) error {
	var req, err = http.NewRequest("GET", u.URL.String(), nil)
	req.Header.Add("Accept", acceptHeader)
	var token []byte
	var resp *http.Response

	if p.BearerToken != "" {
		token, err = ioutil.ReadFile(p.BearerToken)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+string(token))
	}

	resp, err = p.client.Do(req)
	if err != nil {
		return fmt.Errorf("error making HTTP request to %s: %s", u.URL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned HTTP status %s", u.URL, resp.Status)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading body: %s", err)
	}

	metrics, err := Parse(body, resp.Header)
	if err != nil {
		return fmt.Errorf("error reading metrics for %s: %s",
			u.URL, err)
	}
	// Add (or not) collected metrics
	for _, metric := range metrics {
		tags := metric.Tags()
		// strip user and password from URL
		u.OriginalURL.User = nil
		tags["url"] = u.OriginalURL.String()
		if u.Address != "" {
			tags["address"] = u.Address
		}

		switch metric.Type() {
		case telegraf.Counter:
			acc.AddCounter(metric.Name(), metric.Fields(), tags, metric.Time())
		case telegraf.Gauge:
			acc.AddGauge(metric.Name(), metric.Fields(), tags, metric.Time())
		case telegraf.Summary:
			acc.AddSummary(metric.Name(), metric.Fields(), tags, metric.Time())
		case telegraf.Histogram:
			acc.AddHistogram(metric.Name(), metric.Fields(), tags, metric.Time())
		default:
			acc.AddFields(metric.Name(), metric.Fields(), tags, metric.Time())
		}
	}

	return nil
}

// getMesosClient returns an httpcli client configured with the available levels of
// TLS and IAM according to flags set in the config
func (p *Prometheus) getMesosClient() (*httpcli.Client, error) {
	if p.mesosClient != nil {
		return p.mesosClient, nil
	}

	uri := p.MesosAgentUrl + "/api/v1"
	client := httpcli.New(httpcli.Endpoint(uri), httpcli.DefaultHeader("User-Agent",
		dcosutil.GetUserAgent(p.UserAgent)))
	cfgOpts := []httpcli.ConfigOpt{}
	opts := []httpcli.Opt{}

	var rt http.RoundTripper
	var err error

	if p.CACertificatePath != "" {
		if rt, err = p.DCOSConfig.Transport(); err != nil {
			return nil, fmt.Errorf("error creating transport: %s", err)
		}
		if p.IAMConfigPath != "" {
			cfgOpts = append(cfgOpts, httpcli.RoundTripper(rt))
		}
	}
	opts = append(opts, httpcli.Do(httpcli.With(cfgOpts...)))
	client.With(opts...)

	p.mesosClient = client
	return client, nil
}

// getTasks requests tasks from the operator API
func (p *Prometheus) getTasks(ctx context.Context, cli calls.Sender) (*agent.Response_GetTasks, error) {
	resp, err := cli.Send(ctx, calls.NonStreaming(calls.GetTasks()))
	if err != nil {
		return nil, err
	}
	r, err := processResponse(resp, agent.Response_GET_TASKS)
	if err != nil {
		return nil, err
	}

	gs := r.GetGetTasks()
	if gs == nil {
		return nil, errors.New("the getTasks response from the mesos agent was empty")
	}
	return gs, nil
}

// processResponse reads the response from a triggered request, verifies its
// type, and returns an agent response
func processResponse(resp mesos.Response, t agent.Response_Type) (agent.Response, error) {
	var r agent.Response
	defer func() {
		if resp != nil {
			resp.Close()
		}
	}()
	for {
		if err := resp.Decode(&r); err != nil {
			if err == io.EOF {
				break
			}
			return r, err
		}
	}
	if r.GetType() == t {
		return r, nil
	} else {
		return r, fmt.Errorf("processResponse expected type %q, got %q", t, r.GetType())
	}
}

// getMesosTaskPrometheusURLs converts a list of tasks to a list of Prometheus
// URLs to scrape
func getMesosTaskPrometheusURLs(tasks *agent.Response_GetTasks) []*url.URL {
	results := []*url.URL{}
	return results
}
func init() {
	inputs.Add("prometheus", func() telegraf.Input {
		return &Prometheus{
			ResponseTimeout: internal.Duration{Duration: time.Second * 3},
			MesosTimeout:    internal.Duration{Duration: time.Second * 10},
		}
	})
}

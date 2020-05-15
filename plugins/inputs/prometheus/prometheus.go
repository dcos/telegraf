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
	"strconv"
	"strings"
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

	// Location of kubernetes config file
	KubeConfig string

	// The URL of the local mesos agent
	MesosAgentUrl string
	MesosTimeout  internal.Duration
	dcosutil.DCOSConfig

	// Bearer Token authorization file path
	BearerToken string `toml:"bearer_token"`

	ResponseTimeout internal.Duration `toml:"response_timeout"`

	tls.ClientConfig

	client *http.Client

	// Should we scrape Kubernetes services for prometheus annotations
	MonitorPods    bool `toml:"monitor_kubernetes_pods"`
	lock           sync.Mutex
	kubernetesPods map[string]URLAndAddress
	cancel         context.CancelFunc
	wg             sync.WaitGroup

	mesosClient   *httpcli.Client
	mesosHostname string
}

var sampleConfig = `
  ## An array of urls to scrape metrics from.
  urls = ["http://localhost:9100/metrics"]

  ## An array of Kubernetes services to scrape metrics from.
  # kubernetes_services = ["http://my-service-dns.my-namespace:9100/metrics"]

  ## Kubernetes config file to create client from.
  # kube_config = "/path/to/kubernetes.config"

  ## Scrape Kubernetes pods for the following prometheus annotations:
  ## - prometheus.io/scrape: Enable scraping for this pod
  ## - prometheus.io/scheme: If the metrics endpoint is secured then you will need to
  ##     set this to 'https' & most likely set the tls config.
  ## - prometheus.io/path: If the metrics path is not /metrics, define it with this annotation.
  ## - prometheus.io/port: If port is not 9102 use this annotation
  # monitor_kubernetes_pods = true

  ## The URL of the local mesos agent
  mesos_agent_url = "http://$NODE_PRIVATE_IP:5051"
  ## The period after which requests to mesos agent should time out
  mesos_timeout = "10s"

  ## The user agent to send with requests
  user_agent = "Telegraf-prometheus"
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
	Tags        map[string]string
}

func (p *Prometheus) GetAllURLs() (map[string]URLAndAddress, error) {
	allURLs := make(map[string]URLAndAddress, 0)
	for _, u := range p.URLs {
		URL, err := url.Parse(u)
		if err != nil {
			log.Printf("prometheus: Could not parse %s, skipping it. Error: %s", u, err.Error())
			continue
		}
		allURLs[URL.String()] = URLAndAddress{URL: URL, OriginalURL: URL}
	}

	p.lock.Lock()
	defer p.lock.Unlock()
	// loop through all pods scraped via the prometheus annotation on the pods
	for k, v := range p.kubernetesPods {
		allURLs[k] = v
	}

	// Kubernetes service discovery
	for _, service := range p.KubernetesServices {
		URL, err := url.Parse(service)
		if err != nil {
			return nil, err
		}

		resolvedAddresses, err := net.LookupHost(URL.Hostname())
		if err != nil {
			log.Printf("prometheus: Could not resolve %s, skipping it. Error: %s", URL.Host, err.Error())
			continue
		}
		for _, resolved := range resolvedAddresses {
			serviceURL := p.AddressToURL(URL, resolved)
			allURLs[serviceURL.String()] = URLAndAddress{
				URL:         serviceURL,
				Address:     resolved,
				OriginalURL: URL,
			}
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

		for _, url := range getMesosTaskPrometheusURLs(tasks, p.mesosHostname) {
			allURLs[url.URL.String()] = url
		}
	}

	return allURLs, nil
}

// Reads stats from all configured servers accumulates stats.
// Returns one of the errors encountered while gather stats (if any).
func (p *Prometheus) Gather(acc telegraf.Accumulator) error {
	if p.client == nil {
		client, err := p.createHTTPClient()
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

func (p *Prometheus) createHTTPClient() (*http.Client, error) {
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
	var req *http.Request
	var err error
	var uClient *http.Client
	if u.URL.Scheme == "unix" {
		path := u.URL.Query().Get("path")
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
					c, err := net.Dial("unix", u.URL.Path)
					return c, err
				},
			},
			Timeout: p.ResponseTimeout.Duration,
		}
	} else {
		if u.URL.Path == "" {
			u.URL.Path = "/metrics"
		}
		req, err = http.NewRequest("GET", u.URL.String(), nil)
	}

	req.Header.Add("Accept", acceptHeader)

	var token []byte
	if p.BearerToken != "" {
		token, err = ioutil.ReadFile(p.BearerToken)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+string(token))
	}

	var resp *http.Response
	if u.URL.Scheme != "unix" {
		resp, err = p.client.Do(req)
	} else {
		resp, err = uClient.Do(req)
	}
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

	for _, metric := range metrics {
		tags := metric.Tags()
		// strip user and password from URL
		u.OriginalURL.User = nil
		tags["url"] = u.OriginalURL.String()
		if u.Address != "" {
			tags["address"] = u.Address
		}
		for k, v := range u.Tags {
			tags[k] = v
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

// Start will start the Kubernetes scraping if enabled in the configuration
func (p *Prometheus) Start(a telegraf.Accumulator) error {
	// Check that the mesos agent url is well-formed
	if p.MesosAgentUrl != "" {
		// gofmt prevents us using := assignment below, hence declaration
		var err error
		p.mesosHostname, err = getMesosHostname(p.MesosAgentUrl)
		if err != nil {
			return fmt.Errorf("the mesos agent URL was malformed: %s", err)
		}
	}
	if p.MonitorPods {
		var ctx context.Context
		ctx, p.cancel = context.WithCancel(context.Background())
		return p.start(ctx)
	}
	return nil
}

func (p *Prometheus) Stop() {
	if p.MonitorPods {
		p.cancel()
	}
	p.wg.Wait()
}

// getMesosHostname extracts the node's hostname from the mesos agent url
func getMesosHostname(mesosAgentUrl string) (string, error) {
	u, err := url.Parse(mesosAgentUrl)
	if err != nil {
		return "", err
	}
	hostname := u.Host

	// SplitHostPort will error if passed an input with no port
	if strings.ContainsRune(hostname, ':') {
		hostname, _, err = net.SplitHostPort(hostname)
	}
	if hostname == "" {
		return hostname, fmt.Errorf("could not extract hostname from: %s", mesosAgentUrl)
	}
	return hostname, err
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
func getMesosTaskPrometheusURLs(tasks *agent.Response_GetTasks, hostname string) []URLAndAddress {
	results := []URLAndAddress{}
	for _, t := range tasks.GetLaunchedTasks() {
		for _, endpoint := range getEndpointsFromTaskPorts(&t, hostname) {
			uat, err := makeURLAndAddress(t, endpoint)
			if err != nil {
				log.Printf("E! %s", err)
				continue
			}
			results = append(results, uat)
		}
		if endpoint, ok := getEndpointFromTaskLabels(&t, hostname); ok {
			uat, err := makeURLAndAddress(t, endpoint)
			if err != nil {
				log.Printf("E! %s", err)
				continue
			}
			results = append(results, uat)
		}
	}
	return results
}

func makeURLAndAddress(task mesos.Task, endpoint string) (URLAndAddress, error) {
	URL, err := url.Parse(endpoint)
	cid, _ := getContainerIDs(task.GetStatuses())
	return URLAndAddress{
		URL:         URL,
		OriginalURL: URL,
		Tags:        map[string]string{"container_id": cid},
	}, err
}

// getEndpointsFromTaskPorts retrieves a map of ports end endpoints from which
// Prometheus metrics can be retrieved from a given task.
func getEndpointsFromTaskPorts(t *mesos.Task, nodeHostname string) []string {
	endpoints := []string{}

	// loop over the task's ports, adding them if they are appropriately labelled
	taskPorts := getPortsFromTask(t)

	for _, p := range taskPorts {
		portLabels := simplifyLabels(p.GetLabels())
		if portLabels["DCOS_METRICS_FORMAT"] == "prometheus" {
			hostname, err := getHostnameForPort(&p, t, nodeHostname)
			if err != nil {
				log.Printf("E! %s", err)
				continue
			}
			route := "/metrics"
			if ep := portLabels["DCOS_METRICS_ENDPOINT"]; ep != "" {
				route = ep
			}
			endpoints = append(endpoints, fmt.Sprintf("http://%s:%d%s", hostname, p.Number, route))
		}
	}
	return endpoints
}

// getEndpointFromTaskLabels cross-references the task's DCOS_METRICS_PORT_INDEX
// label, if present, with its ports to yield an endpoint.
func getEndpointFromTaskLabels(t *mesos.Task, nodeHostname string) (string, bool) {
	taskPorts := getPortsFromTask(t)
	taskLabels := simplifyLabels(t.GetLabels())

	if taskLabels["DCOS_METRICS_FORMAT"] != "prometheus" {
		return "", false
	}

	portIndex := taskLabels["DCOS_METRICS_PORT_INDEX"]
	portName := taskLabels["DCOS_METRICS_PORT_NAME"]
	// port number 0 means auto-assign, in theory it should not appear in
	// the task's port list
	var port mesos.Port

	if len(portIndex) == 0 && len(portName) == 0 {
		// no usable metrics endpoint
		return "", false
	}

	// specifying port via port index has priority of specifying via port name,
	// and port obtained via name must not override port obtained via index
	if len(portIndex) > 0 {
		index, err := strconv.Atoi(portIndex)
		if err != nil {
			// non-empty non-int port index is treated as an error, there is no
			// fallback into name-based association
			log.Printf("E! Could not retrieve port index for %s: %s", t.GetTaskID(), err)
			return "", false
		}
		if index < 0 || index >= len(taskPorts) {
			// same here - no fallback to name-based association
			log.Printf("E! Could not retrieve port index %d for task %s", index, t.GetTaskID())
			return "", false
		}
		port = taskPorts[index]
	} else {
		for _, taskPort := range taskPorts {
			if *taskPort.Name == portName {
				port = taskPort
			}
		}
		if port.Number == 0 {
			log.Printf("E! Could not match port name %s for task %s", portName, t.GetTaskID())
			return "", false
		}
	}

	hostname, err := getHostnameForPort(&port, t, nodeHostname)
	if err != nil {
		log.Printf("E! %s", err)
		return "", false
	}

	route := "/metrics"
	if ep := taskLabels["DCOS_METRICS_ENDPOINT"]; ep != "" {
		route = ep
	}
	return fmt.Sprintf("http://%s:%d%s", hostname, port.Number, route), true
}

// getPortsFromTask is a convenience method to retrieve a task's ports
func getPortsFromTask(t *mesos.Task) []mesos.Port {
	if d := t.GetDiscovery(); d != nil {
		if pp := d.GetPorts(); pp != nil {
			return pp.Ports
		}
	}
	return []mesos.Port{}
}

// getHostnameForPort inspects the port for its network-scope label. If present,
// and set to container, it returns the task's IP address. If absent, or set to
// host, it returns the node's hostname.
func getHostnameForPort(p *mesos.Port, t *mesos.Task, nodeHostname string) (string, error) {
	if !isHostPort(p, t) {
		taskIP, err := getTaskIP(t.GetStatuses())
		if err != nil {
			return nodeHostname, fmt.Errorf("could not retrieve IP address for %s: %s", t.GetTaskID(), err)
		}
		return taskIP, nil

	}

	return nodeHostname, nil
}

func isHostPort(p *mesos.Port, t *mesos.Task) bool {

	// Handle network scope label
	portLabels := simplifyLabels(p.GetLabels())
	if s, ok := portLabels["network-scope"]; ok {
		if s == "host" {
			return true
		}

		return false
	}

	// Handle task resources
	for _, r := range t.GetResources() {
		if n := r.GetName(); n == "ports" {
			for _, pr := range r.GetRanges().GetRange() {
				if pr.GetBegin() <= uint64(p.Number) && pr.GetEnd() >= uint64(p.Number) {
					return true
				}
			}

			if r.GetScalar().GetValue() == float64(p.Number) {
				return true
			}
		}
	}

	// Handle Pod portmappings
	for _, s := range t.GetStatuses() {
		for _, ni := range s.GetContainerStatus().GetNetworkInfos() {
			for _, pm := range ni.GetPortMappings() {
				if h := pm.GetHostPort(); h == p.Number {
					return true
				}
			}
		}
	}

	// Handle MESOS and Docker portmappings
	if c := t.GetContainer(); c != nil {
		for _, ni := range c.GetNetworkInfos() {
			for _, pm := range ni.GetPortMappings() {
				if h := pm.GetHostPort(); h == p.Number {
					return true
				}
			}
		}

		if d := c.GetDocker(); d != nil {
			for _, pm := range d.GetPortMappings() {
				if h := pm.GetHostPort(); h == p.Number {
					return true
				}
			}
		}
	}

	return false
}

// getContainerIDs retrieves the container ID and the parent container ID of a
// task from its TaskStatus. The container ID corresponds to the task's
// container, the parent container ID corresponds to the task's executor's
// container.
func getContainerIDs(statuses []mesos.TaskStatus) (containerID string, parentContainerID string) {
	// Container ID is held in task status
	for _, s := range statuses {
		if cs := s.GetContainerStatus(); cs != nil {
			if cid := cs.GetContainerID(); cid != nil {
				containerID = cid.GetValue()
				if pcid := cid.GetParent(); pcid != nil {
					parentContainerID = pcid.GetValue()
					return
				}
				return
			}
		}
	}
	return
}

// getTaskIP retrieves the IP Address assigned to a task, given its statuses
func getTaskIP(statuses []mesos.TaskStatus) (string, error) {
	// Statuses are in chronological order, the last is the latest
	if len(statuses) == 0 {
		return "", errors.New("task had no associated statuses")
	}
	status := statuses[len(statuses)-1]

	cs := status.GetContainerStatus()
	if cs != nil {
		// Any IP address will work, we pick the first
		ni := cs.GetNetworkInfos()
		if len(ni) > 0 {
			info := ni[0]
			ip := info.GetIPAddresses()
			if len(ip) > 0 {
				return ip[0].GetIPAddress(), nil
			}
		}
	}
	return "", errors.New("task had no associated IP address")
}

// simplifyLabels converts a Labels object to a hashmap
func simplifyLabels(ll *mesos.Labels) map[string]string {
	results := map[string]string{}
	if ll != nil {
		for _, l := range ll.Labels {
			results[l.GetKey()] = l.GetValue()
		}
	}
	return results
}

func init() {
	inputs.Add("prometheus", func() telegraf.Input {
		return &Prometheus{
			ResponseTimeout: internal.Duration{Duration: time.Second * 3},
			MesosTimeout:    internal.Duration{Duration: time.Second * 10},
			kubernetesPods:  map[string]URLAndAddress{},
		}
	})
}

package dcos_containers

const sampleConfig = `
## The URL of the local mesos agent
mesos_agent_url = "http://$NODE_PRIVATE_IP:5051"
## The period after which requests to mesos agent should time out
timeout = 10s
`

// DCOSContainers describes the options available to this plugin
type DCOSContainers struct {
	MesosAgentUrl string
	Timeout       time.Duration
}

// measurement is a combination of fields and tags specific to those fields
type measurement struct {
	name   string
	fields map[string]interface{}
	tags   map[string]string
}

// combineTags combines this measurement's tags with some other tags. In the
// event of a collision, this measurement's tags take priority.
func (m *measurement) combineTags(newTags map[string]string) map[string]string {
	results := make(map[string]string)
	for k, v := range newTags {
		results[k] = v
	}
	for k, v := range m.tags {
		results[k] = v
	}
	return results
}

// newMeasurement is a convenience method for instantiating new measurements
func newMeasurement(name string) measurement {
	return measurement{
		name:   name,
		fields: make(map[string]interface{}),
		tags:   make(map[string]string),
	}
}

// SampleConfig returns the default configuration
func (dc *DCOSContainers) SampleConfig() string {
	return sampleConfig
}

// Description returns a one-sentence description of dcos_containers
func (dc *DCOSContainers) Description() string {
	return "Plugin for monitoring mesos container resource consumption"
}

// Gather takes in an accumulator and adds the metrics that the plugin gathers.
// It is invoked on a schedule (default every 10s) by the telegraf runtime.
func (dc *DCOSContainers) Gather(acc telegraf.Accumulator) error {
	// TODO
	return nil
}

// getContainers requests a list of containers from the operator API
func (dc *DCOSContainers) getContainers(ctx context.Context, cli calls.Sender) (*agent.Response_GetContainers, error) {
	resp, err := cli.Send(ctx, calls.NonStreaming(calls.GetContainers()))
	if err != nil {
		return nil, err
	}
	r, err := processResponse(resp, agent.Response_GET_CONTAINERS)
	if err != nil {
		return nil, err
	}

	gc := r.GetGetContainers()
	if gc == nil {
		return gc, errors.New("the getContainers response from the mesos agent was empty")
	}

	return gc, nil
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

// setIfNotNil runs get() and adds its value to a map, if not nil
func setIfNotNil(target map[string]interface{}, key string, get interface{}) error {
	var val interface{}
	var zero interface{}

	switch get.(type) {
	case func() uint32:
		val = get.(func() uint32)()
		zero = uint32(0)
		break
	case func() uint64:
		val = get.(func() uint64)()
		zero = uint64(0)
		break
	case func() float64:
		val = get.(func() float64)()
		zero = float64(0)
		break
	default:
		return fmt.Errorf("get function for key %s was not of a recognized type", key)
	}
	// Zero is nil for numeric types
	if val != zero {
		target[key] = val
	}
	return nil
}

// init is called once when telegraf starts
func init() {
	inputs.Add("dcos_containers", func() telegraf.Input {
		return &DCOSContainers{
			Timeout: 10 * time.Second,
		}
	})
}

package nginx_vts_filter

import (
	"testing"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/stretchr/testify/assert"
)

func newMetric(name string, tags map[string]string) telegraf.Metric {
	if tags == nil {
		tags = map[string]string{}
	}
	fields := map[string]interface{}{}
	m, _ := metric.New(name, tags, fields, time.Now())
	return m
}

func TestVTSFilterConvertSuccess(t *testing.T) {
	n := NginxVTSFilter{
		Conversions: []Convert{
			{Measurement: "nginx_vts_filter_requests_total"},
		},
	}
	m := newMetric("nginx_vts_filter_requests_total", map[string]string{"filter": "upstream=Bouncer", "filter_name": "uri=/acs/api/v1/auth/login"})
	r := n.Apply(m)
	upstream, ok := r[0].GetTag("upstream")
	assert.True(t, ok)
	assert.Equal(t, upstream, "Bouncer")
	assert.False(t, r[0].HasTag("filter"))
	uri, ok := r[0].GetTag("uri")
	assert.True(t, ok)
	assert.Equal(t, uri, "/acs/api/v1/auth/login")
	assert.False(t, r[0].HasTag("filter_name"))
	assert.Equal(t, r[0].Name(), "nginx_upstream_uri_requests_total")
}

func TestVTSFilterConvertMultipleSuccess(t *testing.T) {
	n := NginxVTSFilter{
		Conversions: []Convert{
			{Measurement: "nginx_vts_filter_requests_total"},
		},
	}
	m := newMetric("nginx_vts_filter_requests_total", map[string]string{"filter": "upstream=Bouncer,backend=,status=401", "filter_name": "uri=/acs/api/v1/auth/login"})
	r := n.Apply(m)
	upstream, ok := r[0].GetTag("upstream")
	assert.True(t, ok)
	assert.Equal(t, upstream, "Bouncer")
	backend, ok := r[0].GetTag("backend")
	assert.True(t, ok)
	assert.Equal(t, backend, "")
	status, ok := r[0].GetTag("status")
	assert.True(t, ok)
	assert.Equal(t, status, "401")
	assert.False(t, r[0].HasTag("filter"))
	uri, ok := m.GetTag("uri")
	assert.True(t, ok)
	assert.Equal(t, uri, "/acs/api/v1/auth/login")
	assert.False(t, r[0].HasTag("filter_name"))
	assert.Equal(t, r[0].Name(), "nginx_upstream_uri_requests_total")
}

func TestVTSFilterConvertMultipleSuccessCustomDelimiter(t *testing.T) {
	n := NginxVTSFilter{
		Conversions: []Convert{
			{
				Measurement:       "nginx_vts_filter_requests_total",
				KeyValueDelimiter: ":=",
				TagDelimiter:      "::",
			},
		},
	}
	m := newMetric("nginx_vts_filter_requests_total", map[string]string{"filter": "upstream:=Bouncer::backend:=::status:=401", "filter_name": "uri:=/acs/api/v1/auth/login"})
	r := n.Apply(m)
	upstream, ok := r[0].GetTag("upstream")
	assert.True(t, ok)
	assert.Equal(t, upstream, "Bouncer")
	backend, ok := r[0].GetTag("backend")
	assert.True(t, ok)
	assert.Equal(t, backend, "")
	status, ok := r[0].GetTag("status")
	assert.True(t, ok)
	assert.Equal(t, status, "401")
	assert.False(t, r[0].HasTag("filter"))
	uri, ok := m.GetTag("uri")
	assert.True(t, ok)
	assert.Equal(t, uri, "/acs/api/v1/auth/login")
	assert.False(t, r[0].HasTag("filter_name"))
	assert.Equal(t, r[0].Name(), "nginx_upstream_uri_requests_total")
}

func TestVTSFilterMissing(t *testing.T) {
	n := NginxVTSFilter{
		Conversions: []Convert{
			{Measurement: "nginx_vts_filter_requests_total"},
		},
	}
	m := newMetric("nginx_vts_filter_requests_total", map[string]string{"filter_name": "uri=/acs/api/v1/auth/login"})
	r := n.Apply(m)
	assert.True(t, r[0].HasTag("filter_name"))
	assert.Equal(t, r[0].Name(), "nginx_vts_filter_requests_total")
}

func TestVTSFilterNameMissing(t *testing.T) {
	n := NginxVTSFilter{
		Conversions: []Convert{
			{Measurement: "nginx_vts_filter_requests_total"},
		},
	}
	m := newMetric("nginx_vts_filter_requests_total", map[string]string{"filter": "upstream=Bouncer"})
	r := n.Apply(m)
	upstream, ok := r[0].GetTag("upstream")
	assert.True(t, ok)
	assert.Equal(t, upstream, "Bouncer")
	assert.False(t, r[0].HasTag("filter"))
	assert.Equal(t, r[0].Name(), "nginx_vts_filter_requests_total")
}

func TestVTSFilterInvalidKeyValuePair(t *testing.T) {
	n := NginxVTSFilter{
		Conversions: []Convert{
			{Measurement: "nginx_vts_filter_requests_total"},
		},
	}
	m := newMetric("nginx_vts_filter_requests_total", map[string]string{"filter": "upstream=Bouncer,status"})
	r := n.Apply(m)
	assert.True(t, r[0].HasTag("filter"))
	assert.Equal(t, r[0].Name(), "nginx_vts_filter_requests_total")
}

func TestVTSFilterEmpty(t *testing.T) {
	n := NginxVTSFilter{
		Conversions: []Convert{
			{Measurement: "nginx_vts_filter_requests_total"},
		},
	}
	m := newMetric("nginx_vts_filter_requests_total", map[string]string{"filter": ""})
	r := n.Apply(m)
	assert.True(t, r[0].HasTag("filter"))
	assert.Equal(t, r[0].Name(), "nginx_vts_filter_requests_total")
}

func TestVTSFilterNameInvalidKeyValuePair(t *testing.T) {
	n := NginxVTSFilter{
		Conversions: []Convert{
			{Measurement: "nginx_vts_filter_requests_total"},
		},
	}
	m := newMetric("nginx_vts_filter_requests_total", map[string]string{"filter": "upstream=Bouncer", "filter_name": "client"})
	r := n.Apply(m)
	upstream, ok := r[0].GetTag("upstream")
	assert.True(t, ok)
	assert.Equal(t, upstream, "Bouncer")
	assert.False(t, r[0].HasTag("filter"))
	assert.True(t, r[0].HasTag("filter_name"))
	assert.Equal(t, r[0].Name(), "nginx_vts_filter_requests_total")
}

func TestVTSFilterNameEmpty(t *testing.T) {
	n := NginxVTSFilter{
		Conversions: []Convert{
			{Measurement: "nginx_vts_filter_requests_total"},
		},
	}
	m := newMetric("nginx_vts_filter_requests_total", map[string]string{"filter": "upstream=Bouncer", "filter_name": ""})
	r := n.Apply(m)
	upstream, ok := r[0].GetTag("upstream")
	assert.True(t, ok)
	assert.Equal(t, upstream, "Bouncer")
	assert.False(t, r[0].HasTag("filter"))
	assert.True(t, r[0].HasTag("filter_name"))
	assert.Equal(t, r[0].Name(), "nginx_vts_filter_requests_total")
}

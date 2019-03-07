package nginx_vts_filter

import (
	"fmt"
	"log"
	"strings"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/processors"
	"github.com/pkg/errors"
)

const sampleConfig = `
[[processors.nginx_vts_filter]]

  [[processors.nginx_vts_filter.convert]]
    measurement = "nginx_vts_filter_requests_total"
    tags_delimiter = ","
    key_value_delimiter = "="

  [[processors.nginx_vts_filter.convert]]
    measurement = "nginx_vts_filter_bytes_total"
    tags_delimiter = ","
    key_value_delimiter = "="

  [[processors.nginx_vts_filter.convert]]
    measurement = "nginx_vts_filter_request_seconds"
    tags_delimiter = ","
    key_value_delimiter = "="
`

type Convert struct {
	Measurement       string `toml:"measurement"`
	TagDelimiter      string `toml:"tag_delimiter"`
	KeyValueDelimiter string `toml:"key_value_delimiter"`
}

type NginxVTSFilter struct {
	Conversions []Convert `toml:"convert"`
}

func (n *NginxVTSFilter) SampleConfig() string {
	return sampleConfig
}

func (n *NginxVTSFilter) Description() string {
	return "NginxVTSFilter measurements that pass through this filter."
}

func (n *NginxVTSFilter) Apply(in ...telegraf.Metric) []telegraf.Metric {
	for _, point := range in {
		for _, convert := range n.Conversions {
			// Step 0: Match metric by convert.Measurement config setting
			name := point.Name()
			if convert.Measurement == "" || name != convert.Measurement {
				continue
			}
			kvDelimiter := "="
			if convert.KeyValueDelimiter != "" {
				kvDelimiter = convert.KeyValueDelimiter
			}
			tagDelimiter := ","
			if convert.TagDelimiter != "" {
				tagDelimiter = convert.TagDelimiter
			}
			//
			// Step 1: Convert filter tag value into individual tags.
			// ------------------------------------------------------------
			// nginx_vts_filter_requests_total{
			//     filter = ",upstream=Bouncer,backend=,status=401,",
			//     filter_name = "client=Mesos/1.8.0 authorizer (master)",
			// }
			// ------------------------------------------------------------
			// nginx_vts_filter_requests_total{
			//     upstream = "Bouncer",
			//     backend = "",
			//     status = "401",
			//     filter_name = "client=Mesos/1.8.0 authorizer (master)",
			// }
			filter, ok := point.GetTag("filter")
			if !ok {
				log.Printf("E! [processors.nginx_vts_filter] %s has no tag filter", name)
				continue
			}
			filterTags, err := unwrapTags(filter, kvDelimiter, tagDelimiter)
			if err != nil {
				log.Printf("E! [processors.nginx_vts_filter] could not unwrap filter tags %s: %v", filter, err)
				continue
			}
			for _, t := range filterTags {
				point.AddTag(t.Key, t.Value)
			}
			point.RemoveTag("filter")
			//
			// Step 2: Convert filter_name tag value into new tag.
			// ------------------------------------------------------------
			// nginx_vts_filter_requests_total{
			//     upstream = "Bouncer",
			//     backend = "",
			//     status = "401",
			//     filter_name = "client=Mesos/1.8.0 authorizer (master)",
			// }
			// ------------------------------------------------------------
			// nginx_vts_filter_requests_total{
			//     upstream = "Bouncer",
			//     backend = "",
			//     status = "401",
			//     client = "Mesos/1.8.0 authorizer (master)",
			// }
			filterName, ok := point.GetTag("filter_name")
			if !ok {
				log.Printf("E! [processors.nginx_vts_filter] %s has no tag filter_name", name)
				continue
			}
			filterNameTags, err := unwrapTags(filterName, kvDelimiter, tagDelimiter)
			if err != nil {
				log.Printf("E! [processors.nginx_vts_filter] could not unwrap filter_name tags %s: %v", filterName, err)
				continue
			}
			for _, t := range filterNameTags {
				point.AddTag(t.Key, t.Value)
			}
			point.RemoveTag("filter_name")
			//
			// Step 3: Convert metric name via filter/filter_name tag keys.
			// ------------------------------------------------------------
			// nginx_vts_filter_requests_total{
			//     upstream = "Bouncer",
			//     backend = "",
			//     status = "401",
			//     client = "Mesos/1.8.0 authorizer (master)",
			// }
			// ------------------------------------------------------------
			// nginx_upstream_client_requests_total{
			//     upstream = "Bouncer",
			//     backend = "",
			//     status = "401",
			//     client = "Mesos/1.8.0 authorizer (master)",
			// }
			newName, err := vtsFilterName(name, filterTags, filterNameTags)
			if err != nil {
				log.Printf("E! [processors.nginx_vts_filter] could not rename %s: %v", name, err)
				continue
			}
			point.SetName(newName)
		}
	}
	return in
}

func init() {
	processors.Add("nginx_vts_filter", func() telegraf.Processor {
		return &NginxVTSFilter{}
	})
}

func kvStringToTag(kv, delimiter string) (telegraf.Tag, error) {
	tuple := strings.Split(kv, delimiter)
	if len(tuple) != 2 {
		return telegraf.Tag{}, fmt.Errorf("not a key-value pair: %s", kv)
	}
	return telegraf.Tag{
		Key:   tuple[0],
		Value: tuple[1],
	}, nil
}

func unwrapTags(kvPairs, kvDelimiter, tagDelimiter string) ([]telegraf.Tag, error) {
	// Valid filter value format: "a=x,b=y,c=z"
	kvs := strings.Split(kvPairs, tagDelimiter)
	tags := make([]telegraf.Tag, 0)
	for _, s := range kvs {
		t, err := kvStringToTag(s, kvDelimiter)
		if err != nil {
			return nil, errors.Wrap(err, "could not make tag")
		}
		tags = append(tags, t)
	}
	return tags, nil
}

func vtsFilterName(name string, filterTags, filterNameTags []telegraf.Tag) (string, error) {
	if len(filterTags) < 1 {
		return "", fmt.Errorf("unwrapped tag from filter required")
	}
	if len(filterNameTags) < 1 {
		return "", fmt.Errorf("unwrapped tag from filter_name required")
	}
	s := strings.Split(name, "_")
	if len(s) < 3 {
		return "", fmt.Errorf("require at least 3 syllabus measurement")
	}
	s[1] = filterTags[0].Key
	s[2] = filterNameTags[0].Key
	return strings.Join(s, "_"), nil
}

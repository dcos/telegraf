package lowercase

import (
	"strings"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/processors"
)

type Lowercase struct {
	SendOriginal bool `toml:"send_original"`
}

const capitals = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

var sampleConfig = `
  ## Sends both Some_Metric and some_metric if true. 
  ## If false, sends only some_metric.
  # send_original = false
`

func (l *Lowercase) SampleConfig() string {
	return sampleConfig
}

func (l *Lowercase) Description() string {
	return "Coerce all metrics that pass through this filter to lowercase."
}

func (l *Lowercase) Apply(in ...telegraf.Metric) []telegraf.Metric {
	out := []telegraf.Metric{}
	for _, metric := range in {
		if l.SendOriginal {
			out = append(out, metric.Copy())
		}
		for key, value := range metric.Fields() {
			if strings.ContainsAny(key, capitals) {
				metric.RemoveField(key)
				metric.AddField(strings.ToLower(key), value)
			}
		}
		out = append(out, metric)
	}
	return out
}

func init() {
	processors.Add("lowercase", func() telegraf.Processor {
		return &Lowercase{}
	})
}

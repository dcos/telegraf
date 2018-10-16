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
	out := make([]telegraf.Metric, 0, len(in))

	for _, metric := range in {
		// Optimisation: only test for uppercase metrics if we wish to
		// preserve the original metric.
		if l.SendOriginal && isUpper(metric) {
			out = append(out, metric.Copy())
		}

		out = append(out, toLower(metric))
	}

	return out
}

func isUpper(metric telegraf.Metric) bool {
	if strings.ContainsAny(metric.Name(), capitals) {
		return true
	}
	for key, _ := range metric.Fields() {
		if strings.ContainsAny(key, capitals) {
			return true
		}
	}
	return false
}

func toLower(metric telegraf.Metric) telegraf.Metric {
	metric.SetName(strings.ToLower(metric.Name()))
	for key, value := range metric.Fields() {
		// The metric interface does not expose fields; we
		// therefore remove and re-add the affected key.
		metric.RemoveField(key)
		metric.AddField(strings.ToLower(key), value)
	}
	return metric
}

func init() {
	processors.Add("lowercase", func() telegraf.Processor {
		return &Lowercase{}
	})
}

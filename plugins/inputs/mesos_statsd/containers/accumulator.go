package containers

import (
	"time"

	"github.com/influxdata/telegraf"
)

// Accumulator is an implementation of telegraf.Accumulator. It passes all
// calls through to its inner accumulator, but adds a container_id tag to any
// metric on the way through.
type Accumulator struct {
	Accumulator *telegraf.Accumulator
	CId         string
}

// AddFields adds a metric to the accumulator with the given measurement
func (a *Accumulator) AddFields(measurement string,
	fields map[string]interface{},
	tags map[string]string,
	t ...time.Time) {
	(*a.Accumulator).AddFields(measurement, fields, a.ctags(tags), t...)
}

// AddGauge is the same as AddFields, but will add the metric as a "Gauge" type
func (a *Accumulator) AddGauge(measurement string,
	fields map[string]interface{},
	tags map[string]string,
	t ...time.Time) {
	(*a.Accumulator).AddGauge(measurement, fields, a.ctags(tags), t...)
}

// AddCounter is the same as AddFields, but will add the metric as a "Counter" type
func (a *Accumulator) AddCounter(measurement string,
	fields map[string]interface{},
	tags map[string]string,
	t ...time.Time) {
	(*a.Accumulator).AddCounter(measurement, fields, a.ctags(tags), t...)
}

// AddSummary is the same as AddFields, but will add the metric as a "Summary" type
func (a *Accumulator) AddSummary(measurement string,
	fields map[string]interface{},
	tags map[string]string,
	t ...time.Time) {
	(*a.Accumulator).AddSummary(measurement, fields, a.ctags(tags), t...)
}

// AddHistogram is the same as AddFields, but will add the metric as a "Histogram" type
func (a *Accumulator) AddHistogram(measurement string,
	fields map[string]interface{},
	tags map[string]string,
	t ...time.Time) {
	(*a.Accumulator).AddHistogram(measurement, fields, a.ctags(tags), t...)
}

func (a *Accumulator) AddMetric(m telegraf.Metric) {
	metric := m.Copy()
	metric.AddTag("container_id", a.CId)
	(*a.Accumulator).AddMetric(metric)
}

func (a *Accumulator) SetPrecision(precision time.Duration) {
	(*a.Accumulator).SetPrecision(precision)
}

func (a *Accumulator) AddError(err error) {
	(*a.Accumulator).AddError(err)
}

func (a *Accumulator) WithTracking(maxTracking int) telegraf.TrackingAccumulator {
	return (*a.Accumulator).WithTracking(maxTracking)
}

// ctags updates an array of tags with the container_id
func (a *Accumulator) ctags(tags map[string]string) map[string]string {
	result := map[string]string{"container_id": a.CId}
	for k, v := range tags {
		result[k] = v
	}
	return result
}

package lowercase

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/stretchr/testify/assert"
)

var (
	fields = map[string]map[string]interface{}{
		"ChAnGeD": map[string]interface{}{
			"lower_case": "abc123",
			"UPPER_CASE": "ABC123",
			"Mixed_Case": "Abc123",
		},
		"unchanged": map[string]interface{}{
			"lower_case": "abc123",
		},
	}
)

// By default, we don't send original metrics, only lowercased metrics
func TestApply_Defaults(t *testing.T) {
	inputs := make([]telegraf.Metric, 2)
	inputs[0], _ = metric.New("ChAnGeD", map[string]string{}, fields["ChAnGeD"], time.Now())
	inputs[1], _ = metric.New("unchanged", map[string]string{}, fields["unchanged"], time.Now())

	lc := Lowercase{}
	output := lc.Apply(inputs...)
	assert.Equal(t, 2, len(output))

	assert.Equal(t, "changed", output[0].Name())
	assert.Equal(t, map[string]interface{}{
		"lower_case": "abc123",
		"upper_case": "ABC123",
		"mixed_case": "Abc123",
	}, output[0].Fields())

	assert.Equal(t, "unchanged", output[1].Name())
	assert.Equal(t, map[string]interface{}{
		"lower_case": "abc123",
	}, output[1].Fields())
}

// With SendOriginals enabled, we send original metrics and also lowercased metrics
func TestApply_SendOriginals(t *testing.T) {
	inputs := make([]telegraf.Metric, 2)
	inputs[0], _ = metric.New("ChAnGeD", map[string]string{}, fields["ChAnGeD"], time.Now())
	inputs[1], _ = metric.New("unchanged", map[string]string{}, fields["unchanged"], time.Now())

	lc := Lowercase{SendOriginal: true}
	output := lc.Apply(inputs...)
	assert.Equal(t, 3, len(output))

	assert.Equal(t, "ChAnGeD", output[0].Name())
	assert.Equal(t, map[string]interface{}{
		"lower_case": "abc123",
		"UPPER_CASE": "ABC123",
		"Mixed_Case": "Abc123",
	}, output[0].Fields())

	assert.Equal(t, "changed", output[1].Name())
	assert.Equal(t, map[string]interface{}{
		"lower_case": "abc123",
		"upper_case": "ABC123",
		"mixed_case": "Abc123",
	}, output[1].Fields())

	assert.Equal(t, "unchanged", output[2].Name())
	assert.Equal(t, map[string]interface{}{
		"lower_case": "abc123",
	}, output[2].Fields())
}

// The following two tests demonstrate that using strings.ContainsAny is ~6
// times faster than a compiled regexp MatchString.

func BenchmarkRegexpMatch(b *testing.B) {
	input := "hello, World"
	uppers := regexp.MustCompile("[A-Z]")
	for i := 0; i < b.N; i++ {
		uppers.MatchString(input)
	}
}

func BenchmarkStringsMatch(b *testing.B) {
	input := "hello, World"
	uppers := "ABCDEFGHIJKLNMNOPQRSTUVWXYZ"
	for i := 0; i < b.N; i++ {
		strings.ContainsAny(input, uppers)
	}
}

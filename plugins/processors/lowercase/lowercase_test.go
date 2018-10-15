package lowercase

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/telegraf/metric"
	"github.com/stretchr/testify/assert"
)

// By default, we don't send original metrics, only lowercased metrics
func TestApply_Defaults(t *testing.T) {
	input, err := metric.New(
		"tEsT",
		map[string]string{},
		map[string]interface{}{
			"lower_case": "abc123",
			"UPPER_CASE": "ABC123",
			"Mixed_Case": "Abc123",
		},
		time.Now(),
	)
	assert.Nil(t, err)

	lc := Lowercase{}
	output := lc.Apply(input)
	assert.Equal(t, 1, len(output))
	assert.Equal(t, "test", output[0].Name())
	assert.Equal(t, map[string]interface{}{
		"lower_case": "abc123",
		"upper_case": "ABC123",
		"mixed_case": "Abc123",
	}, output[0].Fields())
}

// With SendOriginals enabled, we send original metrics, and also lowercased metrics
func TestApply_SendOriginals(t *testing.T) {
	input, err := metric.New(
		"tEsT",
		map[string]string{},
		map[string]interface{}{
			"lower_case": "abc123",
			"UPPER_CASE": "ABC123",
			"Mixed_Case": "Abc123",
		},
		time.Now(),
	)
	assert.Nil(t, err)

	lc := Lowercase{SendOriginal: true}
	output := lc.Apply(input)
	assert.Equal(t, 2, len(output))
	assert.Equal(t, "tEsT", output[0].Name())
	assert.Equal(t, "test", output[1].Name())
	assert.Equal(t, map[string]interface{}{
		"lower_case": "abc123",
		"UPPER_CASE": "ABC123",
		"Mixed_Case": "Abc123",
	}, output[0].Fields())
	assert.Equal(t, map[string]interface{}{
		"lower_case": "abc123",
		"upper_case": "ABC123",
		"mixed_case": "Abc123",
	}, output[1].Fields())
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

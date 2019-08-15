package containers

import (
	"github.com/influxdata/telegraf/plugins/inputs/statsd"
)

type Container struct {
	Id         string `json:"container_id"`
	StatsdHost string `json:"statsd_host,omitempty"`
	StatsdPort int    `json:"statsd_port,omitempty"`
	// Server is a telegraf statsd input plugin instance
	Server *statsd.Statsd `json:"-"`
}

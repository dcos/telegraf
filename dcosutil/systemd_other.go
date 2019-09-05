// +build !linux

package dcosutil

import (
	"errors"
	"net"
)

// ListenersWithNames returns an error stating that it is not supported.
func ListenersWithNames() (map[string][]net.Listener, error) {
	return nil, errors.New("systemd socket activation is not supported on this operating system")
}

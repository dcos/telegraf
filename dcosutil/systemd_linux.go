package dcosutil

import (
	"net"

	"github.com/coreos/go-systemd/activation"
)

var cachedListeners map[string][]net.Listener

// ListenersWithNames wraps activation.ListenersWithNames. It caches the return value after the first call, and returns
// the cached value in subsequent calls. activation.ListenersWithNames will return nil after the first call, so it can't be used by multiple plugins without coordination.
func ListenersWithNames() (map[string][]net.Listener, error) {
	if cachedListeners != nil {
		return cachedListeners, nil
	}

	listeners, err := activation.ListenersWithNames()
	if err != nil {
		return nil, err
	}

	cachedListeners = listeners
	return cachedListeners, nil
}

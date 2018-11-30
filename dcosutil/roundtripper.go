package dcosutil

import "net/http"

type roundTripper struct {
	r         http.RoundTripper
	userAgent string
}

func NewRoundTripper(rt http.RoundTripper, userAgent string) *roundTripper {
	return &roundTripper{
		r:         rt,
		userAgent: userAgent,
	}
}

// RoundTrip is an implementation of the RoundTripper interface.
func (rt roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", rt.userAgent)
	return rt.r.RoundTrip(req)
}

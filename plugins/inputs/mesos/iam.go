package mesos

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/dcos/dcos-go/dcos/http/transport"
)

type DCOSConfig struct {
	CACertificatePath string `toml:"ca_certificate_path"`
	IAMConfigPath     string `toml:"iam_config_path"`
	UserAgent         string `toml:"user_agent"`
}

// transport returns a transport implementing http.RoundTripper
func (c *DCOSConfig) transport() (http.RoundTripper, error) {
	tr, err := getTransport(c.CACertificatePath)
	if err != nil {
		return nil, err
	}

	if c.IAMConfigPath != "" {
		var rt http.RoundTripper
		var err error
		if c.UserAgent != "" {
			rt, err = transport.NewRoundTripper(
				tr,
				transport.OptionReadIAMConfig(c.IAMConfigPath),
				transport.OptionUserAgent(c.UserAgent),
			)
		} else {
			rt, err = transport.NewRoundTripper(
				tr,
				transport.OptionReadIAMConfig(c.IAMConfigPath),
			)
		}
		if err != nil {
			return nil, err
		}
		return rt, nil
	}

	return tr, nil
}

// loadCAPool will load a valid x509 cert.
func loadCAPool(path string) (*x509.CertPool, error) {
	caPool := x509.NewCertPool()
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	if !caPool.AppendCertsFromPEM(b) {
		return nil, errors.New("CACertFile parsing failed")
	}

	return caPool, nil
}

// getTransport will return transport for http.Client
func getTransport(caCertificatePath string) (*http.Transport, error) {
	log.Printf("I! Loading CA cert: %s", caCertificatePath)
	caPool, err := loadCAPool(caCertificatePath)
	if err != nil {
		return nil, err
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs: caPool,
		},
	}
	return tr, nil
}

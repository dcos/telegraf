package dcosutil

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/influxdata/telegraf/internal"

	"github.com/dcos/dcos-go/dcos/http/transport"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
)

type DCOSConfig struct {
	CACertificatePath string `toml:"ca_certificate_path"`
	IAMConfigPath     string `toml:"iam_config_path"`
	UserAgent         string `toml:"user_agent"`
}

const defaultUserAgent = "Telegraf"

// MesosClient returns a *httpcli.Client with TLS and IAM configured according to config.
func MesosClient(mesosUrl string, config DCOSConfig) (*httpcli.Client, error) {
	uri := mesosUrl + "/api/v1"
	client := httpcli.New(httpcli.Endpoint(uri), httpcli.DefaultHeader("User-Agent", GetUserAgent(config.UserAgent)))
	cfgOpts := []httpcli.ConfigOpt{}
	opts := []httpcli.Opt{}

	var rt http.RoundTripper
	var err error

	if config.CACertificatePath != "" {
		if rt, err = config.Transport(); err != nil {
			return nil, fmt.Errorf("error creating transport: %s", err)
		}
		if config.IAMConfigPath != "" {
			cfgOpts = append(cfgOpts, httpcli.RoundTripper(rt))
		}
	}

	opts = append(opts, httpcli.Do(httpcli.With(cfgOpts...)))
	client.With(opts...)

	return client, nil
}

func GetUserAgent(override string) string {
	userAgent := defaultUserAgent
	if override != "" {
		userAgent = override
	}
	return fmt.Sprintf("%s/%s", userAgent, internal.Version())
}

// Transport returns a transport implementing http.RoundTripper
func (c *DCOSConfig) Transport() (http.RoundTripper, error) {
	tr, err := getTransport(c.CACertificatePath)
	if err != nil {
		return nil, err
	}

	if c.IAMConfigPath != "" {
		rt, err := transport.NewRoundTripper(
			tr,
			transport.OptionReadIAMConfig(c.IAMConfigPath),
			transport.OptionUserAgent(GetUserAgent(c.UserAgent)),
		)
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

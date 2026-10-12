/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package proxy

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
)

// UpstreamTLS holds the proxy's credentials for connecting to model pods
// fronted by the TLS sidecar (spec §5). The client certificate is re-read on
// each new connection, so cert-manager renewal needs no restart; the CA
// bundle is read when a pod's transport is created, which follows pod churn.
type UpstreamTLS struct {
	CertFile string
	KeyFile  string
	CAFile   string
}

// transport returns an HTTP transport that presents the proxy's client
// certificate and verifies the pod's certificate against the CA, requiring
// serverName. A nil receiver means no credentials are configured, which is
// reported as an error so an https upstream is never reached insecurely.
func (u *UpstreamTLS) transport(serverName string) (*http.Transport, error) {
	if u == nil {
		return nil, errors.New("https upstream but no upstream TLS credentials are configured")
	}
	if serverName == "" {
		return nil, errors.New("https upstream has no TLS server name to verify")
	}
	pem, err := os.ReadFile(u.CAFile) // #nosec G304 -- operator-supplied path
	if err != nil {
		return nil, fmt.Errorf("reading upstream CA bundle: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("upstream CA bundle %s contains no valid certificates", u.CAFile)
	}
	return &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    pool,
			ServerName: serverName,
			GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
				pair, err := tls.LoadX509KeyPair(u.CertFile, u.KeyFile)
				if err != nil {
					return nil, fmt.Errorf("loading proxy client certificate: %w", err)
				}
				return &pair, nil
			},
		},
		ForceAttemptHTTP2: false,
	}, nil
}

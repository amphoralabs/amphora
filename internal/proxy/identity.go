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
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
)

// ServerTLSConfig builds the proxy's listener TLS configuration (spec §5
// Spoofing). The serving certificate and key are re-read whenever they change
// on disk, so cert-manager rotation needs no restart. If clientCAFile is set,
// clients may present a certificate that must chain to it; whether one is
// *required* is decided per request by the handler (Config.RequireClientIdentity),
// not at the handshake, so /healthz and /metrics stay reachable by probes and
// scrapers that have no client certificate. The client CA bundle is read once;
// rotating the CA requires a restart. ctx bounds the certificate watcher.
func ServerTLSConfig(ctx context.Context, certFile, keyFile, clientCAFile string) (*tls.Config, error) {
	if certFile == "" || keyFile == "" {
		return nil, errors.New("both a certificate and a key file are required for TLS")
	}
	watcher, err := certwatcher.New(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("loading serving certificate: %w", err)
	}
	go func() { _ = watcher.Start(ctx) }()

	cfg := &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: watcher.GetCertificate,
	}
	if clientCAFile != "" {
		pem, err := os.ReadFile(clientCAFile) // #nosec G304 -- operator-supplied path
		if err != nil {
			return nil, fmt.Errorf("reading client CA bundle: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("client CA bundle %s contains no valid certificates", clientCAFile)
		}
		cfg.ClientCAs = pool
		// A presented certificate must verify; absence is handled by the handler.
		cfg.ClientAuth = tls.VerifyClientCertIfGiven
	}
	return cfg, nil
}

// clientIdentity returns the tenant identity of the verified client
// certificate (its subject common name). It reports false when the request
// has no verified client certificate or the name is empty. Only
// VerifiedChains is consulted: a certificate that was sent but not verified
// is never trusted.
func clientIdentity(r *http.Request) (string, bool) {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
		return "", false
	}
	cn := strings.TrimSpace(r.TLS.VerifiedChains[0][0].Subject.CommonName)
	return cn, cn != ""
}

// modelOwner returns the tenant that owns a registry model key. Keys are
// "<namespace>/<name>" and a deployment's tenant is its namespace.
func modelOwner(model string) string {
	owner, _, ok := strings.Cut(model, "/")
	if !ok {
		return ""
	}
	return owner
}

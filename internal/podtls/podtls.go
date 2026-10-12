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

// Package podtls is the TLS-terminating sidecar that runs beside every model
// container (spec §5 Spoofing). The model image is the user's and its
// container args cannot change after a pause pod is hijacked, so encryption
// and caller authentication live in this separate container instead: it
// accepts only clients that present a certificate signed by the configured CA
// whose common name is on an allow-list, then forwards to the model server on
// the pod's loopback interface.
package podtls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
)

// ServerTLSConfig returns a TLS config that presents certFile/keyFile
// (re-read whenever they change, so cert-manager renewal needs no restart)
// and REQUIRES a client certificate verified against clientCAFile. Unlike the
// proxy, there is no unauthenticated path here: the sidecar fronts nothing
// but the model server.
func ServerTLSConfig(ctx context.Context, certFile, keyFile, clientCAFile string) (*tls.Config, error) {
	if certFile == "" || keyFile == "" || clientCAFile == "" {
		return nil, errors.New("a serving certificate, key and client CA bundle are all required")
	}
	watcher, err := certwatcher.New(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("loading serving certificate: %w", err)
	}
	go func() { _ = watcher.Start(ctx) }()

	pem, err := os.ReadFile(clientCAFile) // #nosec G304 -- operator-supplied path
	if err != nil {
		return nil, fmt.Errorf("reading client CA bundle: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("client CA bundle %s contains no valid certificates", clientCAFile)
	}
	return &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: watcher.GetCertificate,
		ClientCAs:      pool,
		ClientAuth:     tls.RequireAndVerifyClientCert,
	}, nil
}

// ParseAllowedClients splits a comma-separated allow-list and rejects an empty
// one: with no allowed identity the sidecar would deny everything, which is
// always a misconfiguration worth failing loudly on.
func ParseAllowedClients(csv string) ([]string, error) {
	var out []string
	for _, s := range strings.Split(csv, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("at least one allowed client common name is required")
	}
	return out, nil
}

// NewHandler forwards requests from allowed clients to upstream (the model
// server on loopback). The client identity is the verified certificate's
// common name; only VerifiedChains is consulted, never a merely presented
// certificate.
func NewHandler(upstream *url.URL, allowed []string, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	allow := make(map[string]struct{}, len(allowed))
	for _, a := range allowed {
		allow[a] = struct{}{}
	}
	rp := httputil.NewSingleHostReverseProxy(upstream)
	rp.FlushInterval = -1 // stream token-by-token responses without buffering
	rp.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
		logger.Error("upstream model server unreachable", "error", err)
		http.Error(w, "model server unavailable", http.StatusBadGateway)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := ClientIdentity(r)
		if !ok {
			http.Error(w, "a verified client certificate is required", http.StatusUnauthorized)
			return
		}
		if _, allowed := allow[id]; !allowed {
			logger.Warn("denied client", "identity", id)
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		rp.ServeHTTP(w, r)
	})
}

// ClientIdentity returns the common name of the verified client certificate.
func ClientIdentity(r *http.Request) (string, bool) {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
		return "", false
	}
	cn := strings.TrimSpace(r.TLS.VerifiedChains[0][0].Subject.CommonName)
	return cn, cn != ""
}

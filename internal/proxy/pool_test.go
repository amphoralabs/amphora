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
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ramin-fazli/amphora/internal/controller"
	"github.com/ramin-fazli/amphora/internal/testpki"
)

// acceptCounter counts TCP connections accepted by an upstream.
type acceptCounter struct {
	net.Listener
	n *atomic.Int64
}

func (l acceptCounter) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.n.Add(1)
	}
	return c, err
}

func countedListener(t *testing.T, n *atomic.Int64) net.Listener {
	t.Helper()
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return acceptCounter{Listener: base, n: n}
}

// hammer sends total requests through url from conc concurrent clients sharing one client.
func hammer(t *testing.T, c *http.Client, url string, conc, total int) {
	t.Helper()
	var wg sync.WaitGroup
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < total/conc; j++ {
				if err := get(c, url, benchModel); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// Regression: with Go's default transport (2 idle connections per host) a
// model serving more than 2 concurrent requests made the proxy open a new
// upstream connection for 20-46% of requests (measured: 2,732 new connections
// for 6,400 requests at concurrency 16). On the sidecar path each was a full
// mutual-TLS handshake. Connections must now be reused: new connections stay
// on the order of the concurrency, far below the request count.
func TestUpstreamConnectionsAreReused(t *testing.T) {
	const total = 3200
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("plaintext upstream", func(t *testing.T) {
		var accepted atomic.Int64
		up := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }))
		up.Listener = countedListener(t, &accepted)
		up.Start()
		defer up.Close()
		reg := NewRegistry()
		reg.MarkWarm(Target{Model: benchModel, Tenant: tenantA, Upstream: up.URL})
		p := httptest.NewServer(NewHandler(reg, nil, Config{}, nil, logger))
		defer p.Close()

		for _, conc := range []int{16, 64} {
			accepted.Store(0)
			hammer(t, newClient(nil), p.URL, conc, total)
			if got, limit := accepted.Load(), int64(conc*3); got > limit {
				t.Errorf("concurrency %d: %d new upstream connections for %d requests, want <= %d (connection churn)", conc, got, total, limit)
			}
		}
	})

	t.Run("mutual-TLS upstream (the sidecar path)", func(t *testing.T) {
		ca := testpki.New(t, "pool-ca")
		name := controller.PodTLSServerName(tenantA)
		sc, sk := ca.Issue(t, name, x509.ExtKeyUsageServerAuth, name)
		pair, err := tls.X509KeyPair(sc, sk)
		if err != nil {
			t.Fatal(err)
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(ca.PEM)
		var accepted atomic.Int64
		pod := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }))
		pod.Listener = countedListener(t, &accepted)
		pod.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12}
		pod.StartTLS()
		defer pod.Close()
		cc, ck := ca.Issue(t, "amphora-proxy", x509.ExtKeyUsageClientAuth)
		f := ca.WriteFiles(t, cc, ck)
		reg := NewRegistry()
		reg.MarkWarm(Target{Model: benchModel, Tenant: tenantA, Upstream: pod.URL, ServerName: name})
		p := httptest.NewServer(NewHandler(reg, nil, Config{UpstreamTLS: &UpstreamTLS{CertFile: f.Cert, KeyFile: f.Key, CAFile: f.CA}}, nil, logger))
		defer p.Close()

		accepted.Store(0)
		hammer(t, newClient(nil), p.URL, 16, total)
		if got, limit := accepted.Load(), int64(16*3); got > limit {
			t.Errorf("%d new upstream TLS connections for %d requests, want <= %d (each is a full handshake)", got, total, limit)
		}
	})
}

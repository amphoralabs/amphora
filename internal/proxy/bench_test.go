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
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ramin-fazli/amphora/internal/controller"
	"github.com/ramin-fazli/amphora/internal/testpki"
)

// Warm-path routing overhead against the Technical Specification §6 SLO
// (proxy queueing overhead, p99 < 15ms). Run with `make bench-proxy`. Every
// scenario sends requests through a real TCP listener with keep-alive and
// reports the proxy's ADDED latency as the difference to calling the same
// upstream directly, at the same concurrency, so the upstream's own time cancels
// out. Numbers from a shared development machine are not reference-hardware
// results.

const benchModel = modelA

// percentile returns the q-quantile (0..1) of sorted durations.
func percentile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(float64(len(sorted)-1)*q)]
}

// drive issues b.N requests across parallel workers via do, returning every
// request's latency, sorted.
func drive(b *testing.B, do func() error) []time.Duration {
	b.Helper()
	var mu sync.Mutex
	all := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		local := make([]time.Duration, 0, 1024)
		for pb.Next() {
			start := time.Now()
			if err := do(); err != nil {
				b.Error(err)
				return
			}
			local = append(local, time.Since(start))
		}
		mu.Lock()
		all = append(all, local...)
		mu.Unlock()
	})
	b.StopTimer()
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	return all
}

func get(c *http.Client, url, model string) error {
	req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
	if err != nil {
		return err
	}
	if model != "" {
		req.Header.Set(ModelHeader, model)
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.Body.Close()
}

func newClient(cfg *tls.Config) *http.Client {
	return &http.Client{Transport: &http.Transport{
		TLSClientConfig: cfg, MaxIdleConns: 1024, MaxIdleConnsPerHost: 1024, ForceAttemptHTTP2: false,
	}}
}

// report records the proxied latency and the overhead over the direct call.
func report(b *testing.B, direct, proxied []time.Duration) {
	b.Helper()
	us := func(d time.Duration) float64 { return float64(d) / float64(time.Microsecond) }
	b.ReportMetric(us(percentile(proxied, 0.50)), "proxied-p50-µs")
	b.ReportMetric(us(percentile(proxied, 0.99)), "proxied-p99-µs")
	b.ReportMetric(us(percentile(direct, 0.50)), "direct-p50-µs")
	b.ReportMetric(us(percentile(direct, 0.99)), "direct-p99-µs")
	b.ReportMetric(us(percentile(proxied, 0.50)-percentile(direct, 0.50)), "added-p50-µs")
	b.ReportMetric(us(percentile(proxied, 0.99)-percentile(direct, 0.99)), "added-p99-µs")
}

func BenchmarkWarmPathPlainHTTP(b *testing.B) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer upstream.Close()
	reg := NewRegistry()
	reg.MarkWarm(Target{Model: benchModel, Tenant: tenantA, Upstream: upstream.URL})
	proxy := httptest.NewServer(NewHandler(reg, nil, Config{}, nil, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer proxy.Close()
	c := newClient(nil)

	direct := drive(b, func() error { return get(c, upstream.URL, "") })
	proxied := drive(b, func() error { return get(c, proxy.URL, benchModel) })
	report(b, direct, proxied)
}

// clientIdentity: client mTLS terminated at the proxy, tenant from the cert.
func BenchmarkWarmPathClientIdentity(b *testing.B) {
	ca := testpki.New(b, "bench-ca")
	srvCert, srvKey := ca.Issue(b, "proxy.test", x509.ExtKeyUsageServerAuth, "proxy.test")
	files := ca.WriteFiles(b, srvCert, srvKey)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srvTLS, err := ServerTLSConfig(ctx, files.Cert, files.Key, files.CA)
	if err != nil {
		b.Fatal(err)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer upstream.Close()
	reg := NewRegistry()
	reg.MarkWarm(Target{Model: benchModel, Tenant: tenantA, Upstream: upstream.URL})
	proxy := httptest.NewUnstartedServer(NewHandler(reg, nil, Config{RequireClientIdentity: true}, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil))))
	proxy.TLS = srvTLS
	proxy.StartTLS()
	defer proxy.Close()

	cc, ck := ca.Issue(b, tenantA, x509.ExtKeyUsageClientAuth)
	pair, err := tls.X509KeyPair(cc, ck)
	if err != nil {
		b.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca.PEM)
	c := newClient(&tls.Config{RootCAs: pool, ServerName: "proxy.test", Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12})

	plain := newClient(nil) // one client: the baseline must reuse connections like the proxied path does
	direct := drive(b, func() error { return get(plain, upstream.URL, "") })
	proxied := drive(b, func() error { return get(c, proxy.URL, benchModel) })
	report(b, direct, proxied)
}

// sidecar: the proxy reaching the model through the mutual-TLS sidecar hop.
func BenchmarkWarmPathTLSUpstream(b *testing.B) {
	ca := testpki.New(b, "bench-pod-ca")
	name := controller.PodTLSServerName(tenantA)
	sc, sk := ca.Issue(b, name, x509.ExtKeyUsageServerAuth, name)
	pair, err := tls.X509KeyPair(sc, sk)
	if err != nil {
		b.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca.PEM)
	pod := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }))
	pod.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12}
	pod.StartTLS()
	defer pod.Close()

	cc, ck := ca.Issue(b, "amphora-proxy", x509.ExtKeyUsageClientAuth)
	f := ca.WriteFiles(b, cc, ck)
	reg := NewRegistry()
	reg.MarkWarm(Target{Model: benchModel, Tenant: tenantA, Upstream: pod.URL, ServerName: name})
	proxy := httptest.NewServer(NewHandler(reg, nil, Config{UpstreamTLS: &UpstreamTLS{CertFile: f.Cert, KeyFile: f.Key, CAFile: f.CA}}, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer proxy.Close()
	// "direct" here is the same mutual-TLS call without the proxy.
	dc := newClient(&tls.Config{RootCAs: pool, ServerName: name, Certificates: []tls.Certificate{func() tls.Certificate {
		p, e := tls.X509KeyPair(cc, ck)
		if e != nil {
			b.Fatal(e)
		}
		return p
	}()}, MinVersion: tls.VersionTLS12})
	c := newClient(nil)

	direct := drive(b, func() error { return get(dc, pod.URL, "") })
	proxied := drive(b, func() error { return get(c, proxy.URL, benchModel) })
	report(b, direct, proxied)
}

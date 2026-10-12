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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

const (
	tenantA  = "team-a"
	tenantB  = "team-b"
	modelA   = "team-a/llama"
	modelB   = "team-b/llama"
	cnServer = "proxy.test"
)

// pki is a throwaway CA that can sign server and client certificates.
type pki struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
	next int64
}

func newPKI(t *testing.T, name string) *pki {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &pki{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), next: 1}
}

// issue returns a PEM certificate/key for cn, usable as a server (DNS SAN
// cnServer, 127.0.0.1) or as a client.
func (p *pki) issue(t *testing.T, cn string, usage x509.ExtKeyUsage) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	p.next++
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(p.next), Subject: pkix.Name{CommonName: cn},
		DNSNames: []string{cnServer}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.cert, &key.PublicKey, p.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func writeFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// identityFixture runs the proxy handler behind a real TLS listener that
// requires client identity, in front of a plain upstream that records calls.
type identityFixture struct {
	ca        *pki
	proxyURL  string
	upstream  *httptest.Server
	upstreamN atomic.Int32
	lastTen   atomic.Value // string: X-Amphora-Tenant seen by the upstream
	cert      string
	key       string
	dir       string
}

func newIdentityFixture(t *testing.T) *identityFixture {
	t.Helper()
	f := &identityFixture{ca: newPKI(t, "clients-ca"), dir: t.TempDir()}
	f.upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.upstreamN.Add(1)
		f.lastTen.Store(r.Header.Get(TenantHeader))
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(f.upstream.Close)

	reg := NewRegistry()
	reg.MarkWarm(Target{Model: modelA, Tenant: tenantA, Upstream: f.upstream.URL})
	reg.MarkWarm(Target{Model: modelB, Tenant: tenantB, Upstream: f.upstream.URL})
	h := NewHandler(reg, nil, Config{RequireClientIdentity: true}, nil, nil)

	serverCert, serverKey := f.ca.issue(t, cnServer, x509.ExtKeyUsageServerAuth)
	f.cert = writeFile(t, f.dir, "tls.crt", serverCert)
	f.key = writeFile(t, f.dir, "tls.key", serverKey)
	caFile := writeFile(t, f.dir, "ca.crt", f.ca.pem)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg, err := ServerTLSConfig(ctx, f.cert, f.key, caFile)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(h)
	srv.TLS = cfg
	srv.StartTLS()
	t.Cleanup(srv.Close)
	f.proxyURL = srv.URL
	return f
}

// client builds an HTTP client that trusts the fixture CA's server cert and
// presents clientCert (nil for none).
func (f *identityFixture) client(clientCert *tls.Certificate) *http.Client {
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(f.ca.pem)
	cfg := &tls.Config{RootCAs: pool, ServerName: cnServer, MinVersion: tls.VersionTLS12}
	if clientCert != nil {
		cfg.Certificates = []tls.Certificate{*clientCert}
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}, Timeout: 5 * time.Second}
}

func (f *identityFixture) clientCert(t *testing.T, ca *pki, cn string) *tls.Certificate {
	t.Helper()
	c, k := ca.issue(t, cn, x509.ExtKeyUsageClientAuth)
	pair, err := tls.X509KeyPair(c, k)
	if err != nil {
		t.Fatal(err)
	}
	return &pair
}

func (f *identityFixture) do(t *testing.T, c *http.Client, model, tenantHeader string) (int, error) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, f.proxyURL+"/", http.NoBody)
	req.Header.Set(ModelHeader, model)
	if tenantHeader != "" {
		req.Header.Set(TenantHeader, tenantHeader)
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func TestClientIdentityEnforcement(t *testing.T) {
	f := newIdentityFixture(t)
	a := f.clientCert(t, f.ca, tenantA)

	t.Run("no client certificate is 401 and never reaches the upstream", func(t *testing.T) {
		before := f.upstreamN.Load()
		code, err := f.do(t, f.client(nil), modelA, "")
		if err != nil || code != http.StatusUnauthorized {
			t.Fatalf("got %d, %v; want 401", code, err)
		}
		if f.upstreamN.Load() != before {
			t.Fatal("unauthenticated request reached the upstream")
		}
	})

	t.Run("a tenant reaches its own model and the tenant tag comes from the certificate", func(t *testing.T) {
		code, err := f.do(t, f.client(a), modelA, "")
		if err != nil || code != http.StatusOK {
			t.Fatalf("got %d, %v; want 200", code, err)
		}
		if got := f.lastTen.Load(); got != tenantA {
			t.Fatalf("upstream saw tenant %v, want %q taken from the certificate", got, tenantA)
		}
	})

	t.Run("an asserted tenant header matching the identity is accepted", func(t *testing.T) {
		if code, err := f.do(t, f.client(a), modelA, tenantA); err != nil || code != http.StatusOK {
			t.Fatalf("got %d, %v; want 200", code, err)
		}
	})

	t.Run("spoofing another tenant in the header is 403", func(t *testing.T) {
		before := f.upstreamN.Load()
		code, err := f.do(t, f.client(a), modelA, tenantB)
		if err != nil || code != http.StatusForbidden {
			t.Fatalf("got %d, %v; want 403", code, err)
		}
		if f.upstreamN.Load() != before {
			t.Fatal("spoofed request reached the upstream")
		}
	})

	t.Run("another tenant's model is 403 even though it is warm", func(t *testing.T) {
		before := f.upstreamN.Load()
		code, err := f.do(t, f.client(a), modelB, "")
		if err != nil || code != http.StatusForbidden {
			t.Fatalf("got %d, %v; want 403", code, err)
		}
		if f.upstreamN.Load() != before {
			t.Fatal("cross-tenant request reached the upstream")
		}
	})

	t.Run("a nonexistent model of another tenant is indistinguishable from an existing one", func(t *testing.T) {
		code, err := f.do(t, f.client(a), "team-b/does-not-exist", "")
		if err != nil || code != http.StatusForbidden {
			t.Fatalf("got %d, %v; want the same 403 (no existence oracle)", code, err)
		}
	})

	t.Run("a certificate from an untrusted CA is never authenticated", func(t *testing.T) {
		// Go's TLS client only sends a certificate that chains to a CA the
		// server advertised, so this surfaces as "no certificate" (401) or a
		// failed handshake depending on the stack. Either way it must not
		// reach the upstream.
		before := f.upstreamN.Load()
		rogue := newPKI(t, "rogue-ca")
		code, err := f.do(t, f.client(f.clientCert(t, rogue, tenantA)), modelA, "")
		if err == nil && code != http.StatusUnauthorized {
			t.Fatalf("got %d; want a handshake error or 401", code)
		}
		if f.upstreamN.Load() != before {
			t.Fatal("a request with an untrusted certificate reached the upstream")
		}
	})
}

func TestClientIdentityIgnoresUnverifiedCertificates(t *testing.T) {
	cert := &x509.Certificate{Subject: pkix.Name{CommonName: tenantA}}
	r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	r.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}} // presented, not verified
	if id, ok := clientIdentity(r); ok {
		t.Fatalf("an unverified certificate produced identity %q", id)
	}
	r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}}
	if id, ok := clientIdentity(r); !ok || id != tenantA {
		t.Fatalf("verified chain: got %q, %v", id, ok)
	}
	if _, ok := clientIdentity(httptest.NewRequest(http.MethodGet, "/", http.NoBody)); ok {
		t.Fatal("a plaintext request produced an identity")
	}
	if got := modelOwner("noslash"); got != "" {
		t.Fatalf("modelOwner(noslash) = %q, want empty", got)
	}
}

func TestServerTLSConfigRejectsBadInput(t *testing.T) {
	dir := t.TempDir()
	ca := newPKI(t, "ca")
	c, k := ca.issue(t, cnServer, x509.ExtKeyUsageServerAuth)
	cert, key := writeFile(t, dir, "c", c), writeFile(t, dir, "k", k)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := ServerTLSConfig(ctx, "", "", ""); err == nil {
		t.Error("empty cert/key accepted")
	}
	if _, err := ServerTLSConfig(ctx, cert, filepath.Join(dir, "missing"), ""); err == nil {
		t.Error("missing key accepted")
	}
	if _, err := ServerTLSConfig(ctx, cert, key, filepath.Join(dir, "missing")); err == nil {
		t.Error("missing CA bundle accepted")
	}
	if _, err := ServerTLSConfig(ctx, cert, key, writeFile(t, dir, "junk", []byte("not pem"))); err == nil {
		t.Error("CA bundle with no certificates accepted")
	}
	cfg, err := ServerTLSConfig(ctx, cert, key, "")
	if err != nil || cfg.ClientAuth != tls.NoClientCert || cfg.MinVersion < tls.VersionTLS12 {
		t.Errorf("TLS-only config = %+v, %v", cfg, err)
	}
}

func TestServingCertificateIsReloadedOnRotation(t *testing.T) {
	f := newIdentityFixture(t)
	serial := func() string {
		// SNI is required: httptest installs its own static certificate, which
		// Go prefers over GetCertificate when the client sends no server name.
		// The real listener has no static certificate.
		conn, err := tls.Dial("tcp", f.proxyURL[len("https://"):], &tls.Config{
			ServerName: cnServer, InsecureSkipVerify: true, MinVersion: tls.VersionTLS12, // #nosec G402 -- test reads the served cert
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		return conn.ConnectionState().PeerCertificates[0].SerialNumber.String()
	}
	before := serial()

	c, k := f.ca.issue(t, cnServer, x509.ExtKeyUsageServerAuth) // new serial, like a cert-manager renewal
	writeFile(t, f.dir, "tls.crt", c)
	writeFile(t, f.dir, "tls.key", k)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if serial() != before {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("the proxy kept serving the old certificate after the files changed")
}

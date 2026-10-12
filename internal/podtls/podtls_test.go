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

package podtls

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
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	serverName   = "team-a.model.amphora.internal"
	cnProxy      = "amphora-proxy"
	cnController = "amphora-controller"
	cnStranger   = "some-other-pod"
)

type ca struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
	n    int64
}

func newCA(t *testing.T) *ca {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &ca{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), n: 1}
}

func (c *ca) issue(t *testing.T, cn string, usage x509.ExtKeyUsage) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	c.n++
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(c.n), Subject: pkix.Name{CommonName: cn}, DNSNames: []string{serverName},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

type fixture struct {
	ca       *ca
	url      string
	upstream *httptest.Server
	hits     atomic.Int32
}

func newFixture(t *testing.T, upstreamHandler http.HandlerFunc) *fixture {
	t.Helper()
	f := &fixture{ca: newCA(t)}
	f.upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.hits.Add(1)
		upstreamHandler(w, r)
	}))
	t.Cleanup(f.upstream.Close)

	dir := t.TempDir()
	sc, sk := f.ca.issue(t, serverName, x509.ExtKeyUsageServerAuth)
	write := func(name string, b []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg, err := ServerTLSConfig(ctx, write("tls.crt", sc), write("tls.key", sk), write("ca.crt", f.ca.pem))
	if err != nil {
		t.Fatal(err)
	}
	up, _ := url.Parse(f.upstream.URL)
	srv := httptest.NewUnstartedServer(NewHandler(up, []string{cnProxy, cnController}, slog.New(slog.NewTextHandler(io.Discard, nil))))
	srv.TLS = cfg
	srv.StartTLS()
	t.Cleanup(srv.Close)
	f.url = srv.URL
	return f
}

func (f *fixture) client(t *testing.T, from *ca, cn string) *http.Client {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(f.ca.pem)
	cfg := &tls.Config{RootCAs: pool, ServerName: serverName, MinVersion: tls.VersionTLS12}
	if from != nil {
		c, k := from.issue(t, cn, x509.ExtKeyUsageClientAuth)
		pair, err := tls.X509KeyPair(c, k)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}, Timeout: 5 * time.Second}
}

func TestAllowedClientsAreForwarded(t *testing.T) {
	f := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_, _ = io.WriteString(w, r.URL.Path+"|"+string(body))
	})
	for _, cn := range []string{cnProxy, cnController} {
		resp, err := f.client(t, f.ca, cn).Post(f.url+"/v1/completions", "text/plain", strings.NewReader("hi"))
		if err != nil {
			t.Fatalf("%s: %v", cn, err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || string(b) != "/v1/completions|hi" {
			t.Fatalf("%s: got %d %q", cn, resp.StatusCode, b)
		}
	}
}

func TestUnauthorizedCallersNeverReachTheModel(t *testing.T) {
	f := newFixture(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "secret") })

	t.Run("valid certificate, identity not on the allow-list: 403", func(t *testing.T) {
		resp, err := f.client(t, f.ca, cnStranger).Get(f.url + "/")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("got %d, want 403", resp.StatusCode)
		}
	})
	t.Run("no client certificate: handshake refused", func(t *testing.T) {
		if resp, err := f.client(t, nil, "").Get(f.url + "/"); err == nil {
			_ = resp.Body.Close()
			t.Fatal("a connection without a client certificate was served")
		}
	})
	t.Run("certificate from an untrusted CA: handshake refused", func(t *testing.T) {
		if resp, err := f.client(t, newCA(t), cnProxy).Get(f.url + "/"); err == nil {
			_ = resp.Body.Close()
			t.Fatal("a certificate from an untrusted CA was served")
		}
	})
	if f.hits.Load() != 0 {
		t.Fatalf("model server was reached %d time(s) by unauthorized callers", f.hits.Load())
	}
}

func TestResponsesAreStreamedNotBuffered(t *testing.T) {
	release := make(chan struct{})
	f := newFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "first\n")
		w.(http.Flusher).Flush()
		<-release // the model is "still generating"
		_, _ = io.WriteString(w, "second\n")
	})
	resp, err := f.client(t, f.ca, cnProxy).Get(f.url + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	buf := make([]byte, 6)
	got := make(chan string, 1)
	go func() {
		n, _ := io.ReadFull(resp.Body, buf)
		got <- string(buf[:n])
	}()
	select {
	case s := <-got:
		if s != "first\n" {
			t.Fatalf("got %q", s)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the first chunk was not delivered while the model was still generating (response is buffered)")
	}
	close(release)
}

func TestUnreachableModelServerIs502(t *testing.T) {
	f := newFixture(t, func(http.ResponseWriter, *http.Request) {})
	f.upstream.Close()
	resp, err := f.client(t, f.ca, cnProxy).Get(f.url + "/")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("got %d, want 502", resp.StatusCode)
	}
}

func TestConfigurationIsValidated(t *testing.T) {
	if _, err := ParseAllowedClients(" , "); err == nil {
		t.Error("an empty allow-list was accepted")
	}
	if got, err := ParseAllowedClients("a, b ,,c"); err != nil || len(got) != 3 {
		t.Errorf("ParseAllowedClients = %v, %v", got, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := ServerTLSConfig(ctx, "", "", ""); err == nil {
		t.Error("empty TLS inputs accepted")
	}
	dir := t.TempDir()
	junk := filepath.Join(dir, "junk")
	_ = os.WriteFile(junk, []byte("not pem"), 0o600)
	c := newCA(t)
	cp, kp := c.issue(t, serverName, x509.ExtKeyUsageServerAuth)
	cert, key := filepath.Join(dir, "c"), filepath.Join(dir, "k")
	_ = os.WriteFile(cert, cp, 0o600)
	_ = os.WriteFile(key, kp, 0o600)
	if _, err := ServerTLSConfig(ctx, cert, key, junk); err == nil {
		t.Error("a CA bundle with no certificates was accepted")
	}
}

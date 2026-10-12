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
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
	"github.com/ramin-fazli/amphora/internal/controller"
	"github.com/ramin-fazli/amphora/internal/testpki"
)

// sidecarStandIn is an https upstream that, like the real sidecar, requires a
// client certificate and presents a certificate for serverName.
func sidecarStandIn(t *testing.T, ca *testpki.CA, serverName string, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	cert, key := ca.Issue(t, serverName, x509.ExtKeyUsageServerAuth, serverName)
	pair, err := tls.X509KeyPair(cert, key)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca.PEM)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, "from-pod")
	}))
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{pair}, ClientCAs: pool,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func TestHTTPSUpstreamRequiresCredentialsAndTheRightName(t *testing.T) {
	ca := testpki.New(t, "pod-ca")
	var hits atomic.Int32
	pod := sidecarStandIn(t, ca, controller.PodTLSServerName(tenantA), &hits)
	cc, ck := ca.Issue(t, "amphora-proxy", x509.ExtKeyUsageClientAuth)
	f := ca.WriteFiles(t, cc, ck)
	creds := &UpstreamTLS{CertFile: f.Cert, KeyFile: f.Key, CAFile: f.CA}

	route := func(cfg Config, target Target) *httptest.ResponseRecorder {
		reg := NewRegistry()
		reg.MarkWarm(target)
		h := NewHandler(reg, nil, cfg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.Header.Set(ModelHeader, target.Model)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	target := Target{Model: modelA, Tenant: tenantA, Upstream: pod.URL, ServerName: controller.PodTLSServerName(tenantA)}

	t.Run("with credentials and the tenant's name, the request is served", func(t *testing.T) {
		rec := route(Config{UpstreamTLS: creds}, target)
		if rec.Code != http.StatusOK || rec.Body.String() != "from-pod" {
			t.Fatalf("got %d %q", rec.Code, rec.Body.String())
		}
	})

	before := hits.Load()
	t.Run("an https upstream with no credentials is refused, not reached insecurely", func(t *testing.T) {
		rec := route(Config{}, target)
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("got %d, want 502", rec.Code)
		}
	})
	t.Run("a pod answering for another tenant's name is refused", func(t *testing.T) {
		wrong := target
		wrong.ServerName = controller.PodTLSServerName(tenantB)
		rec := route(Config{UpstreamTLS: creds}, wrong)
		if rec.Code == http.StatusOK {
			t.Fatal("a pod serving team-a's certificate was accepted as team-b")
		}
	})
	t.Run("an https upstream with no server name to verify is refused", func(t *testing.T) {
		noName := target
		noName.ServerName = ""
		if rec := route(Config{UpstreamTLS: creds}, noName); rec.Code != http.StatusBadGateway {
			t.Fatalf("got %d, want 502", rec.Code)
		}
	})
	if hits.Load() != before {
		t.Fatalf("the pod was reached %d extra time(s) by requests that must have been refused", hits.Load()-before)
	}
}

func TestApplyModelDeploymentSetsServerNameForHTTPSEndpoints(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := NewRegistry()
	md := &amphorav1alpha1.ModelDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: tenantA},
		Status:     amphorav1alpha1.ModelDeploymentStatus{Phase: controller.PhaseServing, Endpoint: "https://10.0.0.5:8443"},
	}
	ApplyModelDeployment(reg, md, logger)
	got, ok := reg.Warm(modelA)
	if !ok || got.Upstream != "https://10.0.0.5:8443" || got.ServerName != "team-a.model.amphora.internal" {
		t.Fatalf("target = %+v, %v", got, ok)
	}

	md.Status.Endpoint = "http://10.0.0.5:8000"
	ApplyModelDeployment(reg, md, logger)
	if got, _ := reg.Warm(modelA); got.ServerName != "" {
		t.Fatalf("a plaintext endpoint must carry no TLS name, got %q", got.ServerName)
	}
}

func TestUpstreamTLSRejectsBadCABundle(t *testing.T) {
	ca := testpki.New(t, "ca")
	c, k := ca.Issue(t, "amphora-proxy", x509.ExtKeyUsageClientAuth)
	f := ca.WriteFiles(t, c, k)
	if _, err := (&UpstreamTLS{CertFile: f.Cert, KeyFile: f.Key, CAFile: f.Cert + ".missing"}).transport("n"); err == nil {
		t.Error("a missing CA bundle was accepted")
	}
	var none *UpstreamTLS
	if _, err := none.transport("n"); err == nil {
		t.Error("nil credentials produced a transport")
	}
}

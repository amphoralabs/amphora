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

package controller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
	"github.com/ramin-fazli/amphora/internal/testpki"
)

const (
	tlsNS      = "team-a"
	tlsOther   = "team-b"
	tlsSidecar = "example.com/amphora-podtls:test"
)

func TestPodTLSConfigDisabledByDefault(t *testing.T) {
	var nilCfg *PodTLSConfig
	if nilCfg.Enabled() || (&PodTLSConfig{}).Enabled() {
		t.Fatal("a nil or empty PodTLSConfig must leave pods plaintext")
	}
	if !(&PodTLSConfig{SidecarImage: tlsSidecar}).Enabled() {
		t.Fatal("a configured sidecar image must enable TLS")
	}
	if got := PodTLSServerName(tlsNS); got != tlsNS+".model.amphora.internal" {
		t.Fatalf("PodTLSServerName = %q", got)
	}
}

func TestSidecarIsLockedDownAndForwardsToTheModelPort(t *testing.T) {
	sc := (&PodTLSConfig{SidecarImage: tlsSidecar}).sidecar(9000)
	if !strings.Contains(strings.Join(sc.Args, " "), "-upstream=http://127.0.0.1:9000") {
		t.Errorf("sidecar must forward to the model's loopback port: %v", sc.Args)
	}
	s := sc.SecurityContext
	if s == nil || s.ReadOnlyRootFilesystem == nil || !*s.ReadOnlyRootFilesystem ||
		s.RunAsNonRoot == nil || !*s.RunAsNonRoot ||
		s.AllowPrivilegeEscalation == nil || *s.AllowPrivilegeEscalation ||
		s.Capabilities == nil || len(s.Capabilities.Drop) != 1 || s.Capabilities.Drop[0] != "ALL" {
		t.Errorf("sidecar is not locked down: %+v", s)
	}
	if sc.Resources.Limits.Cpu().IsZero() || sc.Resources.Limits.Memory().IsZero() {
		t.Error("sidecar has no resource limits")
	}
}

func TestPoolPodsOnlyGetTheSidecarWhenEnabled(t *testing.T) {
	pool := &amphorav1alpha1.PausePool{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: tlsNS},
		Spec:       amphorav1alpha1.PausePoolSpec{NodeName: "n", PauseImage: "pause"},
	}
	plain := newPausePod(pool, nil)
	if len(plain.Spec.Containers) != 1 || len(plain.Spec.Volumes) != 0 {
		t.Fatalf("plaintext pod = %d containers, %d volumes; want 1, 0", len(plain.Spec.Containers), len(plain.Spec.Volumes))
	}

	secured := newPausePod(pool, &PodTLSConfig{SidecarImage: tlsSidecar, SecretName: "custom-tls"})
	if len(secured.Spec.Containers) != 2 {
		t.Fatalf("got %d containers, want 2", len(secured.Spec.Containers))
	}
	// Hijack swaps the image of Containers[0]; the model container must stay first.
	if secured.Spec.Containers[0].Name != "pause" || secured.Spec.Containers[1].Name != podTLSSidecarName {
		t.Fatalf("container order = %s, %s; the model container must stay first",
			secured.Spec.Containers[0].Name, secured.Spec.Containers[1].Name)
	}
	vol := secured.Spec.Volumes
	if len(vol) != 1 || vol[0].Secret == nil || vol[0].Secret.SecretName != "custom-tls" {
		t.Fatalf("volumes = %+v, want the serving-certificate Secret", vol)
	}
	if vol[0].Secret.Optional != nil && *vol[0].Secret.Optional {
		t.Fatal("the certificate Secret must not be optional: a pod without it must not start without TLS")
	}
}

// tlsModelServer is a stand-in sidecar: it requires a client certificate and
// presents a certificate valid for serverName.
func tlsModelServer(t *testing.T, ca *testpki.CA, serverName string, handler http.Handler) *httptest.Server {
	t.Helper()
	cert, key := ca.Issue(t, serverName, x509.ExtKeyUsageServerAuth, serverName)
	pair, err := tls.X509KeyPair(cert, key)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca.PEM)
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{pair}, ClientCAs: pool,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func TestProberReachesSidecarOnlyWithCredentialsAndTheRightName(t *testing.T) {
	ca := testpki.New(t, "pod-ca")
	srv := tlsModelServer(t, ca, PodTLSServerName(tlsNS), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	cc, ck := ca.Issue(t, "amphora-controller", x509.ExtKeyUsageClientAuth)
	f := ca.WriteFiles(t, cc, ck)
	creds := &PodClientTLS{CertFile: f.Cert, KeyFile: f.Key, CAFile: f.CA}
	ctx := context.Background()

	good := PodTarget{URL: srv.URL, ServerName: PodTLSServerName(tlsNS)}
	if err := (&HTTPEvalProber{TLS: creds}).Probe(ctx, good); err != nil {
		t.Fatalf("probe with credentials and the right name = %v, want nil", err)
	}

	t.Run("no credentials configured fails closed instead of falling back", func(t *testing.T) {
		if err := (&HTTPEvalProber{}).Probe(ctx, good); err == nil {
			t.Fatal("an https target was probed with no TLS credentials")
		}
	})
	t.Run("another tenant's certificate name is rejected", func(t *testing.T) {
		other := PodTarget{URL: srv.URL, ServerName: PodTLSServerName(tlsOther)}
		if err := (&HTTPEvalProber{TLS: creds}).Probe(ctx, other); err == nil {
			t.Fatal("a pod serving team-a's certificate passed verification as team-b")
		}
	})
	t.Run("a pod signed by an untrusted CA is rejected", func(t *testing.T) {
		rogue := tlsModelServer(t, testpki.New(t, "rogue"), PodTLSServerName(tlsNS), http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		err := (&HTTPEvalProber{TLS: creds}).Probe(ctx, PodTarget{URL: rogue.URL, ServerName: PodTLSServerName(tlsNS)})
		if err == nil {
			t.Fatal("a pod with a certificate from an untrusted CA passed verification")
		}
	})
	t.Run("canary runner uses the same credentials", func(t *testing.T) {
		if err := (&HTTPCanaryRunner{}).Run(ctx, good, "m", []Canary{{Prompt: "p", Expected: "e"}}); err == nil {
			t.Fatal("canaries were sent to an https target with no TLS credentials")
		}
	})
}

func TestPodTargetIsHTTPSOnTheSidecarPortWhenEnabled(t *testing.T) {
	pod := &corev1.Pod{Status: corev1.PodStatus{PodIP: "10.1.2.3"}}

	plain := (&ModelDeploymentReconciler{}).podTarget(pod, tlsNS, 8000)
	if plain.URL != "http://10.1.2.3:8000" || plain.ServerName != "" {
		t.Fatalf("plaintext target = %+v", plain)
	}
	secured := (&ModelDeploymentReconciler{PodTLS: &PodTLSConfig{SidecarImage: tlsSidecar}}).podTarget(pod, tlsNS, 8000)
	if secured.URL != "https://10.1.2.3:8443" || secured.ServerName != tlsNS+".model.amphora.internal" {
		t.Fatalf("TLS target = %+v; the model port must not be exposed in the endpoint", secured)
	}
}

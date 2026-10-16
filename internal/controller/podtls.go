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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// Encrypted, authenticated transport to model pods (spec §5).
//
// A model image is the user's, and a hijacked pause pod's container args and
// volumes cannot change after creation, so TLS cannot live in the model
// container. Instead every serving pod carries a small amphora sidecar that
// terminates TLS and requires a client certificate from the proxy or
// controller. The sidecar and the Secret holding its certificate are declared
// when the pod is CREATED (pool-fill or cold-create), which is why the Secret
// has a fixed per-namespace name rather than a per-deployment one.
//
// Identity is per tenant namespace, matching the isolation boundary: all of a
// tenant's pods share one serving certificate, whose DNS name encodes the
// namespace (PodTLSServerName). A compromised pod can therefore impersonate
// only its own tenant's other pods, and the proxy only routes a tenant's
// models to that tenant.
const (
	// PodTLSPort is where the sidecar listens.
	PodTLSPort int32 = 8443
	// DefaultPodTLSSecretName is the Secret, in each tenant namespace, with the
	// serving certificate (tls.crt, tls.key) and the CA (ca.crt).
	DefaultPodTLSSecretName = "amphora-model-tls"

	podTLSSidecarName = "amphora-tls"
	podTLSVolumeName  = "amphora-tls"
	podTLSMountPath   = "/tls"
	// podTLSServerNameSuffix completes the DNS name in a tenant's serving
	// certificate: <namespace>.model.amphora.internal.
	podTLSServerNameSuffix = ".model.amphora.internal"
)

// PodTLSServerName is the DNS name callers must verify when connecting to a
// model pod in namespace, and that the tenant's serving certificate must carry.
func PodTLSServerName(namespace string) string {
	return namespace + podTLSServerNameSuffix
}

// PodTLSConfig enables the TLS sidecar on serving pods. A nil or zero value
// leaves pods plaintext (the default).
type PodTLSConfig struct {
	// SidecarImage is the amphora-podtls image. Empty disables the feature.
	SidecarImage string
	// SecretName overrides DefaultPodTLSSecretName.
	SecretName string
}

// Enabled reports whether serving pods get the TLS sidecar.
func (c *PodTLSConfig) Enabled() bool {
	return c != nil && c.SidecarImage != ""
}

func (c *PodTLSConfig) secretName() string {
	if c.SecretName != "" {
		return c.SecretName
	}
	return DefaultPodTLSSecretName
}

// sidecar returns the TLS-terminating container, forwarding to the model
// server on loopback port modelPort.
func (c *PodTLSConfig) sidecar(modelPort int32) corev1.Container {
	readOnly := true
	noEscalation := false
	nonRoot := true
	return corev1.Container{
		Name:  podTLSSidecarName,
		Image: c.SidecarImage,
		Args: []string{
			fmt.Sprintf("-upstream=http://127.0.0.1:%d", modelPort),
			fmt.Sprintf("-listen=:%d", PodTLSPort),
		},
		Ports: []corev1.ContainerPort{{Name: podTLSSidecarName, ContainerPort: PodTLSPort}},
		VolumeMounts: []corev1.VolumeMount{
			{Name: podTLSVolumeName, MountPath: podTLSMountPath, ReadOnly: true},
		},
		// Ready as soon as it accepts connections; the model container's own
		// readiness probe decides whether the model is up.
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:  corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(PodTLSPort)}},
			PeriodSeconds: 2,
		},
		Resources: corev1.ResourceRequirements{
			// Requests equal limits: a pod is only Guaranteed QoS (the last to be
			// evicted, which is what a model pod holding GBs of weights wants)
			// if EVERY container, including this one, has them equal.
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("250m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("250m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
		},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: &noEscalation,
			ReadOnlyRootFilesystem:   &readOnly,
			RunAsNonRoot:             &nonRoot,
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
	}
}

// volume returns the Secret volume holding the tenant's serving certificate.
// It is not optional: a pod in a namespace with no certificate stays in
// ContainerCreating (visible, and failing closed) rather than starting
// without TLS.
func (c *PodTLSConfig) volume() corev1.Volume {
	return corev1.Volume{
		Name:         podTLSVolumeName,
		VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: c.secretName()}},
	}
}

// PodTarget is a model pod endpoint the controller calls.
type PodTarget struct {
	// URL is the base or full URL (scheme://ip:port[/path]).
	URL string
	// ServerName is the TLS name to verify. Empty means plaintext (http).
	ServerName string
}

// PodClientTLS holds the controller's client credentials for talking to
// sidecar-protected pods. Files are re-read on use so cert-manager renewal
// needs no restart.
type PodClientTLS struct {
	CertFile string
	KeyFile  string
	CAFile   string
}

// httpClient builds a client that presents the controller's certificate and
// verifies the pod's against the CA, requiring serverName. Redirects are not
// followed: the target is a pod IP taken from the API server.
func (p *PodClientTLS) httpClient(serverName string) (*http.Client, error) {
	if p == nil {
		return nil, errors.New("pod TLS is required (target is https) but no client certificate is configured")
	}
	pair, err := tls.LoadX509KeyPair(p.CertFile, p.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("loading controller client certificate: %w", err)
	}
	pem, err := os.ReadFile(p.CAFile) // #nosec G304 -- operator-supplied path
	if err != nil {
		return nil, fmt.Errorf("reading pod CA bundle: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("pod CA bundle %s contains no valid certificates", p.CAFile)
	}
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				MinVersion:   tls.VersionTLS12,
				RootCAs:      pool,
				Certificates: []tls.Certificate{pair},
				ServerName:   serverName,
			},
			DisableKeepAlives: true,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}, nil
}

// clientFor returns the HTTP client for t: the provided plaintext client (or a
// default) for http targets, and a mutual-TLS client for https ones. An https
// target with no TLS credentials fails closed rather than falling back.
func clientFor(plain *http.Client, tlsCreds *PodClientTLS, t PodTarget) (*http.Client, error) {
	if t.ServerName == "" {
		if plain != nil {
			return plain, nil
		}
		return &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, nil
	}
	return tlsCreds.httpClient(t.ServerName)
}

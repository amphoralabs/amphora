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

package e2e

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"time"

	"sigs.k8s.io/yaml"
)

// webhookCerts returns a PEM CA and a server certificate/key signed by it for
// the given DNS names. The e2e generates its own instead of depending on
// cert-manager, so the webhook is exercised over real TLS with no external
// install. Production uses cert-manager (config/certmanager).
func webhookCerts(dnsNames ...string) (caPEM, certPEM, keyPEM []byte, err error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "amphora-e2e-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, nil, nil, err
	}

	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}
	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: dnsNames[0]},
		DNSNames:     dnsNames,
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(srvKey)
	if err != nil {
		return nil, nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), nil
}

// webhookConfigFor renders the controller-gen generated
// ValidatingWebhookConfiguration (config/webhook/manifests.yaml) for the e2e
// cluster: it points every webhook at serviceName in namespace and trusts
// caPEM. Testing the generated file, not a hand copy, keeps the paths and
// failure policy honest.
func webhookConfigFor(manifestPath, serviceName, namespace string, caPEM []byte) (string, error) {
	raw, err := os.ReadFile(manifestPath) // #nosec G304 -- fixed in-repo path
	if err != nil {
		return "", err
	}
	var cfg map[string]any
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return "", err
	}
	hooks, ok := cfg["webhooks"].([]any)
	if !ok || len(hooks) == 0 {
		return "", fmt.Errorf("%s has no webhooks", manifestPath)
	}
	for _, h := range hooks {
		cc := h.(map[string]any)["clientConfig"].(map[string]any)
		svc := cc["service"].(map[string]any)
		svc["name"] = serviceName
		svc["namespace"] = namespace
		cc["caBundle"] = base64.StdEncoding.EncodeToString(caPEM)
	}
	out, err := yaml.Marshal(cfg)
	return string(out), err
}

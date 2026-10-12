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

package deploy

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	// Pinned: the manifest is fetched over the network on every run.
	certManagerManifest = "https://github.com/cert-manager/cert-manager/releases/download/" +
		"v1.21.2/cert-manager.yaml"
	systemNS  = "amphora-system"
	tenantNS  = "tenant-a"
	modelName = "deploy-model"
	repoRoot  = "../.."
)

func run(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).CombinedOutput() // #nosec G204 -- test helper, fixed binaries
	return strings.TrimSpace(string(out)), err
}

func kubectl(args ...string) string {
	GinkgoHelper()
	out, err := run("kubectl", args...)
	Expect(err).NotTo(HaveOccurred(), "kubectl %s:\n%s", strings.Join(args, " "), out)
	return out
}

func kubectlTry(args ...string) (string, error) { return run("kubectl", args...) }

func applyStdin(manifest string) error {
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, out)
	}
	return nil
}

func jsonpath(kind, name, ns, path string) string {
	out, _ := kubectlTry("get", kind, name, "-n", ns, "-o", "jsonpath="+path)
	return out
}

// curlPod runs curl in a throwaway pod and returns stdout.
func curlPod(name, ns string, args ...string) (string, error) {
	defer func() { _, _ = kubectlTry("delete", "pod", name, "-n", ns, "--ignore-not-found", "--now") }()
	runArgs := append([]string{"run", name, "--restart=Never", "-n", ns, "--image=curlimages/curl:8.10.1",
		"--command", "--", "curl"}, args...)
	if out, err := kubectlTry(runArgs...); err != nil {
		return out, err
	}
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		phase, _ := kubectlTry("get", "pod", name, "-n", ns, "-o", "jsonpath={.status.phase}")
		if phase == "Succeeded" || phase == "Failed" {
			logs, _ := kubectlTry("logs", name, "-n", ns)
			if phase == "Failed" {
				return logs, fmt.Errorf("curl failed: %s", logs)
			}
			return logs, nil
		}
		time.Sleep(2 * time.Second)
	}
	return "", fmt.Errorf("pod %s did not finish", name)
}

// overlay renders config/samples/podtls (what an installer would apply) with
// the locally built images substituted, and returns the YAML.
func overlay() string {
	GinkgoHelper()
	// kustomize rejects absolute paths in resources, so the overlay is created
	// inside the repo tree and refers to the shipped config relatively.
	dir, err := os.MkdirTemp(".", "overlay-")
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { _ = os.RemoveAll(dir) })
	rel, err := filepath.Rel(dir, filepath.Join(repoRoot, "config", "samples", "podtls"))
	Expect(err).NotTo(HaveOccurred())
	kustomization := fmt.Sprintf(`resources:
- %s
images:
- name: controller
  newName: amphora-manager
  newTag: deploy
- name: proxy
  newName: amphora-proxy
  newTag: deploy
patches:
# A later --pod-tls-sidecar-image wins over the component's placeholder image.
- target: {kind: Deployment, labelSelector: control-plane=controller-manager}
  patch: |-
    - op: add
      path: /spec/template/spec/containers/0/args/-
      value: --pod-tls-sidecar-image=amphora-podtls:deploy
`, rel)
	Expect(os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(kustomization), 0o600)).To(Succeed())
	kustomize := os.Getenv("KUSTOMIZE")
	if kustomize == "" {
		kustomize = "kustomize"
	}
	out, err := run(kustomize, "build", dir)
	Expect(err).NotTo(HaveOccurred(), "kustomize build:\n%s", out)
	return out
}

var _ = BeforeSuite(func() {
	cluster := os.Getenv("DEPLOY_KIND_CLUSTER")
	Expect(cluster).NotTo(BeEmpty(), "DEPLOY_KIND_CLUSTER unset: run via `make test-deploy`, which owns the cluster")
	Expect(os.Getenv("KUBECONFIG")).NotTo(BeEmpty(), "KUBECONFIG must point at the deploy cluster's own kubeconfig")
	ctxName, err := run("kubectl", "config", "current-context")
	Expect(err).NotTo(HaveOccurred())
	Expect(ctxName).To(Equal("kind-"+cluster), "refusing to run against context %q", ctxName)

	kind := os.Getenv("KIND")
	if kind == "" {
		kind = "kind"
	}
	images := map[string][]string{
		"amphora-manager:deploy":   {"-f", "Dockerfile"},
		"amphora-proxy:deploy":     {"-f", "Dockerfile.proxy"},
		"amphora-podtls:deploy":    {"-f", "Dockerfile.podtls"},
		"amphora-stubmodel:deploy": {"-f", "test/e2e/stubmodel/Dockerfile"},
	}
	for tag, file := range images {
		cmd := exec.Command("docker", append([]string{"build", "-q", "-t", tag}, append(file, ".")...)...)
		cmd.Dir = repoRoot
		out, err := cmd.CombinedOutput()
		Expect(err).NotTo(HaveOccurred(), "docker build %s:\n%s", tag, out)
		out2, err := run(kind, "load", "docker-image", tag, "--name", cluster)
		Expect(err).NotTo(HaveOccurred(), "kind load %s:\n%s", tag, out2)
	}
})

var _ = Describe("amphora installed from the shipped manifests with a real cert-manager", Ordered, func() {
	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			for _, ns := range []string{systemNS, "cert-manager"} {
				pods, _ := kubectlTry("get", "pods", "-n", ns, "-o", "wide")
				_, _ = fmt.Fprintf(GinkgoWriter, "--- pods in %s ---\n%s\n", ns, pods)
			}
			certs, _ := kubectlTry("get", "certificates,certificaterequests,issuers,clusterissuers", "-A")
			_, _ = fmt.Fprintf(GinkgoWriter, "--- certificates ---\n%s\n", certs)
			for _, d := range []string{"deploy/amphora-controller-manager", "deploy/amphora-proxy"} {
				logs, _ := kubectlTry("logs", d, "-n", systemNS, "--tail=60")
				_, _ = fmt.Fprintf(GinkgoWriter, "--- %s ---\n%s\n", d, logs)
			}
			desc, _ := kubectlTry("describe", "pods", "-n", systemNS)
			_, _ = fmt.Fprintf(GinkgoWriter, "--- describe ---\n%s\n", desc)
		}
	})

	It("installs cert-manager and the shipped amphora manifests, and everything becomes Available and Ready", func() {
		By("cert-manager")
		kubectl("apply", "-f", certManagerManifest)
		kubectl("wait", "--for=condition=Available", "deployment", "--all", "-n", "cert-manager", "--timeout=300s")

		By("amphora, rendered from config/samples/podtls")
		manifest := overlay()
		// CRDs and the resources that use them are in one document set; the
		// first pass can race CRD establishment, so retry.
		Eventually(func() error { return applyStdin(manifest) }, 120*time.Second, 5*time.Second).
			Should(Succeed(), "applying the shipped manifests")

		By("the manager and proxy become Available")
		kubectl("wait", "--for=condition=Available", "deployment/amphora-controller-manager",
			"-n", systemNS, "--timeout=300s")
		kubectl("wait", "--for=condition=Available", "deployment/amphora-proxy", "-n", systemNS, "--timeout=300s")

		By("cert-manager issued every certificate the manifests ask for")
		kubectl("wait", "--for=condition=Ready", "certificate", "--all", "-A", "--timeout=180s")
	})

	It("injects the CA into the webhook configuration and enforces the tenancy rules through it", func() {
		Eventually(func() string {
			return jsonpath("validatingwebhookconfiguration", "amphora-validating-webhook-configuration", "",
				"{.webhooks[0].clientConfig.caBundle}")
		}, 60*time.Second, 2*time.Second).ShouldNot(BeEmpty(), "cert-manager never injected the CA bundle")

		Eventually(func() string {
			cmd := exec.Command("kubectl", "apply", "--dry-run=server", "-f", "-")
			cmd.Stdin = strings.NewReader(`apiVersion: amphora.amphora.sh/v1alpha1
kind: ModelDeployment
metadata: {name: bad, namespace: default}
spec: {image: x, tenancyClass: RegulatedMultiTenant}
`)
			out, _ := cmd.CombinedOutput()
			return string(out)
		}, 90*time.Second, 3*time.Second).Should(ContainSubstring("allowedRegions is required"),
			"the webhook did not reject an invalid regulated spec")
	})

	It("serves a model through the TLS sidecar using certificates issued by cert-manager", func() {
		kubectl("label", "node", "--all", "--overwrite", "amphora.amphora.sh/gpu-vram-mb=80000")
		// kind has no NVIDIA runtime; map the class the controller requests to runc.
		runtimeClass := "apiVersion: node.k8s.io/v1\nkind: RuntimeClass\nmetadata: {name: nvidia}\nhandler: runc\n"
		Expect(applyStdin(runtimeClass)).To(Succeed())

		By("preparing a tenant namespace exactly as the shipped manifests describe")
		kubectl("create", "namespace", tenantNS)
		for _, f := range []string{"serving-certificate.yaml", "model-pod-ingress-tls.yaml"} {
			raw, err := os.ReadFile(filepath.Join(repoRoot, "config", "podtls", "tenant", f)) // #nosec G304 -- in-repo path
			Expect(err).NotTo(HaveOccurred())
			cmd := exec.Command("kubectl", "apply", "-n", tenantNS, "-f", "-")
			cmd.Stdin = strings.NewReader(strings.ReplaceAll(string(raw), "NAMESPACE", tenantNS))
			out, err := cmd.CombinedOutput()
			Expect(err).NotTo(HaveOccurred(), string(out))
		}
		kubectl("wait", "--for=condition=Ready", "certificate/amphora-model-tls", "-n", tenantNS, "--timeout=120s")

		By("a ModelDeployment becomes Serving over https")
		Expect(applyStdin(fmt.Sprintf(`apiVersion: amphora.amphora.sh/v1alpha1
kind: ModelDeployment
metadata: {name: %s, namespace: %s}
spec: {image: "amphora-stubmodel:deploy", tenancyClass: SingleTenant}
`, modelName, tenantNS))).To(Succeed())
		Eventually(func() string { return jsonpath("modeldeployment", modelName, tenantNS, "{.status.phase}") },
			4*time.Minute, 3*time.Second).Should(Equal("Serving"))
		endpoint := jsonpath("modeldeployment", modelName, tenantNS, "{.status.endpoint}")
		Expect(endpoint).To(MatchRegexp(`^https://[0-9.]+:8443$`))

		By("and a request through the shipped proxy Service reaches it")
		out, err := curlPod("deploy-curl", "default", "-sS", "-m", "30",
			"-H", fmt.Sprintf("X-Amphora-Model: %s/%s", tenantNS, modelName),
			"http://amphora-proxy.amphora-system.svc:8080/")
		Expect(err).NotTo(HaveOccurred(), out)
		Expect(out).To(ContainSubstring("model=" + modelName))
	})
})

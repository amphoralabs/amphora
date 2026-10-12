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
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	nsDefault      = "default"
	phaseFailed    = "Failed"
	phaseSucceeded = "Succeeded"
	keyName        = "name"
	kubectlBin     = "kubectl"
	verbGet        = "get"
	verbDelete     = "delete"
	verbApply      = "apply"
	verbBuild      = "build"
	kindPod        = "pod"
	kindMD         = "modeldeployment"
	modelKeyPrefix = "default/"
	systemNS       = "amphora-system"
	// Reaches the proxy from inside the cluster; the model header uses the
	// namespace-qualified "<ns>/<name>" key.
	proxyURL = "http://proxy.amphora-system.svc:8080/"
)

// run executes a command and returns combined output. KUBECONFIG is inherited
// from the environment, which BeforeSuite verifies points at the e2e kind
// cluster, so kubectl can never reach another cluster.
func run(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).CombinedOutput() // #nosec G204 -- test helper, fixed binaries
	return strings.TrimSpace(string(out)), err
}

func kubectl(args ...string) string {
	GinkgoHelper()
	out, err := run(kubectlBin, args...)
	Expect(err).NotTo(HaveOccurred(), "kubectl %s:\n%s", strings.Join(args, " "), out)
	return out
}

func kubectlTry(args ...string) (string, error) { return run(kubectlBin, args...) }

// viaProxy issues a request through the proxy from a throwaway in-cluster
// pod. It runs the pod to completion and reads its logs rather than using
// `kubectl run -i --rm`, which can drop the output of a pod that exits
// before the attach completes. A non-zero curl exit (e.g. timeout) is returned
// as an error.
func viaProxy(podName, model string, maxSeconds int) (string, error) {
	return curlPod(podName, maxSeconds, "-sS", "-m", fmt.Sprint(maxSeconds), "-H", "X-Amphora-Model: "+model, proxyURL)
}

// statusViaProxy returns the HTTP status the proxy gives for model, failing
// the spec if the request itself errors (e.g. times out because it was held).
func statusViaProxy(podName, model string) string {
	GinkgoHelper()
	out, err := curlPod(podName, 20, "-sS", "-m", "20", "-o", "/dev/null", "-w", "%{http_code}",
		"-H", "X-Amphora-Model: "+model, proxyURL)
	Expect(err).NotTo(HaveOccurred(), out)
	return out
}

// curlPod runs curl with args in a throwaway in-cluster pod and returns its
// stdout. A non-zero curl exit (e.g. timeout) is returned as an error.
func curlPod(podName string, maxSeconds int, curlArgs ...string) (string, error) {
	defer func() { _, _ = kubectlTry("delete", "pod", podName, "-n", "default", "--ignore-not-found", "--now") }()
	runArgs := append([]string{"run", podName, "--restart=Never", "-n", "default",
		"--image=curlimages/curl:8.10.1", "--command", "--", "curl"}, curlArgs...)
	if out, err := kubectlTry(runArgs...); err != nil {
		return out, err
	}
	deadline := time.Now().Add(time.Duration(maxSeconds+90) * time.Second)
	for time.Now().Before(deadline) {
		phase, _ := kubectlTry("get", "pod", podName, "-n", "default", "-o", "jsonpath={.status.phase}")
		if phase == phaseSucceeded || phase == phaseFailed {
			logs, _ := kubectlTry("logs", podName, "-n", "default")
			if phase == phaseFailed {
				return logs, fmt.Errorf("curl pod failed: %s", logs)
			}
			return logs, nil
		}
		time.Sleep(2 * time.Second)
	}
	return "", fmt.Errorf("curl pod %s did not finish", podName)
}

// secretManifest renders an Opaque Secret from raw byte values.
func secretManifest(name, ns string, kv map[string][]byte) string {
	s := fmt.Sprintf("apiVersion: v1\nkind: Secret\nmetadata: {name: %s, namespace: %s}\ndata:\n", name, ns)
	for k, v := range kv {
		s += fmt.Sprintf("  %s: %s\n", k, base64.StdEncoding.EncodeToString(v))
	}
	return s
}

// curlTLS runs curl in a pod with the named Secret mounted at /c.
func curlTLS(pod, secret string, curlArgs ...string) (string, error) {
	// The container is defined whole in the override: kubectl replaces,
	// rather than merges, the generated container list.
	spec := map[string]any{"spec": map[string]any{
		"restartPolicy": "Never",
		"volumes":       []any{map[string]any{keyName: "c", "secret": map[string]any{"secretName": secret}}},
		"containers": []any{map[string]any{
			keyName: pod, "image": "curlimages/curl:8.10.1", "command": []string{"curl"}, "args": curlArgs,
			"volumeMounts": []any{map[string]any{keyName: "c", "mountPath": "/c"}},
		}},
	}}
	overrides, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	defer func() { _, _ = kubectlTry("delete", "pod", pod, "-n", nsDefault, "--ignore-not-found", "--now") }()
	if out, err := kubectlTry("run", pod, "--restart=Never", "-n", nsDefault,
		"--image=curlimages/curl:8.10.1", "--overrides="+string(overrides)); err != nil {
		return out, err
	}
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		phase, _ := kubectlTry(verbGet, "pod", pod, "-n", nsDefault, "-o", "jsonpath={.status.phase}")
		if phase == phaseSucceeded || phase == phaseFailed {
			logs, _ := kubectlTry("logs", pod, "-n", nsDefault)
			if phase == phaseFailed {
				return logs, fmt.Errorf("curl failed: %s", logs)
			}
			return logs, nil
		}
		time.Sleep(2 * time.Second)
	}
	return "", fmt.Errorf("pod %s did not finish", pod)
}

func field(kind, name, jsonpath string) string {
	out, _ := kubectlTry(verbGet, kind, name, "-n", nsDefault, "-o", "jsonpath="+jsonpath)
	return out
}

var _ = BeforeSuite(func() {
	cluster := os.Getenv("E2E_KIND_CLUSTER")
	Expect(cluster).NotTo(BeEmpty(), "E2E_KIND_CLUSTER unset: run via `make test-e2e`, which owns the cluster")
	Expect(os.Getenv("KUBECONFIG")).NotTo(BeEmpty(), "KUBECONFIG must point at the e2e cluster's own kubeconfig")
	ctxName, err := run(kubectlBin, "config", "current-context")
	Expect(err).NotTo(HaveOccurred())
	Expect(ctxName).To(Equal("kind-"+cluster), "refusing to run against context %q", ctxName)

	kind := os.Getenv("KIND")
	if kind == "" {
		kind = "kind"
	}
	for _, args := range [][]string{
		{verbBuild, "-q", "-t", "amphora-manager:e2e", "."},
		{verbBuild, "-q", "-t", "amphora-proxy:e2e", "-f", "Dockerfile.proxy", "."},
		{verbBuild, "-q", "-t", "amphora-stubmodel:e2e", "-f", "test/e2e/stubmodel/Dockerfile", "."},
		{verbBuild, "-q", "-t", "amphora-podtls:e2e", "-f", "Dockerfile.podtls", "."},
	} {
		cmd := exec.Command("docker", args...)
		cmd.Dir = "../.."
		out, err := cmd.CombinedOutput()
		Expect(err).NotTo(HaveOccurred(), "docker %v:\n%s", args, out)
	}
	images := []string{"amphora-manager:e2e", "amphora-proxy:e2e", "amphora-stubmodel:e2e", "amphora-podtls:e2e"}
	for _, img := range images {
		out, err := run(kind, "load", "docker-image", img, "--name", cluster)
		Expect(err).NotTo(HaveOccurred(), "kind load %s:\n%s", img, out)
	}

	kubectl(verbApply, "-f", "../../config/crd/bases")
	kubectl(verbApply, "-f", "../../config/rbac/role.yaml")
	kubectl(verbApply, "-f", "../../config/rbac/proxy_role.yaml")
	// Metrics auth: TokenReview/SubjectAccessReview for the manager, and the
	// metrics-reader role that authorized scrapers bind to.
	kubectl(verbApply, "-f", "../../config/rbac/auth_proxy_role.yaml")
	kubectl(verbApply, "-f", "../../config/rbac/auth_proxy_client_clusterrole.yaml")
	kubectl(verbApply, "-f", "testdata/deploy.yaml")
	// kind nodes advertise no GPU; declare capacity via the placeholder labels.
	// The region label is the standard topology label the scheduler enforces
	// allowedRegions against.
	kubectl("label", "node", "--all", "--overwrite", "amphora.amphora.sh/gpu-vram-mb=80000",
		"topology.kubernetes.io/region=eu-west-1")

	// The manager pod waits in ContainerCreating until its webhook serving
	// certificate Secret exists. The test mints its own CA (no cert-manager).
	caPEM, certPEM, keyPEM, err := webhookCerts(
		"webhook-service."+systemNS+".svc", "webhook-service."+systemNS+".svc.cluster.local")
	Expect(err).NotTo(HaveOccurred())
	apply(fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: webhook-server-cert
  namespace: %s
type: kubernetes.io/tls
data:
  tls.crt: %s
  tls.key: %s
`, systemNS, base64.StdEncoding.EncodeToString(certPEM), base64.StdEncoding.EncodeToString(keyPEM)))

	kubectl("rollout", "status", "deploy/manager", "-n", systemNS, "--timeout=180s")
	kubectl("rollout", "status", "deploy/proxy", "-n", systemNS, "--timeout=180s")

	// Registered last, from the generated manifest, so nothing is created
	// before the webhook server is up (failurePolicy is Fail).
	webhookConfig, err := webhookConfigFor("../../config/webhook/manifests.yaml", "webhook-service", systemNS, caPEM)
	Expect(err).NotTo(HaveOccurred())
	apply(webhookConfig)
	waitForAdmission()
})

var _ = AfterSuite(func() {
	_, _ = kubectlTry(verbDelete, "modeldeployments,pausepools", "--all", "-n", nsDefault, "--wait=false")
})

func modelDeployment(name string) string {
	return fmt.Sprintf(`apiVersion: amphora.amphora.sh/v1alpha1
kind: ModelDeployment
metadata:
  name: %s
  namespace: default
spec:
  image: amphora-stubmodel:e2e
  tenancyClass: SingleTenant
`, name)
}

func modelDeploymentInRegions(name, regions string) string {
	return modelDeployment(name) + "  allowedRegions: [" + regions + "]\n"
}

func modelDeploymentWithCanary(name, configMap string) string {
	return modelDeployment(name) + fmt.Sprintf(`  evalGate:
    enabled: true
    timeoutMillis: 5000
    canaryConfigMapRef: %s
`, configMap)
}

func canaryConfigMap(name, expected string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: %s
  namespace: default
data:
  canaries.json: '[{"prompt":"2+2=","expected":"%s"}]'
`, name, expected)
}

func intField(kind, name, jsonpath string) int {
	n, _ := strconv.Atoi(field(kind, name, jsonpath))
	return n
}

func singleTenantPool(node string) string {
	return fmt.Sprintf(`apiVersion: amphora.amphora.sh/v1alpha1
kind: PausePool
metadata:
  name: e2e-pool
  namespace: default
spec:
  nodeName: %s
  tenancyClass: SingleTenant
  gpuSlice: full
  targetSize: 1
`, node)
}

// hijackedPods counts live pods owned by the named deployment (hijacked or
// cold-created).
func hijackedPods(md string) int {
	out, _ := kubectlTry(verbGet, "pods", "-n", nsDefault, "-l", "amphora.amphora.sh/hijacked-by="+md,
		"--field-selector=status.phase!=Failed", "-o", keyName)
	return len(strings.Fields(out))
}

// waitForAdmission blocks until a create is admitted end to end through the
// webhook (server-side dry run). Pod readiness is not enough: after a rollout
// the Service can still reject connections for a moment while routing catches
// up, and with failurePolicy Fail that fails every create.
func waitForAdmission() {
	GinkgoHelper()
	Eventually(func() error {
		cmd := exec.Command(kubectlBin, verbApply, "--dry-run=server", "-f", "-")
		cmd.Stdin = strings.NewReader(modelDeployment("admission-warmup"))
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("%w: %s", err, out)
		}
		return nil
	}, 90*time.Second, 2*time.Second).Should(Succeed(), "the admission webhook never became reachable")
}

func apply(manifest string) {
	GinkgoHelper()
	cmd := exec.Command(kubectlBin, verbApply, "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), string(out))
}

func deleteAndWait(kind, name string) {
	GinkgoHelper()
	kubectl(verbDelete, kind, name, "-n", nsDefault, "--ignore-not-found", "--timeout=120s")
}

var _ = Describe("controller + proxy on a real cluster", Ordered, func() {
	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			for _, d := range []string{"manager", "proxy"} {
				logs, _ := kubectlTry("logs", "deploy/"+d, "-n", systemNS, "--tail=80")
				_, _ = fmt.Fprintf(GinkgoWriter, "--- %s logs ---\n%s\n", d, logs)
			}
			md, _ := kubectlTry(verbGet, "modeldeployments,pausepools,pods", "-n", nsDefault, "-o", "wide")
			_, _ = fmt.Fprintf(GinkgoWriter, "--- objects ---\n%s\n", md)
		}
	})

	It("cold-creates a pod, passes the eval gate, defers then routes a request, and stops routing after delete", func() {
		const name = "e2e-cold"

		By("creating a deployment that exists but cannot be placed yet (its only allowed region matches no node)")
		apply(modelDeploymentInRegions(name, "us-east-1"))
		Eventually(func() string { return field(kindMD, name, "{.status.phase}") }, time.Minute, 2*time.Second).
			Should(Equal("Pending"))

		By("a request for it is held open: it exists, it just is not ready (connection deferral)")
		type result struct {
			out string
			err error
		}
		done := make(chan result, 1)
		go func() {
			defer GinkgoRecover()
			out, err := viaProxy("curl-deferred", modelKeyPrefix+name, 115)
			done <- result{out, err}
		}()
		time.Sleep(8 * time.Second) // long enough for the curl pod to start and park

		By("allowing the node's region lets it be placed, cold-created, and gated")
		kubectl("patch", kindMD, name, "-n", nsDefault, "--type=merge", "-p", `{"spec":{"allowedRegions":["eu-west-1"]}}`)

		By("waiting for the eval gate to promote it to Serving")
		Eventually(func() string { return field(kindMD, name, "{.status.phase}") }, 4*time.Minute, 3*time.Second).
			Should(Equal("Serving"))
		Expect(field(kindMD, name, "{.status.endpoint}")).To(HavePrefix("http://"))
		Expect(field(kindPod, name+"-serve", "{.metadata.labels.amphora\\.amphora\\.sh/cold-start}")).To(Equal("true"))

		By("the deferred request completes and reached the right model")
		var r result
		Eventually(done, 2*time.Minute).Should(Receive(&r))
		Expect(r.err).NotTo(HaveOccurred(), r.out)
		Expect(r.out).To(ContainSubstring("model=" + name))

		By("deleting the deployment makes the model unknown: an immediate 404, not a held connection")
		deleteAndWait(kindMD, name)
		Eventually(func() string { return statusViaProxy("curl-after-delete", modelKeyPrefix+name) },
			30*time.Second, 2*time.Second).Should(Equal("404"))
	})

	It("answers 404 immediately for a model that does not exist instead of parking the connection", func() {
		// The proxy's deferral timeout is 120s; a held connection would hit
		// curl's 20s limit and fail, so a clean 404 proves it was not parked.
		Expect(statusViaProxy("curl-unknown", modelKeyPrefix+"does-not-exist")).To(Equal("404"))
	})

	It("hijacks an idle PausePool pod and the hijacked container learns its model identity", func() {
		const md = "e2e-hijack"
		node := kubectl(verbGet, "nodes", "-o", "jsonpath={.items[0].metadata.name}")

		apply(fmt.Sprintf(`apiVersion: amphora.amphora.sh/v1alpha1
kind: PausePool
metadata:
  name: e2e-pool
  namespace: default
spec:
  nodeName: %s
  tenancyClass: SingleTenant
  gpuSlice: full
  targetSize: 1
`, node))
		poolSize := func() string { return field("pausepool", "e2e-pool", "{.status.currentSize}") }
		Eventually(poolSize, time.Minute, 2*time.Second).Should(Equal("1"))

		apply(modelDeployment(md))
		Eventually(func() string { return field(kindMD, md, "{.status.phase}") }, 4*time.Minute, 3*time.Second).
			Should(Equal("Serving"))

		pod := field(kindMD, md, "{.status.activePod}")
		Expect(pod).To(HavePrefix("e2e-pool-pause-"), "expected a hijacked pool pod, not a cold-created one")
		Expect(field(kindPod, pod, "{.metadata.labels.amphora\\.amphora\\.sh/hijacked-by}")).To(Equal(md))

		By("the downward-API env var resolved to the model after the image swap restarted the container")
		out, err := viaProxy("curl-hijack", modelKeyPrefix+md, 60)
		Expect(err).NotTo(HaveOccurred(), out)
		Expect(out).To(ContainSubstring("model=" + md))

		deleteAndWait(kindMD, md)
		deleteAndWait("pausepool", "e2e-pool")
	})

	It("verifies quality with canary prompts: a matching canary is promoted with QualityVerified=True", func() {
		const md = "e2e-canary-ok"
		apply(canaryConfigMap("canary-ok", "4"))
		apply(modelDeploymentWithCanary(md, "canary-ok"))
		Eventually(func() string { return field(kindMD, md, "{.status.phase}") }, 4*time.Minute, 3*time.Second).
			Should(Equal("Serving"))
		Expect(field(kindMD, md, "{.status.conditions[?(@.type=='QualityVerified')].status}")).To(Equal("True"))
		Expect(field(kindMD, md, "{.status.conditions[?(@.type=='QualityVerified')].reason}")).To(Equal("CanaryPassed"))
		deleteAndWait(kindMD, md)
		deleteAndWait("configmap", "canary-ok")
	})

	It("a wrong answer never reaches Serving: it rolls back and trips the circuit breaker", func() {
		const md = "e2e-canary-bad"
		apply(canaryConfigMap("canary-bad", "5")) // the stub answers "4"
		apply(modelDeploymentWithCanary(md, "canary-bad"))

		Eventually(func() string { return field(kindMD, md, "{.status.evalFailures}") }, 3*time.Minute, 2*time.Second).
			ShouldNot(BeElementOf("", "0"))
		Expect(field(kindMD, md, "{.status.conditions[?(@.type=='QualityVerified')].reason}")).To(Equal("CanaryFailed"))

		By("it is never promoted to Serving, and after repeated failures promotion pauses")
		Consistently(func() string { return field(kindMD, md, "{.status.phase}") }, 8*time.Second, 2*time.Second).
			ShouldNot(Equal("Serving"))
		Eventually(func() string { return field(kindMD, md, "{.status.phase}") }, 3*time.Minute, 3*time.Second).
			Should(Equal("PromotionPaused"))
		Expect(field(kindMD, md, "{.status.endpoint}")).To(BeEmpty())
		out, err := viaProxy("curl-canary-bad", modelKeyPrefix+md, 5)
		Expect(err).To(HaveOccurred(), "an unverified model must not receive traffic: %s", out)

		deleteAndWait(kindMD, md)
		deleteAndWait("configmap", "canary-bad")
	})

	It("refills the pool after a hijack, and keeps serving from the hijacked pod", func() {
		const md = "e2e-refill"
		node := kubectl(verbGet, "nodes", "-o", "jsonpath={.items[0].metadata.name}")
		apply(singleTenantPool(node))
		poolSize := func() string { return field("pausepool", "e2e-pool", "{.status.currentSize}") }
		Eventually(poolSize, time.Minute, 2*time.Second).Should(Equal("1"))
		idle := field("pausepool", "e2e-pool", "{.status.pausePodNames[0]}")

		apply(modelDeployment(md))
		Eventually(func() string { return field(kindMD, md, "{.status.phase}") }, 4*time.Minute, 3*time.Second).
			Should(Equal("Serving"))
		Expect(field(kindMD, md, "{.status.activePod}")).To(Equal(idle), "the pool's idle pod should have been hijacked")

		By("the pool creates a fresh idle pod to replace the hijacked one")
		Eventually(func() string { return field("pausepool", "e2e-pool", "{.status.pausePodNames[0]}") },
			time.Minute, 2*time.Second).ShouldNot(BeElementOf("", idle))
		Expect(poolSize()).To(Equal("1"))

		By("the hijacked pod is untouched and still serves")
		out, err := viaProxy("curl-refill", modelKeyPrefix+md, 30)
		Expect(err).NotTo(HaveOccurred(), out)
		Expect(out).To(ContainSubstring("model=" + md))

		deleteAndWait(kindMD, md)
		deleteAndWait("pausepool", "e2e-pool")
	})

	It("rolls back failing hijacked pods repeatedly without orphaning pods, refilling the pool each time", func() {
		const md = "e2e-hijack-bad"
		node := kubectl(verbGet, "nodes", "-o", "jsonpath={.items[0].metadata.name}")
		apply(singleTenantPool(node))
		poolSize := func() string { return field("pausepool", "e2e-pool", "{.status.currentSize}") }
		Eventually(poolSize, time.Minute, 2*time.Second).Should(Equal("1"))

		apply(canaryConfigMap("canary-bad2", "5")) // the stub answers "4"
		apply(modelDeploymentWithCanary(md, "canary-bad2"))

		By("several fail-closed cycles happen, and the deployment never reaches Serving")
		Eventually(func() int { return intField(kindMD, md, "{.status.evalFailures}") }, 3*time.Minute, 2*time.Second).
			Should(BeNumerically(">=", 2))
		Consistently(func() string { return field(kindMD, md, "{.status.phase}") }, 6*time.Second, 2*time.Second).
			ShouldNot(Equal("Serving"))
		Eventually(func() string { return field(kindMD, md, "{.status.phase}") }, 3*time.Minute, 3*time.Second).
			Should(Equal("PromotionPaused"))

		By("every failed cycle's pod was deleted: at most the one currently held pod remains")
		Expect(hijackedPods(md)).To(BeNumerically("<=", 1))
		By("and the pool was refilled after each consumption")
		Eventually(poolSize, time.Minute, 2*time.Second).Should(Equal("1"))

		deleteAndWait(kindMD, md)
		deleteAndWait("configmap", "canary-bad2")
		deleteAndWait("pausepool", "e2e-pool")
	})

	Context("residency: allowedRegions is enforced against the node region label", func() {
		It("places a deployment whose allowed region matches the node, and routes to it", func() {
			const md = "e2e-region-ok"
			apply(modelDeploymentInRegions(md, "eu-west-1"))
			Eventually(func() string { return field(kindMD, md, "{.status.phase}") }, 4*time.Minute, 3*time.Second).
				Should(Equal("Serving"))
			out, err := viaProxy("curl-region-ok", modelKeyPrefix+md, 30)
			Expect(err).NotTo(HaveOccurred(), out)
			Expect(out).To(ContainSubstring("model=" + md))
			deleteAndWait(kindMD, md)
		})

		It("never places a deployment whose allowed regions exclude every node", func() {
			const md = "e2e-region-none"
			apply(modelDeploymentInRegions(md, "us-east-1"))
			Eventually(func() string { return field(kindMD, md, "{.status.phase}") }, time.Minute, 2*time.Second).
				Should(Equal("Pending"))
			Consistently(func() string {
				_, err := kubectlTry(verbGet, kindPod, md+"-serve", "-n", nsDefault)
				if err != nil {
					return "absent"
				}
				return "present"
			}, 10*time.Second, 2*time.Second).Should(Equal("absent"), "no pod may exist outside the allowed regions")
			deleteAndWait(kindMD, md)
		})

		It("evicts a running deployment when allowedRegions is changed to exclude its node, and stops routing", func() {
			const md = "e2e-region-evict"
			apply(modelDeploymentInRegions(md, "eu-west-1"))
			Eventually(func() string { return field(kindMD, md, "{.status.phase}") }, 4*time.Minute, 3*time.Second).
				Should(Equal("Serving"))

			kubectl("patch", kindMD, md, "-n", nsDefault, "--type=merge", "-p", `{"spec":{"allowedRegions":["us-east-1"]}}`)

			Eventually(func() string { return field(kindMD, md, "{.status.phase}") }, time.Minute, 2*time.Second).
				Should(Equal("Pending"))
			Expect(field(kindMD, md, "{.status.endpoint}")).To(BeEmpty())
			_, err := kubectlTry(verbGet, kindPod, md+"-serve", "-n", nsDefault)
			Expect(err).To(HaveOccurred(), "the out-of-region pod must have been deleted")
			out, err := viaProxy("curl-region-evicted", modelKeyPrefix+md, 5)
			Expect(err).To(HaveOccurred(), "traffic must stop once evicted: %s", out)
			deleteAndWait(kindMD, md)
		})
	})

	Context("admission webhook (real TLS, real apiserver)", func() {
		// rejected applies a manifest and requires the apiserver to refuse it.
		rejected := func(manifest string, wantInMessage ...string) {
			GinkgoHelper()
			cmd := exec.Command(kubectlBin, verbApply, "-f", "-")
			cmd.Stdin = strings.NewReader(manifest)
			out, err := cmd.CombinedOutput()
			Expect(err).To(HaveOccurred(), "expected admission to reject, but it was admitted: %s", out)
			Expect(string(out)).To(ContainSubstring("admission webhook"))
			for _, want := range wantInMessage {
				Expect(string(out)).To(ContainSubstring(want))
			}
		}

		It("rejects a RegulatedMultiTenant ModelDeployment missing gpuFraction and allowedRegions, reporting both", func() {
			rejected(strings.Replace(modelDeployment("wh-regulated-bad"), "SingleTenant", "RegulatedMultiTenant", 1),
				"gpuFraction is required", "allowedRegions is required")
			_, getErr := kubectlTry(verbGet, kindMD, "wh-regulated-bad", "-n", nsDefault)
			Expect(getErr).To(HaveOccurred(), "rejected object must not exist")
		})

		It("rejects a malformed gpuFraction", func() {
			rejected(modelDeployment("wh-badfrac")+"  gpuFraction: half\n", "spec.gpuFraction")
		})

		It("rejects a PausePool whose slice violates the tenancy matrix", func() {
			rejected(strings.Replace(singleTenantPool("any-node"), "SingleTenant", "RegulatedMultiTenant", 1)+"",
				"violates tenancy isolation matrix")
		})

		It("admits a valid regulated spec, then rejects a breaking update", func() {
			const name = "wh-regulated-ok"
			valid := strings.Replace(modelDeployment(name), "SingleTenant", "RegulatedMultiTenant", 1) +
				"  gpuFraction: 1g.10gb\n  allowedRegions: [eu-west-1]\n"
			cmd := exec.Command(kubectlBin, verbApply, "-f", "-")
			cmd.Stdin = strings.NewReader(valid)
			out, err := cmd.CombinedOutput()
			Expect(err).NotTo(HaveOccurred(), string(out))
			Expect(string(out)).NotTo(ContainSubstring("not yet enforced"), "the old warning must be gone")

			By("an update that removes allowedRegions is refused")
			patchOut, patchErr := kubectlTry("patch", kindMD, name, "-n", nsDefault, "--type=merge",
				"-p", `{"spec":{"allowedRegions":[]}}`)
			Expect(patchErr).To(HaveOccurred(), "update was admitted: %s", patchOut)
			Expect(patchOut).To(ContainSubstring("allowedRegions is required"))

			deleteAndWait(kindMD, name)
		})
	})

	Context("metrics endpoint (served by the manager over https, authn/authz enforced)", func() {
		const metricsURL = "https://manager-metrics.amphora-system.svc:8443/metrics"
		code := func(pod, token string) string {
			GinkgoHelper()
			args := []string{"-sk", "-m", "20", "-o", "/dev/null", "-w", "%{http_code}"}
			if token != "" {
				args = append(args, "-H", "Authorization: Bearer "+token)
			}
			out, err := curlPod(pod, 20, append(args, metricsURL)...)
			Expect(err).NotTo(HaveOccurred(), out)
			return out
		}

		It("returns 401 anonymously, 403 without access, and 200 for an authorized caller", func() {
			apply(`apiVersion: v1
kind: ServiceAccount
metadata: {name: metrics-denied, namespace: default}
---
apiVersion: v1
kind: ServiceAccount
metadata: {name: metrics-allowed, namespace: default}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata: {name: e2e-metrics-allowed}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: metrics-reader}
subjects:
- {kind: ServiceAccount, name: metrics-allowed, namespace: default}
`)
			DeferCleanup(func() {
				_, _ = kubectlTry("delete", "clusterrolebinding", "e2e-metrics-allowed", "--ignore-not-found")
				_, _ = kubectlTry("delete", "serviceaccount", "metrics-denied", "metrics-allowed",
					"-n", "default", "--ignore-not-found")
			})
			denied := kubectl("create", "token", "metrics-denied", "-n", "default")
			allowed := kubectl("create", "token", "metrics-allowed", "-n", "default")

			Expect(code("curl-metrics-anon", "")).To(Equal("401"))
			Expect(code("curl-metrics-denied", denied)).To(Equal("403"))
			Expect(code("curl-metrics-ok", allowed)).To(Equal("200"))
		})
	})

	It("keeps a Serving deployment, and its node's capacity accounting, across a manager restart", func() {
		const first, second = "e2e-restart-1", "e2e-restart-2"
		apply(modelDeployment(first))
		Eventually(func() string { return field(kindMD, first, "{.status.phase}") }, 4*time.Minute, 3*time.Second).
			Should(Equal("Serving"))
		podUID := field(kindPod, first+"-serve", "{.metadata.uid}")
		Expect(podUID).NotTo(BeEmpty())

		By("restarting the manager: its in-memory scheduler starts empty")
		kubectl("rollout", "restart", "deploy/manager", "-n", systemNS)
		kubectl("rollout", "status", "deploy/manager", "-n", systemNS, "--timeout=180s")

		By("the running deployment is re-adopted in place: still Serving, same pod, nothing evicted")
		Consistently(func() string { return field(kindPod, first+"-serve", "{.metadata.uid}") },
			20*time.Second, 4*time.Second).Should(Equal(podUID))
		Expect(field(kindMD, first, "{.status.phase}")).To(Equal("Serving"))

		By("and the node is still accounted for: a second SingleTenant cannot share it")
		apply(modelDeployment(second))
		Eventually(func() string { return field(kindMD, second, "{.status.phase}") }, time.Minute, 2*time.Second).
			Should(Equal("Pending"))
		_, err := kubectlTry(verbGet, kindPod, second+"-serve", "-n", nsDefault)
		Expect(err).To(HaveOccurred(), "a second SingleTenant pod must not exist on the occupied node")

		deleteAndWait(kindMD, second)
		deleteAndWait(kindMD, first)
	})

	Context("client identity: mTLS at the proxy, tenant taken from the certificate", func() {
		const (
			mtlsHost = "https://proxy-mtls.amphora-system.svc:8443/"
			model    = "e2e-mtls"
		)
		status := func(pod, secret, modelKey string, extra ...string) string {
			GinkgoHelper()
			args := append([]string{"-sS", "-m", "20", "--cacert", "/c/ca.crt", "-o", "/dev/null", "-w", "%{http_code}",
				"-H", "X-Amphora-Model: " + modelKey}, extra...)
			out, err := curlTLS(pod, secret, append(args, mtlsHost)...)
			Expect(err).NotTo(HaveOccurred(), out)
			return out
		}
		withCert := []string{"--cert", "/c/tls.crt", "--key", "/c/tls.key"}

		BeforeAll(func() {
			ca, err := newTestCA("e2e-clients-ca")
			Expect(err).NotTo(HaveOccurred())
			srvCert, srvKey, err := ca.issue("proxy-mtls", x509.ExtKeyUsageServerAuth,
				"proxy-mtls.amphora-system.svc", "proxy-mtls.amphora-system.svc.cluster.local")
			Expect(err).NotTo(HaveOccurred())
			secret := func(name, ns string, kv map[string][]byte) string {
				s := fmt.Sprintf("apiVersion: v1\nkind: Secret\nmetadata: {name: %s, namespace: %s}\ndata:\n", name, ns)
				for k, v := range kv {
					s += fmt.Sprintf("  %s: %s\n", k, base64.StdEncoding.EncodeToString(v))
				}
				return s
			}
			manifest := secret("proxy-mtls-tls", systemNS, map[string][]byte{
				"tls.crt": srvCert, "tls.key": srvKey, "ca.crt": ca.PEM,
			})
			for _, tenant := range []string{"default", "other"} {
				cert, key, err := ca.issue(tenant, x509.ExtKeyUsageClientAuth)
				Expect(err).NotTo(HaveOccurred())
				// ca.crt here is what the client uses to verify the proxy.
				manifest += "---\n" + secret("mtls-client-"+tenant, nsDefault, map[string][]byte{
					"tls.crt": cert, "tls.key": key, "ca.crt": ca.PEM,
				})
			}
			manifest += `---
apiVersion: apps/v1
kind: Deployment
metadata: {name: proxy-mtls, namespace: amphora-system}
spec:
  replicas: 1
  selector: {matchLabels: {app: proxy-mtls}}
  template:
    metadata: {labels: {app: proxy-mtls}}
    spec:
      serviceAccountName: proxy
      containers:
      - name: proxy
        image: amphora-proxy:e2e
        imagePullPolicy: IfNotPresent
        args:
        - -sync-from-cluster
        - -deferral-timeout=20s
        - -tls-cert=/tls/tls.crt
        - -tls-key=/tls/tls.key
        - -tls-client-ca=/tls/ca.crt
        ports: [{containerPort: 8080}]
        volumeMounts: [{name: tls, mountPath: /tls, readOnly: true}]
      volumes: [{name: tls, secret: {secretName: proxy-mtls-tls}}]
---
apiVersion: v1
kind: Service
metadata: {name: proxy-mtls, namespace: amphora-system}
spec:
  selector: {app: proxy-mtls}
  ports: [{port: 8443, targetPort: 8080}]
`
			apply(manifest)
			kubectl("rollout", "status", "deploy/proxy-mtls", "-n", systemNS, "--timeout=120s")

			apply(modelDeployment(model))
			Eventually(func() string { return field(kindMD, model, "{.status.phase}") }, 4*time.Minute, 3*time.Second).
				Should(Equal("Serving"))
		})

		AfterAll(func() {
			deleteAndWait(kindMD, model)
			_, _ = kubectlTry(verbDelete, "deploy,svc,secret", "proxy-mtls", "-n", systemNS, "--ignore-not-found")
			_, _ = kubectlTry(verbDelete, "secret", "proxy-mtls-tls", "-n", systemNS, "--ignore-not-found")
			_, _ = kubectlTry(verbDelete, "secret", "mtls-client-default", "mtls-client-other",
				"-n", nsDefault, "--ignore-not-found")
		})

		It("rejects a caller with no client certificate (401)", func() {
			Expect(status("mtls-anon", "mtls-client-default", modelKeyPrefix+model)).To(Equal("401"))
		})

		It("serves a tenant its own model, and the model sees the request", func() {
			args := make([]string, 0, 8+len(withCert))
			args = append(args, "-sS", "-m", "20", "--cacert", "/c/ca.crt")
			args = append(args, withCert...)
			args = append(args, "-H", "X-Amphora-Model: "+modelKeyPrefix+model, mtlsHost)
			out, err := curlTLS("mtls-own", "mtls-client-default", args...)
			Expect(err).NotTo(HaveOccurred(), out)
			Expect(out).To(ContainSubstring("model=" + model))
		})

		It("refuses another tenant's model (403), and a spoofed tenant header (403)", func() {
			Expect(status("mtls-other", "mtls-client-other", modelKeyPrefix+model, withCert...)).To(Equal("403"))
			Expect(status("mtls-spoof", "mtls-client-default", modelKeyPrefix+model,
				append([]string{"-H", "X-Amphora-Tenant: other"}, withCert...)...)).To(Equal("403"))
		})
	})

	Context("network isolation of model pods (NetworkPolicy)", func() {
		It("blocks direct access to a model pod from outside amphora-system, while the proxy still works", func() {
			const md = "e2e-netpol"
			apply(modelDeployment(md))
			DeferCleanup(func() {
				_, _ = kubectlTry(verbDelete, "networkpolicy", "amphora-model-pod-ingress", "-n", nsDefault, "--ignore-not-found")
				deleteAndWait(kindMD, md)
			})
			Eventually(func() string { return field(kindMD, md, "{.status.phase}") }, 4*time.Minute, 3*time.Second).
				Should(Equal("Serving"))
			ip := field(kindPod, md+"-serve", "{.status.podIP}")
			Expect(ip).NotTo(BeEmpty())
			direct := fmt.Sprintf("http://%s:8000/health", ip)

			By("control: before the policy, any pod can reach the model pod directly")
			out, err := curlPod("curl-direct-before", 15, "-sS", "-m", "10", "-o", "/dev/null", "-w", "%{http_code}", direct)
			Expect(err).NotTo(HaveOccurred(), out)
			Expect(out).To(Equal("200"))

			By("after applying the shipped policy, direct access from the tenant namespace is blocked")
			kubectl(verbApply, "-n", nsDefault, "-f", "../../config/networkpolicy/model-pod-ingress.yaml")
			Eventually(func() error {
				_, err := curlPod("curl-direct-after", 10, "-sS", "-m", "5", "-o", "/dev/null", direct)
				return err
			}, 60*time.Second, 2*time.Second).Should(HaveOccurred(),
				"the policy never took effect: does the CNI enforce NetworkPolicy?")

			By("while the proxy (in amphora-system) can still reach and serve it")
			out, err = viaProxy("curl-proxied-after", modelKeyPrefix+md, 20)
			Expect(err).NotTo(HaveOccurred(), out)
			Expect(out).To(ContainSubstring("model=" + md))
		})
	})

	Context("encrypted, authenticated transport to model pods (TLS sidecar)", func() {
		const (
			md        = "e2e-podtls"
			podServer = "default.model.amphora.internal"
			proxyHost = "http://proxy-podtls.amphora-system.svc:8080/"
		)
		curlPodDirect := func(pod, secret, ip string, curlArgs ...string) (string, error) {
			args := append([]string{"-sS", "-m", "10", "--cacert", "/c/ca.crt",
				"--resolve", fmt.Sprintf("%s:8443:%s", podServer, ip)}, curlArgs...)
			return curlTLS(pod, secret, append(args, "https://"+podServer+":8443/health")...)
		}

		BeforeAll(func() {
			ca, err := newTestCA("e2e-pod-ca")
			Expect(err).NotTo(HaveOccurred())
			srvCert, srvKey, err := ca.issue(podServer, x509.ExtKeyUsageServerAuth, podServer)
			Expect(err).NotTo(HaveOccurred())
			client := func(cn string) map[string][]byte {
				cert, key, err := ca.issue(cn, x509.ExtKeyUsageClientAuth)
				Expect(err).NotTo(HaveOccurred())
				return map[string][]byte{"tls.crt": cert, "tls.key": key, "ca.crt": ca.PEM}
			}
			apply(secretManifest("amphora-model-tls", nsDefault, map[string][]byte{
				"tls.crt": srvCert, "tls.key": srvKey, "ca.crt": ca.PEM,
			}) + "---\n" +
				secretManifest("e2e-controller-client", systemNS, client("amphora-controller")) + "---\n" +
				secretManifest("e2e-proxy-client", systemNS, client("amphora-proxy")) + "---\n" +
				// What a test caller in the default namespace presents when
				// connecting to a pod directly.
				secretManifest("podtls-caller-proxy", nsDefault, client("amphora-proxy")) + "---\n" +
				secretManifest("podtls-caller-stranger", nsDefault, client("some-other-workload")) + "---\n" +
				secretManifest("podtls-caller-none", nsDefault, map[string][]byte{"ca.crt": ca.PEM}))

			By("turning on the sidecar in the manager")
			kubectl("patch", "deploy/manager", "-n", systemNS, "--type=json", "-p", `[
 {"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--pod-tls-sidecar-image=amphora-podtls:e2e"},
 {"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--pod-client-cert=/pod-tls/tls.crt"},
 {"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--pod-client-key=/pod-tls/tls.key"},
 {"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--pod-ca=/pod-tls/ca.crt"},
 {"op":"add","path":"/spec/template/spec/containers/0/volumeMounts/-",
  "value":{"name":"pod-tls","mountPath":"/pod-tls","readOnly":true}},
 {"op":"add","path":"/spec/template/spec/volumes/-",
  "value":{"name":"pod-tls","secret":{"secretName":"e2e-controller-client"}}}
]`)
			kubectl("rollout", "status", "deploy/manager", "-n", systemNS, "--timeout=180s")
			waitForAdmission()

			By("a proxy that reaches pods over mutual TLS")
			apply(`apiVersion: apps/v1
kind: Deployment
metadata: {name: proxy-podtls, namespace: amphora-system}
spec:
  replicas: 1
  selector: {matchLabels: {app: proxy-podtls}}
  template:
    metadata: {labels: {app: proxy-podtls}}
    spec:
      serviceAccountName: proxy
      containers:
      - name: proxy
        image: amphora-proxy:e2e
        imagePullPolicy: IfNotPresent
        args:
        - -sync-from-cluster
        - -deferral-timeout=20s
        - -upstream-client-cert=/up/tls.crt
        - -upstream-client-key=/up/tls.key
        - -upstream-ca=/up/ca.crt
        ports: [{containerPort: 8080}]
        volumeMounts: [{name: up, mountPath: /up, readOnly: true}]
      volumes: [{name: up, secret: {secretName: e2e-proxy-client}}]
---
apiVersion: v1
kind: Service
metadata: {name: proxy-podtls, namespace: amphora-system}
spec:
  selector: {app: proxy-podtls}
  ports: [{port: 8080, targetPort: 8080}]
`)
			kubectl("rollout", "status", "deploy/proxy-podtls", "-n", systemNS, "--timeout=120s")
		})

		AfterAll(func() {
			deleteAndWait(kindMD, md)
			_, _ = kubectlTry(verbDelete, "networkpolicy", "amphora-model-pod-ingress", "-n", nsDefault, "--ignore-not-found")
			_, _ = kubectlTry(verbDelete, "ns", "podtls-nosecret", "--ignore-not-found", "--wait=false")
			_, _ = kubectlTry(verbDelete, "deploy,svc", "proxy-podtls", "-n", systemNS, "--ignore-not-found")
			_, _ = kubectlTry("rollout", "undo", "deploy/manager", "-n", systemNS)
			_, _ = kubectlTry("rollout", "status", "deploy/manager", "-n", systemNS, "--timeout=180s")
			_, _ = kubectlTry(verbDelete, "secret", "e2e-controller-client", "e2e-proxy-client",
				"-n", systemNS, "--ignore-not-found")
			_, _ = kubectlTry(verbDelete, "secret", "amphora-model-tls", "podtls-caller-proxy", "podtls-caller-stranger",
				"podtls-caller-none", "-n", nsDefault, "--ignore-not-found")
		})

		It("serves traffic through the sidecar, and the endpoint is https on the sidecar port only", func() {
			apply(modelDeployment(md))
			Eventually(func() string { return field(kindMD, md, "{.status.phase}") }, 4*time.Minute, 3*time.Second).
				Should(Equal("Serving"))
			Expect(field(kindMD, md, "{.status.endpoint}")).To(MatchRegexp(`^https://[0-9.]+:8443$`))
			Expect(field(kindPod, md+"-serve", "{.spec.containers[*].name}")).To(Equal("model amphora-tls"))

			out, err := curlPod("curl-podtls-proxied", 30, "-sS", "-m", "20",
				"-H", "X-Amphora-Model: "+modelKeyPrefix+md, proxyHost)
			Expect(err).NotTo(HaveOccurred(), out)
			Expect(out).To(ContainSubstring("model=" + md))
		})

		It("authenticates callers at the sidecar: no certificate and an unlisted identity never reach the model", func() {
			ip := field(kindPod, md+"-serve", "{.status.podIP}")
			Expect(ip).NotTo(BeEmpty())

			_, err := curlPodDirect("podtls-none", "podtls-caller-none", ip)
			Expect(err).To(HaveOccurred(), "a caller with no client certificate was served")

			out, err := curlPodDirect("podtls-stranger", "podtls-caller-stranger", ip,
				"--cert", "/c/tls.crt", "--key", "/c/tls.key", "-o", "/dev/null", "-w", "%{http_code}")
			Expect(err).NotTo(HaveOccurred(), out)
			Expect(out).To(Equal("403"), "a valid certificate with an identity not on the allow-list")

			out, err = curlPodDirect("podtls-proxy", "podtls-caller-proxy", ip,
				"--cert", "/c/tls.crt", "--key", "/c/tls.key", "-o", "/dev/null", "-w", "%{http_code}")
			Expect(err).NotTo(HaveOccurred(), out)
			Expect(out).To(Equal("200"))
		})

		It("with the TLS-only policy, the plaintext model port and direct callers are unreachable", func() {
			kubectl(verbApply, "-n", nsDefault, "-f", "../../config/podtls/tenant/model-pod-ingress-tls.yaml")
			ip := field(kindPod, md+"-serve", "{.status.podIP}")
			Eventually(func() error {
				_, err := curlPod("podtls-plain-after", 10, "-sS", "-m", "5", "-o", "/dev/null",
					fmt.Sprintf("http://%s:8000/health", ip))
				return err
			}, 60*time.Second, 2*time.Second).Should(HaveOccurred(), "the model's plaintext port is still reachable")

			out, err := curlPod("curl-podtls-after-policy", 30, "-sS", "-m", "20",
				"-H", "X-Amphora-Model: "+modelKeyPrefix+md, proxyHost)
			Expect(err).NotTo(HaveOccurred(), out)
			Expect(out).To(ContainSubstring("model=" + md))
			deleteAndWait(kindMD, md)
		})

		It("fails closed in a namespace with no serving certificate: the pod never starts, so nothing is served", func() {
			kubectl("create", "namespace", "podtls-nosecret")
			apply(strings.Replace(modelDeployment("e2e-nosecret"), "namespace: default", "namespace: podtls-nosecret", 1))
			Eventually(func() string {
				return strings.TrimSpace(func() string {
					out, _ := kubectlTry(verbGet, kindPod, "e2e-nosecret-serve", "-n", "podtls-nosecret",
						"-o", "jsonpath={.status.phase}")
					return out
				}())
			}, time.Minute, 2*time.Second).Should(Equal("Pending"))
			Consistently(func() string {
				out, _ := kubectlTry(verbGet, kindMD, "e2e-nosecret", "-n", "podtls-nosecret", "-o", "jsonpath={.status.phase}")
				return out
			}, 15*time.Second, 3*time.Second).ShouldNot(Equal("Serving"))
		})
	})
})

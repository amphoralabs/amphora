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
	"encoding/base64"
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
	defer func() {
		_, _ = kubectlTry(verbDelete, kindPod, podName, "-n", nsDefault, "--ignore-not-found", "--now")
	}()
	if out, err := kubectlTry("run", podName, "--restart=Never", "-n", nsDefault,
		"--image=curlimages/curl:8.10.1", "--command", "--",
		"curl", "-sS", "-m", fmt.Sprint(maxSeconds), "-H", "X-Amphora-Model: "+model, proxyURL); err != nil {
		return out, err
	}
	deadline := time.Now().Add(time.Duration(maxSeconds+90) * time.Second)
	for time.Now().Before(deadline) {
		phase, _ := kubectlTry(verbGet, kindPod, podName, "-n", nsDefault, "-o", "jsonpath={.status.phase}")
		if phase == "Succeeded" || phase == "Failed" {
			logs, _ := kubectlTry("logs", podName, "-n", nsDefault)
			if phase == "Failed" {
				return logs, fmt.Errorf("curl pod failed: %s", logs)
			}
			return logs, nil
		}
		time.Sleep(2 * time.Second)
	}
	return "", fmt.Errorf("curl pod %s did not finish", podName)
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
	} {
		cmd := exec.Command("docker", args...)
		cmd.Dir = "../.."
		out, err := cmd.CombinedOutput()
		Expect(err).NotTo(HaveOccurred(), "docker %v:\n%s", args, out)
	}
	for _, img := range []string{"amphora-manager:e2e", "amphora-proxy:e2e", "amphora-stubmodel:e2e"} {
		out, err := run(kind, "load", "docker-image", img, "--name", cluster)
		Expect(err).NotTo(HaveOccurred(), "kind load %s:\n%s", img, out)
	}

	kubectl(verbApply, "-f", "../../config/crd/bases")
	kubectl(verbApply, "-f", "../../config/rbac/role.yaml")
	kubectl(verbApply, "-f", "../../config/rbac/proxy_role.yaml")
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
		"--field-selector=status.phase!=Failed", "-o", "name")
	return len(strings.Fields(out))
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

		By("sending a request BEFORE the model exists: the proxy must hold it open (connection deferral)")
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
		time.Sleep(5 * time.Second)

		By("creating the ModelDeployment")
		apply(modelDeployment(name))

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

		By("deleting the deployment makes the proxy stop routing to it")
		deleteAndWait(kindMD, name)
		out, err := viaProxy("curl-after-delete", modelKeyPrefix+name, 5)
		Expect(err).To(HaveOccurred(), "request unexpectedly succeeded after delete: %s", out)
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
})

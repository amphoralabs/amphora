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
	"fmt"
	"os"
	"os/exec"
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
	kubectl("label", "node", "--all", "--overwrite", "amphora.amphora.sh/gpu-vram-mb=80000")
	kubectl("rollout", "status", "deploy/manager", "-n", systemNS, "--timeout=180s")
	kubectl("rollout", "status", "deploy/proxy", "-n", systemNS, "--timeout=180s")
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
})

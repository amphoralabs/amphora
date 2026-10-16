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
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
	"github.com/ramin-fazli/amphora/internal/scheduler"
)

const gpuResource corev1.ResourceName = "example.com/gpu"

const (
	testPauseImage = "registry.k8s.io/pause:3.9"
	coldPodE       = "res-md-e-serve"
)

func gpuRequirements(n string) corev1.ResourceRequirements {
	q := resource.MustParse(n)
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{gpuResource: q},
		Limits:   corev1.ResourceList{gpuResource: q},
	}
}

func TestFinishedPodsAreNotCapacity(t *testing.T) {
	running := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "run"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	failed := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "failed"}, Status: corev1.PodStatus{Phase: corev1.PodFailed}}
	done := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "done"}, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}
	pods := []corev1.Pod{running, failed, done}

	if got := activePods(pods); len(got) != 1 || got[0].Name != "run" {
		t.Fatalf("activePods = %v; a Failed or Succeeded pod must not count as capacity", got)
	}
	if got := terminalPods(pods); len(got) != 2 {
		t.Fatalf("terminalPods = %d, want 2", len(got))
	}
	failed.Status.Reason = "OutOfexample.com/gpu"
	failed.Status.Message = "Node didn't have enough resource: example.com/gpu"
	if msg := podFailureMessage(&failed); !strings.Contains(msg, "OutOfexample.com/gpu") || !strings.Contains(msg, "failed") {
		t.Fatalf("podFailureMessage = %q", msg)
	}

	old := failedPodBackoff
	defer func() { failedPodBackoff = old }()
	failedPodBackoff = time.Hour
	fresh := corev1.Pod{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.Now()}}
	if failedPodBackoffRemaining(&fresh) <= 0 {
		t.Fatal("a just-failed pod must be kept for the backoff window")
	}
	failedPodBackoff = 0
	if failedPodBackoffRemaining(&fresh) > 0 {
		t.Fatal("with no backoff a failed pod may be replaced at once")
	}
}

var _ = Describe("Pod resources and finished pods", func() {
	ctx := context.Background()
	const nodeName = "res-node"

	var (
		poolKey types.NamespacedName
		mdKey   types.NamespacedName
		pools   *PausePoolReconciler
		mds     *ModelDeploymentReconciler
	)

	BeforeEach(func() {
		pools = &PausePoolReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		mds = &ModelDeploymentReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Scheduler: scheduler.NewScheduler(nil), EvalProber: &fakeProber{}}
		Expect(k8sClient.Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Name: nodeName, Labels: map[string]string{NodeVRAMLabel: "80000"},
		}})).To(Succeed())
	})

	AfterEach(func() {
		if mdKey.Name != "" {
			deleteAndFinalize(ctx, mds, mdKey)
		}
		var pods corev1.PodList
		_ = k8sClient.List(ctx, &pods, client.InNamespace(testNamespace))
		for i := range pods.Items {
			_ = k8sClient.Delete(ctx, &pods.Items[i], client.GracePeriodSeconds(0))
		}
		if poolKey.Name != "" {
			_ = k8sClient.Delete(ctx, &amphorav1alpha1.PausePool{ObjectMeta: metav1.ObjectMeta{Name: poolKey.Name, Namespace: testNamespace}})
		}
		_ = k8sClient.Delete(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}})
		poolKey, mdKey = types.NamespacedName{}, types.NamespacedName{}
	})

	newPool := func(name string, rr corev1.ResourceRequirements, target int32) {
		poolKey = types.NamespacedName{Name: name, Namespace: testNamespace}
		Expect(k8sClient.Create(ctx, &amphorav1alpha1.PausePool{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec: amphorav1alpha1.PausePoolSpec{
				NodeName: nodeName, TenancyClass: amphorav1alpha1.TenancySingleTenant, GPUSlice: "full",
				TargetSize: target, PauseImage: testPauseImage, Resources: &rr,
			},
		})).To(Succeed())
	}
	newMD := func(name string, rr corev1.ResourceRequirements) {
		mdKey = types.NamespacedName{Name: name, Namespace: testNamespace}
		Expect(k8sClient.Create(ctx, &amphorav1alpha1.ModelDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec: amphorav1alpha1.ModelDeploymentSpec{
				Image: testImage, TenancyClass: amphorav1alpha1.TenancySingleTenant, Resources: &rr,
			},
		})).To(Succeed())
	}
	reconcilePool := func() reconcile.Result {
		GinkgoHelper()
		res, err := pools.Reconcile(ctx, reconcile.Request{NamespacedName: poolKey})
		Expect(err).NotTo(HaveOccurred())
		return res
	}
	reconcileMD := func(n int) {
		GinkgoHelper()
		for i := 0; i < n; i++ {
			_, err := mds.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
			Expect(err).NotTo(HaveOccurred())
		}
	}
	poolPods := func() []corev1.Pod {
		GinkgoHelper()
		var l corev1.PodList
		Expect(k8sClient.List(ctx, &l, client.InNamespace(testNamespace), client.MatchingLabels{poolNameLabel: poolKey.Name})).To(Succeed())
		return l.Items
	}
	fail := func(name, reason string) {
		GinkgoHelper()
		var p corev1.Pod
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNamespace}, &p)).To(Succeed())
		p.Status.Phase = corev1.PodFailed
		p.Status.Reason = reason
		p.Status.Message = "Node didn't have enough resource: " + string(gpuResource)
		Expect(k8sClient.Status().Update(ctx, &p)).To(Succeed())
	}

	It("lets the controller update a deployment that sets no resources, but not change resources", func() {
		newMD("res-md-imm", corev1.ResourceRequirements{})
		var md amphorav1alpha1.ModelDeployment
		Expect(k8sClient.Get(ctx, mdKey, &md)).To(Succeed())
		md.Labels = map[string]string{"touched": "yes"}
		Expect(k8sClient.Update(ctx, &md)).To(Succeed(), "a full-object update (as the controller does) must not trip the immutability rule")

		md.Spec.Resources = &corev1.ResourceRequirements{Limits: corev1.ResourceList{gpuResource: resource.MustParse("1")}}
		err := k8sClient.Update(ctx, &md)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("resources are immutable"))
	})

	It("gives pool pods exactly the pool's resources", func() {
		newPool("res-pool-a", gpuRequirements("1"), 1)
		reconcilePool()
		pods := poolPods()
		Expect(pods).To(HaveLen(1))
		got := pods[0].Spec.Containers[0].Resources.Limits[gpuResource]
		Expect(got.Cmp(resource.MustParse("1"))).To(BeZero(), "the model container must carry the GPU limit")
	})

	It("hijacks a pool pod only when the resources are identical", func() {
		newPool("res-pool-b", gpuRequirements("1"), 1)
		reconcilePool()
		idle := poolPods()[0].Name

		By("a deployment asking for the same GPU takes the idle pod")
		newMD("res-md-match", gpuRequirements("1"))
		reconcileMD(2)
		var md amphorav1alpha1.ModelDeployment
		Expect(k8sClient.Get(ctx, mdKey, &md)).To(Succeed())
		Expect(md.Status.ActivePod).To(Equal(idle))
		deleteAndFinalize(ctx, mds, mdKey)

		By("a deployment asking for different resources must not, and cold-creates its own pod with them")
		reconcilePool() // refill
		newMD("res-md-other", corev1.ResourceRequirements{})
		reconcileMD(2)
		Expect(k8sClient.Get(ctx, mdKey, &md)).To(Succeed())
		Expect(md.Status.ActivePod).To(Equal("res-md-other-serve"))
		var cold corev1.Pod
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: md.Status.ActivePod, Namespace: testNamespace}, &cold)).To(Succeed())
		Expect(cold.Spec.Containers[0].Resources.Limits).To(BeEmpty(), "no resources were asked for, so none are invented")
	})

	It("passes a deployment's resources to its cold-created pod", func() {
		newMD("res-md-cold", gpuRequirements("2"))
		reconcileMD(2)
		var pod corev1.Pod
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "res-md-cold-serve", Namespace: testNamespace}, &pod)).To(Succeed())
		got := pod.Spec.Containers[0].Resources.Limits[gpuResource]
		Expect(got.Cmp(resource.MustParse("2"))).To(BeZero())
	})

	It("does not count a rejected pool pod as capacity, and does not recreate it in a loop", func() {
		old := failedPodBackoff
		DeferCleanup(func() { failedPodBackoff = old })
		failedPodBackoff = time.Hour

		newPool("res-pool-c", gpuRequirements("1"), 1)
		reconcilePool()
		name := poolPods()[0].Name
		fail(name, "OutOfexample.com/gpu")

		res := reconcilePool()
		Expect(res.RequeueAfter).NotTo(BeZero(), "it must wait out the backoff, not retry at once")
		Expect(poolPods()).To(HaveLen(1), "the failed pod is kept as evidence and nothing else is created meanwhile")

		var pool amphorav1alpha1.PausePool
		Expect(k8sClient.Get(ctx, poolKey, &pool)).To(Succeed())
		Expect(pool.Status.CurrentSize).To(BeZero(), "a Failed pod is not capacity")
		ready := findReady(pool.Status.Conditions)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Reason).To(Equal(reasonCapacityUnavailable))
		Expect(ready.Message).To(ContainSubstring("OutOfexample.com/gpu"))

		By("after the backoff window the failed pod is replaced")
		failedPodBackoff = 0
		reconcilePool()
		pods := poolPods()
		Expect(pods).To(HaveLen(1))
		Expect(pods[0].Name).NotTo(Equal(name))
		Expect(activePods(pods)).To(HaveLen(1))
	})

	It("never hijacks a finished pool pod", func() {
		old := failedPodBackoff
		DeferCleanup(func() { failedPodBackoff = old })
		failedPodBackoff = time.Hour

		newPool("res-pool-d", gpuRequirements("1"), 1)
		reconcilePool()
		fail(poolPods()[0].Name, "OutOfexample.com/gpu")

		newMD("res-md-d", gpuRequirements("1"))
		reconcileMD(2)
		var md amphorav1alpha1.ModelDeployment
		Expect(k8sClient.Get(ctx, mdKey, &md)).To(Succeed())
		Expect(md.Status.ActivePod).To(Equal("res-md-d-serve"), "it must fall back to a fresh pod rather than adopt a dead one")
	})

	It("backs off, then replaces, a cold-created pod the kubelet rejected", func() {
		old := failedPodBackoff
		DeferCleanup(func() { failedPodBackoff = old })
		failedPodBackoff = time.Hour

		newMD("res-md-e", gpuRequirements("1"))
		reconcileMD(2)
		fail(coldPodE, "OutOfexample.com/gpu")

		res, err := mds.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).NotTo(BeZero())
		var md amphorav1alpha1.ModelDeployment
		Expect(k8sClient.Get(ctx, mdKey, &md)).To(Succeed())
		Expect(md.Status.Phase).To(Equal(PhasePending), "a capacity problem, not a model that is warming or serving")
		Expect(md.Status.EvalFailures).To(BeZero(), "a rejected pod is not a quality failure and must not trip the circuit breaker")
		Expect(md.Status.Endpoint).To(BeEmpty())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: coldPodE, Namespace: testNamespace}, &corev1.Pod{})).To(Succeed(),
			"kept as evidence during the backoff window")

		By("after the window it is deleted and a fresh pod is created")
		failedPodBackoff = 0
		reconcileMD(1)
		err = k8sClient.Get(ctx, types.NamespacedName{Name: coldPodE, Namespace: testNamespace}, &corev1.Pod{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
		reconcileMD(1)
		var fresh corev1.Pod
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: coldPodE, Namespace: testNamespace}, &fresh)).To(Succeed())
		Expect(isTerminal(&fresh)).To(BeFalse())
	})
})

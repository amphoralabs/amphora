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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
	"github.com/ramin-fazli/amphora/internal/scheduler"
)

// The scheduler holds placements only in memory. After a controller restart
// it is empty, so running deployments must be re-adopted at the node their pod
// is actually on; re-placing them from scratch can pick a different node and
// let another SingleTenant workload share a node with a live one (§4).
var _ = Describe("ModelDeployment placement survives a controller restart", func() {
	ctx := context.Background()
	const (
		bigNode   = "restart-big"
		smallNode = "restart-small"
	)

	node := func(name, vramMB string) {
		Expect(k8sClient.Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Name: name, Labels: map[string]string{NodeVRAMLabel: vramMB},
		}})).To(Succeed())
	}
	newMD := func(name string) types.NamespacedName {
		Expect(k8sClient.Create(ctx, &amphorav1alpha1.ModelDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec:       amphorav1alpha1.ModelDeploymentSpec{Image: testImage, TenancyClass: amphorav1alpha1.TenancySingleTenant},
		})).To(Succeed())
		return types.NamespacedName{Name: name, Namespace: testNamespace}
	}
	podNode := func(md string) string {
		var pod corev1.Pod
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: md + "-serve", Namespace: testNamespace}, &pod)).To(Succeed())
		return pod.Spec.NodeName
	}
	reconcileN := func(r *ModelDeploymentReconciler, key types.NamespacedName, n int) {
		GinkgoHelper()
		for i := 0; i < n; i++ {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
		}
	}

	It("re-adopts a running deployment at its pod's node, so a second SingleTenant cannot share it", func() {
		node(bigNode, "80000")
		first := newMD("restart-first")
		before := &ModelDeploymentReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Scheduler: scheduler.NewScheduler(nil)}
		reconcileN(before, first, 2) // finalizer, then place + cold-create
		Expect(podNode("restart-first")).To(Equal(bigNode))

		By("a smaller node appears, then the controller restarts with an empty scheduler")
		node(smallNode, "40000") // best-fit would now prefer this node for a fresh placement
		restarted := &ModelDeploymentReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Scheduler: scheduler.NewScheduler(nil)}
		reconcileN(restarted, first, 1)

		p, ok := restarted.Scheduler.Placement(first.String())
		Expect(ok).To(BeTrue())
		Expect(p.NodeID).To(Equal(bigNode), "the scheduler must record where the pod really is, not re-place it")

		By("a second SingleTenant deployment must not be placed on the node the first still occupies")
		second := newMD("restart-second")
		reconcileN(restarted, second, 2)
		Expect(podNode("restart-second")).To(Equal(smallNode))

		for _, n := range []types.NamespacedName{first, second} {
			deleteAndFinalize(ctx, restarted, n)
		}
		for _, n := range []string{bigNode, smallNode} {
			_ = k8sClient.Delete(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: n}})
		}
	})

	It("evicts and re-places a deployment whose observed node can no longer host it", func() {
		node(bigNode, "80000")
		node(smallNode, "40000")
		victim := newMD("restart-victim")
		before := &ModelDeploymentReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Scheduler: scheduler.NewScheduler(nil)}
		reconcileN(before, victim, 2)
		observed := podNode("restart-victim")

		By("after a restart another SingleTenant claim on that node is restored first")
		restarted := &ModelDeploymentReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Scheduler: scheduler.NewScheduler(nil)}
		Expect(restarted.syncSchedulerNodes(ctx)).To(Succeed())
		_, err := restarted.Scheduler.Restore(scheduler.PlacementRequest{
			Model: "default/squatter", TenancyClass: amphorav1alpha1.TenancySingleTenant, VRAMMB: 8192,
		}, observed)
		Expect(err).NotTo(HaveOccurred())

		By("the victim cannot be re-adopted, so its pod is evicted and it is re-placed elsewhere")
		result, err := restarted.Reconcile(ctx, reconcile.Request{NamespacedName: victim})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).NotTo(BeZero())
		var md amphorav1alpha1.ModelDeployment
		Expect(k8sClient.Get(ctx, victim, &md)).To(Succeed())
		Expect(md.Status.Phase).To(Equal(PhasePending))
		Expect(md.Status.ActivePod).To(BeEmpty())

		reconcileN(restarted, victim, 1)
		Expect(podNode("restart-victim")).NotTo(Equal(observed))

		deleteAndFinalize(ctx, restarted, victim)
		for _, n := range []string{bigNode, smallNode} {
			_ = k8sClient.Delete(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: n}})
		}
	})
})

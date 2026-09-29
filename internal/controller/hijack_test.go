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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
	"github.com/ramin-fazli/amphora/internal/scheduler"
)

var _ = Describe("ModelDeployment pause-pod hijack", func() {
	Context("When a matching PausePool has an idle pod", func() {
		const mdName = "test-md-hijack"
		const poolName = "test-pool-hijack"
		const nodeName = "gpu-node-hijack"

		ctx := context.Background()
		mdKey := types.NamespacedName{Name: mdName, Namespace: testNamespace}
		poolKey := types.NamespacedName{Name: poolName, Namespace: testNamespace}
		var mdReconciler *ModelDeploymentReconciler
		var poolReconciler *PausePoolReconciler

		BeforeEach(func() {
			mdReconciler = &ModelDeploymentReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Scheduler: scheduler.NewScheduler(nil)}
			poolReconciler = &PausePoolReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: nodeName,
					Labels: map[string]string{
						NodeVRAMLabel:       "80000",
						NodeMIGCapableLabel: nodeMIGCapableTrue,
					},
				},
			}
			Expect(k8sClient.Create(ctx, node)).To(Succeed())

			pool := &amphorav1alpha1.PausePool{
				ObjectMeta: metav1.ObjectMeta{Name: poolName, Namespace: testNamespace},
				Spec: amphorav1alpha1.PausePoolSpec{
					NodeName:     nodeName,
					TenancyClass: amphorav1alpha1.TenancySingleTenant,
					GPUSlice:     "full",
					TargetSize:   1,
					PauseImage:   "registry.k8s.io/pause:3.9",
				},
			}
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())
			_, err := poolReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: poolKey})
			Expect(err).NotTo(HaveOccurred())
			Expect(listPausePods(ctx, poolName)).To(HaveLen(1))

			md := &amphorav1alpha1.ModelDeployment{
				ObjectMeta: metav1.ObjectMeta{Name: mdName, Namespace: testNamespace},
				Spec: amphorav1alpha1.ModelDeploymentSpec{
					Image:        testImage,
					TenancyClass: amphorav1alpha1.TenancySingleTenant,
				},
			}
			Expect(k8sClient.Create(ctx, md)).To(Succeed())
		})

		AfterEach(func() {
			deleteAndFinalize(ctx, mdReconciler, mdKey)
			for _, p := range listPausePods(ctx, poolName) {
				_ = k8sClient.Delete(ctx, &p)
			}
			var hijacked corev1.PodList
			_ = k8sClient.List(ctx, &hijacked, client.InNamespace(testNamespace), client.MatchingLabels{hijackedByLabel: mdName})
			for i := range hijacked.Items {
				_ = k8sClient.Delete(ctx, &hijacked.Items[i])
			}
			var pool amphorav1alpha1.PausePool
			if err := k8sClient.Get(ctx, poolKey, &pool); err == nil {
				_ = k8sClient.Delete(ctx, &pool)
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}
			_ = k8sClient.Delete(ctx, node)
		})

		It("hijacks the idle pod, patches it, and reaches PhaseWarming", func() {
			By("first reconcile: adds the scheduler finalizer")
			_, err := mdReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
			Expect(err).NotTo(HaveOccurred())

			By("second reconcile: schedules and hijacks the pause pod")
			_, err = mdReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
			Expect(err).NotTo(HaveOccurred())

			var warmed amphorav1alpha1.ModelDeployment
			Expect(k8sClient.Get(ctx, mdKey, &warmed)).To(Succeed())
			Expect(warmed.Status.Phase).To(Equal(PhaseWarming))
			Expect(warmed.Status.ActivePod).NotTo(BeEmpty())

			var pod corev1.Pod
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: warmed.Status.ActivePod, Namespace: testNamespace}, &pod)).To(Succeed())
			Expect(pod.Spec.Containers[0].Image).To(Equal(testImage))
			Expect(pod.Spec.RuntimeClassName).NotTo(BeNil())
			Expect(*pod.Spec.RuntimeClassName).To(Equal(nvidiaRuntimeClassName))
			Expect(pod.Annotations).To(HaveKeyWithValue(modelAnnotation, mdName))
			Expect(pod.Labels).To(HaveKeyWithValue(hijackedByLabel, mdName))
			Expect(pod.Labels).NotTo(HaveKey(poolNameLabel))
			Expect(pod.OwnerReferences).To(HaveLen(1))
			Expect(pod.OwnerReferences[0].Name).To(Equal(mdName))
			Expect(pod.OwnerReferences[0].Kind).To(Equal("ModelDeployment"))

			By("the pool no longer counts the hijacked pod")
			Expect(listPausePods(ctx, poolName)).To(BeEmpty())

			By("re-reconciling is idempotent: no second pod is hijacked")
			_, err = mdReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
			Expect(err).NotTo(HaveOccurred())
			var reconfirmed amphorav1alpha1.ModelDeployment
			Expect(k8sClient.Get(ctx, mdKey, &reconfirmed)).To(Succeed())
			Expect(reconfirmed.Status.ActivePod).To(Equal(warmed.Status.ActivePod))
		})
	})

	Context("When no PausePool has an idle pod for the placement", func() {
		const mdName = "test-md-no-pool"
		const nodeName = "gpu-node-no-pool"

		ctx := context.Background()
		mdKey := types.NamespacedName{Name: mdName, Namespace: testNamespace}
		var mdReconciler *ModelDeploymentReconciler

		BeforeEach(func() {
			mdReconciler = &ModelDeploymentReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Scheduler: scheduler.NewScheduler(nil)}

			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: nodeName,
					Labels: map[string]string{
						NodeVRAMLabel:       "80000",
						NodeMIGCapableLabel: nodeMIGCapableTrue,
					},
				},
			}
			Expect(k8sClient.Create(ctx, node)).To(Succeed())

			md := &amphorav1alpha1.ModelDeployment{
				ObjectMeta: metav1.ObjectMeta{Name: mdName, Namespace: testNamespace},
				Spec: amphorav1alpha1.ModelDeploymentSpec{
					Image:        testImage,
					TenancyClass: amphorav1alpha1.TenancySingleTenant,
				},
			}
			Expect(k8sClient.Create(ctx, md)).To(Succeed())
		})

		AfterEach(func() {
			deleteAndFinalize(ctx, mdReconciler, mdKey)
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}
			_ = k8sClient.Delete(ctx, node)
		})

		It("falls back to a cold-created pod and reaches PhaseWarming", func() {
			_, err := mdReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
			Expect(err).NotTo(HaveOccurred())
			_, err = mdReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
			Expect(err).NotTo(HaveOccurred())

			var warmed amphorav1alpha1.ModelDeployment
			Expect(k8sClient.Get(ctx, mdKey, &warmed)).To(Succeed())
			Expect(warmed.Status.Phase).To(Equal(PhaseWarming))
			Expect(warmed.Status.ActivePod).To(Equal(mdName + "-serve"))

			var pod corev1.Pod
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: warmed.Status.ActivePod, Namespace: testNamespace}, &pod)).To(Succeed())
			Expect(pod.Spec.NodeName).To(Equal(nodeName))
			Expect(pod.Spec.Containers[0].Image).To(Equal(testImage))
			Expect(pod.Labels).To(HaveKeyWithValue(coldStartLabel, labelValueTrue))
			Expect(pod.OwnerReferences).To(HaveLen(1))
			Expect(pod.OwnerReferences[0].Name).To(Equal(mdName))

			By("re-reconciling adopts the same pod instead of creating another")
			_, err = mdReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
			Expect(err).NotTo(HaveOccurred())
			var again amphorav1alpha1.ModelDeployment
			Expect(k8sClient.Get(ctx, mdKey, &again)).To(Succeed())
			Expect(again.Status.ActivePod).To(Equal(warmed.Status.ActivePod))
		})
	})
})

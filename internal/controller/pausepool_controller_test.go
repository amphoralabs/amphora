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
)

// listPausePods returns only non-terminating pods for poolName: envtest has
// no kubelet to finish a graceful pod deletion, so a just-deleted pod would
// otherwise still show up with a DeletionTimestamp set — the same condition
// PausePoolReconciler itself filters out via activePods.
func listPausePods(ctx context.Context, poolName string) []corev1.Pod {
	var pods corev1.PodList
	Expect(k8sClient.List(ctx, &pods, client.InNamespace(testNamespace), client.MatchingLabels{poolNameLabel: poolName})).To(Succeed())
	return activePods(pods.Items)
}

var _ = Describe("PausePool Controller", func() {
	Context("When the target node is MIG-capable and matches the requested slice", func() {
		const poolName = "test-pool-mig"
		const nodeName = "gpu-node-pausepool-mig"

		ctx := context.Background()
		key := types.NamespacedName{Name: poolName, Namespace: testNamespace}
		var reconciler *PausePoolReconciler

		BeforeEach(func() {
			reconciler = &PausePoolReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{NodeMIGCapableLabel: nodeMIGCapableTrue},
				},
			}
			Expect(k8sClient.Create(ctx, node)).To(Succeed())

			pool := &amphorav1alpha1.PausePool{
				ObjectMeta: metav1.ObjectMeta{Name: poolName, Namespace: testNamespace},
				Spec: amphorav1alpha1.PausePoolSpec{
					NodeName:     nodeName,
					TenancyClass: amphorav1alpha1.TenancyRegulatedMultiTenant,
					GPUSlice:     "1g.10gb",
					TargetSize:   2,
					PauseImage:   "registry.k8s.io/pause:3.9",
				},
			}
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		})

		AfterEach(func() {
			for _, p := range listPausePods(ctx, poolName) {
				_ = k8sClient.Delete(ctx, &p)
			}
			var pool amphorav1alpha1.PausePool
			if err := k8sClient.Get(ctx, key, &pool); err == nil {
				_ = k8sClient.Delete(ctx, &pool)
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}
			_ = k8sClient.Delete(ctx, node)
		})

		It("creates pods up to targetSize, pre-bound to the node", func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())

			pods := listPausePods(ctx, poolName)
			Expect(pods).To(HaveLen(2))
			for _, p := range pods {
				Expect(p.Spec.NodeName).To(Equal(nodeName))
				Expect(p.Spec.Containers[0].Image).To(Equal("registry.k8s.io/pause:3.9"))
				Expect(p.Labels[pausePodTenancyLabel]).To(Equal(string(amphorav1alpha1.TenancyRegulatedMultiTenant)))
			}

			var updated amphorav1alpha1.PausePool
			Expect(k8sClient.Get(ctx, key, &updated)).To(Succeed())
			Expect(updated.Status.CurrentSize).To(Equal(int32(2)))
			Expect(updated.Status.PausePodNames).To(HaveLen(2))
			readyCond := findCondition(updated.Status.Conditions, conditionReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionTrue))
			Expect(readyCond.Reason).To(Equal(reasonPoolFilled))
		})

		It("scales down excess pods when targetSize shrinks", func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			Expect(listPausePods(ctx, poolName)).To(HaveLen(2))

			var pool amphorav1alpha1.PausePool
			Expect(k8sClient.Get(ctx, key, &pool)).To(Succeed())
			pool.Spec.TargetSize = 1
			Expect(k8sClient.Update(ctx, &pool)).To(Succeed())

			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			Expect(listPausePods(ctx, poolName)).To(HaveLen(1))
		})
	})

	Context("When gpuSlice implies MIG but the node isn't MIG-capable", func() {
		const poolName = "test-pool-invalid-node"
		const nodeName = "gpu-node-pausepool-nomig"

		ctx := context.Background()
		key := types.NamespacedName{Name: poolName, Namespace: testNamespace}
		var reconciler *PausePoolReconciler

		BeforeEach(func() {
			reconciler = &PausePoolReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}
			Expect(k8sClient.Create(ctx, node)).To(Succeed())

			pool := &amphorav1alpha1.PausePool{
				ObjectMeta: metav1.ObjectMeta{Name: poolName, Namespace: testNamespace},
				Spec: amphorav1alpha1.PausePoolSpec{
					NodeName:     nodeName,
					TenancyClass: amphorav1alpha1.TenancyRegulatedMultiTenant,
					GPUSlice:     "1g.10gb",
					TargetSize:   1,
				},
			}
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		})

		AfterEach(func() {
			var pool amphorav1alpha1.PausePool
			if err := k8sClient.Get(ctx, key, &pool); err == nil {
				_ = k8sClient.Delete(ctx, &pool)
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}
			_ = k8sClient.Delete(ctx, node)
		})

		It("marks the pool not-Ready and creates no pods", func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())

			Expect(listPausePods(ctx, poolName)).To(BeEmpty())

			var updated amphorav1alpha1.PausePool
			Expect(k8sClient.Get(ctx, key, &updated)).To(Succeed())
			readyCond := findCondition(updated.Status.Conditions, conditionReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCond.Reason).To(Equal(reasonInvalidSpec))
		})
	})

	Context("When tenancyClass/gpuSlice violates the §4 isolation matrix", func() {
		const poolName = "test-pool-invalid-tenancy"
		const nodeName = "gpu-node-pausepool-tenancy"

		ctx := context.Background()
		key := types.NamespacedName{Name: poolName, Namespace: testNamespace}
		var reconciler *PausePoolReconciler

		BeforeEach(func() {
			reconciler = &PausePoolReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   nodeName,
					Labels: map[string]string{NodeMIGCapableLabel: nodeMIGCapableTrue},
				},
			}
			Expect(k8sClient.Create(ctx, node)).To(Succeed())

			pool := &amphorav1alpha1.PausePool{
				ObjectMeta: metav1.ObjectMeta{Name: poolName, Namespace: testNamespace},
				Spec: amphorav1alpha1.PausePoolSpec{
					NodeName:     nodeName,
					TenancyClass: amphorav1alpha1.TenancyRegulatedMultiTenant,
					GPUSlice:     "timeslice",
					TargetSize:   1,
				},
			}
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		})

		AfterEach(func() {
			var pool amphorav1alpha1.PausePool
			if err := k8sClient.Get(ctx, key, &pool); err == nil {
				_ = k8sClient.Delete(ctx, &pool)
			}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}
			_ = k8sClient.Delete(ctx, node)
		})

		It("marks the pool not-Ready without creating pods", func() {
			// RegulatedMultiTenant requires MIG (hardware-enforced
			// isolation); "timeslice" has no guaranteed VRAM/fault
			// isolation and must be rejected per §4, even though the node
			// itself is MIG-capable.
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())

			Expect(listPausePods(ctx, poolName)).To(BeEmpty())

			var updated amphorav1alpha1.PausePool
			Expect(k8sClient.Get(ctx, key, &updated)).To(Succeed())
			readyCond := findCondition(updated.Status.Conditions, conditionReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCond.Reason).To(Equal(reasonInvalidSpec))
		})
	})
})

func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}

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
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
	"github.com/ramin-fazli/amphora/internal/scheduler"
)

const (
	testNamespace = "default"
	testImage     = "example.com/models/demo:v1"
)

// deleteAndFinalize deletes md and drives reconciler through the deletion
// path so the schedulerFinalizer is removed and envtest can actually
// garbage-collect the object between specs.
func deleteAndFinalize(ctx context.Context, reconciler *ModelDeploymentReconciler, key types.NamespacedName) {
	var current amphorav1alpha1.ModelDeployment
	if err := k8sClient.Get(ctx, key, &current); errors.IsNotFound(err) {
		return
	}
	Expect(k8sClient.Delete(ctx, &current)).To(Succeed())
	_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
	Expect(err).NotTo(HaveOccurred())
}

var _ = Describe("ModelDeployment Controller", func() {
	Context("When a compatible GPU node has capacity", func() {
		const resourceName = "test-resource-scheduled"
		const nodeName = "gpu-node-1"

		ctx := context.Background()
		typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: testNamespace}
		var reconciler *ModelDeploymentReconciler

		BeforeEach(func() {
			reconciler = &ModelDeploymentReconciler{
				Client:    k8sClient,
				Scheme:    k8sClient.Scheme(),
				Scheduler: scheduler.NewScheduler(nil),
			}

			By("registering a GPU node")
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: nodeName,
					Labels: map[string]string{
						NodeVRAMLabel:       "80000",
						NodeMIGCapableLabel: "true",
					},
				},
			}
			Expect(k8sClient.Create(ctx, node)).To(Succeed())

			By("creating the ModelDeployment")
			resource := &amphorav1alpha1.ModelDeployment{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: testNamespace},
				Spec: amphorav1alpha1.ModelDeploymentSpec{
					Image:        testImage,
					TenancyClass: amphorav1alpha1.TenancySingleTenant,
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			deleteAndFinalize(ctx, reconciler, typeNamespacedName)
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}
			_ = k8sClient.Delete(ctx, node)
		})

		It("adds a finalizer on the first reconcile, then schedules the placement", func() {
			By("first reconcile: adds the scheduler finalizer")
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			var afterFinalizer amphorav1alpha1.ModelDeployment
			Expect(k8sClient.Get(ctx, typeNamespacedName, &afterFinalizer)).To(Succeed())
			Expect(afterFinalizer.Finalizers).To(ContainElement(schedulerFinalizer))

			By("second reconcile: places the deployment via the Packing Scheduler")
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			var scheduled amphorav1alpha1.ModelDeployment
			Expect(k8sClient.Get(ctx, typeNamespacedName, &scheduled)).To(Succeed())
			Expect(scheduled.Status.Phase).To(Equal(PhaseScheduled))
			Expect(scheduled.Status.EffectivePackingMode).To(Equal(string(scheduler.PackingModeDedicated)))
			Expect(scheduled.Status.ObservedGeneration).To(Equal(scheduled.Generation))

			placement, ok := reconciler.Scheduler.Placement(typeNamespacedName.String())
			Expect(ok).To(BeTrue())
			Expect(placement.NodeID).To(Equal(nodeName))
		})

		It("releases the placement's capacity when the resource is deleted", func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			_, ok := reconciler.Scheduler.Placement(typeNamespacedName.String())
			Expect(ok).To(BeTrue())

			deleteAndFinalize(ctx, reconciler, typeNamespacedName)

			_, ok = reconciler.Scheduler.Placement(typeNamespacedName.String())
			Expect(ok).To(BeFalse())
		})
	})

	Context("When no GPU node has capacity", func() {
		const resourceName = "test-resource-pending"

		ctx := context.Background()
		typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: testNamespace}
		var reconciler *ModelDeploymentReconciler

		BeforeEach(func() {
			reconciler = &ModelDeploymentReconciler{
				Client:    k8sClient,
				Scheme:    k8sClient.Scheme(),
				Scheduler: scheduler.NewScheduler(nil),
			}
			resource := &amphorav1alpha1.ModelDeployment{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: testNamespace},
				Spec: amphorav1alpha1.ModelDeploymentSpec{
					Image:        testImage,
					TenancyClass: amphorav1alpha1.TenancySingleTenant,
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			deleteAndFinalize(ctx, reconciler, typeNamespacedName)
		})

		It("marks the deployment Pending and requeues rather than erroring", func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(pendingRequeueInterval))

			var pending amphorav1alpha1.ModelDeployment
			Expect(k8sClient.Get(ctx, typeNamespacedName, &pending)).To(Succeed())
			Expect(pending.Status.Phase).To(Equal(PhasePending))
		})
	})

	Context("When the gpuFraction doesn't match the MIG profile pattern", func() {
		const resourceName = "test-resource-rejected"

		ctx := context.Background()
		typeNamespacedName := types.NamespacedName{Name: resourceName, Namespace: testNamespace}
		var reconciler *ModelDeploymentReconciler

		BeforeEach(func() {
			reconciler = &ModelDeploymentReconciler{
				Client:    k8sClient,
				Scheme:    k8sClient.Scheme(),
				Scheduler: scheduler.NewScheduler(nil),
			}
			resource := &amphorav1alpha1.ModelDeployment{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: testNamespace},
				Spec: amphorav1alpha1.ModelDeploymentSpec{
					Image:        testImage,
					TenancyClass: amphorav1alpha1.TenancySingleTenant,
					GPUFraction:  "not-a-mig-profile",
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			deleteAndFinalize(ctx, reconciler, typeNamespacedName)
		})

		It("marks the deployment Rejected without requeueing", func() {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())
			Expect(result.Requeue).To(BeFalse())

			var rejected amphorav1alpha1.ModelDeployment
			Expect(k8sClient.Get(ctx, typeNamespacedName, &rejected)).To(Succeed())
			Expect(rejected.Status.Phase).To(Equal(PhaseRejected))
		})
	})
})

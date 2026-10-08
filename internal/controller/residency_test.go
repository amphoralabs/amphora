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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
	"github.com/ramin-fazli/amphora/internal/scheduler"
)

const (
	euWest1 = "eu-west-1"
	usEast1 = "us-east-1"
	euNode  = "res-eu"
	usNode  = "res-us"
)

var _ = Describe("ModelDeployment residency (allowedRegions, spec §4.3)", func() {
	ctx := context.Background()
	var (
		reconciler *ModelDeploymentReconciler
		mdKey      types.NamespacedName
	)

	regionNode := func(name, region string) {
		labels := map[string]string{NodeVRAMLabel: "80000"}
		if region != "" {
			labels[RegionLabel] = region
		}
		Expect(k8sClient.Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}})).To(Succeed())
	}
	create := func(name string, regions ...string) {
		mdKey = types.NamespacedName{Name: name, Namespace: testNamespace}
		reconciler = &ModelDeploymentReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Scheduler: scheduler.NewScheduler(nil)}
		Expect(k8sClient.Create(ctx, &amphorav1alpha1.ModelDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec: amphorav1alpha1.ModelDeploymentSpec{
				Image: testImage, TenancyClass: amphorav1alpha1.TenancySingleTenant, AllowedRegions: regions,
			},
		})).To(Succeed())
	}
	reconcileOnce := func() {
		GinkgoHelper()
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
		Expect(err).NotTo(HaveOccurred())
	}
	get := func() amphorav1alpha1.ModelDeployment {
		var md amphorav1alpha1.ModelDeployment
		Expect(k8sClient.Get(ctx, mdKey, &md)).To(Succeed())
		return md
	}
	podNode := func() string {
		var pod corev1.Pod
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: mdKey.Name + "-serve", Namespace: testNamespace}, &pod)).To(Succeed())
		return pod.Spec.NodeName
	}

	AfterEach(func() {
		deleteAndFinalize(ctx, reconciler, mdKey)
		for _, n := range []string{euNode, usNode} {
			_ = k8sClient.Delete(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: n}})
		}
	})

	It("places only on a node in an allowed region", func() {
		regionNode(euNode, euWest1)
		regionNode(usNode, usEast1)
		create("res-places-us", usEast1)
		reconcileOnce() // finalizer
		reconcileOnce() // place + pod
		Expect(get().Status.Phase).To(Equal(PhaseWarming))
		Expect(podNode()).To(Equal(usNode))
	})

	It("stays Pending, with no pod, when no node is in an allowed region", func() {
		regionNode(euNode, euWest1)
		create("res-pending", usEast1)
		reconcileOnce()
		reconcileOnce()
		md := get()
		Expect(md.Status.Phase).To(Equal(PhasePending))
		Expect(md.Status.ActivePod).To(BeEmpty())
		err := k8sClient.Get(ctx, types.NamespacedName{Name: "res-pending-serve", Namespace: testNamespace}, &corev1.Pod{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "no pod may be created outside the allowed regions")
	})

	It("never uses a node without a region label for a region-constrained deployment", func() {
		regionNode(euNode, "") // unlabeled
		create("res-unlabeled", euWest1)
		reconcileOnce()
		reconcileOnce()
		Expect(get().Status.Phase).To(Equal(PhasePending))
	})

	It("evicts and re-places when allowedRegions changes out from under a running deployment", func() {
		regionNode(euNode, euWest1)
		regionNode(usNode, usEast1)
		create("res-evict", euWest1)
		reconcileOnce()
		reconcileOnce()
		Expect(podNode()).To(Equal(euNode))

		By("the spec moves the workload to us-east-1")
		md := get()
		md.Spec.AllowedRegions = []string{usEast1}
		Expect(k8sClient.Update(ctx, &md)).To(Succeed())

		By("the next reconcile deletes the out-of-region pod and clears traffic-bearing status")
		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).NotTo(BeZero())
		evicted := get()
		Expect(evicted.Status.Phase).To(Equal(PhasePending))
		Expect(evicted.Status.ActivePod).To(BeEmpty())
		Expect(evicted.Status.Endpoint).To(BeEmpty())
		err = k8sClient.Get(ctx, types.NamespacedName{Name: "res-evict-serve", Namespace: testNamespace}, &corev1.Pod{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "the out-of-region pod must be gone")

		By("and it is re-placed in the new region")
		reconcileOnce()
		Expect(podNode()).To(Equal(usNode))
		Expect(get().Status.Phase).To(Equal(PhaseWarming))
	})
})

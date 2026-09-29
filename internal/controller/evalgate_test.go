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
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
	"github.com/ramin-fazli/amphora/internal/scheduler"
)

type fakeProber struct {
	err     error
	block   bool // wait for the gate deadline, simulating a hung pod
	lastURL string
}

func (f *fakeProber) Probe(ctx context.Context, url string) error {
	f.lastURL = url
	if f.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.err
}

func markPodReady(ctx context.Context, key types.NamespacedName) {
	var pod corev1.Pod
	Expect(k8sClient.Get(ctx, key, &pod)).To(Succeed())
	pod.Status.PodIP = "10.0.0.7"
	pod.Status.PodIPs = []corev1.PodIP{{IP: "10.0.0.7"}}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	Expect(k8sClient.Status().Update(ctx, &pod)).To(Succeed())
}

var _ = Describe("ModelDeployment eval gate", func() {
	const nodeName = "gpu-node-evalgate"
	ctx := context.Background()
	var (
		reconciler *ModelDeploymentReconciler
		prober     *fakeProber
		mdKey      types.NamespacedName
	)

	// warm creates a node + deployment and reconciles it to Warming with a
	// cold-created pod (no PausePool exists).
	warm := func(name string, timeoutMillis int32) {
		mdKey = types.NamespacedName{Name: name, Namespace: testNamespace}
		prober = &fakeProber{}
		reconciler = &ModelDeploymentReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Scheduler: scheduler.NewScheduler(nil), EvalProber: prober}
		Expect(k8sClient.Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName, Labels: map[string]string{NodeVRAMLabel: "80000"}}})).To(Succeed())
		Expect(k8sClient.Create(ctx, &amphorav1alpha1.ModelDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec: amphorav1alpha1.ModelDeploymentSpec{
				Image: testImage, TenancyClass: amphorav1alpha1.TenancySingleTenant,
				EvalGate: amphorav1alpha1.EvalGateSpec{TimeoutMillis: timeoutMillis},
			},
		})).To(Succeed())
		for i := 0; i < 2; i++ {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
			Expect(err).NotTo(HaveOccurred())
		}
		var md amphorav1alpha1.ModelDeployment
		Expect(k8sClient.Get(ctx, mdKey, &md)).To(Succeed())
		Expect(md.Status.Phase).To(Equal(PhaseWarming))
	}
	podKey := func() types.NamespacedName {
		return types.NamespacedName{Name: mdKey.Name + "-serve", Namespace: testNamespace}
	}
	get := func() amphorav1alpha1.ModelDeployment {
		var md amphorav1alpha1.ModelDeployment
		Expect(k8sClient.Get(ctx, mdKey, &md)).To(Succeed())
		return md
	}

	AfterEach(func() {
		deleteAndFinalize(ctx, reconciler, mdKey)
		_ = k8sClient.Delete(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}})
	})

	It("stays Warming until the pod is Ready, without probing", func() {
		warm("eval-notready", 150)
		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).NotTo(BeZero())
		Expect(prober.lastURL).To(BeEmpty())
		Expect(get().Status.Phase).To(Equal(PhaseWarming))
	})

	It("promotes to Serving on a passing probe and records quality as unverified", func() {
		warm("eval-pass", 150)
		markPodReady(ctx, podKey())
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
		Expect(err).NotTo(HaveOccurred())

		md := get()
		Expect(md.Status.Phase).To(Equal(PhaseServing))
		Expect(prober.lastURL).To(Equal("http://10.0.0.7:8000/health"))
		Expect(md.Status.Endpoint).To(Equal("http://10.0.0.7:8000"))
		cond := meta.FindStatusCondition(md.Status.Conditions, conditionQualityVerified)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(reasonLatencyOnly))

		By("re-reconciling a Serving deployment does not re-gate or change phase")
		prober.err = errors.New("would fail if probed again")
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(get().Status.Phase).To(Equal(PhaseServing))
	})

	It("fails closed on a failed probe: rolls back the pod and counts the failure", func() {
		warm("eval-fail", 150)
		markPodReady(ctx, podKey())
		prober.err = errors.New("unhealthy")
		result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).NotTo(BeZero())

		md := get()
		Expect(md.Status.Phase).To(Equal(PhaseRolledBack))
		Expect(md.Status.ActivePod).To(BeEmpty())
		Expect(md.Status.EvalFailures).To(Equal(int32(1)))
		Expect(md.Status.Endpoint).To(BeEmpty())
		err = k8sClient.Get(ctx, podKey(), &corev1.Pod{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})

	It("fails closed on a probe that exceeds the gate timeout", func() {
		warm("eval-timeout", 20)
		markPodReady(ctx, podKey())
		prober.block = true
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(get().Status.Phase).To(Equal(PhaseRolledBack))
	})

	It("pauses promotion after repeated failures until approved", func() {
		warm("eval-breaker", 150)
		md := get()
		md.Status.EvalFailures = evalFailureThreshold
		Expect(k8sClient.Status().Update(ctx, &md)).To(Succeed())
		markPodReady(ctx, podKey())

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(get().Status.Phase).To(Equal(PhasePromotionPaused))
		Expect(prober.lastURL).To(BeEmpty())

		By("approval resets the breaker and lets the gate run")
		md = get()
		md.Annotations = map[string]string{approvePromotionAnnotation: labelValueTrue}
		Expect(k8sClient.Update(ctx, &md)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
		Expect(err).NotTo(HaveOccurred())
		md = get()
		Expect(md.Status.Phase).To(Equal(PhaseServing))
		Expect(md.Status.EvalFailures).To(BeZero())
		Expect(md.Annotations).NotTo(HaveKey(approvePromotionAnnotation))
	})
})

func TestHTTPEvalProber(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer ok.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) }))
	defer bad.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, ok.URL, http.StatusFound) }))
	defer redirect.Close()

	p := &HTTPEvalProber{}
	if err := p.Probe(context.Background(), ok.URL); err != nil {
		t.Errorf("2xx probe = %v, want nil", err)
	}
	if err := p.Probe(context.Background(), bad.URL); err == nil {
		t.Error("5xx probe = nil, want error")
	}
	if err := p.Probe(context.Background(), redirect.URL); err == nil {
		t.Error("redirect probe = nil, want error (redirects must not be followed)")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Probe(ctx, ok.URL); err == nil {
		t.Error("cancelled-context probe = nil, want error")
	}
}

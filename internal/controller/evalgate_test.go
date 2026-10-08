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
	"encoding/json"
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

const (
	mathPrompt   = "2+2="
	otherPrompt  = "other"
	canaryCMName = "canaries"
)

type fakeCanary struct {
	err    error
	called bool
	url    string
	model  string
	got    []Canary
}

func (f *fakeCanary) Run(_ context.Context, baseURL, model string, canaries []Canary) error {
	f.called, f.url, f.model, f.got = true, baseURL, model, canaries
	return f.err
}

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
		canary     *fakeCanary
		mdKey      types.NamespacedName
	)

	// warm creates a node + deployment and reconciles it to Warming with a
	// cold-created pod (no PausePool exists).
	warmWithGate := func(name string, gate amphorav1alpha1.EvalGateSpec, withRunner bool) {
		mdKey = types.NamespacedName{Name: name, Namespace: testNamespace}
		prober = &fakeProber{}
		canary = &fakeCanary{}
		reconciler = &ModelDeploymentReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(), Scheduler: scheduler.NewScheduler(nil),
			EvalProber: prober, APIReader: k8sClient,
		}
		if withRunner {
			reconciler.CanaryRunner = canary
		}
		Expect(k8sClient.Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName, Labels: map[string]string{NodeVRAMLabel: "80000"}}})).To(Succeed())
		Expect(k8sClient.Create(ctx, &amphorav1alpha1.ModelDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec: amphorav1alpha1.ModelDeploymentSpec{
				Image: testImage, TenancyClass: amphorav1alpha1.TenancySingleTenant,
				EvalGate: gate,
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
	warm := func(name string, timeoutMillis int32) {
		warmWithGate(name, amphorav1alpha1.EvalGateSpec{TimeoutMillis: timeoutMillis}, true)
	}
	canaryGate := amphorav1alpha1.EvalGateSpec{TimeoutMillis: 150, Enabled: true, CanaryConfigMapRef: canaryCMName}
	putCanaries := func(data string) {
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: canaryCMName, Namespace: testNamespace}, Data: map[string]string{canaryConfigKey: data}}
		Expect(k8sClient.Create(ctx, cm)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, cm) })
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

	It("promotes with QualityVerified=True when every canary matches", func() {
		putCanaries(`[{"prompt":"2+2=","expected":"4"}]`)
		warmWithGate("eval-canary-pass", canaryGate, true)
		markPodReady(ctx, podKey())
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
		Expect(err).NotTo(HaveOccurred())

		md := get()
		Expect(md.Status.Phase).To(Equal(PhaseServing))
		Expect(canary.url).To(Equal("http://10.0.0.7:8000"))
		Expect(canary.model).To(Equal("eval-canary-pass"))
		Expect(canary.got).To(HaveLen(1))
		cond := meta.FindStatusCondition(md.Status.Conditions, conditionQualityVerified)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal(reasonCanaryPassed))
	})

	It("rolls back and records CanaryFailed when a canary mismatches", func() {
		putCanaries(`[{"prompt":"2+2=","expected":"4"}]`)
		warmWithGate("eval-canary-fail", canaryGate, true)
		markPodReady(ctx, podKey())
		canary.err = errors.New("canary 0: got \"5\", want \"4\"")
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
		Expect(err).NotTo(HaveOccurred())

		md := get()
		Expect(md.Status.Phase).To(Equal(PhaseRolledBack))
		Expect(md.Status.EvalFailures).To(Equal(int32(1)))
		Expect(md.Status.Endpoint).To(BeEmpty())
		cond := meta.FindStatusCondition(md.Status.Conditions, conditionQualityVerified)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(reasonCanaryFailed))
	})

	It("fails closed when the canary ConfigMap is missing or malformed", func() {
		warmWithGate("eval-canary-nocm", canaryGate, true)
		markPodReady(ctx, podKey())
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(get().Status.Phase).To(Equal(PhaseRolledBack))
		Expect(canary.called).To(BeFalse())
	})

	It("fails closed when canaries are configured but no CanaryRunner is wired", func() {
		putCanaries(`[{"prompt":"2+2=","expected":"4"}]`)
		warmWithGate("eval-canary-norunner", canaryGate, false)
		markPodReady(ctx, podKey())
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(get().Status.Phase).To(Equal(PhaseRolledBack))
	})

	It("ignores canaryConfigMapRef unless evalGate.enabled, staying latency-only", func() {
		putCanaries(`[{"prompt":"2+2=","expected":"4"}]`)
		warmWithGate("eval-canary-disabled", amphorav1alpha1.EvalGateSpec{TimeoutMillis: 150, CanaryConfigMapRef: canaryCMName}, true)
		markPodReady(ctx, podKey())
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: mdKey})
		Expect(err).NotTo(HaveOccurred())
		md := get()
		Expect(md.Status.Phase).To(Equal(PhaseServing))
		Expect(canary.called).To(BeFalse())
		Expect(meta.FindStatusCondition(md.Status.Conditions, conditionQualityVerified).Reason).To(Equal(reasonLatencyOnly))
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

func TestParseCanaries(t *testing.T) {
	cm := func(data map[string]string) *corev1.ConfigMap {
		return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"}, Data: data}
	}
	many := "["
	for i := 0; i <= maxCanaries; i++ {
		if i > 0 {
			many += ","
		}
		many += `{"prompt":"p","expected":"e"}`
	}
	many += "]"
	bad := map[string]*corev1.ConfigMap{
		"missing key":     cm(map[string]string{otherPrompt: "[]"}),
		"invalid json":    cm(map[string]string{canaryConfigKey: "{"}),
		"empty set":       cm(map[string]string{canaryConfigKey: "[]"}),
		"too many":        cm(map[string]string{canaryConfigKey: many}),
		"empty prompt":    cm(map[string]string{canaryConfigKey: `[{"prompt":"","expected":"e"}]`}),
		"blank expected":  cm(map[string]string{canaryConfigKey: `[{"prompt":"p","expected":"  "}]`}),
		"negative tokens": cm(map[string]string{canaryConfigKey: `[{"prompt":"p","expected":"e","max_tokens":-1}]`}),
		"unknown shape":   cm(map[string]string{canaryConfigKey: `"nope"`}),
	}
	for name, c := range bad {
		if _, err := parseCanaries(c); err == nil {
			t.Errorf("%s: parseCanaries = nil error, want error", name)
		}
	}
	got, err := parseCanaries(cm(map[string]string{canaryConfigKey: `[{"prompt":"2+2=","expected":"4","max_tokens":3}]`}))
	if err != nil || len(got) != 1 || got[0].MaxTokens != 3 {
		t.Fatalf("parseCanaries = %+v, %v", got, err)
	}
}

func TestHTTPCanaryRunner(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		prompt, _ := gotBody["prompt"].(string)
		switch prompt {
		case mathPrompt:
			_, _ = w.Write([]byte(`{"choices":[{"text":"  4\n"}]}`))
		case "empty":
			_, _ = w.Write([]byte(`{"choices":[]}`))
		case "garbage":
			_, _ = w.Write([]byte(`not json`))
		case "500":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			_, _ = w.Write([]byte(`{"choices":[{"text":"wrong"}]}`))
		}
	}))
	defer srv.Close()
	r := &HTTPCanaryRunner{}
	ctx := context.Background()

	if err := r.Run(ctx, srv.URL, "m", []Canary{{Prompt: mathPrompt, Expected: "4"}}); err != nil {
		t.Fatalf("matching canary (whitespace-trimmed) = %v, want nil", err)
	}
	if gotBody["model"] != "m" || gotBody["temperature"] != float64(0) || gotBody["max_tokens"] != float64(defaultCanaryMaxTokens) {
		t.Errorf("request body = %v", gotBody)
	}
	for _, p := range []string{otherPrompt, "empty", "garbage", "500"} {
		if err := r.Run(ctx, srv.URL, "m", []Canary{{Prompt: p, Expected: "4"}}); err == nil {
			t.Errorf("prompt %q: Run = nil, want error", p)
		}
	}
	if err := r.Run(ctx, srv.URL, "m", []Canary{{Prompt: mathPrompt, Expected: "4"}, {Prompt: otherPrompt, Expected: "4"}}); err == nil {
		t.Error("second canary mismatch must fail the whole run")
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := r.Run(cctx, srv.URL, "m", []Canary{{Prompt: mathPrompt, Expected: "4"}}); err == nil {
		t.Error("cancelled context: Run = nil, want error")
	}
}

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

package proxy

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
	"github.com/ramin-fazli/amphora/internal/controller"
)

// trackingHandler returns a handler over a registry in known-model tracking
// mode (what -sync-from-cluster enables), synced unless stated.
func trackingHandler(t *testing.T, cfg Config, synced bool) (*Handler, *Registry, *fakeNotifier, *prometheus.Registry) {
	t.Helper()
	reg := NewRegistry()
	reg.EnableKnownTracking()
	if synced {
		reg.SetSynced()
	}
	n := &fakeNotifier{registry: reg}
	prom := prometheus.NewRegistry()
	if cfg.DeferralTimeout == 0 {
		cfg.DeferralTimeout = 3 * time.Second
	}
	return NewHandler(reg, n, cfg, prom, slog.New(slog.NewTextHandler(io.Discard, nil))), reg, n, prom
}

func serve(h *Handler, model string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req.Header.Set(ModelHeader, model)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func counterValue(t *testing.T, prom *prometheus.Registry, model, path string) float64 {
	t.Helper()
	mfs, err := prom.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "amphora_proxy_requests_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsMatch(m, model, path) {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func labelsMatch(m *dto.Metric, model, path string) bool {
	got := map[string]string{}
	for _, l := range m.GetLabel() {
		got[l.GetName()] = l.GetValue()
	}
	return got["model"] == model && got["path"] == path
}

func TestUnknownModelIsNotDeferred(t *testing.T) {
	h, _, n, prom := trackingHandler(t, Config{DeferralTimeout: 30 * time.Second}, true)

	start := time.Now()
	rec := serve(h, "team-a/never-created")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("got %d, want 404", rec.Code)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("unknown model took %v: it was parked instead of rejected", took)
	}
	if atomic.LoadInt32(&n.calls) != 0 {
		t.Fatal("a wakeup was sent for a model that does not exist")
	}
	if got := counterValue(t, prom, deniedModelLabel, "unknown_model"); got != 1 {
		t.Fatalf("unknown_model counter = %v, want 1", got)
	}
	if got := counterValue(t, prom, "team-a/never-created", "cold"); got != 0 {
		t.Fatal("the caller-supplied name leaked into a metric label")
	}
}

func TestKnownColdModelIsStillDeferredThenRouted(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer upstream.Close()
	h, reg, _, _ := trackingHandler(t, Config{}, true)
	reg.MarkKnown(modelA)
	go func() {
		time.Sleep(50 * time.Millisecond)
		reg.MarkWarm(Target{Model: modelA, Tenant: tenantA, Upstream: upstream.URL})
	}()

	if rec := serve(h, modelA); rec.Code != http.StatusOK {
		t.Fatalf("got %d, want 200 once the model warms", rec.Code)
	}
}

func TestBeforeInitialSyncTheProxyDoesNotGuess(t *testing.T) {
	h, _, _, _ := trackingHandler(t, Config{}, false)
	rec := serve(h, modelA)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("got %d (Retry-After %q); want 503 with Retry-After, not a 404 for a model that may exist",
			rec.Code, rec.Header().Get("Retry-After"))
	}
}

func TestDeletingAModelReleasesDeferredRequests(t *testing.T) {
	h, reg, _, _ := trackingHandler(t, Config{DeferralTimeout: 30 * time.Second}, true)
	reg.MarkKnown(modelA)

	done := make(chan int, 1)
	go func() { done <- serve(h, modelA).Code }()
	time.Sleep(100 * time.Millisecond) // let it park
	reg.MarkUnknown(modelA)

	select {
	case code := <-done:
		if code != http.StatusNotFound {
			t.Fatalf("got %d, want 404", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a request deferred on a deleted model was not released")
	}
}

func TestAwaitReportsModelGone(t *testing.T) {
	reg := NewRegistry()
	errc := make(chan error, 1)
	go func() {
		_, err := reg.Await(t.Context(), "m")
		errc <- err
	}()
	time.Sleep(50 * time.Millisecond)
	reg.MarkUnknown("m")
	if err := <-errc; !errors.Is(err, ErrModelGone) {
		t.Fatalf("Await = %v, want ErrModelGone", err)
	}
}

func TestDeferredRequestsAreBoundedPerModelAndInTotal(t *testing.T) {
	h, reg, _, prom := trackingHandler(t, Config{MaxDeferredPerModel: 2, MaxDeferredTotal: 3, DeferralTimeout: 10 * time.Second}, true)
	for _, m := range []string{"ns/a", "ns/b", "ns/c"} {
		reg.MarkKnown(m)
	}

	var wg sync.WaitGroup
	park := func(model string) {
		wg.Add(1)
		go func() { defer wg.Done(); _ = serve(h, model) }()
	}
	park("ns/a")
	park("ns/a")
	park("ns/b")
	time.Sleep(150 * time.Millisecond) // all three parked

	over := serve(h, "ns/a") // per-model cap (2) reached
	if over.Code != http.StatusServiceUnavailable || over.Header().Get("Retry-After") == "" {
		t.Fatalf("per-model cap: got %d (Retry-After %q), want 503 with Retry-After", over.Code, over.Header().Get("Retry-After"))
	}
	if got := serve(h, "ns/c"); got.Code != http.StatusServiceUnavailable { // global cap (3) reached
		t.Fatalf("global cap: got %d, want 503", got.Code)
	}
	if counterValue(t, prom, "ns/a", "overloaded") != 1 {
		t.Fatal("overload was not counted")
	}

	// Releasing the parked requests frees capacity again.
	up := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer up.Close()
	for _, m := range []string{"ns/a", "ns/b"} {
		reg.MarkWarm(Target{Model: m, Upstream: up.URL})
	}
	wg.Wait()
	reg.MarkCold("ns/c")
	reg.MarkWarm(Target{Model: "ns/c", Upstream: up.URL})
	if got := serve(h, "ns/c"); got.Code != http.StatusOK {
		t.Fatalf("after release: got %d, want 200", got.Code)
	}
}

func TestApplyModelDeploymentTracksExistence(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := NewRegistry()
	md := &amphorav1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: tenantA}}
	md.Status.Phase = controller.PhaseWarming

	ApplyModelDeployment(reg, md, logger)
	if !reg.IsKnown(modelA) {
		t.Fatal("a Warming deployment exists and must be known (so it is deferred, not 404'd)")
	}
	if _, warm := reg.Warm(modelA); warm {
		t.Fatal("a Warming deployment must not be routable")
	}

	now := metav1.NewTime(time.Now())
	md.DeletionTimestamp = &now
	ApplyModelDeployment(reg, md, logger)
	if reg.IsKnown(modelA) {
		t.Fatal("a deployment being deleted must be forgotten")
	}
}

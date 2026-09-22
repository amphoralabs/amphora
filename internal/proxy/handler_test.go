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
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// fakeNotifier records NotifyWakeup calls and, if warmAfter is set, marks the
// model warm on a background goroutine shortly after being notified —
// simulating the Controller completing a cold start.
type fakeNotifier struct {
	calls     int32
	registry  *Registry
	warmAfter time.Duration
	target    Target
}

func (f *fakeNotifier) NotifyWakeup(_ context.Context, model, _ string) error {
	atomic.AddInt32(&f.calls, 1)
	if f.warmAfter > 0 {
		go func() {
			time.Sleep(f.warmAfter)
			f.registry.MarkWarm(f.target)
		}()
	}
	return nil
}

func TestHandlerWarmPathRoutesImmediately(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get(ModelHeader); got != "m1" {
			t.Errorf("upstream got model header %q, want m1", got)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	registry := NewRegistry()
	registry.MarkWarm(Target{Model: "m1", Upstream: upstream.URL})

	notifier := &fakeNotifier{registry: registry}
	h := NewHandler(registry, notifier, Config{}, prometheus.NewRegistry(), nil)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(ModelHeader, "m1")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if atomic.LoadInt32(&notifier.calls) != 0 {
		t.Fatal("expected no wakeup notification on the warm path")
	}
}

func TestHandlerColdPathDefersThenRoutes(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	registry := NewRegistry()
	notifier := &fakeNotifier{
		registry:  registry,
		warmAfter: 20 * time.Millisecond,
		target:    Target{Model: "m1", Upstream: upstream.URL},
	}
	h := NewHandler(registry, notifier, Config{DeferralTimeout: 2 * time.Second}, prometheus.NewRegistry(), nil)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(ModelHeader, "m1")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if atomic.LoadInt32(&notifier.calls) != 1 {
		t.Fatalf("expected exactly one wakeup notification, got %d", notifier.calls)
	}
}

func TestHandlerColdPathTimesOut(t *testing.T) {
	registry := NewRegistry()
	notifier := &fakeNotifier{registry: registry} // never marks warm
	h := NewHandler(registry, notifier, Config{DeferralTimeout: 20 * time.Millisecond}, prometheus.NewRegistry(), nil)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(ModelHeader, "never-warmed")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("got status %d, want 503", rec.Code)
	}
}

func TestHandlerMissingModelHeader(t *testing.T) {
	registry := NewRegistry()
	h := NewHandler(registry, nil, Config{}, prometheus.NewRegistry(), nil)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got status %d, want 400", rec.Code)
	}
}

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

// Package proxy implements the connection-deferral proxy PoC described in
// Technical Specification v3 §3.1/§10: route to a warm target immediately if
// one exists, otherwise hold the inbound connection open (rather than
// failing fast) while a cold-start wakeup is triggered elsewhere.
package proxy

import (
	"context"
	"errors"
	"sync"
)

// ErrModelGone is returned by Await when the model it was waiting for is
// removed (its ModelDeployment was deleted) rather than becoming warm.
var ErrModelGone = errors.New("proxy: model no longer exists")

// Target is a routable upstream for a model.
type Target struct {
	// Model is the model name the target serves.
	Model string
	// Tenant is the owning tenant, propagated for cost/audit tagging.
	Tenant string
	// Upstream is the target's base URL (e.g. "http://10.0.0.5:8000").
	Upstream string
}

// Registry tracks which models currently have a warm (ready-to-serve)
// target. It is an in-memory PoC stand-in for the warm-capacity map the
// Controller/Packing Scheduler will eventually publish over gRPC; production
// wiring is tracked as follow-up work once the Controller's reconcile/hijack
// logic lands.
type Registry struct {
	mu      sync.Mutex
	targets map[string]Target
	waiters map[string][]chan Target

	// Known-model tracking. When enabled (cluster sync mode), the registry
	// also knows which models EXIST, warm or not, so the handler can answer
	// 404 for names that do not exist instead of parking the connection
	// until the deferral timeout. Disabled by default: standalone/dev use has
	// no source of truth for existence, so every name is treated as deferrable.
	tracking bool
	synced   bool
	known    map[string]struct{}
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		targets: make(map[string]Target),
		waiters: make(map[string][]chan Target),
		known:   make(map[string]struct{}),
	}
}

// Warm returns the current warm target for a model, if any, without
// blocking.
func (r *Registry) Warm(model string) (Target, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.targets[model]
	return t, ok
}

// MarkWarm registers (or updates) the warm target for a model and releases
// any requests currently deferred on it. In production this is invoked by
// the Controller's wakeup-complete callback; the PoC exposes it directly so
// an admin endpoint or tests can simulate that transition.
func (r *Registry) MarkWarm(t Target) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.targets[t.Model] = t
	for _, ch := range r.waiters[t.Model] {
		ch <- t
		close(ch)
	}
	delete(r.waiters, t.Model)
}

// MarkCold removes a model's warm target (e.g. scaled to zero).
func (r *Registry) MarkCold(model string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.targets, model)
}

// EnableKnownTracking turns on known-model tracking (see Registry). Until
// SetSynced is called the handler answers 503 rather than guessing.
func (r *Registry) EnableKnownTracking() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tracking = true
}

// KnownTracking reports whether known-model tracking is enabled.
func (r *Registry) KnownTracking() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tracking
}

// MarkKnown records that model exists (whether or not it is warm).
func (r *Registry) MarkKnown(model string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.known[model] = struct{}{}
}

// MarkUnknown records that model no longer exists: it is forgotten, its warm
// target is dropped, and requests deferred on it are released with
// ErrModelGone instead of waiting out their timeout.
func (r *Registry) MarkUnknown(model string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.known, model)
	delete(r.targets, model)
	for _, ch := range r.waiters[model] {
		close(ch) // closed without a value: Await reports ErrModelGone
	}
	delete(r.waiters, model)
}

// IsKnown reports whether model exists (warm or not).
func (r *Registry) IsKnown(model string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.known[model]
	return ok
}

// SetSynced records that the initial set of known models has been loaded, so
// absence from the known set now means "does not exist".
func (r *Registry) SetSynced() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.synced = true
}

// Synced reports whether the known-model set is complete.
func (r *Registry) Synced() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.synced
}

// Await blocks until model becomes warm, ctx is done, or returns
// immediately if the model is already warm. This backs connection deferral
// (§10): the proxy holds the inbound request open rather than failing fast
// on a cold model.
func (r *Registry) Await(ctx context.Context, model string) (Target, error) {
	r.mu.Lock()
	if t, ok := r.targets[model]; ok {
		r.mu.Unlock()
		return t, nil
	}
	ch := make(chan Target, 1)
	r.waiters[model] = append(r.waiters[model], ch)
	r.mu.Unlock()

	select {
	case t, ok := <-ch:
		if !ok {
			return Target{}, ErrModelGone
		}
		return t, nil
	case <-ctx.Done():
		r.removeWaiter(model, ch)
		return Target{}, ctx.Err()
	}
}

func (r *Registry) removeWaiter(model string, ch chan Target) {
	r.mu.Lock()
	defer r.mu.Unlock()
	waiters := r.waiters[model]
	for i, c := range waiters {
		if c == ch {
			r.waiters[model] = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}
	if len(r.waiters[model]) == 0 {
		delete(r.waiters, model)
	}
}

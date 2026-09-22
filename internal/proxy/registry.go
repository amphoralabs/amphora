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
	"sync"
)

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
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		targets: make(map[string]Target),
		waiters: make(map[string][]chan Target),
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
	case t := <-ch:
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

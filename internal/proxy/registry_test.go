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
	"testing"
	"time"
)

const testUpstream = "http://upstream:8000"

func TestRegistryWarm(t *testing.T) {
	r := NewRegistry()

	if _, ok := r.Warm("m1"); ok {
		t.Fatal("expected model to be cold before MarkWarm")
	}

	target := Target{Model: "m1", Upstream: testUpstream}
	r.MarkWarm(target)

	got, ok := r.Warm("m1")
	if !ok {
		t.Fatal("expected model to be warm after MarkWarm")
	}
	if got != target {
		t.Fatalf("got %+v, want %+v", got, target)
	}

	r.MarkCold("m1")
	if _, ok := r.Warm("m1"); ok {
		t.Fatal("expected model to be cold after MarkCold")
	}
}

func TestRegistryAwaitAlreadyWarm(t *testing.T) {
	r := NewRegistry()
	target := Target{Model: "m1", Upstream: testUpstream}
	r.MarkWarm(target)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	got, err := r.Await(ctx, "m1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != target {
		t.Fatalf("got %+v, want %+v", got, target)
	}
}

func TestRegistryAwaitBlocksUntilWarm(t *testing.T) {
	r := NewRegistry()
	target := Target{Model: "m1", Upstream: testUpstream}

	resultCh := make(chan Target, 1)
	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		got, err := r.Await(ctx, "m1")
		resultCh <- got
		errCh <- err
	}()

	// Give the goroutine a moment to register as a waiter before marking warm.
	time.Sleep(20 * time.Millisecond)
	r.MarkWarm(target)

	select {
	case got := <-resultCh:
		if err := <-errCh; err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != target {
			t.Fatalf("got %+v, want %+v", got, target)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Await to return")
	}
}

func TestRegistryAwaitTimesOut(t *testing.T) {
	r := NewRegistry()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := r.Await(ctx, "never-warmed")
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	// The waiter should have been cleaned up rather than leaked.
	r.mu.Lock()
	defer r.mu.Unlock()
	if waiters, ok := r.waiters["never-warmed"]; ok && len(waiters) != 0 {
		t.Fatalf("expected no leftover waiters, got %d", len(waiters))
	}
}

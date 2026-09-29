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
	"io"
	"log/slog"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
	"github.com/ramin-fazli/amphora/internal/controller"
)

func testMD(phase, endpoint string) *amphorav1alpha1.ModelDeployment {
	return &amphorav1alpha1.ModelDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "team-a"},
		Status:     amphorav1alpha1.ModelDeploymentStatus{Phase: phase, Endpoint: endpoint},
	}
}

func TestApplyModelDeployment(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	const key = "team-a/llama"

	tests := []struct {
		name     string
		md       *amphorav1alpha1.ModelDeployment
		wantWarm bool
	}{
		{"serving with endpoint is warm", testMD(controller.PhaseServing, "http://10.0.0.5:8000"), true},
		{"warming is not routable", testMD(controller.PhaseWarming, "http://10.0.0.5:8000"), false},
		{"rolled back is not routable", testMD(controller.PhaseRolledBack, ""), false},
		{"serving without endpoint is cold", testMD(controller.PhaseServing, ""), false},
		{"non-http endpoint is rejected", testMD(controller.PhaseServing, "file:///etc/passwd"), false},
		{"garbage endpoint is rejected", testMD(controller.PhaseServing, "://bad"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg := NewRegistry()
			ApplyModelDeployment(reg, tc.md, logger)
			tgt, warm := reg.Warm(key)
			if warm != tc.wantWarm {
				t.Fatalf("warm = %v, want %v", warm, tc.wantWarm)
			}
			if warm && (tgt.Upstream != tc.md.Status.Endpoint || tgt.Tenant != "team-a" || tgt.Model != key) {
				t.Fatalf("target = %+v", tgt)
			}
		})
	}
}

func TestApplyModelDeploymentTransitionsAndDeletion(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := NewRegistry()
	md := testMD(controller.PhaseServing, "http://10.0.0.5:8000")
	ApplyModelDeployment(reg, md, logger)
	if _, warm := reg.Warm("team-a/llama"); !warm {
		t.Fatal("want warm after Serving")
	}

	md.Status.Phase = controller.PhaseWarming // pod replaced, being re-gated
	ApplyModelDeployment(reg, md, logger)
	if _, warm := reg.Warm("team-a/llama"); warm {
		t.Fatal("want cold once no longer Serving")
	}

	md.Status.Phase = controller.PhaseServing
	ApplyModelDeployment(reg, md, logger)
	now := metav1.NewTime(time.Now())
	md.DeletionTimestamp = &now
	ApplyModelDeployment(reg, md, logger)
	if _, warm := reg.Warm("team-a/llama"); warm {
		t.Fatal("want cold while being deleted")
	}
}

func TestModelKeyIsNamespaceQualified(t *testing.T) {
	a := testMD("", "")
	b := testMD("", "")
	b.Namespace = "team-b"
	if ModelKey(a) == ModelKey(b) {
		t.Fatal("same-named deployments in different namespaces must not share a key")
	}
}

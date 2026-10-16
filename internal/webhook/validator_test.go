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

package webhook

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
)

const (
	testImage         = "example.com/m:v1"
	euWest1           = "eu-west-1"
	errAllowedRegions = "allowedRegions is required"
	errMatrix         = "violates tenancy isolation matrix"
)

func md(spec amphorav1alpha1.ModelDeploymentSpec) *amphorav1alpha1.ModelDeployment {
	return &amphorav1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"}, Spec: spec}
}

func TestModelDeploymentValidation(t *testing.T) {
	regulated := func(mut func(*amphorav1alpha1.ModelDeploymentSpec)) *amphorav1alpha1.ModelDeployment {
		s := amphorav1alpha1.ModelDeploymentSpec{
			Image: testImage, TenancyClass: amphorav1alpha1.TenancyRegulatedMultiTenant,
			GPUFraction: "1g.10gb", AllowedRegions: []string{euWest1},
		}
		if mut != nil {
			mut(&s)
		}
		return md(s)
	}
	tests := []struct {
		name    string
		obj     *amphorav1alpha1.ModelDeployment
		wantErr string // substring; empty means admitted
	}{
		{"single tenant minimal is admitted", md(amphorav1alpha1.ModelDeploymentSpec{Image: testImage, TenancyClass: amphorav1alpha1.TenancySingleTenant}), ""},
		{"trusted with MIG profile is admitted", md(amphorav1alpha1.ModelDeploymentSpec{Image: testImage, TenancyClass: amphorav1alpha1.TenancyTrustedMultiTenant, GPUFraction: "3g.40gb"}), ""},
		{"fully specified regulated is admitted", regulated(nil), ""},
		{"empty image", md(amphorav1alpha1.ModelDeploymentSpec{Image: "  ", TenancyClass: amphorav1alpha1.TenancySingleTenant}), "spec.image"},
		{"unknown tenancy class", md(amphorav1alpha1.ModelDeploymentSpec{Image: testImage, TenancyClass: "Bogus"}), "not a known tenancy class"},
		{"malformed gpuFraction", md(amphorav1alpha1.ModelDeploymentSpec{Image: testImage, TenancyClass: amphorav1alpha1.TenancySingleTenant, GPUFraction: "half"}), "spec.gpuFraction"},
		{"regulated without gpuFraction", regulated(func(s *amphorav1alpha1.ModelDeploymentSpec) { s.GPUFraction = "" }), "gpuFraction is required"},
		{"regulated without allowedRegions", regulated(func(s *amphorav1alpha1.ModelDeploymentSpec) { s.AllowedRegions = nil }), errAllowedRegions},
		{"regulated with malformed gpuFraction", regulated(func(s *amphorav1alpha1.ModelDeploymentSpec) { s.GPUFraction = "nope" }), "spec.gpuFraction"},
		{"empty region entry", regulated(func(s *amphorav1alpha1.ModelDeploymentSpec) { s.AllowedRegions = []string{euWest1, " "} }), "allowedRegions[1] must not be empty"},
		{"region that can never match a label value", regulated(func(s *amphorav1alpha1.ModelDeploymentSpec) { s.AllowedRegions = []string{"eu west/1"} }), "cannot be a node region label value"},
		{"duplicate region", regulated(func(s *amphorav1alpha1.ModelDeploymentSpec) { s.AllowedRegions = []string{"a", "a"} }), "duplicate region"},
		{"invalid canary configmap name", md(amphorav1alpha1.ModelDeploymentSpec{Image: testImage, TenancyClass: amphorav1alpha1.TenancySingleTenant,
			EvalGate: amphorav1alpha1.EvalGateSpec{Enabled: true, CanaryConfigMapRef: "Bad_Name/../x"}}), "not a valid ConfigMap name"},
		{"all problems are reported together", regulated(func(s *amphorav1alpha1.ModelDeploymentSpec) { s.GPUFraction = ""; s.AllowedRegions = nil }), errAllowedRegions},
	}
	v := &ModelDeploymentValidator{}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for opName, op := range map[string]func() error{
				"create": func() error { _, err := v.ValidateCreate(context.Background(), tc.obj); return err },
				"update": func() error { _, err := v.ValidateUpdate(context.Background(), tc.obj, tc.obj); return err },
			} {
				err := op()
				switch {
				case tc.wantErr == "" && err != nil:
					t.Errorf("%s: unexpected rejection: %v", opName, err)
				case tc.wantErr != "" && err == nil:
					t.Errorf("%s: admitted, want rejection containing %q", opName, tc.wantErr)
				case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
					t.Errorf("%s: error %q does not contain %q", opName, err, tc.wantErr)
				}
			}
		})
	}
}

func TestModelDeploymentAllProblemsReported(t *testing.T) {
	_, err := (&ModelDeploymentValidator{}).ValidateCreate(context.Background(), md(amphorav1alpha1.ModelDeploymentSpec{
		Image: testImage, TenancyClass: amphorav1alpha1.TenancyRegulatedMultiTenant,
	}))
	if err == nil || !strings.Contains(err.Error(), "gpuFraction is required") || !strings.Contains(err.Error(), errAllowedRegions) {
		t.Fatalf("want both problems in one rejection, got %v", err)
	}
}

func TestModelDeploymentWarnings(t *testing.T) {
	v := &ModelDeploymentValidator{}
	ctx := context.Background()
	warn := func(spec amphorav1alpha1.ModelDeploymentSpec) string {
		w, err := v.ValidateCreate(ctx, md(spec))
		if err != nil {
			t.Fatalf("unexpected rejection: %v", err)
		}
		return strings.Join(w, "|")
	}
	base := amphorav1alpha1.ModelDeploymentSpec{Image: testImage, TenancyClass: amphorav1alpha1.TenancySingleTenant}

	if got := warn(base); got != "" {
		t.Errorf("plain spec warnings = %q, want none", got)
	}
	s := base
	s.EvalGate = amphorav1alpha1.EvalGateSpec{Enabled: true}
	if got := warn(s); !strings.Contains(got, "no canaryConfigMapRef") {
		t.Errorf("enabled gate without canaries: warnings = %q", got)
	}
	s = base
	s.EvalGate = amphorav1alpha1.EvalGateSpec{CanaryConfigMapRef: "canaries"}
	if got := warn(s); !strings.Contains(got, "ignored unless") {
		t.Errorf("canary ref without enabled: warnings = %q", got)
	}
	s = base
	s.AllowedRegions = []string{euWest1}
	if got := warn(s); got != "" {
		t.Errorf("allowedRegions is enforced now, so it must not warn; got %q", got)
	}
}

func TestValidatorsRejectWrongType(t *testing.T) {
	if _, err := (&ModelDeploymentValidator{}).ValidateCreate(context.Background(), &corev1.Pod{}); err == nil {
		t.Error("ModelDeploymentValidator accepted a Pod")
	}
	if _, err := (&PausePoolValidator{}).ValidateCreate(context.Background(), &corev1.Pod{}); err == nil {
		t.Error("PausePoolValidator accepted a Pod")
	}
	if w, err := (&ModelDeploymentValidator{}).ValidateDelete(context.Background(), &corev1.Pod{}); w != nil || err != nil {
		t.Error("delete must be a no-op")
	}
	if w, err := (&PausePoolValidator{}).ValidateDelete(context.Background(), &corev1.Pod{}); w != nil || err != nil {
		t.Error("delete must be a no-op")
	}
}

func TestPausePoolValidation(t *testing.T) {
	pool := func(class amphorav1alpha1.TenancyClass, slice, node string) *amphorav1alpha1.PausePool {
		return &amphorav1alpha1.PausePool{
			ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"},
			Spec:       amphorav1alpha1.PausePoolSpec{NodeName: node, TenancyClass: class, GPUSlice: slice, TargetSize: 1},
		}
	}
	const node = "gpu-node-1"
	tests := []struct {
		name    string
		obj     *amphorav1alpha1.PausePool
		wantErr string
	}{
		{"single tenant full", pool(amphorav1alpha1.TenancySingleTenant, "full", node), ""},
		{"trusted timeslice", pool(amphorav1alpha1.TenancyTrustedMultiTenant, "timeslice", node), ""},
		{"regulated MIG", pool(amphorav1alpha1.TenancyRegulatedMultiTenant, "1g.10gb", node), ""},
		{"regulated timeslice violates the matrix", pool(amphorav1alpha1.TenancyRegulatedMultiTenant, "timeslice", node), errMatrix},
		{"regulated full violates the matrix", pool(amphorav1alpha1.TenancyRegulatedMultiTenant, "full", node), errMatrix},
		{"trusted full violates the matrix", pool(amphorav1alpha1.TenancyTrustedMultiTenant, "full", node), errMatrix},
		{"unknown slice form", pool(amphorav1alpha1.TenancySingleTenant, "weird", node), "spec.gpuSlice"},
		{"bad node name", pool(amphorav1alpha1.TenancySingleTenant, "full", "Not A Node!"), "spec.nodeName"},
	}
	v := &PausePoolValidator{}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := v.ValidateCreate(context.Background(), tc.obj)
			_, errU := v.ValidateUpdate(context.Background(), tc.obj, tc.obj)
			for _, e := range []error{err, errU} {
				switch {
				case tc.wantErr == "" && e != nil:
					t.Errorf("unexpected rejection: %v", e)
				case tc.wantErr != "" && (e == nil || !strings.Contains(e.Error(), tc.wantErr)):
					t.Errorf("error %v, want containing %q", e, tc.wantErr)
				}
			}
		})
	}
}

func rl(kv ...string) corev1.ResourceList {
	out := corev1.ResourceList{}
	for i := 0; i+1 < len(kv); i += 2 {
		out[corev1.ResourceName(kv[i])] = resource.MustParse(kv[i+1])
	}
	return out
}

func TestResourceValidation(t *testing.T) {
	const gpu = "nvidia.com/gpu"
	tests := []struct {
		name    string
		rr      corev1.ResourceRequirements
		wantErr string // empty means valid
	}{
		{"nothing requested", corev1.ResourceRequirements{}, ""},
		{"whole GPU limit only", corev1.ResourceRequirements{Limits: rl(gpu, "1")}, ""},
		{"MIG slice, request equals limit", corev1.ResourceRequirements{Requests: rl("nvidia.com/mig-1g.10gb", "1"), Limits: rl("nvidia.com/mig-1g.10gb", "1")}, ""},
		{"guaranteed cpu and memory with a GPU", corev1.ResourceRequirements{Requests: rl("cpu", "4", "memory", "16Gi"), Limits: rl("cpu", "4", "memory", "16Gi", gpu, "1")}, ""},
		{"extended resource requested but not limited", corev1.ResourceRequirements{Requests: rl(gpu, "1")}, "must be set in limits"},
		{"extended request differs from limit", corev1.ResourceRequirements{Requests: rl(gpu, "1"), Limits: rl(gpu, "2")}, "request and limit must be equal"},
		{"fractional GPU", corev1.ResourceRequirements{Limits: rl(gpu, "500m")}, "whole number"},
		{"cpu request above limit", corev1.ResourceRequirements{Requests: rl("cpu", "2"), Limits: rl("cpu", "1")}, "exceeds its limit"},
		{"negative quantity", corev1.ResourceRequirements{Limits: rl("memory", "-1Gi")}, "must not be negative"},
		{"malformed resource name", corev1.ResourceRequirements{Limits: rl("not a/valid name!", "1")}, "not a valid resource name"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			errs := validateResources("spec.resources", tc.rr)
			switch {
			case tc.wantErr == "" && len(errs) > 0:
				t.Fatalf("unexpected errors: %v", errs)
			case tc.wantErr != "" && (len(errs) == 0 || !strings.Contains(errors.Join(errs...).Error(), tc.wantErr)):
				t.Fatalf("errors %v, want containing %q", errs, tc.wantErr)
			}
		})
	}
}

func TestResourcesAreValidatedOnBothKinds(t *testing.T) {
	bad := corev1.ResourceRequirements{Requests: rl("nvidia.com/gpu", "1")}
	_, err := (&ModelDeploymentValidator{}).ValidateCreate(context.Background(), md(amphorav1alpha1.ModelDeploymentSpec{
		Image: testImage, TenancyClass: amphorav1alpha1.TenancySingleTenant, Resources: &bad,
	}))
	if err == nil || !strings.Contains(err.Error(), "spec.resources") {
		t.Errorf("ModelDeployment with invalid resources: %v", err)
	}
	pool := &amphorav1alpha1.PausePool{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"},
		Spec: amphorav1alpha1.PausePoolSpec{NodeName: "n", TenancyClass: amphorav1alpha1.TenancySingleTenant, GPUSlice: "full", Resources: &bad}}
	if _, err := (&PausePoolValidator{}).ValidateCreate(context.Background(), pool); err == nil || !strings.Contains(err.Error(), "spec.resources") {
		t.Errorf("PausePool with invalid resources: %v", err)
	}
}

func TestMissingGPUResourceWarnsWhereItMatters(t *testing.T) {
	ctx := context.Background()
	withGPU := corev1.ResourceRequirements{Limits: rl("nvidia.com/gpu", "1")}
	md2 := func(spec amphorav1alpha1.ModelDeploymentSpec) string {
		w, err := (&ModelDeploymentValidator{}).ValidateCreate(ctx, md(spec))
		if err != nil {
			t.Fatalf("unexpected rejection: %v", err)
		}
		return strings.Join(w, "|")
	}
	base := amphorav1alpha1.ModelDeploymentSpec{Image: testImage, TenancyClass: amphorav1alpha1.TenancySingleTenant}

	if got := md2(base); strings.Contains(got, "no GPU or MIG resource") {
		t.Errorf("a plain spec (a stub or CPU model) must not warn: %q", got)
	}
	s := base
	s.GPUFraction = "1g.10gb"
	if got := md2(s); !strings.Contains(got, "placement policy only") {
		t.Errorf("gpuFraction without a GPU resource must warn that nothing is allocated: %q", got)
	}
	s.Resources = &withGPU
	if got := md2(s); strings.Contains(got, "no GPU or MIG resource") {
		t.Errorf("a spec that requests the GPU must not warn: %q", got)
	}

	pool := func(rr corev1.ResourceRequirements) string {
		p := &amphorav1alpha1.PausePool{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"},
			Spec: amphorav1alpha1.PausePoolSpec{NodeName: "n", TenancyClass: amphorav1alpha1.TenancySingleTenant, GPUSlice: "full", Resources: &rr}}
		w, err := (&PausePoolValidator{}).ValidateCreate(ctx, p)
		if err != nil {
			t.Fatalf("unexpected rejection: %v", err)
		}
		return strings.Join(w, "|")
	}
	if got := pool(corev1.ResourceRequirements{}); !strings.Contains(got, "reserve no device") {
		t.Errorf("a pool that reserves nothing must warn: %q", got)
	}
	if got := pool(withGPU); got != "" {
		t.Errorf("a pool that reserves a GPU must not warn: %q", got)
	}
}

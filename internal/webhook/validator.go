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

// Package webhook implements the validating admission webhooks from
// Technical Specification v3 §3.2/§4/§4.3. They reject policy violations at
// create/update time, ahead of the Packing Scheduler and the reconcilers
// (which keep enforcing the same rules independently: §4 asks for defense in
// depth, not a single enforcement point). The tenancy-matrix rules are the
// scheduler's own (scheduler.ResolvePackingMode), reused rather than copied.
//
// Not enforceable yet: residency against actual nodes (§4.3). No node
// region label convention exists, so allowedRegions is checked for presence
// and shape only; the scheduler does not consult it.
package webhook

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
	"github.com/ramin-fazli/amphora/internal/controller"
	"github.com/ramin-fazli/amphora/internal/scheduler"
)

// failurePolicy=fail: if the webhook is unreachable, creates/updates of these
// kinds are refused rather than admitted unchecked. The cost is that
// ModelDeployments and PausePools cannot be created or changed while the
// manager is down; deletes are not intercepted.

//+kubebuilder:webhook:path=/validate-amphora-amphora-sh-v1alpha1-modeldeployment,mutating=false,failurePolicy=fail,sideEffects=None,groups=amphora.amphora.sh,resources=modeldeployments,verbs=create;update,versions=v1alpha1,name=vmodeldeployment.kb.io,admissionReviewVersions=v1
//+kubebuilder:webhook:path=/validate-amphora-amphora-sh-v1alpha1-pausepool,mutating=false,failurePolicy=fail,sideEffects=None,groups=amphora.amphora.sh,resources=pausepools,verbs=create;update,versions=v1alpha1,name=vpausepool.kb.io,admissionReviewVersions=v1

// SetupWithManager registers both validators with mgr's webhook server.
func SetupWithManager(mgr ctrl.Manager) error {
	if err := ctrl.NewWebhookManagedBy(mgr).For(&amphorav1alpha1.ModelDeployment{}).
		WithValidator(&ModelDeploymentValidator{}).Complete(); err != nil {
		return fmt.Errorf("registering ModelDeployment webhook: %w", err)
	}
	if err := ctrl.NewWebhookManagedBy(mgr).For(&amphorav1alpha1.PausePool{}).
		WithValidator(&PausePoolValidator{}).Complete(); err != nil {
		return fmt.Errorf("registering PausePool webhook: %w", err)
	}
	return nil
}

// ModelDeploymentValidator validates ModelDeployment create/update.
type ModelDeploymentValidator struct{}

var _ admission.CustomValidator = &ModelDeploymentValidator{}

// ValidateCreate implements admission.CustomValidator.
func (v *ModelDeploymentValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	return validateModelDeployment(obj)
}

// ValidateUpdate implements admission.CustomValidator. The same rules apply
// to the new object; tenancyClass immutability is enforced by the CRD's CEL
// rule and is deliberately not duplicated here.
func (v *ModelDeploymentValidator) ValidateUpdate(_ context.Context, _, newObj runtime.Object) (admission.Warnings, error) {
	return validateModelDeployment(newObj)
}

// ValidateDelete implements admission.CustomValidator. Deletes are not
// intercepted (see the webhook markers); this is never called.
func (v *ModelDeploymentValidator) ValidateDelete(context.Context, runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

func validateModelDeployment(obj runtime.Object) (admission.Warnings, error) {
	md, ok := obj.(*amphorav1alpha1.ModelDeployment)
	if !ok {
		return nil, fmt.Errorf("expected a ModelDeployment, got %T", obj)
	}
	var errs []error
	var warnings admission.Warnings
	spec := md.Spec

	if strings.TrimSpace(spec.Image) == "" {
		errs = append(errs, errors.New("spec.image must not be empty"))
	}

	if spec.GPUFraction != "" {
		if _, err := controller.ParseGPUFractionVRAMMB(spec.GPUFraction); err != nil {
			errs = append(errs, fmt.Errorf("spec.gpuFraction: %w", err))
		}
	}

	switch spec.TenancyClass {
	case amphorav1alpha1.TenancySingleTenant, amphorav1alpha1.TenancyTrustedMultiTenant:
	case amphorav1alpha1.TenancyRegulatedMultiTenant:
		// §4: MIG-only, hardware-enforced isolation; the CRD documents
		// gpuFraction as required here.
		if spec.GPUFraction == "" {
			errs = append(errs, fmt.Errorf("spec.gpuFraction is required for %s: it is MIG-only (§4)", spec.TenancyClass))
		} else if _, err := scheduler.ResolvePackingMode(spec.TenancyClass, scheduler.PackingModeMIG); err != nil {
			errs = append(errs, fmt.Errorf("spec.gpuFraction: %w", err))
		}
		// §4.3: regulated workloads must declare where they may run.
		if len(spec.AllowedRegions) == 0 {
			errs = append(errs, fmt.Errorf("spec.allowedRegions is required for %s (§4.3 residency)", spec.TenancyClass))
		}
	default:
		errs = append(errs, fmt.Errorf("spec.tenancyClass %q is not a known tenancy class", spec.TenancyClass))
	}

	seen := map[string]struct{}{}
	for i, region := range spec.AllowedRegions {
		if strings.TrimSpace(region) == "" {
			errs = append(errs, fmt.Errorf("spec.allowedRegions[%d] must not be empty", i))
			continue
		}
		if _, dup := seen[region]; dup {
			errs = append(errs, fmt.Errorf("spec.allowedRegions[%d]: duplicate region %q", i, region))
		}
		seen[region] = struct{}{}
	}
	if len(spec.AllowedRegions) > 0 {
		warnings = append(warnings, "spec.allowedRegions is recorded but not yet enforced against node placement: "+
			"no node region labels exist (§4.3)")
	}

	gate := spec.EvalGate
	if gate.CanaryConfigMapRef != "" {
		if problems := validation.IsDNS1123Subdomain(gate.CanaryConfigMapRef); len(problems) > 0 {
			errs = append(errs, fmt.Errorf("spec.evalGate.canaryConfigMapRef %q is not a valid ConfigMap name: %s",
				gate.CanaryConfigMapRef, strings.Join(problems, "; ")))
		}
		if !gate.Enabled {
			warnings = append(warnings, "spec.evalGate.canaryConfigMapRef is ignored unless spec.evalGate.enabled is true")
		}
	} else if gate.Enabled {
		warnings = append(warnings, "spec.evalGate.enabled has no canaryConfigMapRef: only the latency probe will run "+
			"and output quality stays unverified")
	}

	return warnings, errors.Join(errs...)
}

// PausePoolValidator validates PausePool create/update.
type PausePoolValidator struct{}

var _ admission.CustomValidator = &PausePoolValidator{}

// ValidateCreate implements admission.CustomValidator.
func (v *PausePoolValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	return validatePausePool(obj)
}

// ValidateUpdate implements admission.CustomValidator.
func (v *PausePoolValidator) ValidateUpdate(_ context.Context, _, newObj runtime.Object) (admission.Warnings, error) {
	return validatePausePool(newObj)
}

// ValidateDelete implements admission.CustomValidator; never called.
func (v *PausePoolValidator) ValidateDelete(context.Context, runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

func validatePausePool(obj runtime.Object) (admission.Warnings, error) {
	pool, ok := obj.(*amphorav1alpha1.PausePool)
	if !ok {
		return nil, fmt.Errorf("expected a PausePool, got %T", obj)
	}
	var errs []error
	spec := pool.Spec

	if problems := validation.IsDNS1123Subdomain(spec.NodeName); len(problems) > 0 {
		errs = append(errs, fmt.Errorf("spec.nodeName %q is not a valid node name: %s", spec.NodeName, strings.Join(problems, "; ")))
	}

	// Same check the PausePool reconciler applies, but at admission: the
	// declared slice must be a known form and permitted for the tenancy class
	// by the §4 matrix. Node-dependent checks (existence, MIG capability)
	// stay in the reconciler since the node may not exist yet.
	mode, err := controller.GPUSliceToPackingMode(spec.GPUSlice)
	if err != nil {
		errs = append(errs, fmt.Errorf("spec.gpuSlice: %w", err))
	} else if _, err := scheduler.ResolvePackingMode(spec.TenancyClass, mode); err != nil {
		errs = append(errs, fmt.Errorf("spec.gpuSlice %q for tenancyClass %s: %w", spec.GPUSlice, spec.TenancyClass, err))
	}

	return nil, errors.Join(errs...)
}

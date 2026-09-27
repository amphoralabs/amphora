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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PausePoolSpec defines the desired state of a pre-provisioned pause-pod pool
// for a single (node, tenancyClass, gpuSlice) tuple. See Technical
// Specification §3.2/§10 (pause-pod hijack) and
// https://github.com/ramin-fazli/amphora/issues/12 for the design this
// implements. A PausePool only maintains idle placeholder pods; hijacking one
// onto a scheduled ModelDeployment is a separate, not-yet-implemented step.
type PausePoolSpec struct {
	// NodeName pins this pool to a single Kubernetes node. Pause pods are
	// pre-bound via the pod's spec.nodeName (skipping the kube-scheduler),
	// so a future hijack only has to patch an already-running pod in place —
	// this is what makes §10's T=35ms→70ms hijack timing achievable.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	NodeName string `json:"nodeName"`

	// TenancyClass gates which packing modes/co-location rules apply to pods
	// drawn from this pool (§4). Immutable post-creation, for the same
	// reason ModelDeployment.Spec.TenancyClass is: changing isolation
	// guarantees on a pool that may already have hijacked (live) pods
	// requires deleting and recreating it.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="tenancyClass is immutable; delete and recreate to change isolation guarantees"
	TenancyClass TenancyClass `json:"tenancyClass"`

	// GPUSlice declares the packing mode this pool's pods reserve: "full"
	// for a dedicated whole GPU, "timeslice" for a logical time-sliced share
	// (no guaranteed VRAM/fault isolation), or a MIG profile (e.g.
	// "1g.10gb", same naming convention as ModelDeployment.Spec.GPUFraction)
	// for a hardware-isolated slice. Validated against the §4 isolation
	// matrix for the declared tenancyClass — e.g. RegulatedMultiTenant
	// requires a MIG profile; "full"/"timeslice" are rejected.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	GPUSlice string `json:"gpuSlice"`

	// TargetSize is the number of idle pause pods to keep warm for this
	// tuple. A fixed, operator-set number for now — predictive/autoscaled
	// sizing is explicitly out of scope (Phase 4 "predictive pre-warming").
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=1
	TargetSize int32 `json:"targetSize,omitempty"`

	// PauseImage is the placeholder container image idle pause pods run
	// before being hijacked. The real serving image, nvidia container
	// runtime, and env primitives are injected at hijack time (§10 T=70ms),
	// not at pool-fill time — this image is never expected to serve traffic.
	// +kubebuilder:default="registry.k8s.io/pause:3.9"
	PauseImage string `json:"pauseImage,omitempty"`
}

// PausePoolStatus defines the observed state of a PausePool.
type PausePoolStatus struct {
	// ObservedGeneration is the most recent generation reconciled by the controller.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// CurrentSize is the number of pause pods the controller currently
	// observes for this pool (may transiently differ from TargetSize while
	// pods are being created/deleted, or while capacity is unavailable).
	CurrentSize int32 `json:"currentSize,omitempty"`

	// PausePodNames lists the pods currently backing this pool.
	PausePodNames []string `json:"pausePodNames,omitempty"`

	// Conditions surfaces pool health, e.g. a "Ready" condition that is
	// False with reason InvalidSpec when the declared tenancyClass/gpuSlice
	// combination violates the §4 isolation matrix, or reason
	// CapacityUnavailable when pod creation is failing.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.spec.nodeName`
//+kubebuilder:printcolumn:name="Tenancy",type=string,JSONPath=`.spec.tenancyClass`
//+kubebuilder:printcolumn:name="Slice",type=string,JSONPath=`.spec.gpuSlice`
//+kubebuilder:printcolumn:name="Target",type=integer,JSONPath=`.spec.targetSize`
//+kubebuilder:printcolumn:name="Current",type=integer,JSONPath=`.status.currentSize`

// PausePool is the Schema for the pausepools API
type PausePool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PausePoolSpec   `json:"spec,omitempty"`
	Status PausePoolStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// PausePoolList contains a list of PausePool
type PausePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PausePool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PausePool{}, &PausePoolList{})
}

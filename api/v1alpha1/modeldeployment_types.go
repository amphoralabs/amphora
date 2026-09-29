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

// TenancyClass governs which GPU packing modes the Packing Scheduler and admission
// webhook are allowed to select for this deployment. See Technical Specification §4.
// +kubebuilder:validation:Enum=SingleTenant;TrustedMultiTenant;RegulatedMultiTenant
type TenancyClass string

const (
	// TenancySingleTenant dedicates a full physical GPU; any packing mode is allowed.
	TenancySingleTenant TenancyClass = "SingleTenant"
	// TenancyTrustedMultiTenant allows MIG or time-slicing; co-tenants must share a trust boundary.
	TenancyTrustedMultiTenant TenancyClass = "TrustedMultiTenant"
	// TenancyRegulatedMultiTenant requires MIG-only placement (hard VRAM/fault isolation).
	TenancyRegulatedMultiTenant TenancyClass = "RegulatedMultiTenant"
)

// EvalGateSpec configures the pre-traffic-flip canary probe. See Technical Specification §3.2.1.
type EvalGateSpec struct {
	// Enabled turns on the eval gate. If false, only the default latency-only probe runs
	// and the audit trail records the deployment as "quality unverified".
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// TimeoutMillis is the gate deadline; on timeout the controller fails closed
	// (traffic is not flipped, the swap is rolled back).
	// +kubebuilder:default=150
	// +kubebuilder:validation:Minimum=1
	TimeoutMillis int32 `json:"timeoutMillis,omitempty"`

	// ProbePort is the port on the serving pod the gate's health probe
	// targets. Defaults to 8000 (vLLM's default server port).
	// +kubebuilder:default=8000
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	ProbePort int32 `json:"probePort,omitempty"`

	// CanaryConfigMapRef names a ConfigMap holding the canary prompt set and matcher config.
	CanaryConfigMapRef string `json:"canaryConfigMapRef,omitempty"`
}

// ModelDeploymentSpec defines the desired state of ModelDeployment
type ModelDeploymentSpec struct {
	// Image is the container image serving the model (e.g. a vLLM-based image).
	// +kubebuilder:validation:Required
	Image string `json:"image"`

	// TenancyClass is immutable post-creation: downgrading isolation guarantees on a
	// running workload requires deleting and recreating the ModelDeployment.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="tenancyClass is immutable; delete and recreate to change isolation guarantees"
	TenancyClass TenancyClass `json:"tenancyClass"`

	// GPUFraction is the MIG profile (e.g. "1g.10gb") or fractional share requested.
	// Required when TenancyClass is RegulatedMultiTenant (enforced by the admission webhook).
	GPUFraction string `json:"gpuFraction,omitempty"`

	// PriorityClass governs preemption order among co-located models on a shared GPU.
	PriorityClass string `json:"priorityClass,omitempty"`

	// MaxColocatedModels caps how many models the Packing Scheduler may place on the
	// same physical GPU alongside this one.
	// +kubebuilder:validation:Minimum=1
	MaxColocatedModels int32 `json:"maxColocatedModels,omitempty"`

	// AllowedRegions constrains scheduling/failover to these regions/clusters (data
	// residency enforcement, Technical Specification §4.3). Required for RegulatedMultiTenant.
	AllowedRegions []string `json:"allowedRegions,omitempty"`

	// EvalGate configures the pre-traffic-flip canary probe (§3.2.1).
	EvalGate EvalGateSpec `json:"evalGate,omitempty"`
}

// ModelDeploymentStatus defines the observed state of ModelDeployment
type ModelDeploymentStatus struct {
	// ObservedGeneration is the most recent generation reconciled by the controller.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Phase is a coarse human-readable state (e.g. Pending, Warm, Serving, RolledBack).
	Phase string `json:"phase,omitempty"`

	// EffectivePackingMode records what the Packing Scheduler actually assigned (MIG/TimeSlice/None),
	// so drift from the requested TenancyClass guarantee is observable.
	EffectivePackingMode string `json:"effectivePackingMode,omitempty"`

	// ActivePod names the pod currently serving (or warming up for) this
	// ModelDeployment, after being hijacked from a matching PausePool
	// (Technical Specification §10). Empty until a hijack has occurred.
	ActivePod string `json:"activePod,omitempty"`

	// Endpoint is the base URL (e.g. "http://10.0.0.5:8000") of the pod
	// serving this deployment. Set only while Phase is Serving; the Proxy
	// watches it to learn which models are warm.
	Endpoint string `json:"endpoint,omitempty"`

	// EvalFailures counts consecutive eval-gate failures for this
	// deployment. At the controller's threshold, auto-promotion pauses until
	// an operator approves (circuit breaker, §3.2.1).
	EvalFailures int32 `json:"evalFailures,omitempty"`

	// Conditions surfaces gate outcomes, e.g. QualityVerified=False with
	// reason LatencyOnly while only the default latency probe runs.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Tenancy",type=string,JSONPath=`.spec.tenancyClass`
//+kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
//+kubebuilder:printcolumn:name="Packing",type=string,JSONPath=`.status.effectivePackingMode`

// ModelDeployment is the Schema for the modeldeployments API
type ModelDeployment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ModelDeploymentSpec   `json:"spec,omitempty"`
	Status ModelDeploymentStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// ModelDeploymentList contains a list of ModelDeployment
type ModelDeploymentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ModelDeployment `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ModelDeployment{}, &ModelDeploymentList{})
}

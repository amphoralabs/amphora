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

package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
	"github.com/ramin-fazli/amphora/internal/scheduler"
)

// Labels set on every pod a PausePool creates. poolNameLabel is how the
// reconciler finds "its" pods again without relying on owner-reference
// listing (controller-runtime has no free owner index out of the box).
const (
	pausePodLabel         = "amphora.amphora.sh/pause-pod"
	pausePodTenancyLabel  = "amphora.amphora.sh/tenancy-class"
	pausePodGPUSliceLabel = "amphora.amphora.sh/gpu-slice"
	poolNameLabel         = "amphora.amphora.sh/pause-pool"
)

// Condition type/reasons recorded on PausePool.Status.Conditions.
const (
	conditionReady = "Ready"

	reasonInvalidSpec         = "InvalidSpec"
	reasonCapacityUnavailable = "CapacityUnavailable"
	reasonPoolFilled          = "PoolFilled"
)

// PausePoolReconciler maintains a fixed-size pool of idle, pre-bound pause
// pods per PausePool (Technical Specification §3.2/§10, issue #12). It does
// not perform pause-pod hijack (patching a pool pod onto a scheduled
// ModelDeployment) — that is a separate, not-yet-implemented step that
// depends on resolving issue #12's eval-gate-rollback open question first.
type PausePoolReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=amphora.amphora.sh,resources=pausepools,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=amphora.amphora.sh,resources=pausepools/status,verbs=get;update;patch
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

// Reconcile drives a PausePool's live pod count towards spec.TargetSize:
// it validates the declared tenancyClass/gpuSlice combination against the
// target node and the §4 isolation matrix, then creates or deletes idle
// pause pods (pre-bound to spec.nodeName, skipping the kube-scheduler) to
// close the gap, recording the outcome on status.
func (r *PausePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var pool amphorav1alpha1.PausePool
	if err := r.Get(ctx, req.NamespacedName, &pool); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// No custom finalizer: pause pods are created with an owner reference to
	// the PausePool, so the built-in Kubernetes garbage collector cleans
	// them up on delete. Unlike ModelDeploymentReconciler's schedulerFinalizer,
	// there's no external (non-Kubernetes) state to release here.
	if !pool.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	if err := r.validateSpec(ctx, &pool); err != nil {
		return r.recordOutcome(ctx, &pool, nil, metav1.ConditionFalse, reasonInvalidSpec, err.Error())
	}

	pods, err := r.listPoolPods(ctx, &pool)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("listing pause pods: %w", err)
	}
	// Pods with a non-nil DeletionTimestamp are terminating, not yet gone —
	// treat them as already released rather than double-counting them as
	// live capacity or re-deleting them on the next reconcile.
	active := activePods(pods)

	switch diff := int(pool.Spec.TargetSize) - len(active); {
	case diff > 0:
		if err := r.growPool(ctx, &pool, diff); err != nil {
			logger.Error(err, "creating pause pods", "pool", req.NamespacedName, "shortfall", diff)
			return r.recordOutcome(ctx, &pool, active, metav1.ConditionFalse, reasonCapacityUnavailable, err.Error())
		}
	case diff < 0:
		// Safe to consider every pod returned by listPoolPods a scale-down
		// candidate: hijackPausePod (internal/controller/hijack.go) removes
		// poolNameLabel and re-parents the owner reference the moment a pod
		// is hijacked, so a hijacked pod no longer matches this list at all
		// — it has fully graduated out of the pool's accounting. Deleting
		// the newest-created excess pods is arbitrary but deterministic.
		if err := r.shrinkPool(ctx, active, -diff); err != nil {
			return ctrl.Result{}, fmt.Errorf("deleting excess pause pods: %w", err)
		}
	}

	// Re-list after mutating so status reflects what's actually there rather
	// than the pre-reconcile snapshot.
	pods, err = r.listPoolPods(ctx, &pool)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("re-listing pause pods: %w", err)
	}
	active = activePods(pods)
	return r.recordOutcome(ctx, &pool, active, metav1.ConditionTrue, reasonPoolFilled, fmt.Sprintf("%d/%d pause pods ready", len(active), pool.Spec.TargetSize))
}

// activePods filters out pods with a non-nil DeletionTimestamp (terminating,
// but not yet removed by the garbage collector/kubelet).
func activePods(pods []corev1.Pod) []corev1.Pod {
	active := make([]corev1.Pod, 0, len(pods))
	for _, p := range pods {
		if p.DeletionTimestamp.IsZero() {
			active = append(active, p)
		}
	}
	return active
}

// validateSpec enforces defense-in-depth checks independent of the
// admission webhook (not yet implemented, §3.2): the target node must exist
// and, if gpuSlice implies MIG, must be MIG-capable (mirrors the node
// labels internal/controller/gpu_capacity.go already uses for the Packing
// Scheduler's node discovery); and the tenancyClass/gpuSlice combination
// must satisfy the §4 isolation matrix, reusing the Packing Scheduler's own
// rules (internal/scheduler.ValidatePackingMode) rather than duplicating them.
func (r *PausePoolReconciler) validateSpec(ctx context.Context, pool *amphorav1alpha1.PausePool) error {
	requestedMode, err := GPUSliceToPackingMode(pool.Spec.GPUSlice)
	if err != nil {
		return err
	}
	// resolvedMode is what the §4 matrix actually requires, which may be
	// stricter than requestedMode (e.g. RegulatedMultiTenant always
	// resolves to MIG even from an empty/"full" request) — capability
	// checks below must use this, not requestedMode.
	mode, err := scheduler.ResolvePackingMode(pool.Spec.TenancyClass, requestedMode)
	if err != nil {
		return err
	}

	var node corev1.Node
	if err := r.Get(ctx, client.ObjectKey{Name: pool.Spec.NodeName}, &node); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("node %q not found", pool.Spec.NodeName)
		}
		return err
	}
	if mode == scheduler.PackingModeMIG && node.Labels[NodeMIGCapableLabel] != nodeMIGCapableTrue {
		return fmt.Errorf("node %q is not MIG-capable (missing/false %s label); gpuSlice %q requires MIG", pool.Spec.NodeName, NodeMIGCapableLabel, pool.Spec.GPUSlice)
	}
	return nil
}

// GPUSliceToPackingMode derives the explicit packing mode a gpuSlice value
// requests: "full" means the whole physical GPU (PackingModeDedicated),
// "timeslice" means a logical time-sliced share (PackingModeTimeSlice, no
// guaranteed VRAM/fault isolation), and anything matching the MIG-profile
// pattern (reusing the same convention ModelDeployment.Spec.GPUFraction
// follows) means PackingModeMIG. Every value maps to an explicit mode
// (never "unspecified") precisely so scheduler.ResolvePackingMode can
// actually reject a violating combination (e.g. RegulatedMultiTenant +
// "timeslice") instead of silently falling back to a permissive default.
func GPUSliceToPackingMode(gpuSlice string) (scheduler.PackingMode, error) {
	switch {
	case gpuSlice == "full":
		return scheduler.PackingModeDedicated, nil
	case gpuSlice == "timeslice":
		return scheduler.PackingModeTimeSlice, nil
	case migProfileVRAMPattern.MatchString(gpuSlice):
		return scheduler.PackingModeMIG, nil
	default:
		return "", fmt.Errorf("gpuSlice %q must be \"full\", \"timeslice\", or a MIG profile matching \"<slices>g.<N>gb\"", gpuSlice)
	}
}

func (r *PausePoolReconciler) listPoolPods(ctx context.Context, pool *amphorav1alpha1.PausePool) ([]corev1.Pod, error) {
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.InNamespace(pool.Namespace), client.MatchingLabels{poolNameLabel: pool.Name}); err != nil {
		return nil, err
	}
	return podList.Items, nil
}

func (r *PausePoolReconciler) growPool(ctx context.Context, pool *amphorav1alpha1.PausePool, count int) error {
	var errs []error
	for i := 0; i < count; i++ {
		pod := newPausePod(pool)
		if err := controllerutil.SetControllerReference(pool, pod, r.Scheme); err != nil {
			return fmt.Errorf("setting owner reference: %w", err)
		}
		if err := r.Create(ctx, pod); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (r *PausePoolReconciler) shrinkPool(ctx context.Context, pods []corev1.Pod, count int) error {
	sort.Slice(pods, func(i, j int) bool {
		return pods[i].CreationTimestamp.After(pods[j].CreationTimestamp.Time)
	})
	var errs []error
	for i := 0; i < count && i < len(pods); i++ {
		if err := r.Delete(ctx, &pods[i]); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// newPausePod builds an idle placeholder pod pre-bound to pool.Spec.NodeName.
// It carries no model-specific identity yet, but the RuntimeClassName and
// the container's env shape ARE fixed here, not at hijack time: Kubernetes
// only allows a running pod's spec.containers[*].image (plus tolerations/
// grace-period fields) to be mutated post-creation — runtimeClassName and
// adding/removing env entries are rejected by the apiserver. AMPHORA_MODEL
// is therefore pre-wired as a downward-API reference to the modelAnnotation
// annotation (initially unset): hijackPausePod (hijack.go) sets that
// annotation — pod metadata is always mutable — then swaps the image, which
// forces a container restart; the kubelet re-resolves the downward-API env
// var against the pod's now-updated annotation at that restart.
func newPausePod(pool *amphorav1alpha1.PausePool) *corev1.Pod {
	nvidiaRuntimeClass := nvidiaRuntimeClassName
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: pool.Name + "-pause-",
			Namespace:    pool.Namespace,
			Labels: map[string]string{
				pausePodLabel:         labelValueTrue,
				pausePodTenancyLabel:  string(pool.Spec.TenancyClass),
				pausePodGPUSliceLabel: pool.Spec.GPUSlice,
				poolNameLabel:         pool.Name,
			},
		},
		Spec: corev1.PodSpec{
			NodeName:         pool.Spec.NodeName,
			RuntimeClassName: &nvidiaRuntimeClass,
			Containers: []corev1.Container{
				{
					Name:  "pause",
					Image: pool.Spec.PauseImage,
					// Idle pause pods are deliberately NotReady (the pause
					// image serves nothing); they become Ready only after a
					// hijack swaps in a server. Port is the default probe
					// port: a ModelDeployment with a different
					// evalGate.probePort cold-creates instead of hijacking.
					ReadinessProbe: readinessProbe(defaultProbePort),
					Env: []corev1.EnvVar{
						{Name: "AMPHORA_TENANCY_CLASS", Value: string(pool.Spec.TenancyClass)},
						{
							Name: "AMPHORA_MODEL",
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{
									FieldPath: fmt.Sprintf("metadata.annotations['%s']", modelAnnotation),
								},
							},
						},
					},
				},
			},
		},
	}
}

// recordOutcome persists status.{currentSize,pausePodNames,observedGeneration}
// and the Ready condition. err returned is only non-nil on a failure to
// persist status itself — an invalid spec or a capacity shortfall is an
// expected, observable outcome recorded via the condition, not a Reconcile
// error (same convention as ModelDeploymentReconciler.recordOutcome).
func (r *PausePoolReconciler) recordOutcome(ctx context.Context, pool *amphorav1alpha1.PausePool, pods []corev1.Pod, status metav1.ConditionStatus, reason, message string) (ctrl.Result, error) {
	names := make([]string, 0, len(pods))
	for _, p := range pods {
		names = append(names, p.Name)
	}
	sort.Strings(names)

	pool.Status.CurrentSize = int32(len(pods))
	pool.Status.PausePodNames = names
	pool.Status.ObservedGeneration = pool.Generation
	meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: pool.Generation,
	})
	if err := r.Status().Update(ctx, pool); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PausePoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&amphorav1alpha1.PausePool{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}

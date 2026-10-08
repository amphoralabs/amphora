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
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
	"github.com/ramin-fazli/amphora/internal/scheduler"
)

// schedulerFinalizer ensures a ModelDeployment's Packing Scheduler placement
// is released before the CR is deleted, freeing its VRAM/slice for reuse.
const schedulerFinalizer = "modeldeployment.amphora.amphora.sh/scheduler-release"

// pendingRequeueInterval bounds how soon a capacity-starved ModelDeployment
// is retried. Simple polling fallback in lieu of watching Node events to
// eagerly re-trigger pending placements (follow-up work).
const pendingRequeueInterval = 30 * time.Second

// Phases recorded in ModelDeploymentStatus.Phase. The field's doc comment
// lists phases as non-exhaustive examples; see Technical Specification §3.2.
const (
	// PhaseScheduled means the Packing Scheduler assigned this deployment a
	// node and packing mode; pod assignment (hijack or cold-create) has not
	// been persisted to status yet. Transient in practice.
	PhaseScheduled = "Scheduled"
	// PhaseWarming means a pod was assigned (hijacked from a matching
	// PausePool, or cold-created if none had an idle pod) and
	// status.activePod is set. This does not yet mean
	// traffic is flowing: the eval gate (§3.2.1, not yet implemented) and
	// the Proxy's warm-target registration (also not yet wired) both still
	// have to happen before a deployment is genuinely "Serving".
	PhaseWarming = "Warming"
	// PhaseServing means the pod passed the eval gate; traffic may flip.
	PhaseServing = "Serving"
	// PhaseRolledBack means the gate failed (or timed out): fail-closed, the
	// pod was removed and the deployment will be re-placed after a backoff.
	PhaseRolledBack = "RolledBack"
	// PhasePromotionPaused means consecutive gate failures hit
	// evalFailureThreshold; an operator must set approvePromotionAnnotation.
	PhasePromotionPaused = "PromotionPaused"
	// PhasePending means no node currently has capacity; the controller
	// retries on pendingRequeueInterval.
	PhasePending = "Pending"
	// PhaseRejected means the request violates the §4 tenancy matrix or is
	// otherwise invalid; retrying without a spec change won't help, so the
	// controller does not requeue.
	PhaseRejected = "Rejected"
)

// ModelDeploymentReconciler reconciles a ModelDeployment object
type ModelDeploymentReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Scheduler is the Packing Scheduler (§3.3) this reconciler places
	// ModelDeployments onto. Required.
	Scheduler *scheduler.Scheduler

	// EvalProber runs the eval gate's probe (§3.2.1). Required: the gate
	// fails closed, so a missing prober is an error, never a silent pass.
	EvalProber EvalProber

	// CanaryRunner evaluates canary prompts when spec.evalGate names a
	// canaryConfigMapRef. Required only for such deployments; absent, they
	// fail closed rather than being promoted unverified.
	CanaryRunner CanaryRunner

	// APIReader reads the canary ConfigMap uncached (get-only RBAC). Use
	// mgr.GetAPIReader().
	APIReader client.Reader
}

//+kubebuilder:rbac:groups=amphora.amphora.sh,resources=modeldeployments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=amphora.amphora.sh,resources=modeldeployments/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=amphora.amphora.sh,resources=modeldeployments/finalizers,verbs=update
//+kubebuilder:rbac:groups=amphora.amphora.sh,resources=pausepools,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives a ModelDeployment towards a Packing Scheduler placement:
// it discovers GPU node capacity (§3.3), asks the scheduler to place the
// model per its declared tenancy class (§4), then hijacks an idle pod from
// a matching PausePool (same node/tenancyClass/packing mode, §10) onto this
// deployment, recording the outcome on status. When no PausePool has an idle
// pod it falls back to cold-creating a pod (slower; counted by
// amphora_controller_cold_create_fallbacks_total). PausePool lookup is scoped to the ModelDeployment's own namespace (the
// cross-namespace/tenant pool-sharing boundary from issue #12 is
// unresolved, so this is deliberately conservative rather than guessed at).
func (r *ModelDeploymentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var md amphorav1alpha1.ModelDeployment
	if err := r.Get(ctx, req.NamespacedName, &md); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	model := req.String()

	if !md.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&md, schedulerFinalizer) {
			r.Scheduler.Release(model)
			controllerutil.RemoveFinalizer(&md, schedulerFinalizer)
			if err := r.Update(ctx, &md); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&md, schedulerFinalizer) {
		controllerutil.AddFinalizer(&md, schedulerFinalizer)
		if err := r.Update(ctx, &md); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if err := r.syncSchedulerNodes(ctx); err != nil {
		logger.Error(err, "syncing GPU node inventory")
		return ctrl.Result{}, err
	}

	vramMB, err := ParseGPUFractionVRAMMB(md.Spec.GPUFraction)
	if err != nil {
		return r.recordOutcome(ctx, &md, PhaseRejected, "", err)
	}

	var requestedMode scheduler.PackingMode
	if md.Spec.GPUFraction != "" {
		requestedMode = scheduler.PackingModeMIG
	}

	placement, err := r.Scheduler.Place(scheduler.PlacementRequest{
		Model:              model,
		TenancyClass:       md.Spec.TenancyClass,
		VRAMMB:             vramMB,
		RequestedMode:      requestedMode,
		MaxColocatedModels: md.Spec.MaxColocatedModels,
		AllowedRegions:     md.Spec.AllowedRegions,
	})
	if err != nil {
		if errors.Is(err, scheduler.ErrResidencyViolation) {
			return r.evictForResidency(ctx, &md, model, err)
		}
		if errors.Is(err, scheduler.ErrNoCapacity) {
			if res, statusErr := r.recordOutcome(ctx, &md, PhasePending, "", err); statusErr != nil {
				return res, statusErr
			}
			return ctrl.Result{RequeueAfter: pendingRequeueInterval}, nil
		}
		return r.recordOutcome(ctx, &md, PhaseRejected, "", err)
	}

	logger.Info("model deployment scheduled", "model", model, "node", placement.NodeID, "mode", placement.Mode)

	podName, hijacked, err := r.hijackPausePod(ctx, &md, placement)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("hijacking pause pod: %w", err)
	}
	if !hijacked {
		// No idle pod in any matching PausePool: degrade to a cold-created
		// pod rather than leaving the deployment stuck (slower, but serves).
		podName, err = r.coldCreatePod(ctx, &md, placement)
		if err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("pause pool empty, cold-created pod", "model", model, "pod", podName)
	}

	logger.Info("pod assigned", "model", model, "pod", podName)
	if md.Status.Phase == PhaseServing && md.Status.ActivePod == podName {
		return ctrl.Result{}, nil // already promoted; don't re-gate every reconcile
	}
	md.Status.ActivePod = podName
	return r.runEvalGate(ctx, &md, placement)
}

// recordOutcome persists phase/effectivePackingMode/observedGeneration onto
// md's status subresource. placeErr, if non-nil, is logged for
// observability but doesn't itself make Reconcile return an error — a
// capacity miss or tenancy rejection is an expected, observable outcome,
// not a transient failure; only a failure to persist status is.
func (r *ModelDeploymentReconciler) recordOutcome(ctx context.Context, md *amphorav1alpha1.ModelDeployment, phase, packingMode string, placeErr error) (ctrl.Result, error) {
	if placeErr != nil {
		log.FromContext(ctx).Info("placement not scheduled", "phase", phase, "reason", placeErr)
	}
	md.Status.Phase = phase
	if phase != PhaseServing {
		md.Status.Endpoint = "" // only a gated, Serving pod may receive traffic
	}
	md.Status.EffectivePackingMode = packingMode
	md.Status.ObservedGeneration = md.Generation
	if err := r.Status().Update(ctx, md); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ModelDeploymentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&amphorav1alpha1.ModelDeployment{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}

// evictForResidency enforces §4.3 when a placed deployment is found outside
// its allowedRegions (the spec changed, or its node was relabeled): the
// placement is released and the pod deleted so no regulated workload keeps
// running out of region, then the deployment is re-placed from scratch on the
// next reconcile (Pending if no allowed node exists). Traffic stops because
// status.endpoint is cleared by the non-Serving phase.
func (r *ModelDeploymentReconciler) evictForResidency(ctx context.Context, md *amphorav1alpha1.ModelDeployment, model string, cause error) (ctrl.Result, error) {
	log.FromContext(ctx).Info("evicting: placement violates allowedRegions", "model", model, "reason", cause.Error())
	var owned corev1.PodList
	if err := r.List(ctx, &owned, client.InNamespace(md.Namespace), client.MatchingLabels{hijackedByLabel: md.Name}); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing pods to evict: %w", err)
	}
	for i := range owned.Items {
		if err := r.Delete(ctx, &owned.Items[i], client.GracePeriodSeconds(0)); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("evicting pod %s: %w", owned.Items[i].Name, err)
		}
	}
	r.Scheduler.Release(model)
	md.Status.ActivePod = ""
	if res, err := r.recordOutcome(ctx, md, PhasePending, "", cause); err != nil {
		return res, err
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

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
	"sync"
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

	// restoreMu/restored make the one-time scheduler rebuild (ensureRestored)
	// safe across concurrent reconciles. Zero values are ready to use.
	restoreMu sync.Mutex
	restored  bool
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

	placementReq, err := placementRequestFor(&md)
	if err != nil {
		return r.recordOutcome(ctx, &md, PhaseRejected, "", err)
	}

	// Rebuild the scheduler's whole view before the first placement decision
	// (see ensureRestored), so the order deployments reconcile in after a
	// restart cannot let a new one take a node a running one occupies.
	if err := r.ensureRestored(ctx); err != nil {
		return ctrl.Result{}, err
	}

	// The scheduler is in memory: after a controller restart it has no record
	// of deployments whose pods are still running. Re-adopt them where they
	// actually are rather than re-placing, which could choose another node and
	// let a second workload share a node with a live SingleTenant pod (§4).
	if _, placed := r.Scheduler.Placement(model); !placed {
		observed, obsErr := r.observedNode(ctx, &md)
		if obsErr != nil {
			return ctrl.Result{}, obsErr
		}
		if observed != "" {
			if _, restoreErr := r.Scheduler.Restore(placementReq, observed); restoreErr != nil {
				return r.evictAndReplace(ctx, &md, model, restoreErr)
			}
		}
	}

	placement, err := r.Scheduler.Place(placementReq)
	if err != nil {
		if errors.Is(err, scheduler.ErrResidencyViolation) {
			return r.evictAndReplace(ctx, &md, model, err)
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

// observedNode returns the node of md's live pod (hijacked or cold-created),
// or "" if it has none. Pods are matched by the hijackedBy label the
// controller sets on every pod it assigns to a deployment.
func (r *ModelDeploymentReconciler) observedNode(ctx context.Context, md *amphorav1alpha1.ModelDeployment) (string, error) {
	var owned corev1.PodList
	if err := r.List(ctx, &owned, client.InNamespace(md.Namespace), client.MatchingLabels{hijackedByLabel: md.Name}); err != nil {
		return "", fmt.Errorf("listing pods to find the observed node: %w", err)
	}
	for _, p := range activePods(owned.Items) {
		if p.Spec.NodeName != "" {
			return p.Spec.NodeName, nil
		}
	}
	return "", nil
}

// evictAndReplace handles a deployment whose live placement can no longer be
// kept: it is outside its allowedRegions (§4.3: the spec changed or the node
// was relabeled), or it cannot be re-adopted at the node its pod occupies
// after a restart. The placement is released and the pod deleted so nothing
// keeps running somewhere the rules forbid, then the deployment is re-placed
// from scratch on the next reconcile (Pending if no suitable node exists).
// Traffic stops because status.endpoint is cleared by the non-Serving phase.
func (r *ModelDeploymentReconciler) evictAndReplace(ctx context.Context, md *amphorav1alpha1.ModelDeployment, model string, cause error) (ctrl.Result, error) {
	log.FromContext(ctx).Info("evicting: live placement violates current constraints", "model", model, "reason", cause.Error())
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

// placementRequestFor derives the Packing Scheduler request from md's spec. An
// unparseable gpuFraction is an error (the webhook normally rejects it first).
func placementRequestFor(md *amphorav1alpha1.ModelDeployment) (scheduler.PlacementRequest, error) {
	vramMB, err := ParseGPUFractionVRAMMB(md.Spec.GPUFraction)
	if err != nil {
		return scheduler.PlacementRequest{}, err
	}
	var requestedMode scheduler.PackingMode
	if md.Spec.GPUFraction != "" {
		requestedMode = scheduler.PackingModeMIG
	}
	return scheduler.PlacementRequest{
		Model:              md.Namespace + "/" + md.Name,
		TenancyClass:       md.Spec.TenancyClass,
		VRAMMB:             vramMB,
		RequestedMode:      requestedMode,
		MaxColocatedModels: md.Spec.MaxColocatedModels,
		AllowedRegions:     md.Spec.AllowedRegions,
	}, nil
}

// ensureRestored rebuilds the scheduler's placements from the cluster, once.
// The scheduler is in memory, so after a controller restart (or a new leader
// taking over) it is empty while pods keep running. Restoring a deployment
// only when it itself reconciles left a window in which a NEW deployment,
// reconciled first, could be placed on a node a running SingleTenant one
// occupies (§4). Doing the whole rebuild up front, before any placement
// decision, makes reconcile order irrelevant.
//
// A deployment that cannot be restored (its node no longer fits, or is not a
// GPU node) is skipped here: its own reconcile re-checks and evicts it. The
// rebuild is retried on the next reconcile if listing fails, so a failure
// never leaves placement running on a partial view.
func (r *ModelDeploymentReconciler) ensureRestored(ctx context.Context) error {
	r.restoreMu.Lock()
	defer r.restoreMu.Unlock()
	if r.restored {
		return nil
	}
	if err := r.syncSchedulerNodes(ctx); err != nil {
		return fmt.Errorf("syncing GPU nodes before restoring placements: %w", err)
	}
	var all amphorav1alpha1.ModelDeploymentList
	if err := r.List(ctx, &all); err != nil {
		return fmt.Errorf("listing ModelDeployments to restore placements: %w", err)
	}
	logger := log.FromContext(ctx)
	restoredCount := 0
	for i := range all.Items {
		md := &all.Items[i]
		node, err := r.observedNode(ctx, md)
		if err != nil {
			return err
		}
		if node == "" {
			continue
		}
		req, err := placementRequestFor(md)
		if err != nil {
			continue // invalid spec: its own reconcile rejects it
		}
		if _, err := r.Scheduler.Restore(req, node); err != nil {
			logger.Info("could not restore placement; its reconcile will re-check", "model", req.Model, "node", node, "reason", err.Error())
			continue
		}
		restoredCount++
	}
	r.restored = true
	logger.Info("scheduler placements rebuilt from cluster state", "restored", restoredCount, "deployments", len(all.Items))
	return nil
}

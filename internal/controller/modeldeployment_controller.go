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
	// node and packing mode, but no matching PausePool has an idle pod
	// available yet to hijack (pool cold/exhausted). The controller retries
	// on pendingRequeueInterval; there is no cold-create fallback pod path
	// yet (issue #12's deferred item), so a permanently empty pool leaves a
	// deployment stuck here rather than ever reaching PhaseWarming.
	PhaseScheduled = "Scheduled"
	// PhaseWarming means a matching PausePool's idle pod was hijacked
	// in place (image swapped, model annotation set, re-owned to this
	// ModelDeployment) and status.activePod is set. This does not yet mean
	// traffic is flowing: the eval gate (§3.2.1, not yet implemented) and
	// the Proxy's warm-target registration (also not yet wired) both still
	// have to happen before a deployment is genuinely "Serving".
	PhaseWarming = "Warming"
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
}

//+kubebuilder:rbac:groups=amphora.amphora.sh,resources=modeldeployments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=amphora.amphora.sh,resources=modeldeployments/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=amphora.amphora.sh,resources=modeldeployments/finalizers,verbs=update
//+kubebuilder:rbac:groups=amphora.amphora.sh,resources=pausepools,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch

// Reconcile drives a ModelDeployment towards a Packing Scheduler placement:
// it discovers GPU node capacity (§3.3), asks the scheduler to place the
// model per its declared tenancy class (§4), then hijacks an idle pod from
// a matching PausePool (same node/tenancyClass/packing mode, §10) onto this
// deployment, recording the outcome on status. There is no cold-create
// fallback yet when no PausePool has an idle pod (issue #12 follow-up), and
// PausePool lookup is scoped to the ModelDeployment's own namespace (the
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

	vramMB, err := parseGPUFractionVRAMMB(md.Spec.GPUFraction)
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
	})
	if err != nil {
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
		// No idle pod available from a matching PausePool yet (pool
		// cold/exhausted). TODO(issue #12 follow-up): fall back to a
		// cold-created pod here instead of only retrying — not implemented
		// yet, so a permanently empty pool leaves the deployment Scheduled
		// indefinitely.
		if res, statusErr := r.recordOutcome(ctx, &md, PhaseScheduled, string(placement.Mode), nil); statusErr != nil {
			return res, statusErr
		}
		return ctrl.Result{RequeueAfter: pendingRequeueInterval}, nil
	}

	logger.Info("hijacked pause pod", "model", model, "pod", podName)
	md.Status.ActivePod = podName
	return r.recordOutcome(ctx, &md, PhaseWarming, string(placement.Mode), nil)
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
		Complete(r)
}

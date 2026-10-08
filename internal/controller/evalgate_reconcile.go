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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
	"github.com/ramin-fazli/amphora/internal/scheduler"
)

// runEvalGate gates promotion of md's active pod to Serving (§3.2.1).
// A pod that is not Ready yet is waited on (Warming), not failed — the
// deadline applies to probing a live pod, not to pod startup. A failed or
// timed-out probe fails closed: the pod is deleted (rollback), the failure
// is counted, and at evalFailureThreshold promotion pauses for approval.
func (r *ModelDeploymentReconciler) runEvalGate(ctx context.Context, md *amphorav1alpha1.ModelDeployment, placement scheduler.Placement) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	class := string(md.Spec.TenancyClass)

	if md.Status.EvalFailures >= evalFailureThreshold {
		if md.Annotations[approvePromotionAnnotation] != labelValueTrue {
			evalGateResults.WithLabelValues("paused", class).Inc()
			return r.recordOutcome(ctx, md, PhasePromotionPaused, string(placement.Mode), errors.New("eval gate failure threshold reached; awaiting approval"))
		}
		delete(md.Annotations, approvePromotionAnnotation)
		if err := r.Update(ctx, md); err != nil {
			return ctrl.Result{}, err
		}
		md.Status.EvalFailures = 0
	}

	var pod corev1.Pod
	if err := r.Get(ctx, client.ObjectKey{Namespace: md.Namespace, Name: md.Status.ActivePod}, &pod); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	if !podReady(&pod) {
		res, err := r.recordOutcome(ctx, md, PhaseWarming, string(placement.Mode), nil)
		if err != nil {
			return res, err
		}
		return ctrl.Result{RequeueAfter: evalRetryInterval * time.Second}, nil
	}
	if r.EvalProber == nil {
		return ctrl.Result{}, errors.New("eval gate requires an EvalProber; refusing to promote without one")
	}

	port := probePortFor(md)
	timeout := time.Duration(md.Spec.EvalGate.TimeoutMillis) * time.Millisecond
	if timeout <= 0 {
		timeout = 150 * time.Millisecond
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	url := fmt.Sprintf("http://%s:%d%s", pod.Status.PodIP, port, probePath)

	if err := r.EvalProber.Probe(probeCtx, url); err != nil {
		logger.Info("eval gate failed, rolling back", "model", md.Name, "pod", pod.Name, "reason", err.Error())
		evalGateResults.WithLabelValues("fail", class).Inc()
		// Force-delete: a failed pod holds no state worth draining, and a
		// lingering Terminating pod would block re-creating <md>-serve.
		if delErr := r.Delete(ctx, &pod, client.GracePeriodSeconds(0)); delErr != nil && !apierrors.IsNotFound(delErr) {
			return ctrl.Result{}, fmt.Errorf("rolling back pod: %w", delErr)
		}
		md.Status.ActivePod = ""
		md.Status.EvalFailures++
		res, statusErr := r.recordOutcome(ctx, md, PhaseRolledBack, string(placement.Mode), err)
		if statusErr != nil {
			return res, statusErr
		}
		return ctrl.Result{RequeueAfter: evalRetryInterval * time.Second}, nil
	}

	evalGateResults.WithLabelValues("pass", class).Inc()
	md.Status.EvalFailures = 0
	// Canary prompt/matcher evaluation is not implemented, so quality is
	// never verified here even when spec.evalGate is configured: record that
	// rather than imply a canary ran (§3.2.1).
	meta.SetStatusCondition(&md.Status.Conditions, metav1.Condition{
		Type: conditionQualityVerified, Status: metav1.ConditionFalse, Reason: reasonLatencyOnly,
		Message: "only the default latency probe ran; output quality unverified", ObservedGeneration: md.Generation,
	})
	md.Status.Endpoint = fmt.Sprintf("http://%s:%d", pod.Status.PodIP, port)
	return r.recordOutcome(ctx, md, PhaseServing, string(placement.Mode), nil)
}

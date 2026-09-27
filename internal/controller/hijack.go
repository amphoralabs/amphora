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
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
	"github.com/ramin-fazli/amphora/internal/scheduler"
)

// hijackedByLabel records which ModelDeployment a formerly-idle pause pod
// now belongs to, for observability/audit (§5 Repudiation groundwork,
// mirrors the Proxy's X-Amphora-Model/Tenant header-tagging convention).
const hijackedByLabel = "amphora.amphora.sh/hijacked-by"

// nvidiaRuntimeClassName is the RuntimeClass every pause pod is created
// with (internal/controller/pausepool_controller.go's newPausePod) so its
// container gets GPU access from the start (§10 "inject nvidia runtime").
// This is a placeholder convention — like NodeVRAMLabel/NodeMIGCapableLabel,
// it assumes a RuntimeClass named "nvidia" already exists in-cluster (e.g.
// via the NVIDIA GPU Operator) rather than provisioning one. It cannot be
// set at hijack time: Kubernetes rejects any post-creation change to
// spec.runtimeClassName on a running pod.
const nvidiaRuntimeClassName = "nvidia"

// modelAnnotation is the pod annotation hijackPausePod sets to the claiming
// ModelDeployment's name. The pause pod's AMPHORA_MODEL env var is wired at
// creation time as a downward-API reference to this annotation (see
// newPausePod) — pod metadata is mutable post-creation even though env
// entries themselves are not, so setting this annotation before the image
// swap is what lets the hijacked container start up already knowing which
// model it's serving.
const modelAnnotation = "amphora.amphora.sh/model"

// hijackPausePod finds an idle pod from a PausePool matching placement
// (same namespace as md, same node, tenancyClass, and resolved packing
// mode) and patches it in place — container image plus the annotation its
// downward-API AMPHORA_MODEL env var reads from (§10 T=70ms; RuntimeClass
// and the env var's shape are fixed earlier, at pool-fill time, since
// Kubernetes forbids changing those on a running pod — see newPausePod) —
// then re-parents its owner reference from the PausePool to md so the pool
// reconciler stops managing it and native GC ties its lifecycle to md
// instead.
//
// It is idempotent: if md.Status.ActivePod already names a pod that still
// exists, that pod is returned unchanged rather than hijacking a second one
// (Reconcile calls this on every reconcile once Scheduled, not just once).
//
// Returns hijacked=false, err=nil when no matching PausePool currently has
// an idle pod — this is an expected, retryable outcome (pool cold/
// exhausted), not an error; there is no cold-create fallback yet (§12
// follow-up).
func (r *ModelDeploymentReconciler) hijackPausePod(ctx context.Context, md *amphorav1alpha1.ModelDeployment, placement scheduler.Placement) (podName string, hijacked bool, err error) {
	if md.Status.ActivePod != "" {
		var existing corev1.Pod
		err := r.Get(ctx, client.ObjectKey{Namespace: md.Namespace, Name: md.Status.ActivePod}, &existing)
		if err == nil {
			return md.Status.ActivePod, true, nil
		}
		if !apierrors.IsNotFound(err) {
			return "", false, err
		}
		// Previously hijacked pod is gone (deleted/evicted externally):
		// fall through and attempt a fresh hijack below.
	}

	pool, pod, err := r.findIdlePausePod(ctx, md, placement)
	if err != nil {
		return "", false, err
	}
	if pool == nil {
		return "", false, nil
	}

	if err := r.patchHijackedPod(ctx, md, pool, pod); err != nil {
		return "", false, err
	}
	return pod.Name, true, nil
}

// findIdlePausePod locates the first (by pool name, then pod name, for
// determinism) idle pod from a PausePool in md.Namespace whose nodeName/
// tenancyClass/resolved packing mode match placement. Returns a nil pool
// when no matching pool currently has an idle pod.
func (r *ModelDeploymentReconciler) findIdlePausePod(ctx context.Context, md *amphorav1alpha1.ModelDeployment, placement scheduler.Placement) (*amphorav1alpha1.PausePool, *corev1.Pod, error) {
	var pools amphorav1alpha1.PausePoolList
	if err := r.List(ctx, &pools, client.InNamespace(md.Namespace)); err != nil {
		return nil, nil, fmt.Errorf("listing PausePools: %w", err)
	}

	candidates := make([]amphorav1alpha1.PausePool, 0, len(pools.Items))
	for _, pool := range pools.Items {
		if pool.Spec.NodeName != placement.NodeID || pool.Spec.TenancyClass != md.Spec.TenancyClass {
			continue
		}
		requestedMode, err := gpuSliceToPackingMode(pool.Spec.GPUSlice)
		if err != nil {
			continue // malformed pool spec; PausePoolReconciler already surfaces this on the pool's own status
		}
		resolvedMode, err := scheduler.ResolvePackingMode(pool.Spec.TenancyClass, requestedMode)
		if err != nil || resolvedMode != placement.Mode {
			continue
		}
		candidates = append(candidates, pool)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Name < candidates[j].Name })

	for i := range candidates {
		pool := &candidates[i]
		var podList corev1.PodList
		if err := r.List(ctx, &podList, client.InNamespace(md.Namespace), client.MatchingLabels{poolNameLabel: pool.Name}); err != nil {
			return nil, nil, fmt.Errorf("listing pods for PausePool %s: %w", pool.Name, err)
		}
		idle := activePods(podList.Items)
		if len(idle) == 0 {
			continue
		}
		sort.Slice(idle, func(i, j int) bool { return idle[i].Name < idle[j].Name })
		pod := idle[0]
		return pool, &pod, nil
	}
	return nil, nil, nil
}

// patchHijackedPod mutates pod in place and re-parents its controller
// owner reference from pool to md, then persists both in a single Update.
// Only metadata (labels/annotations, always mutable) and
// spec.containers[*].image are touched — Kubernetes rejects any other
// spec.containers[*] or spec.runtimeClassName change on a running pod, so
// those are fixed at pool-fill time instead (see newPausePod).
func (r *ModelDeploymentReconciler) patchHijackedPod(ctx context.Context, md *amphorav1alpha1.ModelDeployment, pool *amphorav1alpha1.PausePool, pod *corev1.Pod) error {
	if len(pod.Spec.Containers) == 0 {
		return fmt.Errorf("pause pod %s/%s has no containers to hijack", pod.Namespace, pod.Name)
	}
	// Set before the image swap below: the kubelet resolves
	// newPausePod's AMPHORA_MODEL downward-API env var against the pod's
	// annotations at container (re)start, so this must already be in
	// place by the time the image change triggers that restart.
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[modelAnnotation] = md.Name

	pod.Spec.Containers[0].Image = md.Spec.Image

	delete(pod.Labels, pausePodLabel)
	delete(pod.Labels, poolNameLabel)
	delete(pod.Labels, pausePodTenancyLabel)
	delete(pod.Labels, pausePodGPUSliceLabel)
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	pod.Labels[hijackedByLabel] = md.Name

	if err := controllerutil.RemoveControllerReference(pool, pod, r.Scheme); err != nil {
		return fmt.Errorf("removing PausePool owner reference: %w", err)
	}
	if err := controllerutil.SetControllerReference(md, pod, r.Scheme); err != nil {
		return fmt.Errorf("setting ModelDeployment owner reference: %w", err)
	}

	if err := r.Update(ctx, pod); err != nil {
		return fmt.Errorf("patching hijacked pod: %w", err)
	}
	return nil
}

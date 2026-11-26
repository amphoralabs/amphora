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
	"maps"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
)

// nodeLabelsChanged lets through only the Node events that can change a
// placement decision: a node appearing, disappearing, or having its labels
// changed (region, GPU capacity, MIG capability). Kubelet heartbeats update a
// Node's status every few seconds and are filtered out.
var nodeLabelsChanged = predicate.Funcs{
	CreateFunc: func(event.CreateEvent) bool { return true },
	DeleteFunc: func(event.DeleteEvent) bool { return true },
	UpdateFunc: func(e event.UpdateEvent) bool {
		if e.ObjectOld == nil || e.ObjectNew == nil {
			return false
		}
		return !maps.Equal(e.ObjectOld.GetLabels(), e.ObjectNew.GetLabels())
	},
	GenericFunc: func(event.GenericEvent) bool { return false },
}

// deploymentsForNode maps a Node event to the ModelDeployments it can affect:
//   - every deployment with a live pod on that node, so a node relabeled out of
//     a deployment's allowedRegions is evicted promptly (§4.3) instead of on
//     some unrelated later event;
//   - every Pending deployment, so new or relabeled capacity is used without
//     waiting for the pendingRequeueInterval poll.
func (r *ModelDeploymentReconciler) deploymentsForNode(ctx context.Context, obj client.Object) []reconcile.Request {
	logger := log.FromContext(ctx)
	seen := map[types.NamespacedName]struct{}{}

	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.HasLabels{hijackedByLabel}); err != nil {
		logger.Error(err, "listing assigned pods for a node event", "node", obj.GetName())
	} else {
		for _, p := range activePods(pods.Items) {
			if p.Spec.NodeName == obj.GetName() {
				seen[types.NamespacedName{Namespace: p.Namespace, Name: p.Labels[hijackedByLabel]}] = struct{}{}
			}
		}
	}

	var mds amphorav1alpha1.ModelDeploymentList
	if err := r.List(ctx, &mds); err != nil {
		logger.Error(err, "listing ModelDeployments for a node event", "node", obj.GetName())
	} else {
		for _, md := range mds.Items {
			if md.Status.Phase == PhasePending {
				seen[types.NamespacedName{Namespace: md.Namespace, Name: md.Name}] = struct{}{}
			}
		}
	}

	reqs := make([]reconcile.Request, 0, len(seen))
	for key := range seen {
		reqs = append(reqs, reconcile.Request{NamespacedName: key})
	}
	return reqs
}

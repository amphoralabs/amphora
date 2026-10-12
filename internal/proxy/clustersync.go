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

package proxy

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
	"github.com/ramin-fazli/amphora/internal/controller"
)

// ModelKey is the registry key (and X-Amphora-Model value) for a
// ModelDeployment: "<namespace>/<name>". Namespace-qualified so two tenants
// deploying the same model name can never be cross-routed.
func ModelKey(md *amphorav1alpha1.ModelDeployment) string {
	return md.Namespace + "/" + md.Name
}

// ApplyModelDeployment reconciles the registry with one ModelDeployment: a
// deployment is warm only while the controller reports it Serving (i.e. its
// pod passed the eval gate) with a valid http endpoint; anything else,
// including deletion, marks it cold so no traffic reaches an ungated pod.
func ApplyModelDeployment(reg *Registry, md *amphorav1alpha1.ModelDeployment, logger *slog.Logger) {
	key := ModelKey(md)
	if !md.DeletionTimestamp.IsZero() {
		reg.MarkUnknown(key)
		return
	}
	reg.MarkKnown(key)
	if md.Status.Phase != controller.PhaseServing {
		reg.MarkCold(key)
		return
	}
	u, err := url.Parse(md.Status.Endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		logger.Error("ignoring Serving deployment with invalid endpoint", "model", key, "endpoint", md.Status.Endpoint)
		reg.MarkCold(key)
		return
	}
	target := Target{Model: key, Tenant: md.Namespace, Upstream: md.Status.Endpoint}
	if u.Scheme == "https" {
		// The pod fronts its model with the TLS sidecar; verify the tenant's
		// serving-certificate name, not the pod IP.
		target.ServerName = controller.PodTLSServerName(md.Namespace)
	}
	reg.MarkWarm(target)
}

// SyncFromCluster keeps reg in step with ModelDeployment status by watching
// the Kubernetes API, and blocks until ctx is done. It needs only
// get/list/watch on modeldeployments (see config/rbac/proxy_role.yaml).
// This replaces the spec's gRPC capacity-map push with the CRD status as the
// single source of truth: no new dependency, and access control is plain RBAC.
func SyncFromCluster(ctx context.Context, cfg *rest.Config, scheme *runtime.Scheme, reg *Registry, logger *slog.Logger) error {
	c, err := cache.New(cfg, cache.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("creating cache: %w", err)
	}
	informer, err := c.GetInformer(ctx, &amphorav1alpha1.ModelDeployment{})
	if err != nil {
		return fmt.Errorf("getting ModelDeployment informer: %w", err)
	}
	apply := func(obj interface{}) {
		if md, ok := obj.(*amphorav1alpha1.ModelDeployment); ok {
			ApplyModelDeployment(reg, md, logger)
		}
	}
	// Existence tracking must be on before any event is delivered, so a model
	// is never judged unknown merely because its Add has not been seen yet.
	reg.EnableKnownTracking()
	registration, err := informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    apply,
		UpdateFunc: func(_, obj interface{}) { apply(obj) },
		DeleteFunc: func(obj interface{}) {
			if tomb, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
				obj = tomb.Obj
			}
			if md, ok := obj.(*amphorav1alpha1.ModelDeployment); ok {
				reg.MarkUnknown(ModelKey(md))
			}
		},
	})
	if err != nil {
		return fmt.Errorf("adding event handler: %w", err)
	}
	go func() { _ = c.Start(ctx) }()
	if !c.WaitForCacheSync(ctx) {
		return fmt.Errorf("cache failed to sync")
	}
	// The cache having synced is not enough: our handler must also have
	// processed the initial list, or real models would briefly look unknown.
	if !toolscache.WaitForCacheSync(ctx.Done(), registration.HasSynced) {
		return fmt.Errorf("event handler failed to sync")
	}
	reg.SetSynced()
	logger.Info("cluster sync started")
	<-ctx.Done()
	return nil
}

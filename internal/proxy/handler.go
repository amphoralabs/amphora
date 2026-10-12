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
	"errors"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// deniedModelLabel replaces the client-controlled model name as the metric
// label for requests rejected before routing.
const deniedModelLabel = "-"

// ModelHeader is the request header naming the target model. TenantHeader
// optionally names the requesting tenant, propagated for cost/audit tagging
// (§5 Repudiation: every request must be tagged with model/tenant/GPU-time/
// region at the Proxy layer).
const (
	ModelHeader    = "X-Amphora-Model"
	TenantHeader   = "X-Amphora-Tenant"
	routedAtHeader = "X-Amphora-Routed-At"

	metricsNamespace = "amphora"
	metricsSubsystem = "proxy"
)

// DefaultDeferralTimeout is used when Config.DeferralTimeout is unset.
const DefaultDeferralTimeout = 30 * time.Second

// Config configures a Handler.
type Config struct {
	// DeferralTimeout bounds how long a request is held open waiting for a
	// cold model to become warm before failing with 503. Zero uses
	// DefaultDeferralTimeout.
	DeferralTimeout time.Duration

	// RequireClientIdentity makes the handler take the tenant from the
	// verified client certificate instead of trusting the X-Amphora-Tenant
	// header, and only let a tenant reach its own models (spec §5: the
	// header was previously client-asserted, so cost/audit tags and access
	// were spoofable). Requests without a verified certificate get 401; a
	// tenant asking for another tenant's model, or asserting a different
	// tenant in the header, gets 403. Requires the listener to be configured
	// with ServerTLSConfig and a client CA.
	RequireClientIdentity bool

	// MaxDeferredPerModel and MaxDeferredTotal bound how many requests may be
	// parked waiting on cold models, per model and overall; beyond them the
	// proxy answers 503 with Retry-After instead of holding the connection.
	// Zero uses DefaultMaxDeferredPerModel / DefaultMaxDeferredTotal.
	MaxDeferredPerModel int
	MaxDeferredTotal    int

	// UpstreamTLS are the credentials for https upstreams (model pods fronted
	// by the TLS sidecar, spec §5). Without them an https upstream is refused
	// rather than reached insecurely.
	UpstreamTLS *UpstreamTLS
}

const (
	// DefaultMaxDeferredPerModel caps parked requests for one cold model.
	DefaultMaxDeferredPerModel = 256
	// DefaultMaxDeferredTotal caps parked requests across all models.
	DefaultMaxDeferredTotal = 4096
	// retryAfterSeconds is advertised when shedding load.
	retryAfterSeconds = "5"
)

// Handler is the connection-deferral proxy PoC's HTTP entrypoint: it routes
// immediately to a warm target if one exists (§10 T=0..5ms), otherwise
// triggers a wakeup and holds the connection open until the target becomes
// warm or the deferral timeout elapses.
type Handler struct {
	registry *Registry
	notifier WakeupNotifier
	cfg      Config
	logger   *slog.Logger

	proxies sync.Map // upstream URL string -> *httputil.ReverseProxy

	deferMu         sync.Mutex
	deferredTotal   int
	deferredByModel map[string]int

	requestsTotal    *prometheus.CounterVec
	warmRouteLatency prometheus.Histogram
	coldWaitSeconds  prometheus.Histogram
}

// NewHandler builds a Handler, registering its metrics against reg (pass a
// dedicated *prometheus.Registry in tests to avoid collisions with the
// default global registry).
func NewHandler(registry *Registry, notifier WakeupNotifier, cfg Config, reg prometheus.Registerer, logger *slog.Logger) *Handler {
	if cfg.DeferralTimeout <= 0 {
		cfg.DeferralTimeout = DefaultDeferralTimeout
	}
	if logger == nil {
		logger = slog.Default()
	}
	if notifier == nil {
		notifier = NoopWakeupNotifier{Logger: logger}
	}

	if cfg.MaxDeferredPerModel <= 0 {
		cfg.MaxDeferredPerModel = DefaultMaxDeferredPerModel
	}
	if cfg.MaxDeferredTotal <= 0 {
		cfg.MaxDeferredTotal = DefaultMaxDeferredTotal
	}

	h := &Handler{
		deferredByModel: make(map[string]int),
		registry:        registry,
		notifier:        notifier,
		cfg:             cfg,
		logger:          logger,
		requestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "requests_total",
			Help:      "Total inbound requests by model and route path (warm/cold/timeout).",
		}, []string{"model", "path"}),
		warmRouteLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "warm_route_latency_seconds",
			Help:      "Latency from inbound request to route decision on the warm path (§7 target: p99 < 15ms).",
			Buckets:   prometheus.DefBuckets,
		}),
		coldWaitSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "cold_wait_seconds",
			Help:      "Time spent holding a request on the cold/deferred path before the target became warm or timed out.",
			Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30},
		}),
	}

	if reg != nil {
		reg.MustRegister(h.requestsTotal, h.warmRouteLatency, h.coldWaitSeconds)
	}

	return h
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	model := r.Header.Get(ModelHeader)
	tenant := r.Header.Get(TenantHeader)
	if model == "" {
		http.Error(w, "missing "+ModelHeader+" header", http.StatusBadRequest)
		return
	}

	if h.cfg.RequireClientIdentity {
		identity, ok := clientIdentity(r)
		if !ok {
			// The model label is client-controlled, so denied requests are
			// not labeled with it (bounded metric cardinality).
			h.requestsTotal.WithLabelValues(deniedModelLabel, "unauthenticated").Inc()
			http.Error(w, "a verified client certificate is required", http.StatusUnauthorized)
			return
		}
		// Same answer for "not yours" and "does not exist": a tenant cannot
		// probe for other tenants' models.
		if (tenant != "" && tenant != identity) || modelOwner(model) != identity {
			h.requestsTotal.WithLabelValues(deniedModelLabel, "forbidden").Inc()
			h.logger.Warn("denied request", "identity", identity, "asserted_tenant", tenant)
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		tenant = identity
	}

	start := time.Now()
	if target, warm := h.registry.Warm(model); warm {
		h.warmRouteLatency.Observe(time.Since(start).Seconds())
		h.requestsTotal.WithLabelValues(model, "warm").Inc()
		h.tagAndProxy(w, r, target, tenant)
		return
	}

	// Cold path. With known-model tracking (cluster sync), only models that
	// exist are worth waiting for: an unknown name can never become warm, so
	// parking it would only let a caller hold connections for the whole
	// deferral timeout and mint metric series. Answer immediately instead.
	if h.registry.KnownTracking() {
		if !h.registry.Synced() {
			w.Header().Set("Retry-After", "1")
			h.requestsTotal.WithLabelValues(deniedModelLabel, "starting").Inc()
			http.Error(w, "proxy is still loading the model list", http.StatusServiceUnavailable)
			return
		}
		if !h.registry.IsKnown(model) {
			h.requestsTotal.WithLabelValues(deniedModelLabel, "unknown_model").Inc()
			http.Error(w, "unknown model", http.StatusNotFound)
			return
		}
	}

	// Bound how many connections can be parked (§10 deferral is not free).
	if !h.acquireDeferral(model) {
		w.Header().Set("Retry-After", retryAfterSeconds)
		h.requestsTotal.WithLabelValues(model, "overloaded").Inc()
		http.Error(w, "too many requests waiting for this model", http.StatusServiceUnavailable)
		return
	}
	defer h.releaseDeferral(model)

	// Defer the connection rather than failing fast (§10).
	if err := h.notifier.NotifyWakeup(r.Context(), model, tenant); err != nil {
		h.logger.Error("wakeup notify failed", "model", model, "error", err)
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.cfg.DeferralTimeout)
	defer cancel()

	waitStart := time.Now()
	target, err := h.registry.Await(ctx, model)
	h.coldWaitSeconds.Observe(time.Since(waitStart).Seconds())
	if errors.Is(err, ErrModelGone) {
		h.requestsTotal.WithLabelValues(deniedModelLabel, "unknown_model").Inc()
		http.Error(w, "unknown model", http.StatusNotFound)
		return
	}
	if err != nil {
		h.requestsTotal.WithLabelValues(model, "timeout").Inc()
		http.Error(w, "timed out waiting for model to become available", http.StatusServiceUnavailable)
		return
	}

	h.requestsTotal.WithLabelValues(model, "cold").Inc()
	h.tagAndProxy(w, r, target, tenant)
}

// tagAndProxy attaches cost/audit tags and forwards the request to target.
func (h *Handler) tagAndProxy(w http.ResponseWriter, r *http.Request, target Target, tenant string) {
	rp := h.reverseProxyFor(target)
	if rp == nil {
		http.Error(w, "invalid upstream target", http.StatusBadGateway)
		return
	}

	routedAt := time.Now().UTC().Format(time.RFC3339Nano)
	r.Header.Set(ModelHeader, target.Model)
	if tenant != "" {
		r.Header.Set(TenantHeader, tenant)
	}
	r.Header.Set(routedAtHeader, routedAt)

	h.logger.Info("routing request",
		"model", target.Model,
		"tenant", tenant,
		"upstream", target.Upstream,
		"routed_at", routedAt,
	)

	rp.ServeHTTP(w, r)
}

func (h *Handler) reverseProxyFor(target Target) *httputil.ReverseProxy {
	// Keyed by name as well as address: the TLS name verified differs per
	// tenant even though the upstream URL identifies the pod.
	key := target.Upstream + "|" + target.ServerName
	if v, ok := h.proxies.Load(key); ok {
		return v.(*httputil.ReverseProxy)
	}
	u, err := url.Parse(target.Upstream)
	if err != nil || u.Scheme == "" || u.Host == "" {
		h.logger.Error("invalid upstream URL", "upstream", target.Upstream, "error", err)
		return nil
	}
	rp := httputil.NewSingleHostReverseProxy(u)
	if u.Scheme == schemeHTTPS {
		transport, err := h.cfg.UpstreamTLS.transport(target.ServerName)
		if err != nil {
			h.logger.Error("cannot reach https upstream", "upstream", target.Upstream, "error", err)
			return nil
		}
		rp.Transport = transport
	}
	actual, _ := h.proxies.LoadOrStore(key, rp)
	return actual.(*httputil.ReverseProxy)
}

// acquireDeferral reserves a parked-request slot for model, or reports false
// if the per-model or global cap is reached.
func (h *Handler) acquireDeferral(model string) bool {
	h.deferMu.Lock()
	defer h.deferMu.Unlock()
	if h.deferredTotal >= h.cfg.MaxDeferredTotal || h.deferredByModel[model] >= h.cfg.MaxDeferredPerModel {
		return false
	}
	h.deferredTotal++
	h.deferredByModel[model]++
	return true
}

func (h *Handler) releaseDeferral(model string) {
	h.deferMu.Lock()
	defer h.deferMu.Unlock()
	h.deferredTotal--
	if h.deferredByModel[model]--; h.deferredByModel[model] <= 0 {
		delete(h.deferredByModel, model)
	}
}

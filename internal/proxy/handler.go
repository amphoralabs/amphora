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
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

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
}

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

	h := &Handler{
		registry: registry,
		notifier: notifier,
		cfg:      cfg,
		logger:   logger,
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

	start := time.Now()
	if target, warm := h.registry.Warm(model); warm {
		h.warmRouteLatency.Observe(time.Since(start).Seconds())
		h.requestsTotal.WithLabelValues(model, "warm").Inc()
		h.tagAndProxy(w, r, target, tenant)
		return
	}

	// Cold path: defer the connection rather than failing fast (§10).
	if err := h.notifier.NotifyWakeup(r.Context(), model, tenant); err != nil {
		h.logger.Error("wakeup notify failed", "model", model, "error", err)
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.cfg.DeferralTimeout)
	defer cancel()

	waitStart := time.Now()
	target, err := h.registry.Await(ctx, model)
	h.coldWaitSeconds.Observe(time.Since(waitStart).Seconds())
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
	rp := h.reverseProxyFor(target.Upstream)
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

func (h *Handler) reverseProxyFor(upstream string) *httputil.ReverseProxy {
	if v, ok := h.proxies.Load(upstream); ok {
		return v.(*httputil.ReverseProxy)
	}
	u, err := url.Parse(upstream)
	if err != nil || u.Scheme == "" || u.Host == "" {
		h.logger.Error("invalid upstream URL", "upstream", upstream, "error", err)
		return nil
	}
	rp := httputil.NewSingleHostReverseProxy(u)
	actual, _ := h.proxies.LoadOrStore(upstream, rp)
	return actual.(*httputil.ReverseProxy)
}

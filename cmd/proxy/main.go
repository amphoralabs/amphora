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

// Command proxy is the connection-deferral proxy PoC (Technical
// Specification v3 §3.1/§10/§12.3): it routes inbound inference requests to
// a warm target immediately, or holds the connection open while a cold
// start is triggered elsewhere.
//
// With -sync-from-cluster the warm-target registry follows ModelDeployment
// status (Serving == warm). The opt-in -enable-admin-endpoints /admin/warm
// and /admin/cold simulate that locally without a cluster.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
	"github.com/ramin-fazli/amphora/internal/proxy"
)

func main() {
	var (
		addr              string
		deferralTimeout   time.Duration
		shutdownTimeout   time.Duration
		readHeaderTimeout time.Duration
		syncFromCluster   bool
		enableAdmin       bool
		tlsCert           string
		tlsKey            string
		tlsClientCA       string
	)
	flag.StringVar(&addr, "addr", ":8080", "address to listen on for inbound inference traffic")
	flag.DurationVar(&deferralTimeout, "deferral-timeout", proxy.DefaultDeferralTimeout,
		"max time to hold a request open waiting for a cold model to become warm")
	flag.DurationVar(&shutdownTimeout, "shutdown-timeout", 10*time.Second, "graceful shutdown deadline")
	flag.DurationVar(&readHeaderTimeout, "read-header-timeout", 5*time.Second, "max time to read request headers")
	flag.BoolVar(&syncFromCluster, "sync-from-cluster", false,
		"watch ModelDeployments via the Kubernetes API and route only to models the controller reports Serving")
	flag.BoolVar(&enableAdmin, "enable-admin-endpoints", false,
		"expose unauthenticated /admin/warm and /admin/cold for local simulation; never enable alongside real traffic")
	flag.StringVar(&tlsCert, "tls-cert", "", "serve inference traffic over TLS with this certificate (re-read on change)")
	flag.StringVar(&tlsKey, "tls-key", "", "private key for -tls-cert")
	flag.StringVar(&tlsClientCA, "tls-client-ca", "",
		"require clients to present a certificate signed by this CA bundle; the tenant is then taken from the "+
			"certificate's common name and a tenant may only reach its own models (spec §5)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	registry := proxy.NewRegistry()
	notifier := proxy.NoopWakeupNotifier{Logger: logger}
	registerer := prometheus.NewRegistry()

	if (tlsCert == "") != (tlsKey == "") {
		logger.Error("-tls-cert and -tls-key must be set together")
		os.Exit(1)
	}
	if tlsClientCA != "" && tlsCert == "" {
		logger.Error("-tls-client-ca requires -tls-cert and -tls-key: client identity is only meaningful over TLS")
		os.Exit(1)
	}

	handler := proxy.NewHandler(registry, notifier, proxy.Config{
		DeferralTimeout:       deferralTimeout,
		RequireClientIdentity: tlsClientCA != "",
	}, registerer, logger)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("/metrics", promhttp.HandlerFor(registerer, promhttp.HandlerOpts{}))
	if enableAdmin {
		// Unauthenticated and on the public listener: anyone reaching it can
		// redirect a model's traffic. Local simulation only.
		logger.Warn("admin endpoints enabled; do not use in production")
		mux.HandleFunc("/admin/warm", adminWarmHandler(registry, logger))
		mux.HandleFunc("/admin/cold", adminColdHandler(registry, logger))
	}
	mux.Handle("/", handler)

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
	}
	tlsCtx, cancelTLS := context.WithCancel(context.Background())
	defer cancelTLS()
	if tlsCert != "" {
		tlsCfg, err := proxy.ServerTLSConfig(tlsCtx, tlsCert, tlsKey, tlsClientCA)
		if err != nil {
			logger.Error("configuring TLS", "error", err)
			os.Exit(1)
		}
		srv.TLSConfig = tlsCfg
		if tlsClientCA == "" {
			logger.Warn("TLS without -tls-client-ca: callers are not authenticated and the " +
				"X-Amphora-Tenant header is client-asserted")
		}
	} else {
		logger.Warn("serving plaintext HTTP with no client authentication; the X-Amphora-Tenant header " +
			"is client-asserted. Use -tls-cert/-tls-key/-tls-client-ca outside local development")
	}

	syncCtx, cancelSync := context.WithCancel(context.Background())
	defer cancelSync()
	if syncFromCluster {
		scheme := runtime.NewScheme()
		if err := amphorav1alpha1.AddToScheme(scheme); err != nil {
			logger.Error("registering scheme", "error", err)
			os.Exit(1)
		}
		cfg, err := ctrl.GetConfig()
		if err != nil {
			logger.Error("loading kubeconfig", "error", err)
			os.Exit(1)
		}
		go func() {
			if err := proxy.SyncFromCluster(syncCtx, cfg, scheme, registry, logger); err != nil {
				logger.Error("cluster sync failed", "error", err)
				os.Exit(1)
			}
		}()
	}

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("proxy listening", "addr", addr)
		if srv.TLSConfig != nil {
			serveErr <- srv.ListenAndServeTLS("", "") // certificates come from TLSConfig
			return
		}
		serveErr <- srv.ListenAndServe()
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serveErr:
		if err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	case <-stop:
		logger.Info("shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			logger.Error("graceful shutdown failed", "error", err)
			os.Exit(1)
		}
	}
}

// warmRequest is the /admin/warm request body.
type warmRequest struct {
	Model    string `json:"model"`
	Tenant   string `json:"tenant"`
	Upstream string `json:"upstream"`
}

func adminWarmHandler(registry *proxy.Registry, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req warmRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if req.Model == "" || req.Upstream == "" {
			http.Error(w, "model and upstream are required", http.StatusBadRequest)
			return
		}
		registry.MarkWarm(proxy.Target{Model: req.Model, Tenant: req.Tenant, Upstream: req.Upstream})
		logger.Info("model marked warm", "model", req.Model, "upstream", req.Upstream)
		w.WriteHeader(http.StatusNoContent)
	}
}

func adminColdHandler(registry *proxy.Registry, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		model := r.URL.Query().Get("model")
		if model == "" {
			http.Error(w, "model query parameter is required", http.StatusBadRequest)
			return
		}
		registry.MarkCold(model)
		logger.Info("model marked cold", "model", model)
		w.WriteHeader(http.StatusNoContent)
	}
}

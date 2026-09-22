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
// The /admin/warm and /admin/cold endpoints stand in for the Controller's
// not-yet-built wakeup-complete callback (see repo STATUS.md's deliberately
// deferred reconcile-loop work) so the deferral path can be exercised
// end-to-end without it.
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

	"github.com/ramin-fazli/amphora/internal/proxy"
)

func main() {
	var (
		addr              string
		deferralTimeout   time.Duration
		shutdownTimeout   time.Duration
		readHeaderTimeout time.Duration
	)
	flag.StringVar(&addr, "addr", ":8080", "address to listen on for inbound inference traffic")
	flag.DurationVar(&deferralTimeout, "deferral-timeout", proxy.DefaultDeferralTimeout,
		"max time to hold a request open waiting for a cold model to become warm")
	flag.DurationVar(&shutdownTimeout, "shutdown-timeout", 10*time.Second, "graceful shutdown deadline")
	flag.DurationVar(&readHeaderTimeout, "read-header-timeout", 5*time.Second, "max time to read request headers")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	registry := proxy.NewRegistry()
	notifier := proxy.NoopWakeupNotifier{Logger: logger}
	registerer := prometheus.NewRegistry()

	handler := proxy.NewHandler(registry, notifier, proxy.Config{DeferralTimeout: deferralTimeout}, registerer, logger)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("/metrics", promhttp.HandlerFor(registerer, promhttp.HandlerOpts{}))
	mux.HandleFunc("/admin/warm", adminWarmHandler(registry, logger))
	mux.HandleFunc("/admin/cold", adminColdHandler(registry, logger))
	mux.Handle("/", handler)

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("proxy listening", "addr", addr)
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

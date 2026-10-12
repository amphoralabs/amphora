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

// Command podtls is the TLS-terminating sidecar for model-serving pods (spec
// §5). It authenticates and encrypts the proxy and controller's connections
// to a model server whose image it does not control.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ramin-fazli/amphora/internal/podtls"
)

func main() {
	var (
		listen          string
		upstream        string
		certFile        string
		keyFile         string
		clientCA        string
		allowedClients  string
		shutdownTimeout time.Duration
	)
	flag.StringVar(&listen, "listen", ":8443", "address to serve TLS on")
	flag.StringVar(&upstream, "upstream", "http://127.0.0.1:8000", "model server to forward to (loopback)")
	flag.StringVar(&certFile, "tls-cert", "/tls/tls.crt", "serving certificate (re-read on change)")
	flag.StringVar(&keyFile, "tls-key", "/tls/tls.key", "serving key")
	flag.StringVar(&clientCA, "client-ca", "/tls/ca.crt", "CA bundle client certificates must chain to")
	flag.StringVar(&allowedClients, "allowed-clients", "amphora-proxy,amphora-controller",
		"comma-separated certificate common names allowed to call the model")
	flag.DurationVar(&shutdownTimeout, "shutdown-timeout", 10*time.Second, "graceful shutdown deadline")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	fatal := func(msg string, err error) {
		logger.Error(msg, "error", err)
		os.Exit(1)
	}

	up, err := url.Parse(upstream)
	if err != nil || up.Scheme == "" || up.Host == "" {
		fatal("invalid -upstream", err)
	}
	allowed, err := podtls.ParseAllowedClients(allowedClients)
	if err != nil {
		fatal("invalid -allowed-clients", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tlsCfg, err := podtls.ServerTLSConfig(ctx, certFile, keyFile, clientCA)
	if err != nil {
		fatal("configuring TLS", err)
	}

	srv := &http.Server{
		Addr:              listen,
		Handler:           podtls.NewHandler(up, allowed, logger),
		TLSConfig:         tlsCfg,
		ReadHeaderTimeout: 10 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("podtls listening", "addr", listen, "upstream", upstream, "allowed", allowed)
		serveErr <- srv.ListenAndServeTLS("", "") // certificates come from TLSConfig
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-serveErr:
		if err != nil && err != http.ErrServerClosed {
			fatal("server error", err)
		}
	case <-stop:
		shutdownCtx, c := context.WithTimeout(context.Background(), shutdownTimeout)
		defer c()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			fatal("graceful shutdown failed", err)
		}
	}
}

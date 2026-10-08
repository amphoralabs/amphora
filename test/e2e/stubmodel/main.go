// Command stubmodel is a stand-in "model server" for e2e tests: GPU-less, it
// serves 200 on /health (what the eval gate probes) and echoes the
// AMPHORA_MODEL env var on every other path, so tests can prove which model
// identity reached the container and that traffic was routed to it.
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	model := os.Getenv("AMPHORA_MODEL")
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprintf(w, "model=%s\n", model) })
	srv := &http.Server{Addr: ":8000", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}

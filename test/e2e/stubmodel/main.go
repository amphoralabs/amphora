// Command stubmodel is a stand-in "model server" for e2e tests: GPU-less, it
// serves 200 on /health (what the eval gate probes), answers
// POST /v1/completions deterministically ("2+2=" -> "4", anything else ->
// "unknown") so canary matching can be tested, and echoes the AMPHORA_MODEL
// env var on every other path, so tests can prove which model identity
// reached the container and that traffic was routed to it.
package main

import (
	"encoding/json"
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
	mux.HandleFunc("/v1/completions", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Prompt string `json:"prompt"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		answer := "unknown"
		if req.Prompt == "2+2=" {
			answer = "4"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"choices": []map[string]string{{"text": " " + answer}}})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprintf(w, "model=%s\n", model) })
	srv := &http.Server{Addr: ":8000", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}

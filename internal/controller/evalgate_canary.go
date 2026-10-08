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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
)

const (
	// canaryConfigKey is the ConfigMap data key holding the canary set: a JSON
	// array of Canary objects.
	canaryConfigKey = "canaries.json"
	// maxCanaries bounds gate duration and config size.
	maxCanaries = 20
	// defaultCanaryMaxTokens caps generation when a canary sets no max_tokens.
	defaultCanaryMaxTokens = 16
	// maxCanaryResponseBytes bounds how much of a pod's reply is read: the
	// pod is untrusted until it passes the gate.
	maxCanaryResponseBytes = 1 << 20
	canaryPath             = "/v1/completions"

	reasonCanaryPassed = "CanaryPassed"
	reasonCanaryFailed = "CanaryFailed"
)

// Canary is one prompt with the completion a healthy model must produce.
// Matching is exact after trimming surrounding whitespace.
type Canary struct {
	Prompt    string `json:"prompt"`
	Expected  string `json:"expected"`
	MaxTokens int    `json:"max_tokens,omitempty"`
}

// CanaryRunner sends canaries to a serving pod and verifies the answers.
// A nil error means every canary matched within ctx's deadline.
type CanaryRunner interface {
	Run(ctx context.Context, baseURL, model string, canaries []Canary) error
}

// parseCanaries validates a canary ConfigMap: present key, valid JSON, 1 to
// maxCanaries entries, each with a non-empty prompt and expected answer.
func parseCanaries(cm *corev1.ConfigMap) ([]Canary, error) {
	raw, ok := cm.Data[canaryConfigKey]
	if !ok {
		return nil, fmt.Errorf("ConfigMap %s/%s has no %q key", cm.Namespace, cm.Name, canaryConfigKey)
	}
	var canaries []Canary
	if err := json.Unmarshal([]byte(raw), &canaries); err != nil {
		return nil, fmt.Errorf("ConfigMap %s/%s: invalid %s: %w", cm.Namespace, cm.Name, canaryConfigKey, err)
	}
	if len(canaries) == 0 || len(canaries) > maxCanaries {
		return nil, fmt.Errorf("ConfigMap %s/%s: need 1 to %d canaries, got %d", cm.Namespace, cm.Name, maxCanaries, len(canaries))
	}
	for i, c := range canaries {
		if c.Prompt == "" || strings.TrimSpace(c.Expected) == "" || c.MaxTokens < 0 {
			return nil, fmt.Errorf("ConfigMap %s/%s: canary %d needs a prompt and a non-empty expected answer", cm.Namespace, cm.Name, i)
		}
	}
	return canaries, nil
}

// loadCanaries reads md's canary ConfigMap, in md's own namespace, through the
// uncached reader so the controller needs only `get` on configmaps rather than
// a cluster-wide ConfigMap informer.
func (r *ModelDeploymentReconciler) loadCanaries(ctx context.Context, md *amphorav1alpha1.ModelDeployment) ([]Canary, error) {
	if r.APIReader == nil {
		return nil, errors.New("no APIReader configured to read the canary ConfigMap")
	}
	var cm corev1.ConfigMap
	key := client.ObjectKey{Namespace: md.Namespace, Name: md.Spec.EvalGate.CanaryConfigMapRef}
	if err := r.APIReader.Get(ctx, key, &cm); err != nil {
		return nil, fmt.Errorf("reading canary ConfigMap: %w", err)
	}
	return parseCanaries(&cm)
}

// HTTPCanaryRunner implements CanaryRunner against an OpenAI-style
// /v1/completions endpoint (what vLLM serves) with temperature 0.
type HTTPCanaryRunner struct {
	Client *http.Client
}

// Run implements CanaryRunner. Redirects are not followed and replies are
// size-limited: the pod is untrusted until it passes.
func (h *HTTPCanaryRunner) Run(ctx context.Context, baseURL, model string, canaries []Canary) error {
	client := h.Client
	if client == nil {
		client = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	for i, c := range canaries {
		maxTokens := c.MaxTokens
		if maxTokens == 0 {
			maxTokens = defaultCanaryMaxTokens
		}
		body, err := json.Marshal(map[string]any{"model": model, "prompt": c.Prompt, "max_tokens": maxTokens, "temperature": 0})
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+canaryPath, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		got, err := doCanary(client, req)
		if err != nil {
			return fmt.Errorf("canary %d: %w", i, err)
		}
		if strings.TrimSpace(got) != strings.TrimSpace(c.Expected) {
			return fmt.Errorf("canary %d: got %q, want %q", i, truncate(got, 80), truncate(c.Expected, 80))
		}
	}
	return nil
}

func doCanary(client *http.Client, req *http.Request) (string, error) {
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}
	var parsed struct {
		Choices []struct {
			Text string `json:"text"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxCanaryResponseBytes)).Decode(&parsed); err != nil {
		return "", fmt.Errorf("decoding reply: %w", err)
	}
	if len(parsed.Choices) == 0 {
		return "", errors.New("reply has no choices")
	}
	return parsed.Choices[0].Text, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

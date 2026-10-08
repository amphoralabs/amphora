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
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	// evalFailureThreshold is the number of consecutive gate failures after
	// which auto-promotion pauses pending manual approval (§3.2.1
	// false-negative handling), so a flaky probe cannot loop
	// swap/rollback forever.
	evalFailureThreshold = 3
	// approvePromotionAnnotation, set to "true" on a paused ModelDeployment,
	// resets the circuit breaker and lets promotion resume.
	approvePromotionAnnotation = "amphora.amphora.sh/approve-promotion"
	// evalRetryInterval is the backoff before re-placing after a rollback and
	// the poll interval while waiting for a pod to become Ready.
	evalRetryInterval = 5

	defaultProbePort int32 = 8000
	probePath              = "/health"

	conditionQualityVerified = "QualityVerified"
	reasonLatencyOnly        = "LatencyOnly"
)

// EvalProber runs one gate probe against a serving pod's URL. A nil error
// means the probe passed within ctx's deadline.
type EvalProber interface {
	Probe(ctx context.Context, url string) error
}

// HTTPEvalProber is the default latency-only probe (§3.2.1): a GET that must
// return 2xx before the gate deadline. It does not verify output quality;
// callers must record that ("quality unverified") rather than imply it.
type HTTPEvalProber struct {
	Client *http.Client
}

// Probe implements EvalProber. Redirects are not followed: the target is a
// pod IP taken from the apiserver and must not be steerable elsewhere.
func (p *HTTPEvalProber) Probe(ctx context.Context, url string) error {
	client := p.Client
	if client == nil {
		client = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("probe %s returned status %d", url, resp.StatusCode)
	}
	return nil
}

var evalGateResults = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "amphora",
	Subsystem: "controller",
	Name:      "eval_gate_results_total",
	Help:      "Eval gate outcomes (pass, fail, paused), by tenancy class.",
}, []string{"result", "tenancy_class"})

func init() {
	ctrlmetrics.Registry.MustRegister(evalGateResults)
}

// readinessProbe is declared on every serving-capable pod at creation (spec
// probes are immutable afterwards). Without it a pause pod reports Ready
// immediately, and the eval gate would probe it in the window right after a
// hijack's image swap while the container is still restarting. With it, a
// pod is Ready only once the container actually answers on the probe path;
// the kubelet resets readiness when a container restarts.
func readinessProbe(port int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:     corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: probePath, Port: intstr.FromInt32(port)}},
		PeriodSeconds:    2,
		FailureThreshold: 1,
	}
}

func podReady(pod *corev1.Pod) bool {
	if pod.Status.PodIP == "" {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

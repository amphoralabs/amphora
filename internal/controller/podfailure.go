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
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// Serving and pool pods are pre-bound to a node with spec.nodeName, which
// bypasses the scheduler. When a pod asks for a resource the node cannot
// provide (an extended resource such as a GPU or MIG slice that is already
// allocated, or a node under pressure), the kubelet rejects it at admission and
// the pod ends up in phase Failed instead of staying Pending. A Failed pod
// never recovers, so it must not count as capacity, be adopted, or be hijacked.
//
// Replacing it immediately would make a tight loop (create, rejected, delete,
// create), because deleting the pod itself triggers another reconcile. So a
// failed pod is kept for failedPodBackoff as visible evidence, no replacement is
// created while it is recent, and it is deleted and retried only after the
// window. That bounds the retry rate to one attempt per window per pod slot.

// failedPodBackoff is how long a failed pod is kept before being replaced. A
// variable so tests can shorten it.
var failedPodBackoff = 30 * time.Second

// isTerminal reports whether pod has finished and will never run again.
func isTerminal(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded
}

// terminalPods returns the finished pods that are not already being deleted.
func terminalPods(pods []corev1.Pod) []corev1.Pod {
	var out []corev1.Pod
	for _, p := range pods {
		if p.DeletionTimestamp.IsZero() && isTerminal(&p) {
			out = append(out, p)
		}
	}
	return out
}

// failedPodBackoffRemaining is how much longer pod should be kept before it may
// be replaced; zero or negative means it may be replaced now.
func failedPodBackoffRemaining(pod *corev1.Pod) time.Duration {
	return failedPodBackoff - time.Since(pod.CreationTimestamp.Time)
}

// podFailureMessage summarizes why a pod finished, for status and logs.
func podFailureMessage(pod *corev1.Pod) string {
	parts := []string{fmt.Sprintf("pod %s is %s", pod.Name, pod.Status.Phase)}
	if pod.Status.Reason != "" {
		parts = append(parts, pod.Status.Reason)
	}
	if pod.Status.Message != "" {
		parts = append(parts, pod.Status.Message)
	}
	return strings.Join(parts, ": ")
}

// podBackoffError is returned when a pod this deployment needs failed recently
// and is being kept for the backoff window. It is an expected, retryable state,
// not a reconcile error.
type podBackoffError struct {
	after   time.Duration
	message string
}

func (e *podBackoffError) Error() string { return e.message }

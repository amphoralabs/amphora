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
)

// WakeupNotifier signals that a model needs to be woken up (pause-pod
// hijack / cold start) because no warm target exists for it.
type WakeupNotifier interface {
	NotifyWakeup(ctx context.Context, model, tenant string) error
}

// NoopWakeupNotifier logs the wakeup request instead of calling the
// Controller. The real gRPC wakeup path (§3.2, §10) lands once the
// Controller's reconcile/hijack logic exists (currently a deliberately
// deferred TODO stub); this stand-in lets the proxy's routing/deferral
// behavior be built and tested independently of that work.
type NoopWakeupNotifier struct {
	Logger *slog.Logger
}

// NotifyWakeup implements WakeupNotifier.
func (n NoopWakeupNotifier) NotifyWakeup(_ context.Context, model, tenant string) error {
	logger := n.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.Info("wakeup requested", "model", model, "tenant", tenant)
	return nil
}

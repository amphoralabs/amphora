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
	"regexp"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ramin-fazli/amphora/internal/scheduler"
)

const (
	// NodeVRAMLabel declares a node's total GPU VRAM capacity in MiB. Nodes
	// without this label are not GPU nodes and are skipped by Packing
	// Scheduler node discovery. This is a placeholder convention: a real GPU
	// fleet would populate node capacity via a device-plugin/node-feature-
	// discovery integration, which doesn't exist yet (§3.3 follow-up).
	NodeVRAMLabel = "amphora.amphora.sh/gpu-vram-mb"
	// NodeMIGCapableLabel declares whether the node's GPU supports MIG
	// (Hopper/Blackwell), required for RegulatedMultiTenant placements
	// (§4). Defaults to false if absent.
	NodeMIGCapableLabel = "amphora.amphora.sh/gpu-mig-capable"

	// labelValueTrue is the canonical truthy label value used across
	// controller labels (shared to satisfy goconst).
	labelValueTrue = "true"
	// nodeMIGCapableTrue is NodeMIGCapableLabel's only truthy value.
	nodeMIGCapableTrue = labelValueTrue
)

// defaultVRAMMB is used when spec.GPUFraction doesn't declare a MIG profile
// name. It's a PoC placeholder standing in for a real per-model footprint
// estimate (e.g. from the model's weight size), pending that mechanism.
const defaultVRAMMB int64 = 8192

// syncSchedulerNodes lists cluster nodes carrying NodeVRAMLabel and
// registers/refreshes them in the Packing Scheduler's inventory (§3.3).
// Malformed labels on an individual node are skipped rather than failing
// the whole sync, so one bad node doesn't block reconciling every
// ModelDeployment in the cluster.
func (r *ModelDeploymentReconciler) syncSchedulerNodes(ctx context.Context) error {
	var nodes corev1.NodeList
	if err := r.List(ctx, &nodes, client.HasLabels{NodeVRAMLabel}); err != nil {
		return fmt.Errorf("listing GPU nodes: %w", err)
	}
	for _, n := range nodes.Items {
		spec, err := nodeSpecFromLabels(n)
		if err != nil {
			continue
		}
		if err := r.Scheduler.SyncNode(spec); err != nil {
			continue
		}
	}
	return nil
}

func nodeSpecFromLabels(n corev1.Node) (scheduler.NodeSpec, error) {
	vramStr := n.Labels[NodeVRAMLabel]
	vramMB, err := strconv.ParseInt(vramStr, 10, 64)
	if err != nil || vramMB <= 0 {
		return scheduler.NodeSpec{}, fmt.Errorf("node %s: invalid %s label %q", n.Name, NodeVRAMLabel, vramStr)
	}
	return scheduler.NodeSpec{
		ID:          n.Name,
		TotalVRAMMB: vramMB,
		MIGCapable:  n.Labels[NodeMIGCapableLabel] == nodeMIGCapableTrue,
	}, nil
}

// migProfileVRAMPattern matches the trailing VRAM component of NVIDIA's MIG
// profile naming convention, e.g. "1g.10gb" -> "10".
var migProfileVRAMPattern = regexp.MustCompile(`(?i)(\d+)gb$`)

// parseGPUFractionVRAMMB extracts the VRAM footprint (MiB) implied by a MIG
// profile name. Empty input returns defaultVRAMMB. A non-empty value that
// doesn't match the "<slices>g.<N>gb" convention is rejected rather than
// guessed at.
func parseGPUFractionVRAMMB(gpuFraction string) (int64, error) {
	if gpuFraction == "" {
		return defaultVRAMMB, nil
	}
	m := migProfileVRAMPattern.FindStringSubmatch(gpuFraction)
	if m == nil {
		return 0, fmt.Errorf("gpuFraction %q does not match the MIG profile pattern \"<slices>g.<N>gb\"", gpuFraction)
	}
	gb, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil || gb <= 0 {
		return 0, fmt.Errorf("gpuFraction %q has an invalid VRAM component", gpuFraction)
	}
	return gb * 1024, nil
}

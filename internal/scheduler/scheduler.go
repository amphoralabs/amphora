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

// Package scheduler implements the single-node Packing Scheduler described in
// Technical Specification v3 §3.3: it bin-packs ModelDeployments onto GPU
// nodes by VRAM footprint and enforces the §4 tenancy/isolation matrix at
// placement time. It is an in-memory PoC/community-tier scheduler (§8.1) —
// standing in for the eventual gRPC-published capacity map shared with the
// Controller and Proxy (see internal/proxy.Registry for that pattern) — and
// operates purely on declared node capacity, not live driver/nvidia-smi
// queries, since no MIG-capable hardware is available to validate against in
// this environment.
//
// Enforcement here is defense in depth alongside the admission webhook
// (§3.2, not yet implemented): the scheduler must independently reject any
// placement that would violate §4, not merely trust that the webhook already
// did.
package scheduler

import (
	"errors"
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
)

// PackingMode is the GPU-sharing mechanism a placement was assigned.
type PackingMode string

const (
	// PackingModeMIG assigns a hardware-isolated MIG slice (VRAM + fault
	// isolation). Required for RegulatedMultiTenant (§4).
	PackingModeMIG PackingMode = "MIG"
	// PackingModeTimeSlice assigns a logical time-sliced share. No
	// guaranteed VRAM/fault isolation; only permitted for SingleTenant/
	// TrustedMultiTenant (§4).
	PackingModeTimeSlice PackingMode = "TimeSlice"
	// PackingModeDedicated reserves the entire physical GPU; only valid for
	// SingleTenant, which requires exclusive node ownership.
	PackingModeDedicated PackingMode = "Dedicated"

	metricsNamespace = "amphora"
	metricsSubsystem = "scheduler"
)

// Sentinel errors returned by Scheduler methods. Callers should match on
// these with errors.Is rather than string comparison.
var (
	// ErrInvalidRequest is returned for malformed PlacementRequests.
	ErrInvalidRequest = errors.New("scheduler: invalid placement request")
	// ErrTenancyViolation is returned when a request's TenancyClass and
	// RequestedMode combination violates the §4 isolation matrix.
	ErrTenancyViolation = errors.New("scheduler: placement violates tenancy isolation matrix")
	// ErrNoCapacity is returned when no registered node can satisfy the
	// request (VRAM, MIG capability, colocation limits, or trust-boundary
	// compatibility with a node's existing tenants).
	ErrNoCapacity = errors.New("scheduler: no node has capacity for this placement")
	// ErrAlreadyPlaced is returned by Place when the model already has an
	// active placement with a different TenancyClass than requested (the
	// CRD's tenancyClass field is immutable post-creation, so this should
	// only occur on a caller bug — e.g. skipping Release after a delete).
	ErrAlreadyPlaced = errors.New("scheduler: model already placed under a different tenancy class")
	// ErrNodeExists is returned by RegisterNode for a duplicate node ID.
	ErrNodeExists = errors.New("scheduler: node already registered")
	// ErrNodeNotFound is returned when referencing an unregistered node ID.
	ErrNodeNotFound = errors.New("scheduler: node not found")
)

// NodeSpec describes a GPU node's static capacity, as declared by whatever
// inventories cluster nodes (a real implementation would source this from
// node labels/annotations; the PoC takes it directly).
type NodeSpec struct {
	// ID uniquely identifies the node (e.g. Kubernetes node name).
	ID string
	// TotalVRAMMB is the GPU's total VRAM capacity in MiB.
	TotalVRAMMB int64
	// MIGCapable indicates the GPU supports MIG (Hopper/Blackwell). Required
	// for any RegulatedMultiTenant placement (§4).
	MIGCapable bool
}

// PlacementRequest is a placement ask derived from a ModelDeployment's spec.
type PlacementRequest struct {
	// Model is the model/deployment identifier; used as the placement key.
	Model string
	// TenancyClass gates which packing modes are permissible (§4).
	TenancyClass amphorav1alpha1.TenancyClass
	// VRAMMB is the requested VRAM footprint in MiB.
	VRAMMB int64
	// RequestedMode optionally pins the packing mode (e.g. derived from a
	// non-empty GPUFraction implying MIG). Empty lets the scheduler pick the
	// cheapest mode permitted for TenancyClass.
	RequestedMode PackingMode
	// MaxColocatedModels caps how many distinct models may share a node with
	// this one. Zero means no additional constraint beyond node capacity
	// (mirrors the CRD field's unset/omitempty default).
	MaxColocatedModels int32
}

// Placement is the scheduler's decision for a PlacementRequest.
type Placement struct {
	Model  string
	NodeID string
	Mode   PackingMode
	VRAMMB int64
}

type nodeState struct {
	spec       NodeSpec
	usedVRAMMB int64
	placements map[string]Placement // model -> placement
}

func (n *nodeState) freeVRAMMB() int64 {
	return n.spec.TotalVRAMMB - n.usedVRAMMB
}

// tenancyClasses returns the set of distinct tenancy classes currently
// placed on the node.
func (n *nodeState) tenancyClasses(placedClass map[string]amphorav1alpha1.TenancyClass) map[amphorav1alpha1.TenancyClass]struct{} {
	classes := make(map[amphorav1alpha1.TenancyClass]struct{})
	for model := range n.placements {
		classes[placedClass[model]] = struct{}{}
	}
	return classes
}

// Scheduler is the single-node Packing Scheduler PoC. It is safe for
// concurrent use.
type Scheduler struct {
	mu sync.Mutex

	nodes map[string]*nodeState
	// tenancyClass tracks the TenancyClass each currently-placed model was
	// placed under, so re-derivation from placements alone doesn't require
	// threading the CRD spec back through.
	tenancyClass map[string]amphorav1alpha1.TenancyClass

	vramUsedRatio     *prometheus.GaugeVec
	colocatedModels   *prometheus.GaugeVec
	violationsBlocked *prometheus.CounterVec
}

// NewScheduler returns an empty Scheduler. Pass a dedicated
// *prometheus.Registry in tests to avoid collisions with the default global
// registry; reg may be nil to skip metric registration.
func NewScheduler(reg prometheus.Registerer) *Scheduler {
	s := &Scheduler{
		nodes:        make(map[string]*nodeState),
		tenancyClass: make(map[string]amphorav1alpha1.TenancyClass),
		vramUsedRatio: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "vram_used_ratio",
			Help:      "Fraction of a node's VRAM currently allocated to placements.",
		}, []string{"node"}),
		colocatedModels: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "colocated_models",
			Help:      "Number of distinct models currently placed on a node.",
		}, []string{"node"}),
		violationsBlocked: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "tenancy_class_violations_blocked_total",
			Help:      "Placements rejected for violating the §4 tenancy isolation matrix, by tenancy class.",
		}, []string{"tenancy_class"}),
	}
	if reg != nil {
		reg.MustRegister(s.vramUsedRatio, s.colocatedModels, s.violationsBlocked)
	}
	return s
}

// RegisterNode adds a node to the scheduler's inventory.
func (s *Scheduler) RegisterNode(spec NodeSpec) error {
	if spec.ID == "" || spec.TotalVRAMMB <= 0 {
		return fmt.Errorf("%w: node ID and TotalVRAMMB are required", ErrInvalidRequest)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.nodes[spec.ID]; exists {
		return fmt.Errorf("%w: %s", ErrNodeExists, spec.ID)
	}
	s.nodes[spec.ID] = &nodeState{spec: spec, placements: make(map[string]Placement)}
	s.vramUsedRatio.WithLabelValues(spec.ID).Set(0)
	s.colocatedModels.WithLabelValues(spec.ID).Set(0)
	return nil
}

// SyncNode registers spec if the node is unknown, or refreshes its declared
// capacity if already registered (e.g. relabeled). It never evicts existing
// placements: if an update shrinks capacity below what's already allocated,
// the node simply won't accept further placements until it grows back or
// placements are released elsewhere. Intended for callers that periodically
// reconcile node inventory from an external source (e.g. Node labels) and
// want idempotent register-or-update semantics rather than RegisterNode's
// error-on-duplicate behavior.
func (s *Scheduler) SyncNode(spec NodeSpec) error {
	if spec.ID == "" || spec.TotalVRAMMB <= 0 {
		return fmt.Errorf("%w: node ID and TotalVRAMMB are required", ErrInvalidRequest)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n, exists := s.nodes[spec.ID]
	if !exists {
		n = &nodeState{placements: make(map[string]Placement)}
		s.nodes[spec.ID] = n
	}
	n.spec = spec
	s.updateNodeMetricsLocked(n)
	return nil
}

// RemoveNode deletes a node from the inventory. Any placements on it are
// dropped without being reassigned elsewhere; callers are responsible for
// re-placing affected models.
func (s *Scheduler) RemoveNode(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNodeNotFound, id)
	}
	for model := range n.placements {
		delete(s.tenancyClass, model)
	}
	delete(s.nodes, id)
	s.vramUsedRatio.DeleteLabelValues(id)
	s.colocatedModels.DeleteLabelValues(id)
	return nil
}

// Placement returns the current placement for model, if any.
func (s *Scheduler) Placement(model string) (Placement, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range s.nodes {
		if p, ok := n.placements[model]; ok {
			return p, true
		}
	}
	return Placement{}, false
}

// Release frees model's placement, if any, making its VRAM/slice available
// for future placements (e.g. on scale-to-zero or ModelDeployment deletion).
func (s *Scheduler) Release(model string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range s.nodes {
		if p, ok := n.placements[model]; ok {
			n.usedVRAMMB -= p.VRAMMB
			delete(n.placements, model)
			delete(s.tenancyClass, model)
			s.updateNodeMetricsLocked(n)
			return
		}
	}
}

// Place assigns req to a node, enforcing the §4 tenancy isolation matrix and
// node capacity. It is idempotent: calling Place again for a model that
// already has an active placement under the same TenancyClass returns the
// existing placement unchanged (the CRD's tenancyClass is immutable, so a
// changed request for an already-placed model indicates a caller bug rather
// than a legitimate update — callers must Release before re-placing under a
// different spec).
func (s *Scheduler) Place(req PlacementRequest) (Placement, error) {
	if err := validateRequest(req); err != nil {
		return Placement{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.existingPlacementLocked(req.Model); ok {
		if s.tenancyClass[req.Model] != req.TenancyClass {
			return Placement{}, fmt.Errorf("%w: %s placed as %s, requested %s", ErrAlreadyPlaced, req.Model, s.tenancyClass[req.Model], req.TenancyClass)
		}
		return existing, nil
	}

	mode, err := resolveMode(req)
	if err != nil {
		s.violationsBlocked.WithLabelValues(string(req.TenancyClass)).Inc()
		return Placement{}, err
	}

	node, err := s.selectNodeLocked(req, mode)
	if err != nil {
		return Placement{}, err
	}

	p := Placement{Model: req.Model, NodeID: node.spec.ID, Mode: mode, VRAMMB: req.VRAMMB}
	node.placements[req.Model] = p
	node.usedVRAMMB += req.VRAMMB
	s.tenancyClass[req.Model] = req.TenancyClass
	s.updateNodeMetricsLocked(node)
	return p, nil
}

func (s *Scheduler) existingPlacementLocked(model string) (Placement, bool) {
	for _, n := range s.nodes {
		if p, ok := n.placements[model]; ok {
			return p, true
		}
	}
	return Placement{}, false
}

func validateRequest(req PlacementRequest) error {
	if req.Model == "" || req.VRAMMB <= 0 {
		return fmt.Errorf("%w: model and VRAMMB are required", ErrInvalidRequest)
	}
	switch req.TenancyClass {
	case amphorav1alpha1.TenancySingleTenant, amphorav1alpha1.TenancyTrustedMultiTenant, amphorav1alpha1.TenancyRegulatedMultiTenant:
	default:
		return fmt.Errorf("%w: unknown tenancyClass %q", ErrInvalidRequest, req.TenancyClass)
	}
	switch req.RequestedMode {
	case "", PackingModeMIG, PackingModeTimeSlice, PackingModeDedicated:
	default:
		return fmt.Errorf("%w: unknown requested mode %q", ErrInvalidRequest, req.RequestedMode)
	}
	return nil
}

// resolveMode maps a PlacementRequest to a concrete PackingMode, enforcing
// §4: RegulatedMultiTenant is MIG-only; SingleTenant is always Dedicated
// (full-GPU exclusivity, packing mode is moot); TrustedMultiTenant may use
// either MIG or TimeSlice.
func resolveMode(req PlacementRequest) (PackingMode, error) {
	switch req.TenancyClass {
	case amphorav1alpha1.TenancySingleTenant:
		// SingleTenant may request MIG/time-slice per the matrix's "Either,
		// or no packing" column, but this PoC's single-node scheduler always
		// grants full exclusivity for SingleTenant rather than modeling
		// intra-tenant sub-slicing, which the matrix's isolation guarantee
		// ("full physical GPU, no sharing") doesn't require anyway.
		return PackingModeDedicated, nil

	case amphorav1alpha1.TenancyRegulatedMultiTenant:
		if req.RequestedMode == PackingModeTimeSlice {
			return "", fmt.Errorf("%w: %s requires MIG, time-slicing has no fault/VRAM isolation", ErrTenancyViolation, req.TenancyClass)
		}
		if req.RequestedMode == PackingModeDedicated {
			return "", fmt.Errorf("%w: %s must not claim a dedicated GPU (defeats multi-tenant packing intent)", ErrTenancyViolation, req.TenancyClass)
		}
		return PackingModeMIG, nil

	case amphorav1alpha1.TenancyTrustedMultiTenant:
		if req.RequestedMode == PackingModeDedicated {
			return "", fmt.Errorf("%w: %s must not claim a dedicated GPU (defeats multi-tenant packing intent)", ErrTenancyViolation, req.TenancyClass)
		}
		if req.RequestedMode == "" {
			return PackingModeMIG, nil // prefer hardware isolation when the caller has no preference
		}
		return req.RequestedMode, nil

	default:
		return "", fmt.Errorf("%w: unknown tenancyClass %q", ErrInvalidRequest, req.TenancyClass)
	}
}

// selectNodeLocked finds the best-fit node for req/mode: the compatible node
// with the least remaining free VRAM that still satisfies the request,
// maximizing packing density (§3.3). Caller must hold s.mu.
func (s *Scheduler) selectNodeLocked(req PlacementRequest, mode PackingMode) (*nodeState, error) {
	var best *nodeState
	for _, n := range s.nodes {
		if !s.compatibleLocked(n, req, mode) {
			continue
		}
		if n.freeVRAMMB() < req.VRAMMB {
			continue
		}
		if best == nil || n.freeVRAMMB() < best.freeVRAMMB() {
			best = n
		}
	}
	if best == nil {
		return nil, fmt.Errorf("%w: model=%s tenancyClass=%s mode=%s vramMB=%d", ErrNoCapacity, req.Model, req.TenancyClass, mode, req.VRAMMB)
	}
	return best, nil
}

// compatibleLocked reports whether node can host req under mode without
// violating §4's trust-boundary/isolation rules or MaxColocatedModels.
// Caller must hold s.mu.
func (s *Scheduler) compatibleLocked(n *nodeState, req PlacementRequest, mode PackingMode) bool {
	if mode == PackingModeMIG && !n.spec.MIGCapable {
		return false
	}

	existingClasses := n.tenancyClasses(s.tenancyClass)
	if len(existingClasses) > 0 {
		// SingleTenant requires exclusive ownership: reject any node that
		// already has placements, and reject placing anything else onto a
		// node already hosting a SingleTenant placement.
		if req.TenancyClass == amphorav1alpha1.TenancySingleTenant {
			return false
		}
		if _, hasSingle := existingClasses[amphorav1alpha1.TenancySingleTenant]; hasSingle {
			return false
		}
		// RegulatedMultiTenant may only co-locate with other
		// RegulatedMultiTenant placements (never mix trust boundaries with
		// Trusted, even though both would technically fit under MIG).
		_, hasRegulated := existingClasses[amphorav1alpha1.TenancyRegulatedMultiTenant]
		_, hasTrusted := existingClasses[amphorav1alpha1.TenancyTrustedMultiTenant]
		if req.TenancyClass == amphorav1alpha1.TenancyRegulatedMultiTenant && hasTrusted {
			return false
		}
		if req.TenancyClass == amphorav1alpha1.TenancyTrustedMultiTenant && hasRegulated {
			return false
		}
	}

	if req.MaxColocatedModels > 0 && int32(len(n.placements)) >= req.MaxColocatedModels {
		return false
	}

	return true
}

func (s *Scheduler) updateNodeMetricsLocked(n *nodeState) {
	ratio := 0.0
	if n.spec.TotalVRAMMB > 0 {
		ratio = float64(n.usedVRAMMB) / float64(n.spec.TotalVRAMMB)
	}
	s.vramUsedRatio.WithLabelValues(n.spec.ID).Set(ratio)
	s.colocatedModels.WithLabelValues(n.spec.ID).Set(float64(len(n.placements)))
}

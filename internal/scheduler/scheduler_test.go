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

package scheduler

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	amphorav1alpha1 "github.com/ramin-fazli/amphora/api/v1alpha1"
)

const testNodeID = "gpu-0"

func newTestScheduler(t *testing.T) *Scheduler {
	t.Helper()
	return NewScheduler(nil)
}

func mustRegisterNode(t *testing.T, s *Scheduler, spec NodeSpec) {
	t.Helper()
	if err := s.RegisterNode(spec); err != nil {
		t.Fatalf("RegisterNode(%+v) = %v, want nil", spec, err)
	}
}

func TestPlaceSingleTenantDedicatesFullNode(t *testing.T) {
	s := newTestScheduler(t)
	mustRegisterNode(t, s, NodeSpec{ID: testNodeID, TotalVRAMMB: 80_000, MIGCapable: true})

	p, err := s.Place(PlacementRequest{Model: "m1", TenancyClass: amphorav1alpha1.TenancySingleTenant, VRAMMB: 10_000})
	if err != nil {
		t.Fatalf("Place() = %v, want nil", err)
	}
	if p.Mode != PackingModeDedicated {
		t.Errorf("Mode = %v, want %v", p.Mode, PackingModeDedicated)
	}

	// A second model, even a tiny one, must not co-locate on a node holding
	// a SingleTenant placement.
	_, err = s.Place(PlacementRequest{Model: "m2", TenancyClass: amphorav1alpha1.TenancyTrustedMultiTenant, VRAMMB: 1_000})
	if !errors.Is(err, ErrNoCapacity) {
		t.Errorf("Place(m2) = %v, want ErrNoCapacity", err)
	}
}

func TestPlaceSingleTenantRejectsNodeAlreadyOccupied(t *testing.T) {
	s := newTestScheduler(t)
	mustRegisterNode(t, s, NodeSpec{ID: testNodeID, TotalVRAMMB: 80_000, MIGCapable: true})

	if _, err := s.Place(PlacementRequest{Model: "m1", TenancyClass: amphorav1alpha1.TenancyTrustedMultiTenant, VRAMMB: 1_000}); err != nil {
		t.Fatalf("Place(m1) = %v, want nil", err)
	}

	_, err := s.Place(PlacementRequest{Model: "m2", TenancyClass: amphorav1alpha1.TenancySingleTenant, VRAMMB: 1_000})
	if !errors.Is(err, ErrNoCapacity) {
		t.Errorf("Place(m2) = %v, want ErrNoCapacity", err)
	}
}

func TestPlaceRegulatedMultiTenantRequiresMIG(t *testing.T) {
	s := newTestScheduler(t)
	mustRegisterNode(t, s, NodeSpec{ID: testNodeID, TotalVRAMMB: 80_000, MIGCapable: true})

	p, err := s.Place(PlacementRequest{Model: "m1", TenancyClass: amphorav1alpha1.TenancyRegulatedMultiTenant, VRAMMB: 10_000})
	if err != nil {
		t.Fatalf("Place() = %v, want nil", err)
	}
	if p.Mode != PackingModeMIG {
		t.Errorf("Mode = %v, want %v", p.Mode, PackingModeMIG)
	}
}

func TestPlaceRegulatedMultiTenantRejectsTimeSlice(t *testing.T) {
	s := newTestScheduler(t)
	mustRegisterNode(t, s, NodeSpec{ID: testNodeID, TotalVRAMMB: 80_000, MIGCapable: true})

	_, err := s.Place(PlacementRequest{
		Model:         "m1",
		TenancyClass:  amphorav1alpha1.TenancyRegulatedMultiTenant,
		VRAMMB:        10_000,
		RequestedMode: PackingModeTimeSlice,
	})
	if !errors.Is(err, ErrTenancyViolation) {
		t.Fatalf("Place() = %v, want ErrTenancyViolation", err)
	}

	if got := testCounterValue(t, s.violationsBlocked, string(amphorav1alpha1.TenancyRegulatedMultiTenant)); got != 1 {
		t.Errorf("violationsBlocked = %v, want 1", got)
	}
}

func TestPlaceRegulatedMultiTenantRejectsNonMIGCapableNode(t *testing.T) {
	s := newTestScheduler(t)
	mustRegisterNode(t, s, NodeSpec{ID: testNodeID, TotalVRAMMB: 80_000, MIGCapable: false})

	_, err := s.Place(PlacementRequest{Model: "m1", TenancyClass: amphorav1alpha1.TenancyRegulatedMultiTenant, VRAMMB: 10_000})
	if !errors.Is(err, ErrNoCapacity) {
		t.Errorf("Place() = %v, want ErrNoCapacity", err)
	}
}

func TestPlaceTrustedMultiTenantCanShareViaTimeSlice(t *testing.T) {
	s := newTestScheduler(t)
	mustRegisterNode(t, s, NodeSpec{ID: testNodeID, TotalVRAMMB: 80_000, MIGCapable: false})

	p, err := s.Place(PlacementRequest{
		Model:         "m1",
		TenancyClass:  amphorav1alpha1.TenancyTrustedMultiTenant,
		VRAMMB:        10_000,
		RequestedMode: PackingModeTimeSlice,
	})
	if err != nil {
		t.Fatalf("Place() = %v, want nil", err)
	}
	if p.Mode != PackingModeTimeSlice {
		t.Errorf("Mode = %v, want %v", p.Mode, PackingModeTimeSlice)
	}

	p2, err := s.Place(PlacementRequest{
		Model:         "m2",
		TenancyClass:  amphorav1alpha1.TenancyTrustedMultiTenant,
		VRAMMB:        10_000,
		RequestedMode: PackingModeTimeSlice,
	})
	if err != nil {
		t.Fatalf("Place(m2) = %v, want nil", err)
	}
	if p2.NodeID != p.NodeID {
		t.Errorf("m2 placed on %s, want co-located on %s", p2.NodeID, p.NodeID)
	}
}

func TestPlaceRejectsMixingRegulatedAndTrustedOnSameNode(t *testing.T) {
	s := newTestScheduler(t)
	mustRegisterNode(t, s, NodeSpec{ID: testNodeID, TotalVRAMMB: 80_000, MIGCapable: true})

	if _, err := s.Place(PlacementRequest{Model: "m1", TenancyClass: amphorav1alpha1.TenancyRegulatedMultiTenant, VRAMMB: 10_000}); err != nil {
		t.Fatalf("Place(m1) = %v, want nil", err)
	}

	_, err := s.Place(PlacementRequest{Model: "m2", TenancyClass: amphorav1alpha1.TenancyTrustedMultiTenant, VRAMMB: 10_000, RequestedMode: PackingModeMIG})
	if !errors.Is(err, ErrNoCapacity) {
		t.Errorf("Place(m2) = %v, want ErrNoCapacity", err)
	}
}

func TestPlaceEnforcesMaxColocatedModels(t *testing.T) {
	s := newTestScheduler(t)
	mustRegisterNode(t, s, NodeSpec{ID: testNodeID, TotalVRAMMB: 80_000, MIGCapable: true})

	req := func(model string) PlacementRequest {
		return PlacementRequest{
			Model:              model,
			TenancyClass:       amphorav1alpha1.TenancyTrustedMultiTenant,
			VRAMMB:             1_000,
			MaxColocatedModels: 1,
		}
	}
	if _, err := s.Place(req("m1")); err != nil {
		t.Fatalf("Place(m1) = %v, want nil", err)
	}
	_, err := s.Place(req("m2"))
	if !errors.Is(err, ErrNoCapacity) {
		t.Errorf("Place(m2) = %v, want ErrNoCapacity (MaxColocatedModels=1)", err)
	}
}

func TestPlaceEnforcesVRAMCapacity(t *testing.T) {
	s := newTestScheduler(t)
	mustRegisterNode(t, s, NodeSpec{ID: testNodeID, TotalVRAMMB: 10_000, MIGCapable: true})

	if _, err := s.Place(PlacementRequest{Model: "m1", TenancyClass: amphorav1alpha1.TenancyTrustedMultiTenant, VRAMMB: 9_000}); err != nil {
		t.Fatalf("Place(m1) = %v, want nil", err)
	}
	_, err := s.Place(PlacementRequest{Model: "m2", TenancyClass: amphorav1alpha1.TenancyTrustedMultiTenant, VRAMMB: 2_000})
	if !errors.Is(err, ErrNoCapacity) {
		t.Errorf("Place(m2) = %v, want ErrNoCapacity", err)
	}
}

func TestPlaceBestFitPacksDenserNodeFirst(t *testing.T) {
	s := newTestScheduler(t)
	mustRegisterNode(t, s, NodeSpec{ID: "roomy", TotalVRAMMB: 80_000, MIGCapable: true})
	mustRegisterNode(t, s, NodeSpec{ID: "tight", TotalVRAMMB: 12_000, MIGCapable: true})

	p, err := s.Place(PlacementRequest{Model: "m1", TenancyClass: amphorav1alpha1.TenancyTrustedMultiTenant, VRAMMB: 10_000, RequestedMode: PackingModeMIG})
	if err != nil {
		t.Fatalf("Place() = %v, want nil", err)
	}
	if p.NodeID != "tight" {
		t.Errorf("NodeID = %q, want %q (best-fit should prefer the node with least remaining free VRAM that still fits)", p.NodeID, "tight")
	}
}

func TestPlaceIsIdempotentForSameTenancyClass(t *testing.T) {
	s := newTestScheduler(t)
	mustRegisterNode(t, s, NodeSpec{ID: testNodeID, TotalVRAMMB: 80_000, MIGCapable: true})

	req := PlacementRequest{Model: "m1", TenancyClass: amphorav1alpha1.TenancyTrustedMultiTenant, VRAMMB: 10_000}
	p1, err := s.Place(req)
	if err != nil {
		t.Fatalf("Place() = %v, want nil", err)
	}
	p2, err := s.Place(req)
	if err != nil {
		t.Fatalf("Place() (repeat) = %v, want nil", err)
	}
	if p1 != p2 {
		t.Errorf("repeat Place() = %+v, want unchanged %+v", p2, p1)
	}
}

func TestPlaceRejectsDifferentTenancyClassForAlreadyPlacedModel(t *testing.T) {
	s := newTestScheduler(t)
	mustRegisterNode(t, s, NodeSpec{ID: testNodeID, TotalVRAMMB: 80_000, MIGCapable: true})

	if _, err := s.Place(PlacementRequest{Model: "m1", TenancyClass: amphorav1alpha1.TenancyTrustedMultiTenant, VRAMMB: 10_000}); err != nil {
		t.Fatalf("Place() = %v, want nil", err)
	}
	_, err := s.Place(PlacementRequest{Model: "m1", TenancyClass: amphorav1alpha1.TenancyRegulatedMultiTenant, VRAMMB: 10_000})
	if !errors.Is(err, ErrAlreadyPlaced) {
		t.Errorf("Place() = %v, want ErrAlreadyPlaced", err)
	}
}

func TestReleaseFreesCapacity(t *testing.T) {
	s := newTestScheduler(t)
	mustRegisterNode(t, s, NodeSpec{ID: testNodeID, TotalVRAMMB: 10_000, MIGCapable: true})

	if _, err := s.Place(PlacementRequest{Model: "m1", TenancyClass: amphorav1alpha1.TenancyTrustedMultiTenant, VRAMMB: 9_000}); err != nil {
		t.Fatalf("Place(m1) = %v, want nil", err)
	}
	s.Release("m1")

	if _, ok := s.Placement("m1"); ok {
		t.Errorf("Placement(m1) found after Release, want not found")
	}
	if _, err := s.Place(PlacementRequest{Model: "m2", TenancyClass: amphorav1alpha1.TenancyTrustedMultiTenant, VRAMMB: 9_000}); err != nil {
		t.Errorf("Place(m2) after Release = %v, want nil", err)
	}
}

func TestSyncNodeRegistersUnknownNode(t *testing.T) {
	s := newTestScheduler(t)

	if err := s.SyncNode(NodeSpec{ID: testNodeID, TotalVRAMMB: 40_000, MIGCapable: true}); err != nil {
		t.Fatalf("SyncNode() = %v, want nil", err)
	}
	if _, err := s.Place(PlacementRequest{Model: "m1", TenancyClass: amphorav1alpha1.TenancyTrustedMultiTenant, VRAMMB: 10_000}); err != nil {
		t.Errorf("Place() after SyncNode = %v, want nil", err)
	}
}

func TestSyncNodeUpdatesCapacityWithoutEvictingPlacements(t *testing.T) {
	s := newTestScheduler(t)
	mustRegisterNode(t, s, NodeSpec{ID: testNodeID, TotalVRAMMB: 40_000, MIGCapable: false})

	if _, err := s.Place(PlacementRequest{Model: "m1", TenancyClass: amphorav1alpha1.TenancyTrustedMultiTenant, VRAMMB: 10_000, RequestedMode: PackingModeTimeSlice}); err != nil {
		t.Fatalf("Place() = %v, want nil", err)
	}

	if err := s.SyncNode(NodeSpec{ID: testNodeID, TotalVRAMMB: 80_000, MIGCapable: true}); err != nil {
		t.Fatalf("SyncNode() (update) = %v, want nil", err)
	}

	if _, ok := s.Placement("m1"); !ok {
		t.Fatalf("Placement(m1) not found after SyncNode update, want preserved")
	}
	if _, err := s.Place(PlacementRequest{Model: "m2", TenancyClass: amphorav1alpha1.TenancyTrustedMultiTenant, VRAMMB: 10_000, RequestedMode: PackingModeMIG}); err != nil {
		t.Errorf("Place(m2) after capacity/MIG update = %v, want nil (node is now MIG-capable with room)", err)
	}
}

func TestSyncNodeRejectsInvalidSpec(t *testing.T) {
	s := newTestScheduler(t)
	if err := s.SyncNode(NodeSpec{ID: "", TotalVRAMMB: 1_000}); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("SyncNode() = %v, want ErrInvalidRequest", err)
	}
}

func TestPlaceRejectsInvalidRequest(t *testing.T) {
	s := newTestScheduler(t)
	mustRegisterNode(t, s, NodeSpec{ID: testNodeID, TotalVRAMMB: 80_000, MIGCapable: true})

	cases := []PlacementRequest{
		{Model: "", TenancyClass: amphorav1alpha1.TenancySingleTenant, VRAMMB: 1_000},
		{Model: "m1", TenancyClass: amphorav1alpha1.TenancySingleTenant, VRAMMB: 0},
		{Model: "m1", TenancyClass: "NotARealClass", VRAMMB: 1_000},
		{Model: "m1", TenancyClass: amphorav1alpha1.TenancySingleTenant, VRAMMB: 1_000, RequestedMode: "NotAMode"},
	}
	for _, req := range cases {
		if _, err := s.Place(req); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("Place(%+v) = %v, want ErrInvalidRequest", req, err)
		}
	}
}

func TestRegisterNodeRejectsDuplicateID(t *testing.T) {
	s := newTestScheduler(t)
	mustRegisterNode(t, s, NodeSpec{ID: testNodeID, TotalVRAMMB: 80_000})

	err := s.RegisterNode(NodeSpec{ID: testNodeID, TotalVRAMMB: 40_000})
	if !errors.Is(err, ErrNodeExists) {
		t.Errorf("RegisterNode() = %v, want ErrNodeExists", err)
	}
}

func TestRemoveNodeNotFound(t *testing.T) {
	s := newTestScheduler(t)
	if err := s.RemoveNode("missing"); !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("RemoveNode() = %v, want ErrNodeNotFound", err)
	}
}

// testCounterValue reads the current value of a CounterVec's child metric
// without needing a full Prometheus registry/gatherer round-trip.
func testCounterValue(t *testing.T, cv *prometheus.CounterVec, label string) float64 {
	t.Helper()
	m := &dto.Metric{}
	if err := cv.WithLabelValues(label).Write(m); err != nil {
		t.Fatalf("Write() = %v", err)
	}
	return m.GetCounter().GetValue()
}

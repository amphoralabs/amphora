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

package coldstart

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGenerateSyntheticModelAndStreamRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "model.bin")

	const sizeBytes = 1 << 20 // 1 MiB, kept small so this runs fast in CI
	if err := GenerateSyntheticModel(path, sizeBytes); err != nil {
		t.Fatalf("GenerateSyntheticModel failed: %v", err)
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat generated file: %v", err)
	}
	if fi.Size() != sizeBytes {
		t.Fatalf("got file size %d, want %d", fi.Size(), sizeBytes)
	}

	d, err := StreamRead(path)
	if err != nil {
		t.Fatalf("StreamRead failed: %v", err)
	}
	if d < 0 {
		t.Fatalf("got negative duration %v", d)
	}
}

func TestStreamReadEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.bin")
	if err := GenerateSyntheticModel(path, 0); err != nil {
		t.Fatalf("GenerateSyntheticModel failed: %v", err)
	}

	d, err := StreamRead(path)
	if err != nil {
		t.Fatalf("StreamRead failed: %v", err)
	}
	if d != 0 {
		t.Fatalf("got duration %v for empty file, want 0", d)
	}
}

func TestStreamReadMissingFile(t *testing.T) {
	if _, err := StreamRead(filepath.Join(t.TempDir(), "does-not-exist.bin")); err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestStreamReadDirect(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "model.bin")

	// A few MiB, aligned to typical O_DIRECT block-size requirements.
	const sizeBytes = 4 << 20
	if err := GenerateSyntheticModel(path, sizeBytes); err != nil {
		t.Fatalf("GenerateSyntheticModel failed: %v", err)
	}

	// StreamReadDirect falls back to the cached path if the underlying
	// filesystem (e.g. tmpfs/overlay in some CI sandboxes) doesn't support
	// O_DIRECT, so this should never error even where direct I/O isn't
	// available.
	d, err := StreamReadDirect(path)
	if err != nil {
		t.Fatalf("StreamReadDirect failed: %v", err)
	}
	if d < 0 {
		t.Fatalf("got negative duration %v", d)
	}
}

func TestRunBurst(t *testing.T) {
	dir := t.TempDir()
	const n = 4
	const sizeBytes = 256 << 10 // 256 KiB per stream

	paths := make([]string, n)
	for i := 0; i < n; i++ {
		p := filepath.Join(dir, fmt.Sprintf("model-%d.bin", i))
		if err := GenerateSyntheticModel(p, sizeBytes); err != nil {
			t.Fatalf("GenerateSyntheticModel failed: %v", err)
		}
		paths[i] = p
	}

	res, err := RunBurst(paths, StreamRead)
	if err != nil {
		t.Fatalf("RunBurst failed: %v", err)
	}
	if res.N != n {
		t.Fatalf("got N=%d, want %d", res.N, n)
	}
	if len(res.StreamDurations) != n {
		t.Fatalf("got %d stream durations, want %d", len(res.StreamDurations), n)
	}
	if res.Makespan <= 0 {
		t.Fatalf("got non-positive makespan %v", res.Makespan)
	}
	if res.MedianStreamDuration() < 0 {
		t.Fatalf("got negative median stream duration")
	}
}

func TestDegradationFactor(t *testing.T) {
	tests := []struct {
		name     string
		baseline time.Duration
		atN      time.Duration
		want     float64
	}{
		{"no degradation", 100 * time.Millisecond, 100 * time.Millisecond, 1.0},
		{"2x slower under contention", 100 * time.Millisecond, 200 * time.Millisecond, 2.0},
		{"faster than baseline (noise)", 100 * time.Millisecond, 50 * time.Millisecond, 0.5},
		{"zero baseline guards against div-by-zero", 0, 200 * time.Millisecond, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DegradationFactor(tt.baseline, tt.atN)
			if got != tt.want {
				t.Fatalf("DegradationFactor(%v, %v) = %v, want %v", tt.baseline, tt.atN, got, tt.want)
			}
		})
	}
}

func TestMedian(t *testing.T) {
	tests := []struct {
		name string
		in   []time.Duration
		want time.Duration
	}{
		{"empty", nil, 0},
		{"single", []time.Duration{5 * time.Second}, 5 * time.Second},
		{"odd count", []time.Duration{3 * time.Second, 1 * time.Second, 2 * time.Second}, 2 * time.Second},
		{
			"even count",
			[]time.Duration{1 * time.Second, 2 * time.Second, 3 * time.Second, 4 * time.Second},
			(2*time.Second + 3*time.Second) / 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Median(tt.in)
			if got != tt.want {
				t.Fatalf("Median(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

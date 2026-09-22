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

// Command coldstart-bench runs the Phase 1 concurrent cold-start benchmark
// (Technical Specification v3 §3.4.1) and publishes the measured f(N)
// storage-layer degradation curve for N ∈ {1,4,8,16} (configurable).
//
// See benchmark/coldstart/README.md for what this does and does not
// measure, and for how to run it on reference GPU/NVMe hardware to produce
// the officially publishable Phase 1 numbers.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ramin-fazli/amphora/benchmark/coldstart"
)

// concurrencyResult is one row of the published f(N) table.
type concurrencyResult struct {
	N                      int     `json:"n"`
	MedianStreamDurationS  float64 `json:"median_stream_duration_s"`
	MakespanS              float64 `json:"makespan_s"`
	EffectiveThroughputGBs float64 `json:"effective_per_stream_throughput_gb_s"`
	AggregateThroughputGBs float64 `json:"aggregate_throughput_gb_s"`
	DegradationFactor      float64 `json:"f_n"`
}

func main() {
	var (
		modelSizeGB float64
		concurrency string
		dataDir     string
		iterations  int
		outputPath  string
		keepFiles   bool
		directIO    bool
	)
	flag.Float64Var(&modelSizeGB, "model-size-gb", 4.0,
		"synthetic model shard size in GB per stream (default approximates a quantized 7B model)")
	flag.StringVar(&concurrency, "concurrency", "1,4,8,16",
		"comma-separated list of concurrent cold-start counts to benchmark")
	flag.StringVar(&dataDir, "data-dir", filepath.Join(os.TempDir(), "amphora-coldstart-bench"),
		"directory to create synthetic model files in")
	flag.IntVar(&iterations, "iterations", 3, "number of bursts to run per concurrency level (median is reported)")
	flag.StringVar(&outputPath, "output", "", "optional path to write results as JSON")
	flag.BoolVar(&keepFiles, "keep-files", false, "keep synthetic model files after the run instead of deleting them")
	flag.BoolVar(&directIO, "o-direct", false,
		"bypass the OS page cache with O_DIRECT reads instead of mmap; required for genuine "+
			"disk-bound numbers on machines without root access to drop caches (falls back to "+
			"the cached path if the filesystem doesn't support O_DIRECT)")
	flag.Parse()

	readFn := coldstart.StreamRead
	if directIO {
		readFn = coldstart.StreamReadDirect
	}

	levels, err := parseConcurrency(concurrency)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid -concurrency:", err)
		os.Exit(1)
	}

	maxN := 0
	for _, n := range levels {
		if n > maxN {
			maxN = n
		}
	}

	sizeBytes := int64(modelSizeGB * (1 << 30))

	fmt.Printf("preparing %d synthetic model files (%.2f GB each) in %s ...\n", maxN, modelSizeGB, dataDir)
	paths := make([]string, maxN)
	for i := 0; i < maxN; i++ {
		p := filepath.Join(dataDir, fmt.Sprintf("model-%d.bin", i))
		if err := coldstart.GenerateSyntheticModel(p, sizeBytes); err != nil {
			fmt.Fprintln(os.Stderr, "failed to generate synthetic model:", err)
			os.Exit(1)
		}
		paths[i] = p
	}
	if !keepFiles {
		defer func() {
			for _, p := range paths {
				_ = os.Remove(p)
			}
		}()
	}

	var baseline time.Duration
	results := make([]concurrencyResult, 0, len(levels))

	for _, n := range levels {
		var durations []time.Duration
		var makespans []time.Duration
		for iter := 0; iter < iterations; iter++ {
			res, err := coldstart.RunBurst(paths[:n], readFn)
			if err != nil {
				fmt.Fprintf(os.Stderr, "burst failed at N=%d: %v\n", n, err)
				os.Exit(1)
			}
			durations = append(durations, res.MedianStreamDuration())
			makespans = append(makespans, res.Makespan)
		}

		medianStream := medianOf(durations)
		medianMakespan := medianOf(makespans)

		if n == 1 {
			baseline = medianStream
		}

		effectiveThroughput := modelSizeGB / medianStream.Seconds()
		aggregateThroughput := (float64(n) * modelSizeGB) / medianMakespan.Seconds()

		results = append(results, concurrencyResult{
			N:                      n,
			MedianStreamDurationS:  medianStream.Seconds(),
			MakespanS:              medianMakespan.Seconds(),
			EffectiveThroughputGBs: effectiveThroughput,
			AggregateThroughputGBs: aggregateThroughput,
			DegradationFactor:      coldstart.DegradationFactor(baseline, medianStream),
		})

		latest := results[len(results)-1]
		fmt.Printf("N=%-3d median_stream=%.3fs makespan=%.3fs eff_throughput=%.2fGB/s\n",
			n, medianStream.Seconds(), medianMakespan.Seconds(), effectiveThroughput)
		fmt.Printf("      aggregate_throughput=%.2fGB/s f(N)=%.2f\n",
			aggregateThroughput, latest.DegradationFactor)
	}

	if outputPath != "" {
		f, err := os.Create(outputPath) //nolint:gosec // user-supplied output path is expected CLI usage
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to create output file:", err)
			os.Exit(1)
		}
		defer func() { _ = f.Close() }()
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(results); err != nil {
			fmt.Fprintln(os.Stderr, "failed to write output file:", err)
			os.Exit(1)
		}
		fmt.Println("results written to", outputPath)
	}
}

func parseConcurrency(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	levels := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("%q is not an integer: %w", p, err)
		}
		if n <= 0 {
			return nil, fmt.Errorf("%d must be positive", n)
		}
		levels = append(levels, n)
	}
	if len(levels) == 0 {
		return nil, fmt.Errorf("no concurrency levels provided")
	}
	return levels, nil
}

func medianOf(durations []time.Duration) time.Duration {
	return coldstart.Median(durations)
}

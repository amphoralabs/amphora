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

// Package coldstart implements the Phase 1 concurrent cold-start benchmark
// required by Technical Specification v3 §3.4.1: measure the storage-layer
// contention-degradation factor f(N) for N ∈ {1,4,8,16} concurrent cold
// starts, rather than publishing only a single-request best case.
//
// This harness measures the NVMe→memory sequential-read term of t_load(N)
// (mmap-based streaming, matching §3.4's storage architecture). It does
// NOT measure GPU/VRAM copy time or scheduling latency (t_scheduling),
// since those require real GPU hardware — see README.md for what a full
// reference-hardware run additionally requires.
package coldstart

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// GenerateSyntheticModel creates a file at path filled with sizeBytes of
// pseudo-random content, standing in for a Safetensors model shard of that
// size. Content doesn't need to be a real model — only the byte volume and
// sequential-read access pattern matter for this benchmark.
func GenerateSyntheticModel(path string, sizeBytes int64) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create parent dir: %w", err)
	}
	f, err := os.Create(path) //nolint:gosec // benchmark scratch file, path is caller-controlled
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer func() { _ = f.Close() }()

	const chunkSize = 4 << 20 // 4 MiB
	chunk := make([]byte, chunkSize)
	rng := rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec // benchmark data, not security-sensitive

	var written int64
	for written < sizeBytes {
		n := int64(chunkSize)
		if remaining := sizeBytes - written; remaining < n {
			n = remaining
		}
		if _, err := rng.Read(chunk[:n]); err != nil {
			return fmt.Errorf("fill chunk: %w", err)
		}
		if _, err := f.Write(chunk[:n]); err != nil {
			return fmt.Errorf("write chunk: %w", err)
		}
		written += n
	}
	return f.Sync()
}

// StreamRead sequentially reads path via mmap (matching the production
// mmap-based streaming design, §3.4), touching every page to force it to
// actually be paged in, and returns the wall-clock duration. Falls back to
// buffered sequential reads if mmap is unavailable (e.g. unsupported
// filesystem).
func StreamRead(path string) (time.Duration, error) {
	f, err := os.Open(path) //nolint:gosec // benchmark scratch file, path is caller-controlled
	if err != nil {
		return 0, fmt.Errorf("open file: %w", err)
	}
	defer func() { _ = f.Close() }()

	fi, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat file: %w", err)
	}
	size := fi.Size()
	if size == 0 {
		return 0, nil
	}

	start := time.Now()

	data, mmapErr := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if mmapErr != nil {
		return streamReadFallback(f, size, start)
	}
	defer func() { _ = unix.Munmap(data) }()
	_ = unix.Madvise(data, unix.MADV_SEQUENTIAL)

	// Touch one byte per 4KiB page to force every page to be read from
	// disk into the process's address space, without materializing the
	// whole buffer in Go-managed memory.
	const pageSize = 4096
	var sink byte
	for i := 0; i < len(data); i += pageSize {
		sink ^= data[i]
	}
	_ = sink

	return time.Since(start), nil
}

func streamReadFallback(f *os.File, size int64, start time.Time) (time.Duration, error) {
	const bufSize = 4 << 20 // 4 MiB
	buf := make([]byte, bufSize)
	var read int64
	for read < size {
		n, err := f.Read(buf)
		read += int64(n)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0, fmt.Errorf("fallback sequential read: %w", err)
		}
		if n == 0 {
			break
		}
	}
	return time.Since(start), nil
}

// StreamReadDirect sequentially reads path using O_DIRECT, bypassing the OS
// page cache. Without this, a benchmark run on a machine without root
// access to drop caches (e.g. `drop_caches`) measures memory-speed reads of
// a just-written file rather than genuine device I/O, which silently
// invalidates the published f(N) curve. Falls back to StreamRead if
// O_DIRECT isn't supported by the filesystem (e.g. tmpfs/overlay).
func StreamReadDirect(path string) (time.Duration, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECT, 0)
	if err != nil {
		return StreamRead(path)
	}
	defer func() { _ = unix.Close(fd) }()

	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return 0, fmt.Errorf("fstat: %w", err)
	}
	size := stat.Size
	if size == 0 {
		return 0, nil
	}

	const alignment = 4096
	const bufSize = 4 << 20 // 4 MiB; must stay a multiple of alignment for O_DIRECT
	buf := alignedBuffer(bufSize, alignment)

	start := time.Now()
	var read int64
	for read < size {
		n, err := unix.Read(fd, buf)
		if err != nil {
			return 0, fmt.Errorf("O_DIRECT read: %w", err)
		}
		if n == 0 {
			break
		}
		read += int64(n)
	}
	return time.Since(start), nil
}

// alignedBuffer returns a byte slice of size bytes whose backing address is
// aligned to align bytes, as required by O_DIRECT I/O buffers.
func alignedBuffer(size, align int) []byte {
	buf := make([]byte, size+align)
	addr := uintptr(unsafe.Pointer(&buf[0]))
	offset := 0
	if rem := addr % uintptr(align); rem != 0 {
		offset = align - int(rem)
	}
	return buf[offset : offset+size]
}

// ReadFunc streams a file and reports how long the read took, standing in
// for one cold-start's storage-layer streaming time.
type ReadFunc func(path string) (time.Duration, error)

// BurstResult holds the outcome of one N-way concurrent cold-start burst.
type BurstResult struct {
	// N is the number of concurrent streams in this burst.
	N int
	// StreamDurations holds the wall-clock duration of each individual
	// stream's sequential read.
	StreamDurations []time.Duration
	// Makespan is the wall-clock time from burst start until the last
	// stream finished — the time a "burst" of N simultaneous cold starts
	// actually takes end-to-end.
	Makespan time.Duration
}

// MedianStreamDuration returns the median of StreamDurations.
func (r BurstResult) MedianStreamDuration() time.Duration {
	return median(r.StreamDurations)
}

// RunBurst concurrently streams every path in paths using read, and
// returns per-stream durations plus the overall makespan, simulating N
// models/tenants waking simultaneously (§3.4.1).
func RunBurst(paths []string, read ReadFunc) (BurstResult, error) {
	n := len(paths)
	durations := make([]time.Duration, n)
	errs := make([]error, n)

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(n)
	for i, path := range paths {
		go func(i int, path string) {
			defer wg.Done()
			d, err := read(path)
			durations[i] = d
			errs[i] = err
		}(i, path)
	}
	wg.Wait()
	makespan := time.Since(start)

	for _, err := range errs {
		if err != nil {
			return BurstResult{}, err
		}
	}

	return BurstResult{N: n, StreamDurations: durations, Makespan: makespan}, nil
}

// DegradationFactor computes f(N) as defined in §3.4.1:
//
//	t_load(N) ≈ modelSize / (throughput / f(N)) + t_scheduling
//
// Since throughput = modelSize / duration, f(N) reduces to the ratio of
// the median per-stream duration under N-way concurrency to the median
// single-request (N=1) duration — how much slower a single stream gets
// when N-1 others are contending for the same NVMe/PCIe path.
func DegradationFactor(baselineDuration, atNDuration time.Duration) float64 {
	if baselineDuration <= 0 {
		return 0
	}
	return float64(atNDuration) / float64(baselineDuration)
}

// Median returns the median duration in d (unsorted input is not
// mutated).
func Median(d []time.Duration) time.Duration {
	return median(d)
}

func median(d []time.Duration) time.Duration {
	if len(d) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(d))
	copy(sorted, d)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}

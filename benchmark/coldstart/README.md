# Cold-start concurrency benchmark

Implements the Phase 1 benchmark required by Technical Specification v3
§3.4.1: measure the concurrent-load contention-degradation factor `f(N)`
for N ∈ {1, 4, 8, 16} simultaneous cold starts, rather than publishing only
a single-request best case.

```
t_load(N) ≈ modelSize / (throughput / f(N)) + t_scheduling
```

`f(N)` is not assumed to be linear — NVMe controllers and PCIe switches
degrade non-linearly past their queue-depth saturation point, which is
exactly what this harness measures.

## What this harness measures

- **The storage-layer term only**: sequential-read throughput of N
  same-sized synthetic "model shard" files, read concurrently from local
  storage, standing in for N models/tenants cold-starting simultaneously
  and contending for the same NVMe queue depth / PCIe lanes (§3.4).
- `f(N)` is computed as `median(stream duration at N) / median(stream
  duration at N=1)` — the ratio the spec's formula reduces to, since
  `throughput = modelSize / duration`.

## What this harness does **not** measure

- **GPU/VRAM copy time** or **t_scheduling** (pod hijack, nvidia-runtime
  injection) — those require real GPU hardware and the Controller's
  reconcile/hijack logic, neither of which exist yet (see repo
  `STATUS.md`). The published `f(N)` here is a lower bound on the full
  `t_load(N)` curve, not the complete number.
- Real Safetensors model files — synthetic pseudo-random-content files of
  the same byte size are used, since only sequential-read volume and
  pattern matter for this benchmark, not model semantics.

## Running it

```
go run ./cmd/coldstart-bench \
  -model-size-gb 4 \
  -concurrency 1,4,8,16 \
  -iterations 3 \
  -o-direct \
  -output results.json
```

Flags (`go run ./cmd/coldstart-bench -h` for the full list):

- `-model-size-gb` — synthetic shard size per stream (default `4`,
  approximating a quantized 7B model).
- `-concurrency` — comma-separated N values to benchmark.
- `-iterations` — bursts per N (median reported, to smooth out noise).
- `-o-direct` — **use this whenever you don't have root.** Reads bypass
  the OS page cache via `O_DIRECT` instead of standard `mmap`. Without
  it, a freshly-written file is served from page cache at memory speed
  (tens of TB/s), which silently invalidates the whole benchmark — it
  measures RAM, not the disk. `O_DIRECT` falls back to the cached path if
  the filesystem doesn't support it (e.g. tmpfs/overlay).
- `-keep-files` — keep the synthetic shard files after the run.

If you have root, an equivalent (and for some filesystems more
representative) alternative to `-o-direct` is to drop caches between runs
(`sync && echo 3 | sudo tee /proc/sys/vm/drop_caches`) and omit
`-o-direct` to use the production mmap-based read path instead.

## ⚠️ Sandbox validation run — not the Phase 1 reference-hardware publication

[`sample-results.sandbox.json`](./sample-results.sandbox.json) is a real
(not fabricated) run of this harness against this development sandbox's
local NVMe-backed volume (`-model-size-gb 1 -concurrency 1,4,8,16
-iterations 3 -o-direct`):

| N | median stream (s) | eff. per-stream throughput (GB/s) | aggregate throughput (GB/s) | f(N) |
|---|---|---|---|---|
| 1  | 0.305 | 3.28 | 3.28 | 1.00 |
| 4  | 0.997 | 1.00 | 3.83 | 3.27 |
| 8  | 1.643 | 0.61 | 4.36 | 5.39 |
| 16 | 4.047 | 0.25 | 3.90 | 13.27 |

This confirms the harness correctly captures non-linear degradation (the
spec's core claim: `f(N)` is not flat) and is genuinely disk-bound (`f(N)`
grows well past 1.0, and aggregate throughput plateaus near the device's
apparent sequential-read ceiling rather than scaling with N — the expected
signature of queue-depth saturation).

**This is not the Phase 1 publishable number.** It ran on a shared
development sandbox (single NVMe-backed volume behind LUKS encryption, no
GPU, unknown/non-dedicated I/O contention from other tenants of the same
host) — not the dedicated reference hardware §9 requires for a
third-party-reproducible publication, and it excludes the GPU/VRAM and
`t_scheduling` terms entirely. Publishing the official Phase 1 `f(N)`
curve requires running this same harness (plus the GPU-side terms, once
the Controller's hijack/wakeup path exists) on real reference GPU+NVMe
hardware, with the corresponding Terraform/Helm for that cluster committed
alongside it per §9 — that provisioning work needs a chosen cloud
provider/credentials and is tracked as a follow-up, not done here.

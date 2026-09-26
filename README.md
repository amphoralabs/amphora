# Amphora

**Private GPU Fleet & Inference Economics Platform**

Amphora is an open-core Kubernetes operator + proxy + serving fabric that lets enterprises run
private LLM fleets with fast, bandwidth-bound cold starts, tenancy-certified multi-tenant GPU
packing (MIG/time-slicing), live cost & utilization observability, and an enforced (not just
logged) EU AI Act-grade audit trail.

> Status: **early development.** Proxy PoC, Packing Scheduler, and initial reconcile-loop
> placement logic have landed; pause-pod hijack, the admission webhook, and the storage/streaming
> layer are still ahead — see the roadmap below.

## Why

Enterprises running LLMs on private infrastructure face three compounding problems: idle GPU cost
from naive scale-to-zero, poor utilization from one-model-per-GPU deployment, and no
audited control plane that isn't a US-hosted multi-tenant SaaS. See the full Technical
Specification for the detailed problem statement, architecture, threat model, and
tenancy/isolation guarantees.

## Architecture (summary)

Four core components: **Proxy** (cost-aware ingress/queueing), **Controller** (KubeBuilder
operator, `ModelDeployment` CRD, admission webhook), **Packing Scheduler** (MIG/time-slice-aware
bin-packing with tenancy-class enforcement), and the **Storage/Streaming layer** (NVMe→VRAM
`mmap` streaming). A **Cost & Audit Plane** cuts across all four.

## Roadmap

- **Phase 1**: Operator/CRD skeleton, proxy PoC, concurrent cold-start benchmarking
- **Phase 2**: Multi-tenant packing scheduler + admission webhook + mTLS
- **Phase 3**: Audit trail (WORM), eval/regression gate, multi-cluster orchestrator
- **Phase 4**: Cost-aware routing, predictive pre-warming, security review, GA hardening

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Security issues: see [SECURITY.md](SECURITY.md), do not
open a public issue.

## License

[Apache-2.0](LICENSE)

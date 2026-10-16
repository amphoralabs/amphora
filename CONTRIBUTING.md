# Contributing to Amphora

Thanks for your interest in contributing. Amphora is early-stage (pre-`v1alpha1`); expect rapid
iteration and breaking changes until the CRD versioning policy in the Technical Specification
kicks in at `v1beta1`.

## Getting Started

1. Fork and clone the repo.
2. Toolchain requirements (once the operator scaffold lands): Go (matching `go.mod`), `operator-sdk`
   or `kubebuilder`, Docker, and access to a Kubernetes cluster with an NVIDIA GPU node for
   integration testing (a kind/minikube cluster is sufficient for non-GPU-path unit tests).
3. Run `make test` and `make lint` before opening a PR (targets land with the initial operator
   scaffold — see the project board for status).

## Pull Requests

- One logical change per PR. Keep unrelated refactors out.
- All PRs require passing CI (lint, unit tests, build) before review.
- Reference the relevant section of the Technical Specification in the PR description when the
  change implements or deviates from a documented behavior (e.g. "implements §3.2.1 Eval Gate").
- Changes to tenancy/isolation enforcement (§4 of the spec), the admission webhook, or RBAC scope
  require a second reviewer familiar with the threat model (§5).

## Commit Messages

Use a short imperative summary line, e.g. `controller: add tenancyClass admission validation`.

## Reporting Bugs / Requesting Features

Use GitHub Issues. For anything security-sensitive, follow [SECURITY.md](SECURITY.md) instead —
do not open a public issue.

## Code of Conduct

Be respectful and constructive. Maintainers may close issues/PRs that are abusive or off-topic.

## Cutting a release (maintainers)

1. Make sure `main` is green, then check the pipeline without publishing: **Actions -> Release ->
   Run workflow**. A manual run only builds the three images and their SBOMs; it never pushes, signs,
   or touches a release.
2. Push a `vX.Y.Z` tag (a tag with a hyphen, such as `v0.1.0-alpha.1`, is published as a
   pre-release). The workflow pushes `amphora`, `amphora-proxy` and `amphora-podtls` to ghcr.io,
   signs each keylessly, **verifies its own signature with the command the README documents**, and
   attaches one CycloneDX SBOM per image to the GitHub release.
3. **First release only, and by hand:** GHCR creates every new package *private*, and there is no API
   to change that. An organization owner must open each package (Packages -> the package -> Package
   settings -> Change visibility) and make it public. Then confirm an anonymous pull works, with no
   credentials in your Docker config: `docker manifest inspect ghcr.io/amphoralabs/amphora:<tag>`.
4. Bump the pinned submodule commit in the private `amphora-internal` repo.


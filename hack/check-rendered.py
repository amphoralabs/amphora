#!/usr/bin/env python3
"""Fail if rendered manifests contain references that kustomize silently failed to follow.

kustomize renames resources with a name prefix and rewrites references it knows about. For
references it does not know (a cert-manager issuerRef) or cannot match (a ServiceAccount
subject whose namespace differs from the account's), it leaves the stale name in place.
The result builds fine and then fails at runtime: a Certificate waits forever for a missing
issuer, a ServiceAccount has no permissions. Reads rendered YAML on stdin.
"""
import sys

import yaml

docs = [d for d in yaml.safe_load_all(sys.stdin) if d]
problems = []

issuers = {(d["kind"], d["metadata"]["name"]) for d in docs if d["kind"] in ("Issuer", "ClusterIssuer")}
for d in docs:
    if d["kind"] == "Certificate":
        ref = d["spec"]["issuerRef"]
        kind = ref.get("kind", "Issuer")
        if (kind, ref["name"]) not in issuers:
            problems.append(f'Certificate {d["metadata"]["name"]} references missing {kind} {ref["name"]}')

accounts = {(d["metadata"]["name"], d["metadata"].get("namespace")) for d in docs if d["kind"] == "ServiceAccount"}
for d in docs:
    if d["kind"] not in ("ClusterRoleBinding", "RoleBinding"):
        continue
    for s in d.get("subjects", []):
        if s.get("kind") == "ServiceAccount" and (s["name"], s.get("namespace")) not in accounts:
            problems.append(
                f'{d["kind"]} {d["metadata"]["name"]} binds ServiceAccount {s.get("namespace")}/{s["name"]}'
                " which is not in the render (renamed or in another namespace?)"
            )

if problems:
    print("Rendered manifests have dangling references:", *problems, sep="\n  ")
    sys.exit(1)

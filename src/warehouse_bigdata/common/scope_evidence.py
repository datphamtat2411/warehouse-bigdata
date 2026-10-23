"""Immutable scope sidecar I/O and manifest binding; no schema relaxation."""

import json

from warehouse_bigdata.contracts.dataset_scope import (
    ScopeError,
    canonical_bytes,
    require_scope_execution,
    validate_scope,
)

SCOPE_FILE = "_scope.json"
# Includes bounded occupied-stratum evidence and nested immutable input bindings.
# The limit is not a preallocation; ordinary small sidecars remain only kilobytes.
SCOPED_METADATA_LIMIT = 64 * 1024 * 1024


def read_scope(client, root, **expected):
    path = root + "/" + SCOPE_FILE
    # Production clients expose exact-path NameNode status, which distinguishes
    # absence from permission/network errors without spawning another HDFS JVM.
    status = getattr(client, "path_status", None)
    exists = status(path) is not None if status is not None else client.exists(path)
    if not exists:
        raise ScopeError("required scope evidence is missing: " + path)
    try:
        document = json.loads(client.read_remote(path))
    except (ValueError, OSError) as error:
        raise ScopeError("invalid scope evidence: " + path) from error
    return validate_scope(document, **expected)


def write_scope(client, root, scope):
    validate_scope(scope)
    path, payload = root + "/" + SCOPE_FILE, canonical_bytes(scope)
    if client.exists(path):
        if client.read_remote(path) != payload:
            raise ScopeError("refusing to overwrite immutable scope evidence: " + path)
    else:
        client.upload_bytes(payload, path)
    if client.read_remote(path) != payload:
        raise ScopeError("independent scope readback differs: " + path)


def bind_scope(manifest, scope):
    """Keep new fields inside Contract v1's existing JSON coverage field."""
    validate_scope(scope)
    coverage = json.loads(manifest["coverage_and_exclusion_counts"])
    coverage.update(
        dataset_scope=scope["dataset_scope"], selection_digest=scope["selection_digest"]
    )
    return {
        **manifest,
        "coverage_and_exclusion_counts": json.dumps(
            coverage, sort_keys=True, separators=(",", ":")
        ),
    }


def validate_scope_binding(manifest, scope):
    validate_scope(scope, source_snapshot_id=manifest.get("source_snapshot_id"))
    coverage = json.loads(manifest["coverage_and_exclusion_counts"])
    if any(coverage.get(k) != scope[k] for k in ("dataset_scope", "selection_digest")):
        raise ScopeError("manifest scope/selection identity mismatch")


def authorize_upstream(scope, *, dataset_scope=None, confirm_large_workload=False):
    validate_scope(scope)
    if dataset_scope is not None and scope["dataset_scope"] != dataset_scope:
        raise ScopeError("requested scope differs from validated upstream scope")
    if scope["dataset_scope"] == "full_m5" and dataset_scope != "full_m5":
        raise ScopeError("full_m5 requires explicit --dataset-scope full_m5")
    require_scope_execution(scope["dataset_scope"], confirm_large_workload=confirm_large_workload)
    return scope

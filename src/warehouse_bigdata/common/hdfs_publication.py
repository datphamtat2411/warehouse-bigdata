"""Batch owner-side publication of validated J02/J03 candidates and Contract v1 manifests."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import PurePosixPath

from warehouse_bigdata.common.config import HDFSConfig, load_hdfs_config
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.common.hdfs_namespace import (
    HDFS_OWNER,
    HdfsNamespaceError,
    require_owner,
    validate_publication_namespace,
)
from warehouse_bigdata.common.hdfs_paths import (
    render_curated_category_monthly_sales_path,
    render_curated_sales_daily_path,
    render_curated_sales_enriched_path,
    render_curated_store_daily_sales_path,
    render_enrichment_candidate_path,
    render_normalization_candidate_path,
    render_run_manifest_path,
    render_staging_path,
)
from warehouse_bigdata.contracts.manifests import PIPELINE_RUN_MANIFEST, VALIDATED
from warehouse_bigdata.contracts.validation import validate_manifest

_OUTPUT_RENDERERS = {
    "sales_daily": render_curated_sales_daily_path,
    "sales_enriched": render_curated_sales_enriched_path,
    "store_daily_sales": render_curated_store_daily_sales_path,
    "category_monthly_sales": render_curated_category_monthly_sales_path,
}


def _json(client, path: str) -> dict:
    document = json.loads(client.read_remote(path))
    if not isinstance(document, dict):
        raise HdfsNamespaceError(f"publication metadata must be an object: {path}")
    return document


def _bytes(document: dict) -> bytes:
    return (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _request_path(config: HDFSConfig, run_id: str) -> str:
    return render_staging_path(config.base_path, run_id) + "/publication/request.json"


def _outputs(config, manifest):
    run_id = manifest["pipeline_run_id"]
    snapshot = manifest["source_snapshot_id"]
    staging = render_staging_path(config.base_path, run_id)
    validate_manifest(
        PIPELINE_RUN_MANIFEST, manifest, lineage_context={"required_status": VALIDATED}
    )
    names = json.loads(manifest["output_dataset_names"])
    if names not in (
        ["sales_daily"],
        ["sales_enriched", "store_daily_sales", "category_monthly_sales"],
    ):
        raise HdfsNamespaceError(
            "publication request must contain exactly the J02 or J03 output set"
        )
    outputs = []
    for name in names:
        final = _OUTPUT_RENDERERS[name](config.base_path, snapshot)
        candidate = (
            render_normalization_candidate_path(staging)
            if name == "sales_daily"
            else render_enrichment_candidate_path(staging, name)
        )
        sidecar = "_snapshot_manifest.json" if name == "sales_daily" else "_dataset_manifest.json"
        outputs.append((candidate, final, sidecar))
    if json.loads(manifest["output_dataset_uris"]) != [
        config.uri + final for _, final, _ in outputs
    ]:
        raise HdfsNamespaceError(
            "publication output URIs differ from their exact Contract v1 paths"
        )
    if manifest["artifact_uri"] != config.uri + render_run_manifest_path(config.base_path, run_id):
        raise HdfsNamespaceError("publication manifest URI differs from its exact Contract v1 path")
    if manifest["quality_report_uri"] != config.uri + staging + "/quality_report.json":
        raise HdfsNamespaceError("publication quality report must belong to the same staging run")
    return outputs


def _same_manifest(first, second):
    return all(
        first.get(field) == second.get(field)
        for field in (
            "source_version",
            "source_snapshot_id",
            "pipeline_run_id",
            "status",
            "producer",
            "row_count",
            "schema_fingerprint",
            "configuration_digest",
            "code_revision",
            "output_dataset_names",
            "output_dataset_uris",
            "quality_report_uri",
            "artifact_uri",
        )
    )


def stage_publication(client, config, manifest, *, result_status: str) -> str:
    """Write a request in this run's staging area; this is not a published completion marker."""
    _outputs(config, manifest)
    if result_status not in {"VALIDATED", "REUSED"}:
        raise HdfsNamespaceError("only validated candidates may request owner publication")
    path = _request_path(config, manifest["pipeline_run_id"])
    parent = str(PurePosixPath(path).parent)
    if not client.exists(parent):
        client.mkdir_exclusive(parent)
    if client.exists(path):
        if not _same_manifest(_json(client, path)["manifest"], manifest):
            raise HdfsNamespaceError(f"publication request conflicts with retry: {path}")
    else:
        client.upload_bytes(_bytes({"version": 1, "manifest": dict(manifest)}), path)
    return path


def _envelope(final, run_id):
    token = hashlib.sha256((run_id + "\0" + final).encode()).hexdigest()[:32]
    return str(PurePosixPath(final).parent / (".pending-publication-" + token))


def _require_private(client, path):
    metadata = client.path_status(path)
    if metadata is None:
        client.mkdir_exclusive(path)
        client.clear_acl(path)
        client.set_permission(path, "700")
    elif (metadata["type"], metadata["owner"], metadata["permission"]) != (
        "DIRECTORY",
        HDFS_OWNER,
        "700",
    ):
        raise HdfsNamespaceError(f"publication envelope must be private and hadoop-owned: {path}")


def _seal(client, path):
    client.clear_acl(path, recursive=True)
    client.set_owner(path, HDFS_OWNER, recursive=True)
    # Hadoop's X retains traversal on directories without making data files executable.
    client.set_permission(path, "a=rX", recursive=True)


def _validate_sidecar(client, path, final, sidecar, manifest, config):
    document = _json(client, path + "/" + sidecar)
    expected = {
        field: manifest[field]
        for field in ("source_version", "source_snapshot_id", "pipeline_run_id", "status")
    }
    expected["artifact_uri"] = config.uri + final
    if any(document.get(field) != value for field, value in expected.items()):
        raise HdfsNamespaceError(f"dataset publication lineage conflicts with request: {path}")


def finalize_publication(client, config, *, pipeline_run_id: str) -> str:
    """Claim into private owner envelopes, seal, rename2(NONE), then publish the manifest last.

    Spark has no W on the envelopes' parents. This protects the claimed candidate even
    while its inherited staging ACLs and Spark ownership are being removed. Published
    snapshots are never chmodded, chowned, removed, or used as rename containers.
    """
    require_owner(client)
    validate_publication_namespace(client, config)
    request = _json(client, _request_path(config, pipeline_run_id))
    manifest = request["manifest"]
    if request.get("version") != 1 or manifest.get("pipeline_run_id") != pipeline_run_id:
        raise HdfsNamespaceError("publication request identity/version mismatch")
    outputs = _outputs(config, manifest)
    manifest_path = render_run_manifest_path(config.base_path, pipeline_run_id)
    completed = client.exists(manifest_path)
    if completed and not _same_manifest(_json(client, manifest_path), manifest):
        raise HdfsNamespaceError("existing immutable run manifest conflicts with request")
    # Validate all known conflicts before changing any candidate or published path.
    for candidate, final, sidecar in outputs:
        pending = _envelope(final, pipeline_run_id) + "/candidate"
        if client.exists(final):
            if client.exists(candidate) or client.exists(pending):
                raise HdfsNamespaceError(
                    f"candidate and existing immutable snapshot both exist: {final}"
                )
            _validate_sidecar(client, final, final, sidecar, manifest, config)
            status = client.path_status(final)
            if status["owner"] != HDFS_OWNER or int(status["permission"], 8) & 0o022:
                raise HdfsNamespaceError(f"existing immutable snapshot is not sealed: {final}")
        else:
            if completed:
                raise HdfsNamespaceError(f"completed run is missing an immutable output: {final}")
            source = pending if client.exists(pending) else candidate
            if not client.is_directory(source):
                raise HdfsNamespaceError(f"validated publication candidate is missing: {candidate}")
            _validate_sidecar(client, source, final, sidecar, manifest, config)
    if completed:
        return "REUSED"
    staging = render_staging_path(config.base_path, pipeline_run_id)
    if _json(client, staging + "/quality_report.json").get("status") != VALIDATED:
        raise HdfsNamespaceError("publication quality report is not VALIDATED")
    for candidate, final, sidecar in outputs:
        if client.exists(final):
            continue
        envelope = _envelope(final, pipeline_run_id)
        _require_private(client, envelope)
        claimed = envelope + "/candidate"
        if not client.exists(claimed):
            client.rename_exclusive(candidate, claimed)
        _seal(client, claimed)
        _validate_sidecar(client, claimed, final, sidecar, manifest, config)
        client.rename_exclusive(claimed, final)
        client.remove(envelope)
    envelope = _envelope(manifest_path, pipeline_run_id)
    _require_private(client, envelope)
    candidate = envelope + "/manifest.json"
    if client.exists(candidate):
        if not _same_manifest(_json(client, candidate), manifest):
            raise HdfsNamespaceError("pending run manifest conflicts with retry")
    else:
        client.upload_bytes(_bytes(manifest), candidate)
    if _json(client, candidate) != manifest:
        raise HdfsNamespaceError("run manifest independent reread mismatch")
    _seal(client, candidate)
    client.rename_exclusive(candidate, manifest_path)
    client.remove(envelope)
    return "VALIDATED"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--recover", action="store_true", help="Finish pending publication before a driver retry"
    )
    parser.add_argument("--pipeline-run-id", required=True)
    args = parser.parse_args()
    config = load_hdfs_config()
    client = HdfsCliClient()
    require_owner(client)
    if args.recover and not client.exists(_request_path(config, args.pipeline_run_id)):
        return
    status = finalize_publication(client, config, pipeline_run_id=args.pipeline_run_id)
    if not args.recover:
        print(f"status={status}")
        print(
            f"run_manifest_path={render_run_manifest_path(config.base_path, args.pipeline_run_id)}"
        )


if __name__ == "__main__":
    main()

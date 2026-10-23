"""Immutable J06 forecast bundles; v1 completion manifest excludes execution IDs.

The artifact digest is canonical JSON {files: sorted relative path, size and HDFS
checksum entries} plus newline. scoring_manifest.json and evidence/publication.json
are excluded to avoid self-reference; the frozen request binds both independently.
"""

import json
from pathlib import PurePosixPath

from warehouse_bigdata.common.hdfs_namespace import HdfsNamespaceError, require_owner, validate_publication_namespace
from warehouse_bigdata.common.hdfs_paths import render_forecast_output_path, render_staging_path, validate_pipeline_run_id
from warehouse_bigdata.common.hdfs_publication import (
    _bytes, _envelope, _freeze_feature_metadata, _json, _require_private, _seal, document_digest,
)
from warehouse_bigdata.contracts.datasets import FORECAST_DAILY
from warehouse_bigdata.contracts.manifests import FORECAST_MANIFEST, VALIDATED
from warehouse_bigdata.contracts.validation import validate_manifest
from warehouse_bigdata.forecasting.feature_input import require_sealed

DATASETS = ("forecast_daily", "cohort_selection", "coverage", "coverage_exceptions")
EXCLUDED = {"scoring_manifest.json", "evidence/publication.json"}


def forecast_file_integrity(client, root):
    pending, seen, inventory = [root], set(), []
    while pending:
        parent = pending.pop()
        for status in client.list_status(parent):
            path = status["path"]
            if (str(PurePosixPath(path).parent) != parent or str(PurePosixPath(path)) != path or
                path in seen or PurePosixPath(path).name in {".", ".."}):
                raise HdfsNamespaceError("forecast inventory duplicate or non-child path")
            seen.add(path)
            if len(seen) > 4096:
                raise HdfsNamespaceError("forecast bundle exceeds bounded layout")
            relative = str(PurePosixPath(path).relative_to(root))
            parts = PurePosixPath(relative).parts
            directory = status["type"] == "DIRECTORY"
            allowed = ((relative in (*DATASETS, "evidence") and directory) or
                       (relative in EXCLUDED | {"evidence/scoring.json"} and not directory) or
                       (len(parts) == 2 and parts[0] in DATASETS and not directory and
                        (parts[1] == "_SUCCESS" or (parts[1].startswith("part-") and parts[1].endswith(".parquet")))))
            if status["type"] not in {"DIRECTORY", "FILE"} or not allowed:
                raise HdfsNamespaceError("unexpected forecast bundle layout: " + relative)
            if directory:
                pending.append(path)
            elif relative not in EXCLUDED:
                if type(status["length"]) is not int or status["length"] < 0:
                    raise HdfsNamespaceError("invalid forecast inventory file length")
                inventory.append({"path": relative, "size_bytes": status["length"]})
    names = {item["path"] for item in inventory}
    if "evidence/scoring.json" not in names or any(
        not any(p.startswith(name + "/part-") and p.endswith(".parquet") for p in names) for name in DATASETS):
        raise HdfsNamespaceError("incomplete forecast bundle layout")
    inventory.sort(key=lambda item: item["path"])
    paths = [root + "/" + item["path"] for item in inventory]
    checksums = client.checksums(paths)
    if set(checksums) != set(paths) or any(not v for v in checksums.values()):
        raise HdfsNamespaceError("forecast integrity checksums incomplete")
    for item, path in zip(inventory, paths, strict=True):
        item["checksum"] = checksums[path]
    return inventory


def make_forecast_manifest(config, *, binding, quality, created_at, completed_at):
    final = render_forecast_output_path(config.base_path, binding["forecast_run_id"])
    checksum = document_digest({"files": quality["file_integrity"]})
    coverage = {k: quality[k] for k in ("candidate_series", "successful_series", "excluded_series")}
    coverage.update(input_binding_digest=document_digest(binding), publication_evidence_digest=document_digest(quality),
        publication_complete=True, inventory_digest_definition="sha256 of canonical JSON {files: sorted relative paths, sizes, HDFS checksums} plus newline; excludes scoring_manifest.json and evidence/publication.json")
    manifest = dict(manifest_schema_version=FORECAST_MANIFEST.version, contract_name=FORECAST_MANIFEST.name,
        contract_version=FORECAST_MANIFEST.version, manifest_kind=FORECAST_MANIFEST.manifest_kind,
        status=VALIDATED, producer="warehouse-bigdata:forecast-daily", artifact_uri=config.uri + final,
        artifact_checksum=checksum, schema_fingerprint=quality["schema_fingerprint"], grain=FORECAST_DAILY.grain,
        logical_unique_key=",".join(FORECAST_DAILY.unique_key), row_count=quality["row_count"],
        required_field_null_counts=json.dumps({n: 0 for n in FORECAST_DAILY.required_field_names}, sort_keys=True),
        code_revision=binding["code_revision"], configuration_digest=binding["configuration_digest"],
        created_at=created_at, completed_at=completed_at, coverage_and_exclusion_counts=json.dumps(coverage, sort_keys=True),
        **{k: binding[k] for k in ("forecast_run_id", "model_run_id", "source_snapshot_id", "feature_version", "issue_date", "run_type")},
        horizon_days=7, forecast_artifact_uri=config.uri + final + "/forecast_daily", forecast_artifact_checksum=checksum)
    validate_forecast_request(config, manifest, pipeline_run_id=binding["pipeline_run_id"])
    return manifest


def validate_forecast_request(config, manifest, *, pipeline_run_id):
    validate_pipeline_run_id(pipeline_run_id)
    validate_manifest(FORECAST_MANIFEST, manifest, lineage_context={"required_status": VALIDATED})
    final = render_forecast_output_path(config.base_path, manifest["forecast_run_id"])
    if (manifest["artifact_uri"] != config.uri + final or
        manifest["forecast_artifact_uri"] != config.uri + final + "/forecast_daily" or
        manifest["artifact_checksum"] != manifest["forecast_artifact_checksum"]):
        raise HdfsNamespaceError("forecast request paths or checksums conflict")
    return render_staging_path(config.base_path, pipeline_run_id) + "/candidates/forecast", final


def validate_forecast_evidence(client, config, manifest, path, *, pipeline_run_id):
    validate_forecast_request(config, manifest, pipeline_run_id=pipeline_run_id)
    staging = render_staging_path(config.base_path, pipeline_run_id)
    binding, quality = _json(client, staging + "/input_binding.json"), _json(client, staging + "/quality_report.json")
    coverage = json.loads(manifest["coverage_and_exclusion_counts"])
    if (_json(client, path + "/scoring_manifest.json") != manifest or
        _json(client, path + "/evidence/publication.json") != quality or
        _json(client, path + "/evidence/scoring.json") != binding or
        document_digest(binding) != coverage.get("input_binding_digest") or
        document_digest(quality) != coverage.get("publication_evidence_digest")):
        raise HdfsNamespaceError("frozen forecast evidence conflicts with request")
    if (binding.get("pipeline_run_id") != pipeline_run_id or
        binding.get("schema_version") != "forecast-input-binding-v1" or
        quality.get("input_binding") != binding or quality.get("status") != VALIDATED or
        quality.get("persisted_parquet_validated") is not True or coverage.get("publication_complete") is not True):
        raise HdfsNamespaceError("forecast persisted evidence is not VALIDATED")
    for key in ("forecast_run_id", "model_run_id", "source_snapshot_id", "feature_version", "issue_date", "run_type", "code_revision", "configuration_digest"):
        if binding.get(key) != manifest.get(key):
            raise HdfsNamespaceError("forecast frozen lineage conflicts: " + key)
    if document_digest(binding.get("effective_configuration")) != binding["configuration_digest"]:
        raise HdfsNamespaceError("forecast configuration digest conflicts")
    for key in ("candidate_series", "successful_series", "excluded_series"):
        if type(quality.get(key)) is not int or quality[key] < 0 or coverage.get(key) != quality[key]:
            raise HdfsNamespaceError("forecast coverage evidence conflicts")
    if (quality["candidate_series"] != 1000 or
        quality["successful_series"] + quality["excluded_series"] != 1000 or
        quality.get("row_count") != manifest["row_count"] or manifest["row_count"] != 7 * quality["successful_series"] or
        quality.get("schema_fingerprint") != manifest["schema_fingerprint"]):
        raise HdfsNamespaceError("forecast complete-series count/schema evidence conflicts")
    inventory = forecast_file_integrity(client, path)
    if inventory != quality.get("file_integrity") or document_digest({"files": inventory}) != manifest["artifact_checksum"]:
        raise HdfsNamespaceError("forecast file integrity conflicts with persisted validation")


def stage_forecast_publication(client, config, manifest, *, pipeline_run_id):
    validate_forecast_request(config, manifest, pipeline_run_id=pipeline_run_id)
    staging = render_staging_path(config.base_path, pipeline_run_id)
    parent = staging + "/publication"
    request = {"version": 1, "pipeline_run_id": pipeline_run_id, "manifest": manifest}
    if not client.exists(parent):
        client.mkdir_exclusive(parent)
    path = parent + "/request.json"
    if client.exists(path):
        if _json(client, path) != request:
            raise HdfsNamespaceError("forecast immutable publication request conflicts")
    else:
        client.upload_bytes(_bytes(request), path)
    return path


def finalize_forecast_publication(client, config, *, pipeline_run_id, manifest):
    require_owner(client)
    validate_publication_namespace(client, config)
    candidate, final = validate_forecast_request(config, manifest, pipeline_run_id=pipeline_run_id)
    envelope = _envelope(final, pipeline_run_id)
    claimed = envelope + "/candidate"
    exists = lambda p: client.path_status(p) is not None
    if exists(final):
        if exists(candidate) or exists(claimed):
            raise HdfsNamespaceError("candidate and immutable forecast both exist")
        validate_forecast_evidence(client, config, manifest, final, pipeline_run_id=pipeline_run_id)
        for relative in (*EXCLUDED, *(i["path"] for i in forecast_file_integrity(client, final))):
            require_sealed(client, final + "/" + relative, config.base_path)
        return "REUSED"
    if exists(candidate) and exists(claimed):
        raise HdfsNamespaceError("concurrent forecast candidate and claim")
    source = claimed if exists(claimed) else candidate
    if (client.path_status(source) or {}).get("type") != "DIRECTORY":
        raise HdfsNamespaceError("complete forecast candidate is missing")
    validate_forecast_evidence(client, config, manifest, source, pipeline_run_id=pipeline_run_id)
    staging = render_staging_path(config.base_path, pipeline_run_id)
    _freeze_feature_metadata(client, staging)
    _require_private(client, envelope)
    if not exists(claimed):
        client.rename_exclusive(candidate, claimed)
    _seal(client, claimed)
    validate_forecast_evidence(client, config, manifest, claimed, pipeline_run_id=pipeline_run_id)
    _seal(client, staging)
    client.rename_exclusive(claimed, final)
    client.remove(envelope)
    return "VALIDATED"


def validate_forecast_recovery(args, binding):
    for key in ("source_version", "source_snapshot_id", "j03_pipeline_run_id", "j04_pipeline_run_id", "feature_version",
                "model_run_id", "forecast_run_id", "issue_date", "run_type"):
        if getattr(args, key) is None or getattr(args, key) != binding.get(key):
            raise HdfsNamespaceError("J06 recovery input conflicts with frozen binding: " + key)
    effective = binding.get("effective_configuration", {})
    for key in ("hdfs_uri", "spark_master", "seed", "event_log_dir"):
        if getattr(args, key) != effective.get(key):
            raise HdfsNamespaceError("J06 recovery configuration conflicts with frozen binding: " + key)

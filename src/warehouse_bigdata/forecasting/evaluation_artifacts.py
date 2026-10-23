"""Immutable Contract v1 evaluation bundles and verified validation freezes.

Digest definition: SHA-256 of canonical JSON {files: sorted relative path, size,
fresh HDFS checksum entries} plus newline. evaluation_manifest.json and
evidence/publication.json are excluded; the request binds these independently.
"""

import json
from pathlib import PurePosixPath

from warehouse_bigdata.common.hdfs_namespace import HdfsNamespaceError, require_owner, validate_publication_namespace
from warehouse_bigdata.common.hdfs_paths import render_evaluation_output_path, render_staging_path, validate_pipeline_run_id
from warehouse_bigdata.common.hdfs_publication import (
    _bytes, _envelope, _freeze_feature_metadata, _json, _require_private, _seal, document_digest,
)
from warehouse_bigdata.contracts.datasets import FORECAST_METRICS
from warehouse_bigdata.contracts.manifests import EVALUATION_MANIFEST, VALIDATED
from warehouse_bigdata.contracts.validation import validate_manifest
from warehouse_bigdata.forecasting.evaluation import EvaluationContext, ORIGINS, POLICY
from warehouse_bigdata.forecasting.feature_input import require_sealed

DATASETS = ("metrics", "metric_statistics", "paired", "alignment", "coverage", "exceptions",
            "cohort_selection", "diagnostics", "seven_day_errors")
ORIGIN_DATASETS = ("forecast_daily", "baseline_predictions", "baseline_exceptions", "rf_exceptions", "eligibility")
EXCLUDED = {"evaluation_manifest.json", "evidence/publication.json"}


def evaluation_file_integrity(client, root, *, run_type):
    if run_type not in ORIGINS:
        raise HdfsNamespaceError("evaluation inventory requires a locked phase")
    datasets = {*DATASETS, *(f"origins/d_{d}/{name}" for d in ORIGINS[run_type] for name in ORIGIN_DATASETS)}
    directories = datasets | {"evidence", "origins", *(f"origins/d_{d}" for d in ORIGINS[run_type])}
    pending, seen, inventory = [root], set(), []
    while pending:
        parent = pending.pop()
        for status in client.list_status(parent):
            path = status["path"]
            if (str(PurePosixPath(path).parent) != parent or str(PurePosixPath(path)) != path or
                path in seen or PurePosixPath(path).name in {".", ".."}):
                raise HdfsNamespaceError("evaluation inventory duplicate or non-child path")
            seen.add(path)
            if len(seen) > 4096:
                raise HdfsNamespaceError("evaluation bundle exceeds bounded layout")
            relative = str(PurePosixPath(path).relative_to(root))
            is_dir = status["type"] == "DIRECTORY"
            name = PurePosixPath(relative).name
            parquet = (str(PurePosixPath(relative).parent) in datasets and
                (name == "_SUCCESS" or (name.startswith("part-") and name.endswith(".parquet"))))
            if (status["type"] not in {"DIRECTORY", "FILE"} or
                (is_dir and relative not in directories) or
                (not is_dir and relative not in EXCLUDED | {"evidence/evaluation.json"} and not parquet)):
                raise HdfsNamespaceError("unexpected evaluation bundle layout: " + relative)
            if is_dir:
                pending.append(path)
            elif relative not in EXCLUDED:
                if type(status["length"]) is not int or status["length"] < 0:
                    raise HdfsNamespaceError("invalid evaluation inventory file length")
                inventory.append({"path": relative, "size_bytes": status["length"]})
    names = {item["path"] for item in inventory}
    if "evidence/evaluation.json" not in names or any(
        not any(p.startswith(dataset + "/part-") and p.endswith(".parquet") for p in names) for dataset in datasets):
        raise HdfsNamespaceError("incomplete evaluation bundle layout")
    inventory.sort(key=lambda item: item["path"])
    paths = [root + "/" + item["path"] for item in inventory]
    checksums = client.checksums(paths)
    if set(checksums) != set(paths) or any(not v for v in checksums.values()):
        raise HdfsNamespaceError("evaluation integrity checksums incomplete")
    for item, path in zip(inventory, paths, strict=True):
        item["checksum"] = checksums[path]
    return inventory


def _context(binding):
    return EvaluationContext(*(binding[k] for k in
        ("evaluation_run_id", "model_run_id", "source_snapshot_id", "feature_version", "run_type")))


def make_evaluation_manifest(config, *, binding, quality, created_at, completed_at):
    ctx = _context(binding)
    final = render_evaluation_output_path(config.base_path, ctx.evaluation_run_id)
    checksum = document_digest({"files": quality["file_integrity"]})
    uris = [config.uri + final + "/" + name for name in DATASETS]
    uris += [config.uri + final + f"/origins/d_{d}/{name}" for d in ORIGINS[ctx.run_type] for name in ORIGIN_DATASETS]
    coverage = dict(input_binding_digest=document_digest(binding), publication_evidence_digest=document_digest(quality),
        publication_complete=True, coverage=quality["coverage"], origin_forecasts=binding["origin_forecasts"],
        source_version=binding["source_version"], feature_version=binding["feature_version"],
        freeze_digest=document_digest(binding["freeze"]),
        inventory_digest_definition="sha256 canonical JSON {files: sorted relative paths, sizes, HDFS checksums} plus newline; excludes evaluation_manifest.json and evidence/publication.json")
    manifest = dict(manifest_schema_version=EVALUATION_MANIFEST.version, contract_name=EVALUATION_MANIFEST.name,
        contract_version=EVALUATION_MANIFEST.version, manifest_kind=EVALUATION_MANIFEST.manifest_kind,
        status=VALIDATED, producer="warehouse-bigdata:evaluate-forecasts", artifact_uri=config.uri + final,
        artifact_checksum=checksum, schema_fingerprint=quality["schema_fingerprint"], grain=FORECAST_METRICS.grain,
        logical_unique_key=",".join(FORECAST_METRICS.unique_key), row_count=48,
        required_field_null_counts=json.dumps({n: 0 for n in FORECAST_METRICS.required_field_names}, sort_keys=True),
        code_revision=binding["code_revision"], configuration_digest=binding["configuration_digest"],
        created_at=created_at, completed_at=completed_at, coverage_and_exclusion_counts=json.dumps(coverage, sort_keys=True),
        evaluation_run_id=ctx.evaluation_run_id, source_snapshot_id=ctx.source_snapshot_id, model_run_id=ctx.model_run_id,
        holdout_id=ctx.holdout_id, run_type=ctx.run_type,
        evaluation_start=ctx.window[0].isoformat(), evaluation_end=ctx.window[1].isoformat(),
        evaluation_artifact_uris=json.dumps(uris),
        evaluation_artifact_checksums=json.dumps({uri: document_digest({"files": [
            {**item, "path": item["path"].removeprefix(uri.removeprefix(config.uri + final + "/") + "/")}
            for item in quality["file_integrity"] if item["path"].startswith(uri.removeprefix(config.uri + final + "/") + "/")]})
            for uri in uris}, sort_keys=True))
    validate_evaluation_request(config, manifest, pipeline_run_id=binding["pipeline_run_id"])
    return manifest


def validate_evaluation_request(config, manifest, *, pipeline_run_id):
    validate_pipeline_run_id(pipeline_run_id)
    validate_manifest(EVALUATION_MANIFEST, manifest, lineage_context={"required_status": VALIDATED})
    ctx = EvaluationContext(manifest["evaluation_run_id"], manifest["model_run_id"], manifest["source_snapshot_id"],
                            "bound-in-evidence", manifest["run_type"])
    final = render_evaluation_output_path(config.base_path, ctx.evaluation_run_id)
    if (manifest["artifact_uri"] != config.uri + final or manifest["holdout_id"] != ctx.holdout_id or
        manifest["evaluation_start"] != ctx.window[0].isoformat() or manifest["evaluation_end"] != ctx.window[1].isoformat() or
        manifest.get("forecast_run_id") is not None or manifest["row_count"] != 48 or
        manifest["producer"] != "warehouse-bigdata:evaluate-forecasts"):
        raise HdfsNamespaceError("evaluation request identity/window conflicts")
    return render_staging_path(config.base_path, pipeline_run_id) + "/candidates/evaluation", final


def validate_evaluation_evidence(client, config, manifest, path, *, pipeline_run_id):
    validate_evaluation_request(config, manifest, pipeline_run_id=pipeline_run_id)
    staging = render_staging_path(config.base_path, pipeline_run_id)
    binding, quality = _json(client, staging + "/input_binding.json"), _json(client, staging + "/quality_report.json")
    coverage = json.loads(manifest["coverage_and_exclusion_counts"])
    if (_json(client, path + "/evaluation_manifest.json") != manifest or
        _json(client, path + "/evidence/publication.json") != quality or
        _json(client, path + "/evidence/evaluation.json") != binding or
        document_digest(binding) != coverage.get("input_binding_digest") or
        document_digest(quality) != coverage.get("publication_evidence_digest")):
        raise HdfsNamespaceError("frozen evaluation evidence conflicts with request")
    ctx = _context(binding)
    if (binding.get("pipeline_run_id") != pipeline_run_id or
        binding.get("schema_version") != "evaluation-input-binding-v1" or quality.get("input_binding") != binding or
        quality.get("status") != VALIDATED or quality.get("persisted_parquet_validated") is not True or
        coverage.get("publication_complete") is not True or quality.get("row_count") != 48 or
        quality.get("schema_fingerprint") != manifest["schema_fingerprint"] or
        quality.get("coverage") != coverage.get("coverage") or binding.get("origin_forecasts") != ctx.origin_forecasts or
        coverage.get("origin_forecasts") != ctx.origin_forecasts or binding["freeze"].get("policy") != dict(POLICY) or
        document_digest(binding["freeze"]) != coverage.get("freeze_digest")):
        raise HdfsNamespaceError("evaluation persisted evidence is not VALIDATED or conflicts with protocol")
    for key in ("evaluation_run_id", "holdout_id", "model_run_id", "source_snapshot_id", "run_type", "code_revision", "configuration_digest"):
        if binding.get(key) != manifest.get(key):
            raise HdfsNamespaceError("evaluation frozen lineage conflicts: " + key)
    if document_digest(binding.get("effective_configuration")) != binding["configuration_digest"]:
        raise HdfsNamespaceError("evaluation configuration digest conflicts")
    inventory = evaluation_file_integrity(client, path, run_type=ctx.run_type)
    if inventory != quality.get("file_integrity") or document_digest({"files": inventory}) != manifest["artifact_checksum"]:
        raise HdfsNamespaceError("evaluation file integrity conflicts with persisted validation")
    # Reconstruct all public artifact checksums/URIs from the fresh inventory, too.
    rebuilt = make_evaluation_manifest(config, binding=binding, quality=quality,
        created_at=manifest["created_at"], completed_at=manifest["completed_at"])
    if rebuilt != manifest:
        raise HdfsNamespaceError("evaluation manifest conflicts with recomputed artifact evidence")


def stage_evaluation_publication(client, config, manifest, *, pipeline_run_id):
    validate_evaluation_request(config, manifest, pipeline_run_id=pipeline_run_id)
    parent = render_staging_path(config.base_path, pipeline_run_id) + "/publication"
    request = {"version": 1, "pipeline_run_id": pipeline_run_id, "manifest": manifest}
    if not client.exists(parent):
        client.mkdir_exclusive(parent)
    path = parent + "/request.json"
    if client.exists(path):
        if _json(client, path) != request:
            raise HdfsNamespaceError("evaluation immutable publication request conflicts")
    else:
        client.upload_bytes(_bytes(request), path)
    return path


def _require_bundle_sealed(client, config, path, run_type):
    for relative in (*EXCLUDED, *(item["path"] for item in evaluation_file_integrity(client, path, run_type=run_type))):
        require_sealed(client, path + "/" + relative, config.base_path)


def finalize_evaluation_publication(client, config, *, pipeline_run_id, manifest):
    require_owner(client)
    validate_publication_namespace(client, config)
    candidate, final = validate_evaluation_request(config, manifest, pipeline_run_id=pipeline_run_id)
    envelope = _envelope(final, pipeline_run_id)
    claimed = envelope + "/candidate"
    exists = lambda path: client.path_status(path) is not None
    if exists(final):
        if exists(candidate) or exists(claimed):
            raise HdfsNamespaceError("candidate and immutable evaluation both exist")
        validate_evaluation_evidence(client, config, manifest, final, pipeline_run_id=pipeline_run_id)
        _require_bundle_sealed(client, config, final, manifest["run_type"])
        return "REUSED"
    if exists(candidate) and exists(claimed):
        raise HdfsNamespaceError("concurrent evaluation candidate and claim")
    source = claimed if exists(claimed) else candidate
    if (client.path_status(source) or {}).get("type") != "DIRECTORY":
        raise HdfsNamespaceError("complete evaluation candidate is missing")
    validate_evaluation_evidence(client, config, manifest, source, pipeline_run_id=pipeline_run_id)
    staging = render_staging_path(config.base_path, pipeline_run_id)
    _freeze_feature_metadata(client, staging)
    _require_private(client, envelope)
    if not exists(claimed):
        client.rename_exclusive(candidate, claimed)
    _seal(client, claimed)
    validate_evaluation_evidence(client, config, manifest, claimed, pipeline_run_id=pipeline_run_id)
    _seal(client, staging)
    client.rename_exclusive(claimed, final)
    client.remove(envelope)
    return "VALIDATED"


def read_validated_validation(client, config, evaluation_run_id):
    """Explicit sealed pre-test freeze. Never select a latest evaluation."""
    path = render_evaluation_output_path(config.base_path, evaluation_run_id)
    require_sealed(client, path + "/evaluation_manifest.json", config.base_path)
    manifest = _json(client, path + "/evaluation_manifest.json")
    binding = _json(client, path + "/evidence/evaluation.json")
    if (manifest.get("evaluation_run_id") != evaluation_run_id or manifest.get("run_type") != "validation" or
        binding.get("run_type") != "validation" or binding.get("validation_evaluation_run_id") is not None):
        raise HdfsNamespaceError("selected freeze is not the explicit validation evaluation")
    validate_evaluation_evidence(client, config, manifest, path, pipeline_run_id=binding["pipeline_run_id"])
    _require_bundle_sealed(client, config, path, "validation")
    return {"manifest": manifest, "binding": binding, "artifact_path": path}


def validate_evaluation_recovery(args, binding):
    for key in ("forecast_run_id", "issue_date", "upstream_pipeline_run_id", "training_cap", "sampling_seed",
                "resource_justification", "pilot_evidence_uri"):
        if getattr(args, key, None) is not None:
            raise HdfsNamespaceError("evaluation recovery arguments conflict with job: " + key)
    for key in ("source_version", "source_snapshot_id", "j03_pipeline_run_id", "j04_pipeline_run_id", "feature_version",
        "model_run_id", "evaluation_run_id", "pipeline_run_id", "run_type", "validation_evaluation_run_id"):
        if getattr(args, key, None) != binding.get(key) or (key != "validation_evaluation_run_id" and getattr(args, key, None) is None):
            raise HdfsNamespaceError("evaluation recovery input conflicts with frozen binding: " + key)
    for key in ("hdfs_uri", "spark_master", "seed", "event_log_dir"):
        if getattr(args, key, None) != binding.get("effective_configuration", {}).get(key):
            raise HdfsNamespaceError("evaluation recovery configuration conflicts with frozen binding: " + key)

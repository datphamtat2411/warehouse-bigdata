"""Evaluation bundles preserve v1, freeze integrity and owner-only recovery."""

import copy
import json
from types import SimpleNamespace

import pytest

from tests.unit.test_model_artifacts import ModelHdfs
from tests.unit.test_hdfs_namespace import CONFIG
from warehouse_bigdata.common.hdfs_namespace import HdfsNamespaceError, provision_publication_namespace
from warehouse_bigdata.common.hdfs_paths import render_evaluation_output_path, render_staging_path
from warehouse_bigdata.common.hdfs_publication import document_digest, finalize_publication
from warehouse_bigdata.contracts.manifests import EVALUATION_MANIFEST
from warehouse_bigdata.contracts.validation import validate_manifest
from warehouse_bigdata.forecasting.evaluation import EvaluationContext, freeze_protocol
from warehouse_bigdata.forecasting.evaluation_artifacts import (
    DATASETS, ORIGIN_DATASETS, evaluation_file_integrity, make_evaluation_manifest,
    read_validated_validation, stage_evaluation_publication, validate_evaluation_evidence,
    validate_evaluation_recovery,
)


def staged_evaluation():
    client = ModelHdfs()
    provision_publication_namespace(client, CONFIG)
    staging = render_staging_path(CONFIG.base_path, "execution")
    candidate = staging + "/candidates/evaluation"
    for p in (staging, staging + "/candidates", candidate, candidate + "/evidence", candidate + "/origins"):
        client.mkdir_exclusive(p)
    ctx = EvaluationContext("eval", "model", "snapshot", "v1", "validation")
    for name in DATASETS:
        client.mkdir_exclusive(candidate + "/" + name)
        client.upload_bytes(b"parquet stand-in", candidate + "/" + name + "/part-00000.parquet")
    for d in (1885, 1892, 1899, 1906):
        parent = candidate + f"/origins/d_{d}"
        client.mkdir_exclusive(parent)
        for name in ORIGIN_DATASETS:
            client.mkdir_exclusive(parent + "/" + name)
            client.upload_bytes(b"parquet stand-in", parent + "/" + name + "/part-00000.parquet")
    source = dict(source_version="sha256:" + "a" * 64, source_snapshot_id="snapshot",
        j03_pipeline_run_id="j03", j04_pipeline_run_id="j04", feature_version="v1", model_run_id="model")
    frozen = freeze_protocol(model_metadata={"manifest": {"artifact_checksum": "checksum"}, "input_binding": {}},
        cohort_digest="cohort", seed=42, code_revision="code", source_binding=source)
    configuration = dict(hdfs_uri=CONFIG.uri, spark_master="spark://spark-master:7077", seed=42, event_log_dir=None)
    binding = dict(**source, pipeline_run_id="execution", evaluation_run_id="eval", run_type="validation",
        holdout_id=ctx.holdout_id, schema_version="evaluation-input-binding-v1", code_revision="code",
        configuration_digest=document_digest(configuration), effective_configuration=configuration,
        validation_evaluation_run_id=None, freeze=frozen, origin_forecasts=ctx.origin_forecasts)
    client.upload_bytes(json.dumps(binding).encode(), candidate + "/evidence/evaluation.json")
    quality = dict(status="VALIDATED", input_binding=binding, persisted_parquet_validated=True,
        row_count=48, schema_fingerprint=document_digest([]), coverage=[],
        file_integrity=evaluation_file_integrity(client, candidate, run_type="validation"))
    manifest = make_evaluation_manifest(CONFIG, binding=binding, quality=quality,
        created_at="2026-01-01T00:00:00Z", completed_at="2026-01-01T00:00:01Z")
    for path, doc in ((staging + "/input_binding.json", binding), (staging + "/quality_report.json", quality),
        (candidate + "/evidence/publication.json", quality), (candidate + "/evaluation_manifest.json", manifest)):
        client.upload_bytes(json.dumps(doc).encode(), path)
    stage_evaluation_publication(client, CONFIG, manifest, pipeline_run_id="execution")
    return client, candidate, manifest, binding


def test_manifest_v1_tampering_and_incomplete_inventory():
    client, candidate, manifest, _ = staged_evaluation()
    assert "pipeline_run_id" not in manifest
    validate_manifest(EVALUATION_MANIFEST, manifest)
    validate_evaluation_evidence(client, CONFIG, manifest, candidate, pipeline_run_id="execution")
    client.files[candidate + "/paired/part-00000.parquet"] = b"tampered"
    with pytest.raises(HdfsNamespaceError, match="integrity"):
        validate_evaluation_evidence(client, CONFIG, manifest, candidate, pipeline_run_id="execution")
    client.remove(candidate + "/metrics/part-00000.parquet")
    with pytest.raises(HdfsNamespaceError, match="incomplete"):
        evaluation_file_integrity(client, candidate, run_type="validation")


def test_owner_only_interrupted_recovery_sealed_freeze_and_no_write_reuse():
    client, candidate, manifest, binding = staged_evaluation()
    final = render_evaluation_output_path(CONFIG.base_path, "eval")
    client.identity = "spark"
    with pytest.raises(HdfsNamespaceError, match="owner"):
        finalize_publication(client, CONFIG, pipeline_run_id="execution")
    client.identity = "hadoop"
    rename = client.rename_exclusive
    def interrupted(source, destination):
        if destination == final:
            raise OSError("interrupted")
        rename(source, destination)
    client.rename_exclusive = interrupted
    with pytest.raises(OSError):
        finalize_publication(client, CONFIG, pipeline_run_id="execution")
    client.rename_exclusive = rename
    assert finalize_publication(client, CONFIG, pipeline_run_id="execution") == "VALIDATED"
    selected = read_validated_validation(client, CONFIG, "eval")
    assert selected["binding"]["freeze"] == binding["freeze"]
    before = copy.deepcopy((client.entries, client.files))
    client.calls.clear()
    assert finalize_publication(client, CONFIG, pipeline_run_id="execution") == "REUSED"
    assert before == (client.entries, client.files)
    assert client.calls == []


def test_recovery_exact_arguments_include_validation_identity():
    _, _, _, binding = staged_evaluation()
    args = {k: binding.get(k) for k in ("source_version", "source_snapshot_id", "j03_pipeline_run_id",
        "j04_pipeline_run_id", "feature_version", "model_run_id", "evaluation_run_id", "run_type",
        "validation_evaluation_run_id", "pipeline_run_id")}
    args.update(binding["effective_configuration"])
    validate_evaluation_recovery(SimpleNamespace(**args), binding)
    for key in args:
        with pytest.raises(HdfsNamespaceError, match="conflict"):
            validate_evaluation_recovery(SimpleNamespace(**{**args, key: "poison"}), binding)


def test_unsafe_parent_and_unsealed_validation_are_rejected():
    client, candidate, _, _ = staged_evaluation()
    parent = CONFIG.base_path + "/outputs/evaluation"
    client.entries[parent]["permission"] = "777"
    with pytest.raises(HdfsNamespaceError, match="shared write"):
        finalize_publication(client, CONFIG, pipeline_run_id="execution")
    assert client.exists(candidate)
    client.entries[parent]["permission"] = "755"
    finalize_publication(client, CONFIG, pipeline_run_id="execution")
    final = render_evaluation_output_path(CONFIG.base_path, "eval")
    client.entries[final + "/cohort_selection/part-00000.parquet"]["permission"] = "666"
    with pytest.raises(ValueError, match="sealed"):
        read_validated_validation(client, CONFIG, "eval")

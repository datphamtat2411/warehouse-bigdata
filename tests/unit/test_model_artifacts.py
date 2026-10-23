"""M06 frozen model bundle integrity and owner-only publication."""

import copy
import hashlib
import json
from pathlib import PurePosixPath

import pytest

from tests.unit.test_hdfs_namespace import CONFIG, PermissionHdfs
from warehouse_bigdata.common.hdfs_namespace import HdfsNamespaceError, provision_publication_namespace
from warehouse_bigdata.common.hdfs_paths import render_model_artifact_path, render_staging_path
from warehouse_bigdata.common.hdfs_publication import document_digest, finalize_publication, stage_publication
from warehouse_bigdata.forecasting.model_artifacts import (
    make_model_manifest, model_file_integrity, validate_model_evidence,
)


class ModelHdfs(PermissionHdfs):
    def list_status(self, root):
        return [{"path": path, "type": status["type"], "length": len(self.files.get(path, b""))}
                for path, status in self.entries.items() if str(PurePosixPath(path).parent) == root and path != root]

    def checksums(self, paths):
        return {path: hashlib.sha256(self.files[path]).hexdigest() for path in paths}


def staged_model():
    client = ModelHdfs()
    provision_publication_namespace(client, CONFIG)
    staging = render_staging_path(CONFIG.base_path, "j05-001")
    candidate = staging + "/candidates/model"
    for path in (staging, staging + "/candidates", candidate,
                 candidate + "/spark_pipeline_model", candidate + "/spark_pipeline_model/metadata",
                 candidate + "/spark_pipeline_model/stages", candidate + "/training_selection_manifest",
                 candidate + "/evidence"):
        client.mkdir_exclusive(path)
    for relative in ("spark_pipeline_model/metadata/part-00000", "training_selection_manifest/part-00000.parquet",
                     "feature_schema.json", "evidence/training.json"):
        client.upload_bytes(b"{}", candidate + "/" + relative)
    configuration = {"training": {"cap": 150000, "sampling_seed": 42, "resource_justification": None,
                                  "pilot_evidence_uri": "hdfs://pilot/evidence.json"},
                     "hdfs_uri": CONFIG.uri, "spark_master": "spark://spark-master:7077", "runtime": {}}
    binding = {"schema_version": "model-training-binding-v1", "model_run_id": "rf-001",
               "pipeline_run_id": "j05-001", "source_version": "sha256:" + "a" * 64,
               "source_snapshot_id": "snapshot", "upstream_pipeline_run_id": "j04-001",
               "feature_version": "historical_v1", "code_revision": "sha256:" + "c" * 64,
               "configuration_digest": document_digest(configuration),
               "effective_configuration": configuration}
    inventory = model_file_integrity(client, candidate)
    quality = {"status": "VALIDATED", "input_binding": binding, "file_integrity": inventory,
               "reload_verified": True, "schema_fingerprint": document_digest({"schema": []})}
    manifest = make_model_manifest(CONFIG, binding=binding, quality=quality,
                                   summary={"selected_count": 12, "selected_series_count": 3},
                                   parameters={"numTrees": 20}, created_at="2026-01-01T00:00:00Z",
                                   completed_at="2026-01-01T00:00:01Z")
    for path, document in ((staging + "/input_binding.json", binding),
                           (staging + "/quality_report.json", quality),
                           (candidate + "/evidence/publication.json", quality),
                           (candidate + "/model_manifest.json", manifest)):
        client.upload_bytes(json.dumps(document).encode(), path)
    stage_publication(client, CONFIG, manifest, result_status="VALIDATED")
    return client, candidate, manifest


def test_m06_model_inventory_rejects_layout_and_detects_tampering():
    client, candidate, manifest = staged_model()
    validate_model_evidence(client, CONFIG, manifest, candidate)
    client.files[candidate + "/spark_pipeline_model/metadata/part-00000"] = b"tamper"
    with pytest.raises(HdfsNamespaceError, match="integrity"):
        validate_model_evidence(client, CONFIG, manifest, candidate)
    client.upload_bytes(b"bad", candidate + "/unexpected.bin")
    with pytest.raises(HdfsNamespaceError, match="layout|unexpected"):
        model_file_integrity(client, candidate)


def test_m06_owner_model_publication_recovers_and_is_immutable():
    client, candidate, manifest = staged_model()
    final = render_model_artifact_path(CONFIG.base_path, "rf-001")
    client.identity = "spark"
    with pytest.raises(HdfsNamespaceError, match="owner"):
        finalize_publication(client, CONFIG, pipeline_run_id="j05-001")
    client.identity = "hadoop"
    rename = client.rename_exclusive
    def interrupted(source, destination):
        if destination == final:
            raise OSError("interrupted")
        rename(source, destination)
    client.rename_exclusive = interrupted
    with pytest.raises(OSError, match="interrupted"):
        finalize_publication(client, CONFIG, pipeline_run_id="j05-001")
    assert not client.exists(final)
    client.rename_exclusive = rename
    assert finalize_publication(client, CONFIG, pipeline_run_id="j05-001") == "VALIDATED"
    assert client.exists(final + "/model_manifest.json")
    assert not client.exists(candidate)
    before = copy.deepcopy((client.entries, client.files))
    client.calls.clear()
    assert finalize_publication(client, CONFIG, pipeline_run_id="j05-001") == "REUSED"
    assert before == (client.entries, client.files)
    assert not client.calls
    client.identity = "spark"
    with pytest.raises(PermissionError):
        client.remove(final)


@pytest.mark.parametrize("field", ["model_run_id", "feature_version", "configuration_digest"])
def test_m06_model_conflicting_frozen_binding_rejected(field):
    client, candidate, manifest = staged_model()
    changed = dict(manifest, **{field: "conflict"})
    with pytest.raises((ValueError, HdfsNamespaceError)):
        stage_publication(client, CONFIG, changed, result_status="VALIDATED")


def test_m06_incomplete_final_and_concurrent_claim_are_rejected():
    client, candidate, manifest = staged_model()
    final = render_model_artifact_path(CONFIG.base_path, "rf-001")
    client.mkdir_exclusive(final)
    with pytest.raises(HdfsNamespaceError):
        finalize_publication(client, CONFIG, pipeline_run_id="j05-001")
    client.remove(final)
    from warehouse_bigdata.common.hdfs_publication import _envelope
    envelope = _envelope(final, "j05-001")
    client.mkdir_exclusive(envelope)
    client.mkdir_exclusive(envelope + "/candidate")
    with pytest.raises(HdfsNamespaceError, match="concurrent"):
        finalize_publication(client, CONFIG, pipeline_run_id="j05-001")


@pytest.mark.parametrize("conflict", [None, "cap", "missing", "master"])
def test_m06_owner_recovery_checks_complete_config_and_skips_refit(monkeypatch, conflict):
    import sys
    from warehouse_bigdata.common import hdfs_publication as module
    client, candidate, manifest = staged_model()
    monkeypatch.setattr(module, "HdfsCliClient", lambda: client)
    monkeypatch.setattr(module, "load_hdfs_config", lambda: CONFIG)
    arguments = ["publisher", "--recover", "--skip-completed-driver", "--pipeline-run-id", "j05-001",
                 "--source-version", "sha256:" + "a" * 64, "--source-snapshot-id", "snapshot",
                 "--upstream-pipeline-run-id", "j04-001", "--feature-version", "historical_v1",
                 "--model-run-id", "rf-001", "--hdfs-uri", CONFIG.uri,
                 "--spark-master", "spark://spark-master:7077", "--training-cap", "150000",
                 "--sampling-seed", "42", "--pilot-evidence-uri", "hdfs://pilot/evidence.json"]
    if conflict == "cap":
        arguments[arguments.index("150000")] = "10000"
    elif conflict == "master":
        arguments[arguments.index("spark://spark-master:7077")] = "spark://different:7077"
    elif conflict == "missing":
        index = arguments.index("--source-version")
        del arguments[index:index + 2]
    monkeypatch.setattr(sys, "argv", arguments)
    if conflict:
        with pytest.raises(HdfsNamespaceError, match="conflict|complete"):
            module.main()
        assert client.exists(candidate)
    else:
        with pytest.raises(SystemExit) as result:
            module.main()
        assert result.value.code == 10
        assert not client.exists(candidate)

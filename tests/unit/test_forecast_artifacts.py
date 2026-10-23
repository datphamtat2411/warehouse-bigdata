"""J06 Contract v1 bundle, owner publication and interrupted promotion."""

import copy
import json

import pytest

from tests.unit.test_hdfs_namespace import CONFIG
from tests.unit.test_model_artifacts import ModelHdfs
from warehouse_bigdata.common.hdfs_namespace import (
    HdfsNamespaceError,
    provision_publication_namespace,
)
from warehouse_bigdata.common.hdfs_paths import render_forecast_output_path, render_staging_path
from warehouse_bigdata.common.hdfs_publication import document_digest, finalize_publication
from warehouse_bigdata.contracts.manifests import FORECAST_MANIFEST
from warehouse_bigdata.contracts.validation import validate_manifest
from warehouse_bigdata.forecasting.forecast_artifacts import (
    forecast_file_integrity,
    make_forecast_manifest,
    stage_forecast_publication,
    validate_forecast_evidence,
)


def staged_forecast(*, issue_date="2016-03-27", run_type="validation"):
    client = ModelHdfs()
    provision_publication_namespace(client, CONFIG)
    staging = render_staging_path(CONFIG.base_path, "j06")
    candidate = staging + "/candidates/forecast"
    for path in (staging, staging + "/candidates", candidate, candidate + "/evidence"):
        client.mkdir_exclusive(path)
    from tests.unit.test_m5_small import evidence
    from warehouse_bigdata.common.scope_evidence import write_scope

    scope = evidence("snapshot")
    write_scope(client, candidate, scope)
    for name in ("forecast_daily", "cohort_selection", "coverage", "coverage_exceptions"):
        client.mkdir_exclusive(candidate + "/" + name)
        client.upload_bytes(b"parquet stand-in", candidate + "/" + name + "/part-00000.parquet")
    configuration = {
        "hdfs_uri": CONFIG.uri,
        "spark_master": "spark://spark-master:7077",
        "seed": 42,
        "event_log_dir": None,
    }
    binding = dict(
        schema_version="forecast-input-binding-v1",
        source_version="sha256:" + "a" * 64,
        source_snapshot_id="snapshot",
        pipeline_run_id="j06",
        j03_pipeline_run_id="j03",
        j04_pipeline_run_id="j04",
        model_run_id="model",
        forecast_run_id="forecast",
        feature_version="v1",
        issue_date=issue_date,
        run_type=run_type,
        code_revision="sha256:" + "b" * 64,
        effective_configuration=configuration,
        configuration_digest=document_digest(configuration),
        scope=scope,
    )
    client.upload_bytes(json.dumps(binding).encode(), candidate + "/evidence/scoring.json")
    quality = dict(
        status="VALIDATED",
        input_binding=binding,
        persisted_parquet_validated=True,
        file_integrity=forecast_file_integrity(client, candidate),
        schema_fingerprint=document_digest([]),
        row_count=0,
        candidate_series=100,
        successful_series=0,
        excluded_series=100,
    )
    manifest = make_forecast_manifest(
        CONFIG,
        binding=binding,
        quality=quality,
        created_at="2026-01-01T00:00:00Z",
        completed_at="2026-01-01T00:00:01Z",
    )
    for path, doc in (
        (staging + "/input_binding.json", binding),
        (staging + "/quality_report.json", quality),
        (candidate + "/evidence/publication.json", quality),
        (candidate + "/scoring_manifest.json", manifest),
    ):
        client.upload_bytes(json.dumps(doc).encode(), path)
    stage_forecast_publication(client, CONFIG, manifest, pipeline_run_id="j06")
    return client, candidate, manifest


def test_j06_manifest_v1_and_corrupt_persisted_bundle():
    client, candidate, manifest = staged_forecast()
    assert "pipeline_run_id" not in manifest
    validate_manifest(FORECAST_MANIFEST, manifest)
    validate_forecast_evidence(client, CONFIG, manifest, candidate, pipeline_run_id="j06")
    client.files[candidate + "/forecast_daily/part-00000.parquet"] = b"corrupt"
    with pytest.raises(HdfsNamespaceError, match="integrity"):
        validate_forecast_evidence(client, CONFIG, manifest, candidate, pipeline_run_id="j06")


def test_j06_owner_only_recovery_and_immutable_noop():
    client, candidate, manifest = staged_forecast()
    final = render_forecast_output_path(CONFIG.base_path, "forecast")
    client.identity = "spark"
    with pytest.raises(HdfsNamespaceError, match="owner"):
        finalize_publication(client, CONFIG, pipeline_run_id="j06")
    client.identity = "hadoop"
    rename = client.rename_exclusive

    def interrupted(source, destination):
        if destination == final:
            raise OSError("interrupted")
        rename(source, destination)

    client.rename_exclusive = interrupted
    with pytest.raises(OSError):
        finalize_publication(client, CONFIG, pipeline_run_id="j06")
    client.rename_exclusive = rename
    assert finalize_publication(client, CONFIG, pipeline_run_id="j06") == "VALIDATED"
    assert client.exists(final + "/scoring_manifest.json")
    before = copy.deepcopy((client.entries, client.files))
    client.calls.clear()
    assert finalize_publication(client, CONFIG, pipeline_run_id="j06") == "REUSED"
    assert before == (client.entries, client.files)
    assert client.calls == []


@pytest.mark.parametrize("conflict", [None, "seed", "j03_pipeline_run_id", "event_log_dir"])
def test_j06_recovery_compares_effective_defaults_and_inputs(monkeypatch, conflict):
    import sys

    from warehouse_bigdata.common import hdfs_publication as module

    client, candidate, manifest = staged_forecast()
    monkeypatch.setattr(module, "HdfsCliClient", lambda **kwargs: client)
    monkeypatch.setattr(module, "load_hdfs_config", lambda: CONFIG)
    args = dict(
        source_version="sha256:" + "a" * 64,
        source_snapshot_id="snapshot",
        j03_pipeline_run_id="j03",
        j04_pipeline_run_id="j04",
        feature_version="v1",
        model_run_id="model",
        forecast_run_id="forecast",
        issue_date="2016-03-27",
        run_type="validation",
        hdfs_uri=CONFIG.uri,
        spark_master="spark://spark-master:7077",
        seed="42",
    )
    if conflict:
        args[conflict] = "99" if conflict == "seed" else "conflict"
    argv = ["publisher", "--recover", "--skip-completed-driver", "--pipeline-run-id", "j06"]
    for key, value in args.items():
        argv.extend(["--" + key.replace("_", "-"), value])
    monkeypatch.setattr(sys, "argv", argv)
    if conflict:
        with pytest.raises(HdfsNamespaceError, match="conflict"):
            module.main()
        assert client.exists(candidate)
    else:
        with pytest.raises(SystemExit) as error:
            module.main()
        assert error.value.code == 10

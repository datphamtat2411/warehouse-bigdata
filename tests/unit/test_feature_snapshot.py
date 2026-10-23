"""J04 staging, frozen-attempt, upstream, and owner-recovery boundaries."""

import copy
import hashlib
import json
from pathlib import PurePosixPath

import pytest

from tests.unit.test_enrichment_analytics_snapshot import (
    BASE_PATH,
    SOURCE_SNAPSHOT_ID,
    SOURCE_VERSION,
    _LocalHdfs,  # noqa: F401
)
from tests.unit.test_enrichment_analytics_snapshot import (
    PIPELINE_RUN_ID as J03_RUN,
)
from tests.unit.test_hdfs_namespace import CONFIG, PermissionHdfs
from tests.unit.test_hdfs_raw_ingestion import _run_manifest
from tests.unit.test_historical_features import _frame, _rows
from tests.unit.test_historical_features import spark as spark
from warehouse_bigdata.common.hdfs_namespace import (
    HdfsNamespaceError,
    provision_publication_namespace,
)
from warehouse_bigdata.common.hdfs_paths import (
    render_forecast_features_path,
    render_run_manifest_path,
    render_staging_path,
)
from warehouse_bigdata.common.hdfs_publication import finalize_publication, stage_publication
from warehouse_bigdata.forecasting import feature_snapshot as module
from warehouse_bigdata.processing import enrichment_analytics_snapshot as upstream

RUN = "j04-feature-001"
VERSION = "historical_v1"


class FeatureLocalHdfs(_LocalHdfs):
    def list_status(self, path):
        return tuple(
            {
                "path": path + "/" + child.name,
                "type": "DIRECTORY" if child.is_dir() else "FILE",
                "length": child.stat().st_size,
            }
            for child in sorted(self._local(path).iterdir())
            if not child.name.endswith(".crc")
        )

    def checksums(self, paths):
        return {path: self.checksum(path) for path in paths}

    def list_children(self, path):
        return tuple(path + "/" + child.name for child in sorted(self._local(path).iterdir()))

    def checksum(self, path):
        return hashlib.sha256(self._local(path).read_bytes()).hexdigest()

    def stat(self, path):
        from types import SimpleNamespace

        return SimpleNamespace(size_bytes=self._local(path).stat().st_size)


@pytest.fixture
def staged_inputs(spark, tmp_path, monkeypatch):
    hdfs = FeatureLocalHdfs(tmp_path)
    staging = render_staging_path(BASE_PATH, RUN)
    hdfs.mkdir_parents(staging)
    source = _frame(spark, _rows(range(1, 32), zero=True, prices={}))
    from tests.unit.test_m5_small import evidence as small_evidence

    scope = small_evidence(SOURCE_SNAPSHOT_ID)
    monkeypatch.setattr(module, "read_scope", lambda *a, **k: scope)
    evidence = {
        "scope": scope,
        "source_version": SOURCE_VERSION,
        "source_snapshot_id": SOURCE_SNAPSHOT_ID,
        "pipeline_run_id": J03_RUN,
        "metadata_digests": {"marker": "sha256:fixture"},
    }
    monkeypatch.setattr(
        module, "read_validated_sales_enriched_snapshot", lambda *args, **kwargs: (source, evidence)
    )
    monkeypatch.setattr(module, "_uri", lambda config, path: str(hdfs._local(path)))
    monkeypatch.setattr(module, "packaged_code_revision", lambda: "sha256:" + "c" * 64)
    requests = []
    monkeypatch.setattr(
        module, "stage_publication", lambda *args, **kwargs: requests.append(args[2])
    )
    return hdfs, requests


def run(spark, hdfs, **kwargs):
    return module.run_feature_snapshot(
        spark,
        source_version=SOURCE_VERSION,
        source_snapshot_id=SOURCE_SNAPSHOT_ID,
        upstream_pipeline_run_id=J03_RUN,
        feature_version=VERSION,
        pipeline_run_id=kwargs.pop("pipeline_run_id", RUN),
        hdfs_config=CONFIG,
        client=hdfs,
        output_partitions=2,
        **kwargs,
    )


def test_snapshot_validates_actual_parquet_before_staging_owner_request(spark, staged_inputs):
    hdfs, requests = staged_inputs
    result = run(spark, hdfs)
    assert result.status == "STAGED", result.failure_details
    assert result.producing_pipeline_run_id == RUN
    assert result.metrics["row_count"] == 3
    assert not hdfs.exists(result.artifact_path)
    assert not hdfs.exists(render_run_manifest_path(BASE_PATH, RUN))
    candidate = result.staging_path + "/candidates/forecast_features"
    assert hdfs.exists(candidate + "/_dataset_manifest.json")
    assert hdfs.exists(candidate + "/_diagnostics/exclusions")
    assert spark.read.parquet(str(hdfs._local(candidate))).count() == 3
    sidecar = json.loads(hdfs.read_remote(candidate + "/_dataset_manifest.json"))
    assert sidecar["provenance"] == "derived_leakage_controlled_features"
    assert sidecar["file_integrity"]
    assert requests[0]["status"] == "VALIDATED"
    assert "feature_version" not in requests[0]
    assert json.loads(requests[0]["coverage_and_exclusion_counts"])["feature_version"] == VERSION

    # A frozen attempt cannot invoke T01 again, even after a successful candidate write.
    original = hdfs.read_remote(candidate + "/_dataset_manifest.json")
    retry = run(spark, hdfs)
    assert retry.status == "FAILED"
    assert "new pipeline_run_id" in " ".join(retry.failure_details)
    assert hdfs.read_remote(candidate + "/_dataset_manifest.json") == original


def test_failed_write_preserves_binding_and_failure_evidence(spark, staged_inputs, monkeypatch):
    hdfs, requests = staged_inputs
    monkeypatch.setattr(
        module,
        "_write_candidate",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("isolated write failure")),
    )
    result = run(spark, hdfs)
    assert result.status == "FAILED"
    assert hdfs.exists(result.staging_path + "/input_binding.json")
    assert hdfs.exists(result.failure_report_path)
    assert not requests and not hdfs.exists(result.artifact_path)


def test_new_execution_reuses_artifact_without_rewriting_producer(spark, staged_inputs):
    hdfs, requests = staged_inputs
    first = run(spark, hdfs)
    assert first.status == "STAGED", first.failure_details
    candidate = first.staging_path + "/candidates/forecast_features"
    hdfs.rename(candidate, first.artifact_path)
    hdfs.upload_bytes(json.dumps(requests[0]).encode(), render_run_manifest_path(BASE_PATH, RUN))
    original = hdfs.read_remote(first.artifact_path + "/_dataset_manifest.json")
    next_run = RUN + "-reuse"
    hdfs.mkdir_parents(render_staging_path(BASE_PATH, next_run))
    second = run(spark, hdfs, pipeline_run_id=next_run)
    assert second.status == "REUSED", second.failure_details
    assert second.producing_pipeline_run_id == RUN
    assert second.pipeline_run_id == next_run
    assert hdfs.read_remote(first.artifact_path + "/_dataset_manifest.json") == original
    assert len(requests) == 2
    assert (
        json.loads(requests[-1]["coverage_and_exclusion_counts"])["producing_pipeline_run_id"]
        == RUN
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"feature_version": "../escape"},
        {"source_snapshot_id": "../escape"},
        {"upstream_pipeline_run_id": "../escape"},
        {"source_version": "not-a-digest"},
        {"pipeline_run_id": "../escape"},
        {"output_partitions": 0},
        {"max_records_per_file": 0},
        {"output_partitions": True},
    ],
)
def test_invalid_configuration_fails_before_hdfs(kwargs):
    arguments = dict(
        source_version=SOURCE_VERSION,
        source_snapshot_id=SOURCE_SNAPSHOT_ID,
        upstream_pipeline_run_id=J03_RUN,
        feature_version=VERSION,
        pipeline_run_id=RUN,
        hdfs_config=CONFIG,
        client=object(),
    )
    arguments.update(kwargs)
    with pytest.raises((ValueError, module.FeatureSnapshotError)):
        module.run_feature_snapshot(object(), **arguments)


def _feature_staged(client, run_id=RUN):
    provision_publication_namespace(client, CONFIG)
    staging = render_staging_path(BASE_PATH, run_id)
    client.mkdir_exclusive(staging)
    client.mkdir_exclusive(staging + "/candidates")
    candidate = staging + "/candidates/forecast_features"
    client.mkdir_exclusive(candidate)
    from tests.unit.test_m5_small import evidence
    from warehouse_bigdata.common.scope_evidence import bind_scope, write_scope

    scope = evidence(SOURCE_SNAPSHOT_ID)
    write_scope(client, candidate, scope)
    final = render_forecast_features_path(BASE_PATH, SOURCE_SNAPSHOT_ID, VERSION)
    binding = {
        "scope": scope,
        "source_version": SOURCE_VERSION,
        "source_snapshot_id": SOURCE_SNAPSHOT_ID,
        "pipeline_run_id": run_id,
        "feature_version": VERSION,
        "upstream_pipeline_run_id": J03_RUN,
        "code_revision": "sha256:" + "c" * 64,
        "configuration_digest": "sha256:" + "b" * 64,
        "artifact_uri": CONFIG.uri + final,
        "upstream_evidence": {"verified": True},
    }
    client.upload_bytes(b"parquet fixture", candidate + "/part-00000.parquet")
    integrity = [
        {
            "path": "_scope.json",
            "size_bytes": len(client.files[candidate + "/_scope.json"]),
            "checksum": "fixture-hash",
        },
        {"path": "part-00000.parquet", "size_bytes": 15, "checksum": "fixture-hash"},
    ]
    sidecar = {
        **binding,
        "status": "VALIDATED",
        "file_integrity": integrity,
        "persisted_parquet_validated": True,
    }
    quality = {
        **binding,
        "status": "VALIDATED",
        "file_integrity": integrity,
        "persisted_parquet_validated": True,
    }
    for path, document in (
        (staging + "/input_binding.json", binding),
        (staging + "/quality_report.json", quality),
        (candidate + "/_dataset_manifest.json", sidecar),
    ):
        client.upload_bytes(json.dumps(document).encode(), path)
    manifest = _run_manifest(
        run_id, SOURCE_VERSION, CONFIG.uri + render_run_manifest_path(BASE_PATH, run_id)
    )
    manifest.update(
        source_snapshot_id=SOURCE_SNAPSHOT_ID,
        producer="warehouse-bigdata:feature-snapshot",
        output_dataset_names='["forecast_features"]',
        output_dataset_uris=json.dumps([CONFIG.uri + final]),
        quality_report_uri=CONFIG.uri + staging + "/quality_report.json",
        code_revision=binding["code_revision"],
        configuration_digest=binding["configuration_digest"],
    )
    manifest["coverage_and_exclusion_counts"] = json.dumps(
        {
            "feature_version": VERSION,
            "producing_pipeline_run_id": run_id,
            "publication_complete": True,
            "persisted_parquet_validated": True,
            "binding_digest": module.document_digest(binding),
            "quality_digest": module.document_digest(quality),
            "dataset_manifest_digest": module.document_digest(sidecar),
            "file_integrity": integrity,
        }
    )
    client.list_children = lambda path: tuple(
        p for p in client.entries if str(PurePosixPath(p).parent) == path
    )
    client.stat = lambda path: type("Stat", (), {"size_bytes": len(client.files[path])})()
    client.checksum = lambda path: "fixture-hash"
    client.list_status = lambda path: tuple(
        {
            "path": child,
            "type": metadata["type"],
            "length": len(client.files.get(child, b"")),
        }
        for child, metadata in client.entries.items()
        if str(PurePosixPath(child).parent) == path
    )
    client.checksums = lambda paths: {path: client.checksum(path) for path in paths}
    manifest = bind_scope(manifest, scope)
    stage_publication(client, CONFIG, manifest, result_status="VALIDATED")
    return candidate, final, manifest


@pytest.mark.parametrize("phase", ["claim", "seal", "rename", "marker"])
def test_feature_owner_recovery_is_frozen_and_marker_last(phase):
    client = PermissionHdfs()
    candidate, final, manifest = _feature_staged(client)
    marker = render_run_manifest_path(BASE_PATH, RUN)
    original_rename, original_owner = client.rename_exclusive, client.set_owner

    def rename(source, destination):
        if (
            (phase == "claim" and source == candidate)
            or (phase == "rename" and destination == final)
            or (phase == "marker" and destination == marker)
        ):
            raise OSError("isolated publication failure")
        original_rename(source, destination)

    def owner(*args, **kwargs):
        if phase == "seal":
            raise OSError("isolated publication failure")
        original_owner(*args, **kwargs)

    client.rename_exclusive, client.set_owner = rename, owner
    with pytest.raises(OSError, match="isolated"):
        finalize_publication(client, CONFIG, pipeline_run_id=RUN)
    assert not client.exists(marker)
    client.rename_exclusive, client.set_owner = original_rename, original_owner
    assert finalize_publication(client, CONFIG, pipeline_run_id=RUN) == "VALIDATED"
    assert client.files[final + "/part-00000.parquet"] == b"parquet fixture"
    before = copy.deepcopy((client.entries, client.files))
    assert finalize_publication(client, CONFIG, pipeline_run_id=RUN) == "REUSED"
    assert (client.entries, client.files) == before
    client.identity = "spark"
    assert not client.can_access(str(PurePosixPath(final).parent), "w")
    with pytest.raises(PermissionError):
        client.remove(final)


@pytest.mark.parametrize("corruption", ["quality", "binding", "sidecar", "bytes", "concurrent"])
def test_owner_rejects_changed_frozen_evidence_before_publication(corruption):
    client = PermissionHdfs()
    candidate, final, _ = _feature_staged(client)
    if corruption == "concurrent":
        parent = str(PurePosixPath(final).parent)
        client.mkdir_exclusive(parent)
        client.mkdir_exclusive(final)
    elif corruption == "bytes":
        client.files[candidate + "/part-00000.parquet"] = b"changed"
    else:
        path = {
            "quality": render_staging_path(BASE_PATH, RUN) + "/quality_report.json",
            "binding": render_staging_path(BASE_PATH, RUN) + "/input_binding.json",
            "sidecar": candidate + "/_dataset_manifest.json",
        }[corruption]
        document = json.loads(client.files[path])
        document["feature_version"] = "changed"
        client.files[path] = json.dumps(document).encode()
    with pytest.raises(HdfsNamespaceError):
        finalize_publication(client, CONFIG, pipeline_run_id=RUN)
    assert not client.exists(render_run_manifest_path(BASE_PATH, RUN))


def test_public_upstream_requires_safe_identity_before_any_access():
    with pytest.raises(upstream.EnrichmentAnalyticsSnapshotError):
        upstream.read_validated_sales_enriched_snapshot(
            object(),
            source_version=SOURCE_VERSION,
            source_snapshot_id=SOURCE_SNAPSHOT_ID,
            pipeline_run_id="../escape",
            hdfs_config=CONFIG,
            client=object(),
        )


def test_public_upstream_rejects_absent_completion_before_spark(tmp_path):
    with pytest.raises(upstream.EnrichmentAnalyticsSnapshotError, match="completion"):
        upstream.read_validated_sales_enriched_snapshot(
            object(),
            source_version=SOURCE_VERSION,
            source_snapshot_id=SOURCE_SNAPSHOT_ID,
            pipeline_run_id=J03_RUN,
            hdfs_config=CONFIG,
            client=FeatureLocalHdfs(tmp_path),
        )

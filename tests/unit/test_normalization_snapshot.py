"""Focused checks for the M04 T04 normalization snapshot boundary."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import pytest

from warehouse_bigdata.common.config import HDFSConfig
from warehouse_bigdata.common.hdfs_paths import (
    render_curated_sales_daily_path,
    render_normalization_candidate_path,
    render_normalization_chunk_manifest_path,
    render_normalization_chunk_path,
    render_normalization_chunks_path,
    render_normalization_quality_path,
    render_raw_m5_path,
    render_run_manifest_path,
    render_staging_path,
)
from warehouse_bigdata.ingestion.hdfs_raw import ValidatedHdfsRawSnapshot
from warehouse_bigdata.processing.normalization_snapshot import (
    NormalizationSnapshotConfig,
    NormalizationSnapshotError,
    build_chunk_plan,
    run_normalization_snapshot,
    validate_chunk_coverage,
    validate_normalization_identifiers,
)

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import StringType, StructField, StructType  # noqa: E402

BASE_PATH = "/data/warehouse"
SOURCE_VERSION = "sha256:" + "a" * 64


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("warehouse-bigdata-normalization-snapshot-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


class _LocalHdfs:
    def __init__(self, root: Path) -> None:
        self.root = root

    def _local(self, path: str) -> Path:
        return self.root / path.lstrip("/")

    def exists(self, path: str) -> bool:
        return self._local(path).exists()

    def is_directory(self, path: str) -> bool:
        return self._local(path).is_dir()

    def mkdir_parents(self, path: str) -> None:
        self._local(path).mkdir(parents=True, exist_ok=True)

    def mkdir_exclusive(self, path: str) -> None:
        self._local(path).mkdir()

    def list_children(self, path: str) -> tuple[str, ...]:
        return tuple(
            sorted(f"{path.rstrip('/')}/{child.name}" for child in self._local(path).iterdir())
        )

    def upload_bytes(self, payload: bytes, path: str) -> None:
        local = self._local(path)
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(payload)

    def read_remote(self, path: str) -> bytes:
        return self._local(path).read_bytes()

    def rename(self, source: str, destination: str) -> None:
        source_path = self._local(source)
        destination_path = self._local(destination)
        if destination_path.exists():
            raise FileExistsError(destination)
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(source_path, destination_path)


def _small_sales_frame(spark: SparkSession):
    metadata = ("id", "item_id", "dept_id", "cat_id", "store_id", "state_id")
    days = tuple(f"d_{day}" for day in range(1, 1_942))
    fields = [StructField(field, StringType(), True) for field in (*metadata, *days)]
    rows = []
    for series, store, first_day, second_day in (
        ("item_1_CA_1", "CA_1", "0", "2"),
        ("item_1_TX_1", "TX_1", "1", "0"),
    ):
        rows.append(
            (
                series,
                "item_1",
                "dept_1",
                "cat_1",
                store,
                store[:2],
                first_day,
                second_day,
                *("0" for _ in range(1_939)),
            )
        )
    return spark.createDataFrame(rows, StructType(fields))


def _small_calendar_frame(spark: SparkSession):
    schema = StructType(
        [
            StructField("date", StringType(), True),
            StructField("wm_yr_wk", StringType(), True),
            StructField("d", StringType(), True),
        ]
    )
    return spark.createDataFrame(
        [("2011-01-29", "11101", "d_1"), ("2011-01-30", "11101", "d_2")],
        schema,
    )


def _configure_snapshot_inputs(
    monkeypatch: pytest.MonkeyPatch,
    hdfs: _LocalHdfs,
    hdfs_config: HDFSConfig,
    sales,
    calendar,
) -> None:
    from warehouse_bigdata.ingestion import hdfs_raw
    from warehouse_bigdata.processing import normalization_snapshot

    raw_snapshot = ValidatedHdfsRawSnapshot(
        status="VALIDATED",
        source_version=SOURCE_VERSION,
        raw_path="/data/warehouse/raw/m5/source_version=" + SOURCE_VERSION.replace(":", "-"),
        manifests=(
            {"source_file": "calendar.csv"},
            {"source_file": "sales_train_evaluation.csv"},
        ),
        integrity=object(),
    )
    monkeypatch.setattr(
        normalization_snapshot,
        "validate_hdfs_raw_snapshot",
        lambda *args, **kwargs: raw_snapshot,
    )
    monkeypatch.setattr(
        normalization_snapshot,
        "_read_validated_sources",
        lambda *args, **kwargs: (sales, calendar),
    )
    monkeypatch.setattr(
        normalization_snapshot,
        "_hdfs_uri",
        lambda _uri, path: str(hdfs.root / path.lstrip("/")),
    )
    monkeypatch.setattr(
        hdfs_raw,
        "publish_run_manifest",
        lambda manifest, **kwargs: _publish_test_manifest(hdfs, hdfs_config, manifest),
    )


def _publish_test_manifest(
    hdfs: _LocalHdfs,
    hdfs_config: HDFSConfig,
    manifest: dict[str, object],
) -> str:
    path = render_run_manifest_path(hdfs_config.base_path, str(manifest["pipeline_run_id"]))
    hdfs.mkdir_parents(str(Path(path).parent))
    hdfs.upload_bytes(json.dumps(manifest).encode("utf-8"), path)
    return path


def test_snapshot_paths_keep_contract_identities_and_are_immutable() -> None:
    staging = render_staging_path(BASE_PATH, "normalize-20261022")

    assert render_raw_m5_path(BASE_PATH, SOURCE_VERSION) == (
        "/data/warehouse/raw/m5/source_version=sha256-" + "a" * 64
    )
    assert staging == "/data/warehouse/staging/pipeline_run_id=normalize-20261022"
    assert render_curated_sales_daily_path(BASE_PATH, "snapshot-20261022") == (
        "/data/warehouse/curated/sales_daily/source_snapshot_id=snapshot-20261022"
    )
    assert render_run_manifest_path(BASE_PATH, "normalize-20261022") == (
        "/data/warehouse/manifests/runs/normalize-20261022.json"
    )
    assert render_normalization_chunks_path(staging) == staging + "/chunks"
    assert render_normalization_chunk_path(staging, 1, 128) == staging + "/chunks/chunk_0001_0128"
    assert render_normalization_chunk_manifest_path(staging, 1, 128) == (
        staging + "/chunk_manifests/chunk_0001_0128.json"
    )
    assert render_normalization_candidate_path(staging) == staging + "/candidate"
    assert render_normalization_quality_path(staging) == staging + "/quality_report.json"


@pytest.mark.parametrize(
    ("source_version", "source_snapshot_id", "pipeline_run_id"),
    [
        ("sha256:bad", "snapshot-1", "run-1"),
        (SOURCE_VERSION, "", "run-1"),
        (SOURCE_VERSION, "snapshot/1", "run-1"),
        (SOURCE_VERSION, "snapshot-1", ""),
        (SOURCE_VERSION, "snapshot-1", "run/1"),
    ],
)
def test_snapshot_requires_explicit_safe_lineage_identifiers(
    source_version: str, source_snapshot_id: str, pipeline_run_id: str
) -> None:
    with pytest.raises(NormalizationSnapshotError):
        validate_normalization_identifiers(
            source_version,
            source_snapshot_id,
            pipeline_run_id,
        )


def test_chunk_plan_is_contiguous_and_covers_full_history() -> None:
    config = NormalizationSnapshotConfig(
        days_per_chunk=128,
        expected_series_count=3,
        expected_day_count=1_941,
    )
    chunks = build_chunk_plan(config, "/data/warehouse/staging/pipeline_run_id=run-1")

    assert chunks[0].day_start == 1
    assert chunks[-1].day_end == 1_941
    assert len(chunks) == 16
    assert [(chunk.day_start, chunk.day_end) for chunk in chunks[-2:]] == [
        (1_793, 1_920),
        (1_921, 1_941),
    ]
    validate_chunk_coverage(chunks, day_start=1, day_end=1_941)


def test_chunk_coverage_rejects_a_gap_or_overlap() -> None:
    config = NormalizationSnapshotConfig(
        days_per_chunk=1,
        expected_series_count=3,
        expected_day_count=4,
    )
    chunks = build_chunk_plan(config, "/data/warehouse/staging/pipeline_run_id=run-1")

    with pytest.raises(NormalizationSnapshotError, match="coverage"):
        validate_chunk_coverage((chunks[0], chunks[2]), day_start=1, day_end=4)

    with pytest.raises(NormalizationSnapshotError, match="coverage"):
        validate_chunk_coverage(
            (chunks[0], chunks[0], chunks[1]),
            day_start=1,
            day_end=4,
        )


def test_snapshot_config_rejects_inconsistent_reference_cardinality() -> None:
    with pytest.raises(ValueError, match="expected_row_count"):
        NormalizationSnapshotConfig(
            days_per_chunk=128,
            expected_series_count=30_490,
            expected_day_count=1_941,
            expected_row_count=1,
        )


def test_snapshot_config_supports_bounded_calibration_scope() -> None:
    config = NormalizationSnapshotConfig(
        days_per_chunk=64,
        expected_series_count=30_490,
        expected_day_count=128,
    )

    assert config.row_count == 30_490 * 128
    assert build_chunk_plan(config, "/data/warehouse/staging/pipeline_run_id=run-1")[-1].day_end == 128


def test_snapshot_persists_revalidates_promotes_and_reuses_without_duplicate_data(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hdfs_root = tmp_path / "hdfs"
    hdfs_root.mkdir()
    hdfs = _LocalHdfs(hdfs_root)
    hdfs_config = HDFSConfig("hdfs://test-namenode:8020", "/data/warehouse")
    sales = _small_sales_frame(spark)
    calendar = _small_calendar_frame(spark)
    _configure_snapshot_inputs(monkeypatch, hdfs, hdfs_config, sales, calendar)
    monkeypatch.setenv("WAREHOUSE_CODE_REVISION", "sha256:" + "b" * 64)
    config = NormalizationSnapshotConfig(
        days_per_chunk=1,
        expected_series_count=2,
        expected_day_count=2,
    )

    first = run_normalization_snapshot(
        spark,
        source_version=SOURCE_VERSION,
        source_snapshot_id="snapshot-20261022",
        pipeline_run_id="normalize-20261022",
        hdfs_config=hdfs_config,
        client=hdfs,
        config=config,
        now=lambda: "2026-10-22T00:00:00Z",
    )

    assert first.status == "VALIDATED"
    assert first.metrics is not None
    assert first.metrics.row_count == 4
    assert first.metrics.unit_sales_total == 3
    assert first.metrics.zero_sales_count == 2
    assert first.timings["total_seconds"] >= 0
    assert any(key.startswith("chunk_") for key in first.timings)
    assert set(first.chunks) == set(build_chunk_plan(config, first.staging_path))
    curated_local = hdfs_root / first.curated_path.lstrip("/")
    persisted = spark.read.option("pathGlobFilter", "*.parquet").parquet(str(curated_local))
    assert persisted.schema.names == [
        "item_id",
        "dept_id",
        "cat_id",
        "store_id",
        "state_id",
        "series_id",
        "day_key",
        "day_index",
        "date",
        "wm_yr_wk",
        "unit_sales",
    ]
    assert "source_snapshot_id" not in persisted.columns
    parquet_files = tuple(curated_local.rglob("*.parquet"))
    assert parquet_files
    assert all(path.parent == curated_local for path in parquet_files)
    assert len(parquet_files) <= 16
    snapshot_manifest = json.loads(
        (curated_local / "_snapshot_manifest.json").read_text(encoding="utf-8")
    )
    assert snapshot_manifest["code_revision"] == "sha256:" + "b" * 64
    assert snapshot_manifest["parquet_layout"] == {
        "compression": "snappy",
        "max_records_per_file": 1_000_000,
        "coarse_partition": "day_index",
        "coarse_partition_bucket_days": 128,
        "coarse_partition_count": 16,
    }
    assert json.loads(
        hdfs.read_remote(first.chunks[0].manifest_path).decode("utf-8")
    )["code_revision"] == "sha256:" + "b" * 64
    run_manifest = json.loads(
        hdfs.read_remote(first.run_manifest_path).decode("utf-8")
    )
    assert run_manifest["code_revision"] == "sha256:" + "b" * 64
    curated_files_before = sorted(
        path.relative_to(curated_local).as_posix() for path in curated_local.rglob("*")
    )

    second = run_normalization_snapshot(
        spark,
        source_version=SOURCE_VERSION,
        source_snapshot_id="snapshot-20261022",
        pipeline_run_id="normalize-20261022",
        hdfs_config=hdfs_config,
        client=hdfs,
        config=config,
        now=lambda: "2026-10-22T00:00:01Z",
    )

    assert second.status == "REUSED"
    assert second.timings["total_seconds"] >= 0
    assert second.curated_path == first.curated_path
    assert curated_files_before == sorted(
        path.relative_to(curated_local).as_posix() for path in curated_local.rglob("*")
    )


def test_deferred_publication_keeps_validated_snapshot_in_run_staging(
    spark, tmp_path, monkeypatch
):
    from warehouse_bigdata.processing import normalization_snapshot as module

    hdfs = _LocalHdfs(tmp_path)
    settings = HDFSConfig("hdfs://test-namenode:8020", BASE_PATH)
    _configure_snapshot_inputs(
        monkeypatch, hdfs, settings, _small_sales_frame(spark), _small_calendar_frame(spark)
    )
    requests = []
    monkeypatch.setattr(module, "stage_publication", lambda *args, **kw: requests.append(args[2]))
    result = run_normalization_snapshot(
        spark, source_version=SOURCE_VERSION, source_snapshot_id="deferred-001",
        pipeline_run_id="deferred-001", hdfs_config=settings, client=hdfs,
        config=NormalizationSnapshotConfig(days_per_chunk=2, expected_series_count=2, expected_day_count=2),
        defer_publication=True,
    )
    assert result.status == "STAGED", result.failure_details
    assert hdfs.exists(render_normalization_candidate_path(result.staging_path))
    assert not hdfs.exists(result.curated_path)
    assert result.run_manifest_path is None
    assert len(requests) == 1 and requests[0]["status"] == "VALIDATED"


def test_retry_quarantines_interrupted_and_orphaned_staging_artifacts(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hdfs_root = tmp_path / "hdfs"
    hdfs_root.mkdir()
    hdfs = _LocalHdfs(hdfs_root)
    hdfs_config = HDFSConfig("hdfs://test-namenode:8020", "/data/warehouse")
    _configure_snapshot_inputs(
        monkeypatch,
        hdfs,
        hdfs_config,
        _small_sales_frame(spark),
        _small_calendar_frame(spark),
    )
    staging_local = hdfs_root / render_staging_path(BASE_PATH, "normalize-interrupted").lstrip("/")
    (staging_local / "chunks" / "chunk_0001_0001").mkdir(parents=True)
    (staging_local / "chunks" / "chunk_0001_0001" / "partial.parquet").write_bytes(b"partial")
    (staging_local / "_tmp" / "candidate-orphan.part").mkdir(parents=True)
    (staging_local / "chunks" / "orphan-chunk").mkdir(parents=True)
    (staging_local / "chunk_manifests" / "orphan.json").parent.mkdir(parents=True, exist_ok=True)
    (staging_local / "chunk_manifests" / "orphan.json").write_text("{}", encoding="utf-8")

    result = run_normalization_snapshot(
        spark,
        source_version=SOURCE_VERSION,
        source_snapshot_id="snapshot-interrupted",
        pipeline_run_id="normalize-interrupted",
        hdfs_config=hdfs_config,
        client=hdfs,
        config=NormalizationSnapshotConfig(
            days_per_chunk=1,
            expected_series_count=2,
            expected_day_count=2,
        ),
        now=lambda: "2026-10-22T00:00:00Z",
    )

    assert result.status == "VALIDATED"
    quarantine = staging_local / "quarantine"
    quarantined = {path.name for path in quarantine.iterdir()}
    assert any("chunk_0001_0001" in name for name in quarantined)
    assert any("candidate-orphan" in name for name in quarantined)
    assert any("orphan-chunk" in name for name in quarantined)
    assert any("orphan.json" in name for name in quarantined)
    assert not (staging_local / "_tmp").exists() or not tuple(
        (staging_local / "_tmp").iterdir()
    )


def test_conflicting_candidate_lineage_is_preserved_and_not_promoted(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hdfs_root = tmp_path / "hdfs"
    hdfs_root.mkdir()
    hdfs = _LocalHdfs(hdfs_root)
    hdfs_config = HDFSConfig("hdfs://test-namenode:8020", "/data/warehouse")
    _configure_snapshot_inputs(
        monkeypatch,
        hdfs,
        hdfs_config,
        _small_sales_frame(spark),
        _small_calendar_frame(spark),
    )
    staging_local = hdfs_root / render_staging_path(BASE_PATH, "normalize-conflict").lstrip("/")
    candidate_local = staging_local / "candidate"
    candidate_local.mkdir(parents=True)
    marker = candidate_local / "_snapshot_manifest.json"
    marker.write_text(
        json.dumps({"source_snapshot_id": "snapshot-owned-by-another-run"}),
        encoding="utf-8",
    )

    result = run_normalization_snapshot(
        spark,
        source_version=SOURCE_VERSION,
        source_snapshot_id="snapshot-conflict",
        pipeline_run_id="normalize-conflict",
        hdfs_config=hdfs_config,
        client=hdfs,
        config=NormalizationSnapshotConfig(
            days_per_chunk=1,
            expected_series_count=2,
            expected_day_count=2,
        ),
        now=lambda: "2026-10-22T00:00:00Z",
    )

    assert result.status == "FAILED"
    assert marker.read_text(encoding="utf-8") == json.dumps(
        {"source_snapshot_id": "snapshot-owned-by-another-run"}
    )
    assert not hdfs.exists(result.curated_path)


def test_code_revision_is_reproducible_without_repository_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from warehouse_bigdata.common import revision

    monkeypatch.delenv("WAREHOUSE_CODE_REVISION", raising=False)
    monkeypatch.chdir(tmp_path)
    revision._package_revision.cache_clear()

    first = revision.get_code_revision()
    second = revision.get_code_revision()

    assert first == second
    assert first.startswith("sha256:")
    assert len(first) == len("sha256:") + 64


def test_failed_raw_validation_publishes_failed_evidence_without_promotion(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from warehouse_bigdata.processing import normalization_snapshot

    hdfs_root = tmp_path / "hdfs"
    hdfs_root.mkdir()
    hdfs = _LocalHdfs(hdfs_root)
    hdfs_config = HDFSConfig("hdfs://test-namenode:8020", "/data/warehouse")
    monkeypatch.setattr(
        normalization_snapshot,
        "validate_hdfs_raw_snapshot",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            NormalizationSnapshotError("raw snapshot is not validated")
        ),
    )

    result = run_normalization_snapshot(
        spark,
        source_version=SOURCE_VERSION,
        source_snapshot_id="snapshot-failed",
        pipeline_run_id="normalize-failed",
        hdfs_config=hdfs_config,
        client=hdfs,
        config=NormalizationSnapshotConfig(
            days_per_chunk=1,
            expected_series_count=2,
            expected_day_count=2,
        ),
    )

    assert result.status == "FAILED"
    assert result.run_manifest_path is None
    assert not hdfs.exists(result.curated_path)
    assert result.quality_report_path is not None
    failure = json.loads(hdfs.read_remote(result.quality_report_path).decode("utf-8"))
    assert failure["status"] == "FAILED"
    assert failure["source_snapshot_id"] == "snapshot-failed"

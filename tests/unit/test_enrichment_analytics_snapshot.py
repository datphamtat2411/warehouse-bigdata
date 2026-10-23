"""Focused checks for the M04 T05 enrichment and analytics snapshot boundary."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType

import pytest

from warehouse_bigdata.common.config import HDFSConfig
from warehouse_bigdata.common.hdfs_paths import (
    render_curated_category_monthly_sales_path,
    render_curated_sales_daily_path,
    render_curated_sales_enriched_path,
    render_curated_store_daily_sales_path,
    render_enrichment_candidate_path,
    render_enrichment_dataset_manifest_path,
    render_enrichment_failure_path,
    render_enrichment_quality_path,
    render_enrichment_staging_binding_path,
    render_run_manifest_path,
    render_staging_path,
)
from warehouse_bigdata.ingestion.hdfs_raw import ValidatedHdfsRawSnapshot
from warehouse_bigdata.processing.enrichment_analytics_snapshot import (
    run_enrichment_analytics_snapshot,
)

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

BASE_PATH = "/data/warehouse"
SOURCE_VERSION = "sha256:" + "a" * 64
SOURCE_SNAPSHOT_ID = "snapshot-20261022"
PIPELINE_RUN_ID = "enrich-20261022"
UPSTREAM_RUN_ID = "normalize-20261022"


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("warehouse-bigdata-enrichment-analytics-snapshot-tests")
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

    def upload_bytes(self, payload: bytes, path: str) -> None:
        local = self._local(path)
        local.parent.mkdir(parents=True, exist_ok=True)
        if local.exists():
            raise FileExistsError(path)
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


class _FailOnceOnDestination(_LocalHdfs):
    def __init__(self, root: Path, destination_fragment: str) -> None:
        super().__init__(root)
        self.destination_fragment = destination_fragment
        self.failed = False

    def rename(self, source: str, destination: str) -> None:
        if self.destination_fragment in destination and not self.failed:
            self.failed = True
            raise OSError(f"simulated interruption before finalizing {destination}")
        super().rename(source, destination)


def _sales_schema() -> StructType:
    return StructType(
        [
            StructField("item_id", StringType(), False),
            StructField("dept_id", StringType(), False),
            StructField("cat_id", StringType(), False),
            StructField("store_id", StringType(), False),
            StructField("state_id", StringType(), False),
            StructField("series_id", StringType(), False),
            StructField("day_key", StringType(), False),
            StructField("day_index", IntegerType(), False),
            StructField("date", DateType(), False),
            StructField("wm_yr_wk", IntegerType(), False),
            StructField("unit_sales", LongType(), False),
        ]
    )


def _sales_frame(spark: SparkSession):
    from datetime import date

    return spark.createDataFrame(
        [
            (
                "item_1",
                "dept_1",
                "cat_1",
                "CA_1",
                "CA",
                "item_1_CA_1",
                "d_1",
                1,
                date(2026, 1, 5),
                2601,
                0,
            ),
            (
                "item_1",
                "dept_1",
                "cat_1",
                "CA_1",
                "CA",
                "item_1_CA_1",
                "d_2",
                2,
                date(2026, 1, 6),
                2601,
                2,
            ),
            (
                "item_2",
                "dept_2",
                "cat_2",
                "TX_1",
                "TX",
                "item_2_TX_1",
                "d_1",
                1,
                date(2026, 1, 5),
                2601,
                4,
            ),
            (
                "item_2",
                "dept_2",
                "cat_2",
                "TX_1",
                "TX",
                "item_2_TX_1",
                "d_2",
                2,
                date(2026, 1, 6),
                2601,
                0,
            ),
        ],
        _sales_schema(),
    )


CALENDAR_HEADER = (
    "date",
    "wm_yr_wk",
    "weekday",
    "wday",
    "month",
    "year",
    "d",
    "event_name_1",
    "event_type_1",
    "event_name_2",
    "event_type_2",
    "snap_CA",
    "snap_TX",
    "snap_WI",
)


def _calendar_csv() -> str:
    rows = (
        ("2026-01-05", "2601", "Monday", "1", "1", "2026", "d_1", "", "", "", "", "1", "0", "0"),
        ("2026-01-06", "2601", "Tuesday", "2", "1", "2026", "d_2", "", "", "", "", "0", "1", "0"),
    )
    return "\n".join(",".join(row) for row in (CALENDAR_HEADER, *rows)) + "\n"


def _prices_csv(*, duplicate: bool = False) -> str:
    rows = [
        ("store_id", "item_id", "wm_yr_wk", "sell_price"),
        ("CA_1", "item_1", "2601", "2.50"),
    ]
    if duplicate:
        rows.append(("CA_1", "item_1", "2601", "2.75"))
    return "\n".join(",".join(row) for row in rows) + "\n"


def _schema_fingerprint(fields: tuple[str, ...]) -> str:
    import hashlib

    return (
        "sha256:"
        + hashlib.sha256(json.dumps(fields, separators=(",", ":")).encode("utf-8")).hexdigest()
    )


def _pipeline_manifest(hdfs_config: HDFSConfig, *, schema_fingerprint: str) -> dict[str, object]:
    return {
        "manifest_schema_version": "1.0",
        "contract_name": "pipeline_run_manifest",
        "contract_version": "1.0",
        "manifest_kind": "pipeline_output",
        "status": "VALIDATED",
        "producer": "warehouse-bigdata:normalization-snapshot",
        "artifact_uri": f"local://{UPSTREAM_RUN_ID}.json",
        "artifact_checksum": None,
        "schema_fingerprint": schema_fingerprint,
        "grain": "One observed item-store-date within one source snapshot.",
        "logical_unique_key": "item_id,store_id,date",
        "row_count": 4,
        "required_field_null_counts": json.dumps({}, separators=(",", ":")),
        "code_revision": "warehouse-bigdata:normalization-snapshot-v1",
        "configuration_digest": "sha256:" + "b" * 64,
        "created_at": "2026-10-22T00:00:00Z",
        "completed_at": "2026-10-22T00:01:00Z",
        "coverage_and_exclusion_counts": json.dumps({}, separators=(",", ":")),
        "pipeline_run_id": UPSTREAM_RUN_ID,
        "source_version": SOURCE_VERSION,
        "output_dataset_names": json.dumps(["sales_daily"], separators=(",", ":")),
        "output_dataset_uris": json.dumps(
            [f"{hdfs_config.uri}{render_curated_sales_daily_path(BASE_PATH, SOURCE_SNAPSHOT_ID)}"],
            separators=(",", ":"),
        ),
        "quality_report_uri": f"{hdfs_config.uri}/data/warehouse/staging/input/quality.json",
        "source_snapshot_id": SOURCE_SNAPSHOT_ID,
    }


def _snapshot_manifest(hdfs_config: HDFSConfig, *, schema_fingerprint: str) -> dict[str, object]:
    return {
        "schema_version": "sales-daily-snapshot-v1",
        "status": "VALIDATED",
        "dataset_name": "sales_daily",
        "dataset_version": "1.0",
        "provenance": "observed_m5_unit_sales",
        "source_version": SOURCE_VERSION,
        "source_snapshot_id": SOURCE_SNAPSHOT_ID,
        "pipeline_run_id": UPSTREAM_RUN_ID,
        "artifact_uri": f"{hdfs_config.uri}{render_curated_sales_daily_path(BASE_PATH, SOURCE_SNAPSHOT_ID)}",
        "schema": [
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
        ],
        "logical_unique_key": ["item_id", "store_id", "date"],
        "row_count": 4,
        "series_count": 2,
        "day_count": 2,
        "unit_sales_total": 6,
        "zero_sales_count": 2,
        "schema_fingerprint": schema_fingerprint,
        "chunk_count": 1,
        "persisted_parquet_validated": True,
        "created_at": "2026-10-22T00:00:00Z",
    }


def _prepare_inputs(
    spark: SparkSession,
    hdfs: _LocalHdfs,
    hdfs_config: HDFSConfig,
    monkeypatch: pytest.MonkeyPatch,
    *,
    duplicate_price: bool = False,
    snapshot_id: str = SOURCE_SNAPSHOT_ID,
) -> None:
    import warehouse_bigdata.processing.enrichment_analytics_snapshot as snapshot_module

    source_schema = _schema_fingerprint(tuple(_sales_schema().names))
    sales_path = render_curated_sales_daily_path(BASE_PATH, snapshot_id)
    sales_local = hdfs._local(sales_path)
    sales_local.parent.mkdir(parents=True, exist_ok=True)
    _sales_frame(spark).write.mode("errorifexists").parquet(str(sales_local))

    raw_path = f"{BASE_PATH}/raw/m5/source_version={SOURCE_VERSION.replace(':', '-')}"
    raw_local = hdfs._local(raw_path)
    raw_local.mkdir(parents=True, exist_ok=True)
    (raw_local / "calendar.csv").write_text(_calendar_csv(), encoding="utf-8")
    (raw_local / "sell_prices.csv").write_text(
        _prices_csv(duplicate=duplicate_price), encoding="utf-8"
    )

    snapshot = _snapshot_manifest(hdfs_config, schema_fingerprint=source_schema)
    snapshot["source_snapshot_id"] = snapshot_id
    snapshot["artifact_uri"] = str(sales_local)
    upstream = _pipeline_manifest(hdfs_config, schema_fingerprint=source_schema)
    upstream["source_snapshot_id"] = snapshot_id
    upstream["output_dataset_uris"] = json.dumps([str(sales_local)], separators=(",", ":"))
    hdfs.upload_bytes(
        (json.dumps(snapshot, separators=(",", ":")) + "\n").encode("utf-8"),
        sales_path + "/_snapshot_manifest.json",
    )
    upstream_path = render_run_manifest_path(BASE_PATH, UPSTREAM_RUN_ID)
    upstream["artifact_uri"] = str(hdfs._local(upstream_path))
    hdfs.upload_bytes(
        (json.dumps(upstream, separators=(",", ":")) + "\n").encode("utf-8"),
        upstream_path,
    )

    raw_snapshot = ValidatedHdfsRawSnapshot(
        status="VALIDATED",
        source_version=SOURCE_VERSION,
        raw_path=raw_path,
        manifests=(
            {
                "source_file": "calendar.csv",
                "status": "VALIDATED",
                "source_version": SOURCE_VERSION,
            },
            {
                "source_file": "sell_prices.csv",
                "status": "VALIDATED",
                "source_version": SOURCE_VERSION,
            },
        ),
        integrity=object(),
    )
    monkeypatch.setattr(
        snapshot_module, "validate_hdfs_raw_snapshot", lambda *args, **kwargs: raw_snapshot
    )
    monkeypatch.setattr(
        snapshot_module,
        "_hdfs_uri",
        lambda _uri, path: str(hdfs._local(path)),
    )

    def publish(manifest, **_kwargs):
        path = render_run_manifest_path(BASE_PATH, str(manifest["pipeline_run_id"]))
        hdfs.upload_bytes(
            (json.dumps(manifest, separators=(",", ":")) + "\n").encode("utf-8"), path
        )
        return path

    monkeypatch.setattr(snapshot_module, "publish_run_manifest", publish)


def _run(
    spark: SparkSession,
    hdfs: _LocalHdfs,
    hdfs_config: HDFSConfig,
    *,
    pipeline_run_id: str = PIPELINE_RUN_ID,
    source_snapshot_id: str = SOURCE_SNAPSHOT_ID,
):
    return run_enrichment_analytics_snapshot(
        spark,
        source_version=SOURCE_VERSION,
        source_snapshot_id=source_snapshot_id,
        pipeline_run_id=pipeline_run_id,
        hdfs_config=hdfs_config,
        client=hdfs,
        now=lambda: "2026-10-22T00:02:00Z",
    )


@pytest.fixture(scope="module")
def j04_upstream_bundle(spark, tmp_path_factory):
    """Actual J03 output with explicit J02 quality and owner-sealed metadata."""
    from warehouse_bigdata.processing import enrichment_analytics_snapshot as module

    patch = pytest.MonkeyPatch()
    hdfs = _LocalHdfs(tmp_path_factory.mktemp("j04-upstream"))
    config = HDFSConfig("hdfs://test-namenode:8020", BASE_PATH)
    _prepare_inputs(spark, hdfs, config, patch)
    result = _run(spark, hdfs, config)
    assert result.status == "VALIDATED", result.failure_details
    j02_path = render_run_manifest_path(BASE_PATH, UPSTREAM_RUN_ID)
    j02 = json.loads(hdfs.read_remote(j02_path))
    quality_path = render_staging_path(BASE_PATH, UPSTREAM_RUN_ID) + "/quality_report.json"
    j02["quality_report_uri"] = str(hdfs._local(quality_path))
    hdfs._local(j02_path).write_text(json.dumps(j02))
    hdfs.upload_bytes(json.dumps({
        "status": "VALIDATED", "source_version": SOURCE_VERSION,
        "source_snapshot_id": SOURCE_SNAPSHOT_ID, "pipeline_run_id": UPSTREAM_RUN_ID,
        "raw_path": result.raw_path, "curated_path": result.sales_daily_path,
        "measured": {"row_count": 4, "unit_sales_total": 6, "zero_sales_count": 2},
    }).encode(), quality_path)
    patch.setattr(module, "_require_sealed_input", lambda *args: None)
    yield hdfs, config
    patch.undo()


def test_j04_public_reader_returns_verified_ordered_sales_and_evidence(spark, j04_upstream_bundle):
    from warehouse_bigdata.processing import enrichment_analytics_snapshot as module

    hdfs, config = j04_upstream_bundle
    frame, evidence = module.read_validated_sales_enriched_snapshot(
        spark, source_version=SOURCE_VERSION, source_snapshot_id=SOURCE_SNAPSHOT_ID,
        pipeline_run_id=PIPELINE_RUN_ID, hdfs_config=config, client=hdfs,
    )
    assert frame.count() == 4
    assert tuple(frame.columns) == module.SALES_ENRICHED.field_names
    assert evidence["pipeline_run_id"] == PIPELINE_RUN_ID
    assert evidence["upstream_pipeline_run_id"] == UPSTREAM_RUN_ID
    assert evidence["metadata_digests"]


def test_j04_public_reader_digests_immutable_raw_manifests(
    spark, j04_upstream_bundle, monkeypatch,
):
    from warehouse_bigdata.processing import enrichment_analytics_snapshot as module

    hdfs, config = j04_upstream_bundle
    original_raw = module.validate_hdfs_raw_snapshot(SOURCE_VERSION, client=hdfs, config=config)
    documents = [
        {**manifest, "serialization_note": "café"} for manifest in original_raw.manifests
    ]
    manifests = tuple(MappingProxyType(document) for document in documents)
    original_items = tuple(tuple(manifest.items()) for manifest in manifests)
    raw = replace(original_raw, manifests=manifests)
    monkeypatch.setattr(module, "validate_hdfs_raw_snapshot", lambda *args, **kwargs: raw)
    canonical_json = (
        json.dumps(documents, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    expected_digest = "sha256:" + hashlib.sha256(canonical_json).hexdigest()

    _, evidence = module.read_validated_sales_enriched_snapshot(
        spark, source_version=SOURCE_VERSION, source_snapshot_id=SOURCE_SNAPSHOT_ID,
        pipeline_run_id=PIPELINE_RUN_ID, hdfs_config=config, client=hdfs,
    )

    assert evidence["raw_manifest_digest"] == expected_digest
    assert raw.manifests is manifests
    assert tuple(tuple(manifest.items()) for manifest in raw.manifests) == original_items
    for original, manifest in zip(manifests, raw.manifests, strict=True):
        assert manifest is original
        assert isinstance(manifest, MappingProxyType)
        with pytest.raises(TypeError):
            manifest["status"] = "FAILED"


@pytest.mark.parametrize("corruption", [
    "marker", "quality", "dataset", "bundle", "producer", "raw", "j02", "type", "source",
])
def test_j04_public_reader_rejects_incomplete_or_cross_bound_upstream(
    spark, j04_upstream_bundle, monkeypatch, corruption,
):
    from warehouse_bigdata.processing import enrichment_analytics_snapshot as module

    hdfs, config = j04_upstream_bundle
    quality = render_enrichment_quality_path(render_staging_path(BASE_PATH, PIPELINE_RUN_ID))
    dataset = render_enrichment_dataset_manifest_path(
        render_curated_sales_enriched_path(BASE_PATH, SOURCE_SNAPSHOT_ID))
    paths = {
        "marker": render_run_manifest_path(BASE_PATH, PIPELINE_RUN_ID),
        "quality": quality, "dataset": dataset,
        "bundle": render_enrichment_dataset_manifest_path(
            render_curated_store_daily_sales_path(BASE_PATH, SOURCE_SNAPSHOT_ID)),
        "producer": dataset, "source": dataset, "type": dataset,
        "raw": quality, "j02": quality,
    }
    target = paths[corruption]
    original = hdfs.read_remote

    def read(path):
        if path != target:
            return original(path)
        if corruption in {"marker", "quality", "dataset", "bundle"}:
            raise FileNotFoundError(path)
        document = json.loads(original(path))
        if corruption == "producer":
            document["pipeline_run_id"] = "wrong-producer"
        elif corruption == "source":
            document["source_version"] = "sha256:" + "f" * 64
        elif corruption == "type":
            document["physical_schema"]["unit_sales"] = "int"
        elif corruption == "raw":
            document["raw_path"] = "/wrong/raw"
        elif corruption == "j02":
            document["input"]["manifest_pipeline_run_id"] = "wrong-j02"
        return json.dumps(document).encode()

    monkeypatch.setattr(hdfs, "read_remote", read)
    with pytest.raises(module.EnrichmentAnalyticsSnapshotError):
        module.read_validated_sales_enriched_snapshot(
            spark, source_version=SOURCE_VERSION, source_snapshot_id=SOURCE_SNAPSHOT_ID,
            pipeline_run_id=PIPELINE_RUN_ID, hdfs_config=config, client=hdfs,
        )


def test_output_paths_are_snapshot_scoped_and_candidates_remain_run_scoped() -> None:
    staging = render_staging_path(BASE_PATH, PIPELINE_RUN_ID)

    assert render_curated_sales_enriched_path(BASE_PATH, SOURCE_SNAPSHOT_ID) == (
        "/data/warehouse/curated/sales_enriched/source_snapshot_id=snapshot-20261022"
    )
    assert render_curated_store_daily_sales_path(BASE_PATH, SOURCE_SNAPSHOT_ID).endswith(
        "/curated/store_daily_sales/source_snapshot_id=snapshot-20261022"
    )
    assert render_curated_category_monthly_sales_path(BASE_PATH, SOURCE_SNAPSHOT_ID).endswith(
        "/curated/category_monthly_sales/source_snapshot_id=snapshot-20261022"
    )
    assert render_enrichment_candidate_path(staging, "sales_enriched").endswith(
        "/candidates/sales_enriched"
    )
    assert render_enrichment_candidate_path(staging, "store_daily_sales").endswith(
        "/candidates/store_daily_sales"
    )
    assert render_enrichment_candidate_path(staging, "category_monthly_sales").endswith(
        "/candidates/category_monthly_sales"
    )
    assert render_enrichment_quality_path(staging).endswith("/quality_report.json")
    assert render_enrichment_failure_path(staging).endswith("/failure_report.json")
    assert render_enrichment_staging_binding_path(staging).endswith("/input_binding.json")
    assert render_enrichment_dataset_manifest_path(
        render_curated_category_monthly_sales_path(BASE_PATH, SOURCE_SNAPSHOT_ID)
    ).endswith("/_dataset_manifest.json")


def test_deferred_publication_keeps_all_three_outputs_in_run_staging(spark, tmp_path, monkeypatch):
    from warehouse_bigdata.processing import enrichment_analytics_snapshot as module

    hdfs = _LocalHdfs(tmp_path)
    settings = HDFSConfig("hdfs://test-namenode:8020", BASE_PATH)
    _prepare_inputs(spark, hdfs, settings, monkeypatch)
    requests = []
    monkeypatch.setattr(module, "stage_publication", lambda *args, **kw: requests.append(args[2]))
    result = run_enrichment_analytics_snapshot(
        spark,
        source_version=SOURCE_VERSION,
        source_snapshot_id=SOURCE_SNAPSHOT_ID,
        pipeline_run_id=PIPELINE_RUN_ID,
        hdfs_config=settings,
        client=hdfs,
        defer_publication=True,
    )
    assert result.status == "STAGED", result.failure_details
    assert result.run_manifest_path is None
    for name, path in result.output_paths.items():
        assert not hdfs.exists(path)
        assert hdfs.exists(render_enrichment_candidate_path(result.staging_path, name))
    assert len(requests) == 1 and requests[0]["status"] == "VALIDATED"


def test_snapshot_persists_revalidates_and_promotes_all_curated_outputs(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hdfs_root = tmp_path / "hdfs"
    hdfs_root.mkdir()
    hdfs = _LocalHdfs(hdfs_root)
    hdfs_config = HDFSConfig("hdfs://test-namenode:8020", BASE_PATH)
    _prepare_inputs(spark, hdfs, hdfs_config, monkeypatch)

    result = _run(spark, hdfs, hdfs_config)

    assert result.status == "VALIDATED", result.failure_details
    assert result.metrics is not None
    assert result.metrics.row_count == 4
    assert result.metrics.unit_sales_total == 6
    assert result.metrics.zero_sales_count == 2
    assert result.timings["total_seconds"] >= 0
    assert result.timings["enrichment_seconds"] >= 0
    assert result.timings["store_aggregation_seconds"] >= 0
    assert result.timings["category_aggregation_seconds"] >= 0
    assert result.run_manifest_path == render_run_manifest_path(BASE_PATH, PIPELINE_RUN_ID)
    assert hdfs.exists(result.curated_path)
    assert hdfs.exists(result.output_paths["store_daily_sales"])
    assert hdfs.exists(result.output_paths["category_monthly_sales"])
    assert result.output_paths["store_daily_sales"] == render_curated_store_daily_sales_path(
        BASE_PATH, SOURCE_SNAPSHOT_ID
    )
    assert result.output_paths["category_monthly_sales"] == (
        render_curated_category_monthly_sales_path(BASE_PATH, SOURCE_SNAPSHOT_ID)
    )
    for name, path in result.output_paths.items():
        assert hdfs.exists(render_enrichment_dataset_manifest_path(path)), name

    enriched = spark.read.option("pathGlobFilter", "*.parquet").parquet(
        str(hdfs._local(result.curated_path))
    )
    assert enriched.count() == 4
    assert enriched.where("price_missing").select("sell_price").first()[0] is None
    assert enriched.where("is_zero_sale").count() == 2
    assert enriched.agg({"unit_sales": "sum"}).first()[0] == 6

    quality = json.loads(hdfs.read_remote(result.quality_report_path).decode("utf-8"))
    assert quality["status"] == "VALIDATED"
    assert quality["outputs"]["category_monthly_sales"]["candidate_only"] is False
    assert quality["outputs"]["category_monthly_sales"]["contract_name"] == (
        "category_monthly_sales"
    )


def test_retry_reuses_immutable_outputs_without_appending_rows(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hdfs_root = tmp_path / "hdfs"
    hdfs_root.mkdir()
    hdfs = _LocalHdfs(hdfs_root)
    hdfs_config = HDFSConfig("hdfs://test-namenode:8020", BASE_PATH)
    _prepare_inputs(spark, hdfs, hdfs_config, monkeypatch)

    first = _run(spark, hdfs, hdfs_config)
    curated_local = hdfs._local(first.curated_path)
    before = sorted(path.relative_to(curated_local).as_posix() for path in curated_local.rglob("*"))
    second = _run(spark, hdfs, hdfs_config)

    assert second.status == "REUSED"
    assert spark.read.option("pathGlobFilter", "*.parquet").parquet(str(curated_local)).count() == 4
    assert before == sorted(
        path.relative_to(curated_local).as_posix() for path in curated_local.rglob("*")
    )


def test_retry_recovers_when_one_output_was_finalized_before_interruption(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hdfs_root = tmp_path / "hdfs"
    hdfs_root.mkdir()
    hdfs = _FailOnceOnDestination(hdfs_root, "/curated/category_monthly_sales/")
    hdfs_config = HDFSConfig("hdfs://test-namenode:8020", BASE_PATH)
    _prepare_inputs(spark, hdfs, hdfs_config, monkeypatch)

    interrupted = _run(spark, hdfs, hdfs_config)

    assert interrupted.status == "FAILED"
    assert interrupted.run_manifest_path is None
    assert hdfs.exists(render_curated_sales_enriched_path(BASE_PATH, SOURCE_SNAPSHOT_ID))
    assert hdfs.exists(render_curated_store_daily_sales_path(BASE_PATH, SOURCE_SNAPSHOT_ID))
    assert not hdfs.exists(
        render_curated_category_monthly_sales_path(BASE_PATH, SOURCE_SNAPSHOT_ID)
    )

    recovered = _run(spark, hdfs, hdfs_config)

    assert recovered.status == "VALIDATED"
    assert recovered.run_manifest_path is not None
    assert hdfs.exists(render_curated_category_monthly_sales_path(BASE_PATH, SOURCE_SNAPSHOT_ID))
    manifest = json.loads(hdfs.read_remote(recovered.run_manifest_path).decode("utf-8"))
    assert json.loads(manifest["output_dataset_uris"]) == [
        str(hdfs._local(render_curated_sales_enriched_path(BASE_PATH, SOURCE_SNAPSHOT_ID))),
        str(hdfs._local(render_curated_store_daily_sales_path(BASE_PATH, SOURCE_SNAPSHOT_ID))),
        str(hdfs._local(render_curated_category_monthly_sales_path(BASE_PATH, SOURCE_SNAPSHOT_ID))),
    ]


def test_retry_rejects_a_curated_dataset_manifest_schema_conflict(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hdfs_root = tmp_path / "hdfs"
    hdfs_root.mkdir()
    hdfs = _LocalHdfs(hdfs_root)
    hdfs_config = HDFSConfig("hdfs://test-namenode:8020", BASE_PATH)
    _prepare_inputs(spark, hdfs, hdfs_config, monkeypatch)
    first = _run(spark, hdfs, hdfs_config)

    manifest_path = render_enrichment_dataset_manifest_path(
        first.output_paths["category_monthly_sales"]
    )
    manifest = json.loads(hdfs.read_remote(manifest_path).decode("utf-8"))
    manifest["schema"][-1] = "wrong_total_units"
    hdfs._local(manifest_path).write_text(json.dumps(manifest), encoding="utf-8")

    retry = _run(spark, hdfs, hdfs_config)

    assert retry.status == "FAILED"
    assert retry.run_manifest_path is None
    assert "dataset manifest" in " ".join(retry.failure_details)


def test_existing_run_manifest_cannot_certify_a_partial_output_set(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hdfs_root = tmp_path / "hdfs"
    hdfs_root.mkdir()
    hdfs = _LocalHdfs(hdfs_root)
    hdfs_config = HDFSConfig("hdfs://test-namenode:8020", BASE_PATH)
    _prepare_inputs(spark, hdfs, hdfs_config, monkeypatch)
    first = _run(spark, hdfs, hdfs_config)

    shutil.rmtree(hdfs._local(first.output_paths["category_monthly_sales"]))
    retry = _run(spark, hdfs, hdfs_config)

    assert retry.status == "FAILED"
    assert retry.run_manifest_path is None
    assert "missing output" in " ".join(retry.failure_details)


def test_mismatched_sales_manifest_and_snapshot_inputs_fail_without_validated_manifest(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hdfs_root = tmp_path / "hdfs"
    hdfs_root.mkdir()
    hdfs = _LocalHdfs(hdfs_root)
    hdfs_config = HDFSConfig("hdfs://test-namenode:8020", BASE_PATH)
    _prepare_inputs(spark, hdfs, hdfs_config, monkeypatch)

    manifest_path = render_curated_sales_daily_path(BASE_PATH, SOURCE_SNAPSHOT_ID)
    manifest = json.loads(
        hdfs.read_remote(manifest_path + "/_snapshot_manifest.json").decode("utf-8")
    )
    manifest["source_version"] = "sha256:" + "c" * 64
    hdfs._local(manifest_path + "/_snapshot_manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    result = _run(spark, hdfs, hdfs_config)

    assert result.status == "FAILED"
    assert result.run_manifest_path is None
    assert not hdfs.exists(result.curated_path)
    assert (
        json.loads(hdfs.read_remote(result.failure_report_path).decode("utf-8"))["status"]
        == "FAILED"
    )


def test_duplicate_price_keys_fail_and_leave_no_promoted_output(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hdfs_root = tmp_path / "hdfs"
    hdfs_root.mkdir()
    hdfs = _LocalHdfs(hdfs_root)
    hdfs_config = HDFSConfig("hdfs://test-namenode:8020", BASE_PATH)
    _prepare_inputs(spark, hdfs, hdfs_config, monkeypatch, duplicate_price=True)

    result = _run(spark, hdfs, hdfs_config)

    assert result.status == "FAILED"
    assert result.run_manifest_path is None
    assert not hdfs.exists(result.curated_path)
    failure = json.loads(hdfs.read_remote(result.failure_report_path).decode("utf-8"))
    assert "price key" in " ".join(failure["failure_details"])


def test_retry_cannot_mix_a_different_snapshot_into_existing_staging(
    spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hdfs_root = tmp_path / "hdfs"
    hdfs_root.mkdir()
    hdfs = _LocalHdfs(hdfs_root)
    hdfs_config = HDFSConfig("hdfs://test-namenode:8020", BASE_PATH)
    _prepare_inputs(spark, hdfs, hdfs_config, monkeypatch)
    first = _run(spark, hdfs, hdfs_config)

    second = _run(
        spark,
        hdfs,
        hdfs_config,
        source_snapshot_id="snapshot-other",
    )

    assert second.status == "FAILED"
    assert second.run_manifest_path is None
    assert hdfs.exists(first.curated_path)
    binding = json.loads(
        hdfs.read_remote(render_enrichment_staging_binding_path(first.staging_path)).decode("utf-8")
    )
    assert binding["source_snapshot_id"] == SOURCE_SNAPSHOT_ID

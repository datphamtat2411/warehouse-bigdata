"""Independent distributed reconciliation for one M04 J02-to-J03 run."""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

from warehouse_bigdata.common.config import HDFSConfig
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.common.hdfs_paths import (
    render_enrichment_dataset_manifest_path,
    render_enrichment_quality_path,
    render_normalization_quality_path,
    render_run_manifest_path,
    render_staging_path,
)
from warehouse_bigdata.contracts.datasets import (
    CATEGORY_MONTHLY_SALES,
    SALES_DAILY,
    SALES_ENRICHED,
    STORE_DAILY_SALES,
)
from warehouse_bigdata.contracts.manifests import PIPELINE_RUN_MANIFEST, VALIDATED
from warehouse_bigdata.contracts.validation import ContractValidationError, validate_manifest
from warehouse_bigdata.ingestion.m5_source import (
    REFERENCE_CALENDAR_DAYS,
    REFERENCE_EVALUATION_DAYS,
    REFERENCE_EVALUATION_SERIES,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


_SALES_METADATA = ("id", "item_id", "dept_id", "cat_id", "store_id", "state_id")
_CALENDAR_FIELDS = (
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
_PRICE_FIELDS = ("store_id", "item_id", "wm_yr_wk", "sell_price")
_CATEGORY_FIELDS = ("source_snapshot_id", "cat_id", "month_start", "total_units")
_MAX_OUTPUT_FILES = 100_000
_SOURCE_VERSION_PATTERN = re.compile(r"\Asha256:[0-9a-f]{64}\Z")
_IDENTIFIER_PATTERN = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_J03_OUTPUT_NAMES = ("sales_enriched", "store_daily_sales", "category_monthly_sales")


class M04VerificationError(RuntimeError):
    """Raised when independent M04 evidence does not reconcile."""


@dataclass(frozen=True, slots=True)
class M04VerificationResult:
    """Bounded scalar reconciliation and storage evidence."""

    status: str
    source_version: str
    source_snapshot_id: str
    metrics: Mapping[str, Mapping[str, object]]
    timings: Mapping[str, float]
    storage: Mapping[str, object]

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "source_version": self.source_version,
            "source_snapshot_id": self.source_snapshot_id,
            "metrics": {name: dict(values) for name, values in self.metrics.items()},
            "timings": dict(self.timings),
            "storage": dict(self.storage),
        }


def run_m04_verification(
    spark: SparkSession,
    *,
    source_version: str,
    source_snapshot_id: str,
    raw_path: str,
    output_paths: Mapping[str, str],
    hdfs_config: HDFSConfig,
    client: HdfsCliClient,
    expected_series_count: int = REFERENCE_EVALUATION_SERIES,
    expected_day_count: int = REFERENCE_EVALUATION_DAYS,
    normalization_run_id: str | None = None,
    enrichment_run_id: str | None = None,
    hash_output_files: bool = False,
) -> M04VerificationResult:
    """Reread raw CSV and persisted Parquet using only distributed scalar checks."""
    _validate_arguments(
        source_version,
        source_snapshot_id,
        raw_path,
        output_paths,
        expected_series_count,
        expected_day_count,
    )
    started_at = time.perf_counter()
    timings: dict[str, float] = {}
    raw_uri = _hdfs_uri(hdfs_config, raw_path)
    selected_days = tuple(f"d_{day}" for day in range(1, expected_day_count + 1))
    output_uris = {name: _hdfs_uri(hdfs_config, path) for name, path in output_paths.items()}
    normalization_manifest = _read_and_validate_run_manifest(
        client,
        hdfs_config,
        normalization_run_id,
        source_version=source_version,
        source_snapshot_id=source_snapshot_id,
    )
    enrichment_manifest = _read_and_validate_run_manifest(
        client,
        hdfs_config,
        enrichment_run_id,
        source_version=source_version,
        source_snapshot_id=source_snapshot_id,
    )
    _validate_pipeline_manifest_outputs(
        client,
        hdfs_config,
        normalization_manifest,
        expected_names=("sales_daily",),
        expected_uris=(output_uris["sales_daily"],),
        expected_quality_path=render_normalization_quality_path(
            render_staging_path(hdfs_config.base_path, normalization_run_id)
        )
        if normalization_run_id is not None
        else None,
        require_complete_bundle=False,
    )
    _validate_pipeline_manifest_outputs(
        client,
        hdfs_config,
        enrichment_manifest,
        expected_names=_J03_OUTPUT_NAMES,
        expected_uris=tuple(output_uris[name] for name in _J03_OUTPUT_NAMES),
        expected_quality_path=render_enrichment_quality_path(
            render_staging_path(hdfs_config.base_path, enrichment_run_id)
        )
        if enrichment_run_id is not None
        else None,
        require_complete_bundle=True,
    )

    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
    )

    reader = spark.read.option("header", True).option("inferSchema", False)
    phase_started = time.perf_counter()
    sales_wide = reader.csv(f"{raw_uri}/sales_train_evaluation.csv")
    calendar = reader.csv(f"{raw_uri}/calendar.csv")
    sell_prices = reader.csv(f"{raw_uri}/sell_prices.csv")
    _check_schema(
        sales_wide,
        _SALES_METADATA + tuple(f"d_{day}" for day in range(1, REFERENCE_EVALUATION_DAYS + 1)),
        (StringType(),) * (len(_SALES_METADATA) + REFERENCE_EVALUATION_DAYS),
        "raw sales source",
    )
    _check_schema(
        calendar, _CALENDAR_FIELDS, (StringType(),) * len(_CALENDAR_FIELDS), "raw calendar"
    )
    _check_schema(sell_prices, _PRICE_FIELDS, (StringType(),) * len(_PRICE_FIELDS), "raw prices")
    raw_series_count = sales_wide.count()
    raw_identifier_nulls = _null_count(sales_wide, _SALES_METADATA)
    raw_duplicate_ids = _duplicate_count(sales_wide, ("id",))
    raw_duplicate_series = _duplicate_count(sales_wide, ("item_id", "store_id"))
    calendar_count = calendar.count()
    calendar_selected_count = (
        calendar.where(F.col("d").isin(list(selected_days))).select("d").distinct().count()
    )
    calendar_duplicate_days = _duplicate_count(calendar, ("d",))
    calendar_duplicate_dates = _duplicate_count(calendar, ("date",))
    price_count = sell_prices.count()
    price_key_count = sell_prices.select(*_PRICE_FIELDS[:3]).distinct().count()
    timings["raw_input_seconds"] = time.perf_counter() - phase_started
    if raw_series_count != expected_series_count:
        raise M04VerificationError(
            f"raw series count mismatch: expected {expected_series_count}, received {raw_series_count}"
        )
    if raw_identifier_nulls or raw_duplicate_ids or raw_duplicate_series:
        raise M04VerificationError("raw sales identifiers are null or duplicated")
    if calendar_count != REFERENCE_CALENDAR_DAYS or calendar_selected_count != expected_day_count:
        raise M04VerificationError("raw calendar coverage does not match the selected scope")
    if calendar_duplicate_days or calendar_duplicate_dates:
        raise M04VerificationError("raw calendar keys are duplicated")
    if price_count != price_key_count:
        raise M04VerificationError("raw sell-price keys are duplicated")

    phase_started = time.perf_counter()
    raw_totals = sales_wide.agg(
        *[F.sum(F.col(day).cast("decimal(38,0)")).alias(f"total_{day}") for day in selected_days],
        *[
            F.sum(F.when(F.col(day).cast("long") == 0, F.lit(1)).otherwise(F.lit(0))).alias(
                f"zero_{day}"
            )
            for day in selected_days
        ],
    ).first()
    raw_unit_sales_total = sum(int(raw_totals[f"total_{day}"] or 0) for day in selected_days)
    raw_zero_sales_count = sum(int(raw_totals[f"zero_{day}"] or 0) for day in selected_days)
    timings["raw_sales_aggregate_seconds"] = time.perf_counter() - phase_started

    phase_started = time.perf_counter()
    sales_daily = _read_parquet(spark, _hdfs_uri(hdfs_config, output_paths["sales_daily"]))
    _check_schema(
        sales_daily,
        SALES_DAILY.field_names,
        (
            StringType(),
            StringType(),
            StringType(),
            StringType(),
            StringType(),
            StringType(),
            StringType(),
            IntegerType(),
            DateType(),
            IntegerType(),
            LongType(),
        ),
        "persisted SALES_DAILY",
    )
    sales_daily_metrics = _sales_metrics(sales_daily, "unit_sales", SALES_DAILY.field_names)
    _validate_sales_daily_manifest(
        client,
        hdfs_config,
        output_paths["sales_daily"],
        source_version=source_version,
        source_snapshot_id=source_snapshot_id,
        pipeline_run_id=normalization_run_id,
        metrics=sales_daily_metrics,
    )
    calendar_lookup = calendar.select(
        F.col("d").alias("calendar_day_key"),
        F.to_date(F.col("date")).alias("calendar_date"),
        F.col("wm_yr_wk").cast("int").alias("calendar_wm_yr_wk"),
    )
    calendar_value_mismatches = (
        sales_daily.alias("sales")
        .join(
            calendar_lookup.alias("calendar"),
            F.col("sales.day_key") == F.col("calendar.calendar_day_key"),
            "left",
        )
        .where(
            F.col("calendar.calendar_day_key").isNull()
            | (F.col("sales.date") != F.col("calendar.calendar_date"))
            | (F.col("sales.wm_yr_wk") != F.col("calendar.calendar_wm_yr_wk"))
        )
        .count()
    )
    raw_series_keys = sales_wide.select(
        F.col("id").alias("raw_series_id"),
        "item_id",
        "store_id",
    ).distinct()
    daily_series_keys = sales_daily.select(
        F.col("series_id").alias("daily_series_id"),
        "item_id",
        "store_id",
    ).distinct()
    raw_series_key_mismatches = (
        raw_series_keys.join(
            daily_series_keys,
            (raw_series_keys.raw_series_id == daily_series_keys.daily_series_id)
            & (raw_series_keys.item_id == daily_series_keys.item_id)
            & (raw_series_keys.store_id == daily_series_keys.store_id),
            "full_outer",
        )
        .where(F.col("raw_series_id").isNull() | F.col("daily_series_id").isNull())
        .count()
    )
    _require_sales_daily_scope(
        sales_daily,
        sales_daily_metrics,
        expected_series_count=expected_series_count,
        expected_day_count=expected_day_count,
        expected_row_count=expected_series_count * expected_day_count,
        selected_days=selected_days,
    )
    if calendar_value_mismatches or raw_series_key_mismatches:
        raise M04VerificationError("SALES_DAILY keys or raw calendar values do not reconcile")
    if sales_daily_metrics["unit_sales_total"] != raw_unit_sales_total:
        raise M04VerificationError("SALES_DAILY unit-sales total differs from raw M5 values")
    if sales_daily_metrics["zero_sales_count"] != raw_zero_sales_count:
        raise M04VerificationError("SALES_DAILY zero-sales preservation differs from raw M5 values")
    timings["sales_daily_reread_seconds"] = time.perf_counter() - phase_started

    phase_started = time.perf_counter()
    sales_enriched = _read_parquet(spark, _hdfs_uri(hdfs_config, output_paths["sales_enriched"]))
    _check_schema(
        sales_enriched,
        SALES_ENRICHED.field_names,
        (
            StringType(),
            StringType(),
            StringType(),
            StringType(),
            StringType(),
            StringType(),
            StringType(),
            IntegerType(),
            DateType(),
            IntegerType(),
            LongType(),
            DoubleType(),
            BooleanType(),
            IntegerType(),
            IntegerType(),
            IntegerType(),
            BooleanType(),
            BooleanType(),
            StringType(),
            StringType(),
            StringType(),
            StringType(),
        ),
        "persisted SALES_ENRICHED",
        partition_fields=("year",),
    )
    price_field = sales_enriched.schema["sell_price"]
    if not price_field.nullable:
        raise M04VerificationError("SALES_ENRICHED sell_price must remain nullable")
    enriched_metrics = _sales_metrics(sales_enriched, "unit_sales", SALES_DAILY.field_names)
    enriched_nulls = _null_count(
        sales_enriched,
        (
            *SALES_DAILY.field_names,
            "price_missing",
            "wday",
            "month",
            "year",
            "snap_applicable",
            "is_zero_sale",
        ),
    )
    missing_prices = sales_enriched.where(F.col("price_missing")).count()
    nullable_prices = sales_enriched.where(F.col("sell_price").isNull()).count()
    price_flag_mismatches = sales_enriched.where(
        F.col("price_missing") != F.col("sell_price").isNull()
    ).count()
    zero_flag_mismatches = sales_enriched.where(
        F.col("is_zero_sale") != (F.col("unit_sales") == F.lit(0))
    ).count()
    price_lookup = sell_prices.select(
        "store_id",
        "item_id",
        F.col("wm_yr_wk").cast("int").alias("price_wm_yr_wk"),
        F.col("sell_price").cast("double").alias("expected_sell_price"),
    ).alias("price_lookup")
    expected_prices = (
        sales_daily.select(
            "item_id",
            "store_id",
            "wm_yr_wk",
            "day_key",
        )
        .alias("sales_daily")
        .join(
            price_lookup,
            (F.col("sales_daily.store_id") == F.col("price_lookup.store_id"))
            & (F.col("sales_daily.item_id") == F.col("price_lookup.item_id"))
            & (F.col("sales_daily.wm_yr_wk") == F.col("price_lookup.price_wm_yr_wk")),
            "left",
        )
        .select(
            F.col("sales_daily.item_id").alias("item_id"),
            F.col("sales_daily.store_id").alias("store_id"),
            F.col("sales_daily.wm_yr_wk").alias("wm_yr_wk"),
            F.col("sales_daily.day_key").alias("day_key"),
            F.col("price_lookup.expected_sell_price").alias("expected_sell_price"),
            F.lit(1).alias("expected_present"),
        )
    )
    actual_prices = sales_enriched.select(
        "item_id",
        "store_id",
        "wm_yr_wk",
        "day_key",
        F.col("sell_price").alias("actual_sell_price"),
        F.col("price_missing").alias("actual_price_missing"),
        F.lit(1).alias("actual_present"),
    )
    price_reconciliation_mismatches = (
        expected_prices.alias("expected")
        .join(
            actual_prices.alias("actual"),
            ["item_id", "store_id", "wm_yr_wk", "day_key"],
            "full_outer",
        )
        .where(
            F.col("expected_present").isNull()
            | F.col("actual_present").isNull()
            | ~F.col("expected_sell_price").eqNullSafe(F.col("actual_sell_price"))
            | (F.col("expected_sell_price").isNull() != F.col("actual_price_missing"))
        )
        .count()
    )
    if (
        enriched_metrics["row_count"] != sales_daily_metrics["row_count"]
        or enriched_metrics["unit_sales_total"] != raw_unit_sales_total
        or enriched_nulls
        or missing_prices != nullable_prices
        or price_flag_mismatches
        or zero_flag_mismatches
        or price_reconciliation_mismatches
        or _duplicate_count(sales_enriched, SALES_ENRICHED.unique_key)
    ):
        raise M04VerificationError(
            "SALES_ENRICHED cardinality, nullability, key, or total reconciliation failed"
        )
    _validate_enriched_manifest(
        client,
        hdfs_config,
        output_paths["sales_enriched"],
        source_version=source_version,
        source_snapshot_id=source_snapshot_id,
        pipeline_run_id=enrichment_run_id,
        physical_schema=_physical_schema(sales_enriched),
        metrics=enriched_metrics,
        nullable_prices=nullable_prices,
    )
    timings["sales_enriched_reread_seconds"] = time.perf_counter() - phase_started

    phase_started = time.perf_counter()
    store_daily = _read_parquet(spark, _hdfs_uri(hdfs_config, output_paths["store_daily_sales"]))
    category_monthly = _read_parquet(
        spark, _hdfs_uri(hdfs_config, output_paths["category_monthly_sales"])
    )
    _check_schema(
        store_daily,
        STORE_DAILY_SALES.field_names,
        (StringType(), StringType(), DateType(), LongType(), LongType()),
        "persisted STORE_DAILY_SALES",
    )
    _check_schema(
        category_monthly,
        _CATEGORY_FIELDS,
        (StringType(), StringType(), DateType(), LongType()),
        "persisted category_monthly_sales",
    )
    if _null_count(store_daily, STORE_DAILY_SALES.field_names) or _null_count(
        category_monthly, _CATEGORY_FIELDS
    ):
        raise M04VerificationError("aggregate outputs contain null key or measure fields")
    store_duplicates = _duplicate_count(store_daily, ("source_snapshot_id", "store_id", "date"))
    category_duplicates = _duplicate_count(
        category_monthly, ("source_snapshot_id", "cat_id", "month_start")
    )
    if store_duplicates or category_duplicates:
        raise M04VerificationError("aggregate output keys are duplicated")
    store_mismatch = _store_reconciliation(store_daily, sales_daily, source_snapshot_id)
    category_mismatch = _category_reconciliation(category_monthly, sales_daily, source_snapshot_id)
    if store_mismatch or category_mismatch:
        raise M04VerificationError(
            f"aggregate reconciliation failed: store={store_mismatch}, category={category_mismatch}"
        )
    store_row_count = store_daily.count()
    store_total = _total(store_daily, "total_units")
    category_row_count = category_monthly.count()
    category_total = _total(category_monthly, "total_units")
    _validate_dataset_manifest(
        client,
        hdfs_config,
        output_paths["store_daily_sales"],
        contract=STORE_DAILY_SALES,
        source_version=source_version,
        source_snapshot_id=source_snapshot_id,
        pipeline_run_id=enrichment_run_id,
        physical_schema=_physical_schema(store_daily),
        metrics={
            "row_count": store_row_count,
            "unit_sales_total": store_total,
            "zero_sales_count": store_daily.where(F.col("total_units") == 0).count(),
        },
    )
    _validate_dataset_manifest(
        client,
        hdfs_config,
        output_paths["category_monthly_sales"],
        contract=CATEGORY_MONTHLY_SALES,
        source_version=source_version,
        source_snapshot_id=source_snapshot_id,
        pipeline_run_id=enrichment_run_id,
        physical_schema=_physical_schema(category_monthly),
        metrics={
            "row_count": category_row_count,
            "unit_sales_total": category_total,
            "zero_sales_count": category_monthly.where(F.col("total_units") == 0).count(),
        },
    )
    timings["aggregate_reread_seconds"] = time.perf_counter() - phase_started

    metrics = {
        "raw": {
            "series_count": raw_series_count,
            "calendar_row_count": calendar_count,
            "selected_day_count": calendar_selected_count,
            "sell_price_row_count": price_count,
            "unit_sales_total": raw_unit_sales_total,
            "zero_sales_count": raw_zero_sales_count,
        },
        "sales_daily": {
            **sales_daily_metrics,
            "expected_row_count": expected_series_count * expected_day_count,
            "unique_key_duplicates": 0,
            "calendar_value_mismatches": calendar_value_mismatches,
            "raw_series_key_mismatches": raw_series_key_mismatches,
        },
        "sales_enriched": {
            **enriched_metrics,
            "nullable_price_count": nullable_prices,
            "matched_price_count": enriched_metrics["row_count"] - nullable_prices,
            "required_null_count": enriched_nulls,
            "price_reconciliation_mismatches": price_reconciliation_mismatches,
            "unique_key_duplicates": 0,
        },
        "store_daily_sales": {
            "row_count": store_row_count,
            "unit_sales_total": store_total,
            "zero_sales_count": store_daily.where(F.col("total_units") == 0).count(),
            "unique_key_duplicates": _duplicate_count(
                store_daily, ("source_snapshot_id", "store_id", "date")
            ),
        },
        "category_monthly_sales": {
            "row_count": category_row_count,
            "unit_sales_total": category_total,
            "zero_sales_count": category_monthly.where(F.col("total_units") == 0).count(),
            "unique_key_duplicates": _duplicate_count(
                category_monthly, ("source_snapshot_id", "cat_id", "month_start")
            ),
        },
        "lineage": {
            "source_version": source_version,
            "source_snapshot_id": source_snapshot_id,
            "normalization_run_id": normalization_run_id,
            "enrichment_run_id": enrichment_run_id,
            "pipeline_manifests_validated": True,
            "dataset_manifests_validated": list(_J03_OUTPUT_NAMES),
        },
    }
    phase_started = time.perf_counter()
    storage = audit_hdfs_outputs(
        client,
        {name: output_paths[name] for name in output_paths},
        include_hashes=hash_output_files,
    )
    timings["storage_audit_seconds"] = time.perf_counter() - phase_started
    timings["total_seconds"] = time.perf_counter() - started_at
    return M04VerificationResult(
        status="VALIDATED",
        source_version=source_version,
        source_snapshot_id=source_snapshot_id,
        metrics=metrics,
        timings=timings,
        storage=storage,
    )


def audit_hdfs_outputs(
    client: HdfsCliClient,
    paths: Mapping[str, str],
    *,
    include_hashes: bool = False,
) -> dict[str, object]:
    """Measure committed Parquet files without reading their rows into the driver."""
    return {
        name: _audit_hdfs_path(client, path, include_hashes=include_hashes)
        for name, path in paths.items()
    }


def _audit_hdfs_path(
    client: HdfsCliClient,
    path: str,
    *,
    include_hashes: bool = False,
) -> dict[str, object]:
    files: list[tuple[str, int]] = []
    file_stats: dict[str, object] = {}
    partitions: set[str] = set()
    pending = [path]
    while pending:
        current = pending.pop()
        for child in client.list_children(current):
            name = child.rsplit("/", 1)[-1]
            if name.startswith("_") or name.startswith("."):
                continue
            if name.endswith(".parquet"):
                stat = client.stat(child)
                files.append((child, stat.size_bytes))
                file_stats[child] = stat
                if len(files) > _MAX_OUTPUT_FILES:
                    raise M04VerificationError("HDFS output file audit exceeded the bounded limit")
            elif client.is_directory(child):
                relative = child.removeprefix(path).strip("/")
                for component in relative.split("/"):
                    if "=" in component:
                        partitions.add(component)
                pending.append(child)
    if not files:
        raise M04VerificationError(f"HDFS output has no Parquet data files: {path}")
    files.sort()
    sizes = sorted(size for _, size in files)
    file_records = []
    for file_path, size in files:
        stat = file_stats[file_path]
        record: dict[str, object] = {
            "path": file_path,
            "bytes": size,
            "replication": stat.replication,
        }
        if include_hashes:
            remote_hash = client.hash_remote(file_path)
            if remote_hash.size_bytes != size:
                raise M04VerificationError(
                    f"HDFS output file changed during hash audit: {file_path}"
                )
            record["sha256"] = remote_hash.sha256
        file_records.append(record)
    return {
        "path": path,
        "file_count": len(files),
        "total_bytes": sum(sizes),
        "average_bytes": sum(sizes) / len(sizes),
        "size_distribution": {
            "min_bytes": sizes[0],
            "max_bytes": sizes[-1],
            "p50_bytes": sizes[(len(sizes) - 1) // 2],
            "p95_bytes": sizes[min(len(sizes) - 1, int((len(sizes) - 1) * 0.95))],
        },
        "partition_layout": sorted(partitions),
        "files": file_records,
    }


def _validate_arguments(
    source_version: str,
    source_snapshot_id: str,
    raw_path: str,
    output_paths: Mapping[str, str],
    expected_series_count: int,
    expected_day_count: int,
) -> None:
    if not isinstance(source_version, str) or not _SOURCE_VERSION_PATTERN.fullmatch(source_version):
        raise M04VerificationError("source_version must identify the validated M5 snapshot")
    if not isinstance(source_snapshot_id, str) or not _IDENTIFIER_PATTERN.fullmatch(
        source_snapshot_id
    ):
        raise M04VerificationError("source_snapshot_id must be nonempty")
    paths = [raw_path, *output_paths.values()]
    if any(
        not isinstance(path, str)
        or not PurePosixPath(path).is_absolute()
        or any(part in {"", ".", ".."} for part in PurePosixPath(path).parts[1:])
        for path in paths
    ):
        raise M04VerificationError("raw_path must be an absolute HDFS path")
    required = {"sales_daily", "sales_enriched", "store_daily_sales", "category_monthly_sales"}
    if set(output_paths) != required or any(
        not isinstance(path, str) for path in output_paths.values()
    ):
        raise M04VerificationError("output paths do not match the independent M04 scope")
    if type(expected_series_count) is not int or expected_series_count < 1:
        raise M04VerificationError("expected_series_count must be positive")
    if (
        type(expected_day_count) is not int
        or not 1 <= expected_day_count <= REFERENCE_EVALUATION_DAYS
    ):
        raise M04VerificationError("expected_day_count must be within the evaluation scope")


def _hdfs_uri(config: HDFSConfig, path: str) -> str:
    return config.uri.rstrip("/") + path


def _read_parquet(spark: SparkSession, uri: str) -> DataFrame:
    try:
        return spark.read.option("pathGlobFilter", "*.parquet").parquet(uri)
    except Exception as error:
        raise M04VerificationError(f"independent Parquet reread failed: {uri}") from error


def _read_json(client: HdfsCliClient, path: str) -> dict[str, object]:
    try:
        value = json.loads(client.read_remote(path).decode("utf-8"))
    except (AttributeError, UnicodeError, json.JSONDecodeError, OSError) as error:
        raise M04VerificationError(f"metadata is not valid JSON: {path}") from error
    if not isinstance(value, dict):
        raise M04VerificationError(f"metadata must be a JSON object: {path}")
    return value


def _read_and_validate_run_manifest(
    client: HdfsCliClient,
    hdfs_config: HDFSConfig,
    pipeline_run_id: str | None,
    *,
    source_version: str,
    source_snapshot_id: str,
) -> dict[str, object]:
    if pipeline_run_id is None:
        raise M04VerificationError("independent M04 verification requires explicit run identities")
    path = render_run_manifest_path(hdfs_config.base_path, pipeline_run_id)
    if not client.exists(path):
        raise M04VerificationError(f"pipeline run manifest is missing: {path}")
    manifest = _read_json(client, path)
    try:
        validate_manifest(
            PIPELINE_RUN_MANIFEST,
            manifest,
            lineage_context={
                "required_status": VALIDATED,
                "pipeline_run_id": pipeline_run_id,
                "source_version": source_version,
                "source_snapshot_id": source_snapshot_id,
            },
        )
    except ContractValidationError as error:
        raise M04VerificationError(
            f"pipeline run manifest is not an independent VALIDATED match: {path}"
        ) from error
    return manifest


def _validate_pipeline_manifest_outputs(
    client: HdfsCliClient,
    hdfs_config: HDFSConfig,
    manifest: Mapping[str, object],
    *,
    expected_names: Sequence[str],
    expected_uris: Sequence[str],
    expected_quality_path: str | None,
    require_complete_bundle: bool,
) -> None:
    names = _json_string_list(manifest.get("output_dataset_names"), "output dataset names")
    uris = _json_string_list(manifest.get("output_dataset_uris"), "output dataset URIs")
    if names != list(expected_names) or uris != list(expected_uris):
        raise M04VerificationError("pipeline manifest output set or order does not match M04 scope")
    if expected_quality_path is not None:
        expected_quality_uri = _hdfs_uri(hdfs_config, expected_quality_path)
        if manifest.get("quality_report_uri") != expected_quality_uri:
            raise M04VerificationError("pipeline manifest quality-report lineage is inconsistent")
        if not client.exists(expected_quality_path):
            raise M04VerificationError(
                f"pipeline quality report is missing: {expected_quality_path}"
            )
    try:
        coverage = json.loads(str(manifest["coverage_and_exclusion_counts"]))
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise M04VerificationError("pipeline manifest coverage metadata is malformed") from error
    if not isinstance(coverage, dict) or coverage.get("persisted_parquet_validated") is not True:
        raise M04VerificationError(
            "pipeline manifest does not certify persisted Parquet validation"
        )
    if require_complete_bundle:
        expected_manifests = {
            name: _hdfs_uri(
                hdfs_config,
                render_enrichment_dataset_manifest_path(_hdfs_path_from_uri(hdfs_config, uri)),
            )
            for name, uri in zip(expected_names, expected_uris, strict=True)
        }
        if coverage.get("publication_complete") is not True:
            raise M04VerificationError(
                "J03 pipeline manifest does not certify complete publication"
            )
        if coverage.get("output_paths_are_curated") is not True:
            raise M04VerificationError("J03 pipeline manifest contains non-curated output paths")
        if coverage.get("output_dataset_manifests") != expected_manifests:
            raise M04VerificationError("J03 dataset-manifest lineage is incomplete")


def _validate_sales_daily_manifest(
    client: HdfsCliClient,
    hdfs_config: HDFSConfig,
    dataset_path: str,
    *,
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str | None,
    metrics: Mapping[str, object],
) -> None:
    path = str(PurePosixPath(dataset_path) / "_snapshot_manifest.json")
    manifest = _read_json(client, path)
    expected = {
        "schema_version": "sales-daily-snapshot-v1",
        "status": VALIDATED,
        "dataset_name": SALES_DAILY.name,
        "dataset_version": SALES_DAILY.version,
        "provenance": SALES_DAILY.provenance,
        "source_version": source_version,
        "source_snapshot_id": source_snapshot_id,
        "artifact_uri": _hdfs_uri(hdfs_config, dataset_path),
        "schema": list(SALES_DAILY.field_names),
        "schema_fingerprint": _schema_fingerprint(SALES_DAILY.field_names),
        "logical_unique_key": list(SALES_DAILY.unique_key),
        "persisted_parquet_validated": True,
        "row_count": metrics["row_count"],
        "series_count": metrics["series_count"],
        "day_count": metrics["day_count"],
        "unit_sales_total": metrics["unit_sales_total"],
        "zero_sales_count": metrics["zero_sales_count"],
    }
    if pipeline_run_id is not None:
        expected["pipeline_run_id"] = pipeline_run_id
    if any(manifest.get(field) != value for field, value in expected.items()):
        raise M04VerificationError(
            "SALES_DAILY snapshot manifest does not match persisted evidence"
        )


def _validate_enriched_manifest(
    client: HdfsCliClient,
    hdfs_config: HDFSConfig,
    dataset_path: str,
    *,
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str | None,
    physical_schema: Mapping[str, str],
    metrics: Mapping[str, object],
    nullable_prices: int,
) -> None:
    matched_prices = metrics["row_count"] - nullable_prices
    _validate_dataset_manifest(
        client,
        hdfs_config,
        dataset_path,
        contract=SALES_ENRICHED,
        source_version=source_version,
        source_snapshot_id=source_snapshot_id,
        pipeline_run_id=pipeline_run_id,
        physical_schema=physical_schema,
        metrics={
            "row_count": metrics["row_count"],
            "unit_sales_total": metrics["unit_sales_total"],
            "zero_sales_count": metrics["zero_sales_count"],
            "matched_price_count": matched_prices,
            "missing_price_count": nullable_prices,
            "price_match_rate": matched_prices / metrics["row_count"],
        },
    )


def _validate_dataset_manifest(
    client: HdfsCliClient,
    hdfs_config: HDFSConfig,
    dataset_path: str,
    *,
    contract,
    source_version: str,
    source_snapshot_id: str,
    pipeline_run_id: str | None,
    physical_schema: Mapping[str, str],
    metrics: Mapping[str, object],
) -> None:
    path = render_enrichment_dataset_manifest_path(dataset_path)
    manifest = _read_json(client, path)
    expected = {
        "schema_version": "enrichment-dataset-manifest-v1",
        "status": VALIDATED,
        "dataset_name": contract.name,
        "dataset_version": contract.version,
        "provenance": contract.provenance,
        "source_version": source_version,
        "source_snapshot_id": source_snapshot_id,
        "artifact_uri": _hdfs_uri(hdfs_config, dataset_path),
        "schema": list(contract.field_names),
        "schema_fingerprint": _schema_fingerprint(contract.field_names),
        "physical_schema": dict(physical_schema),
        "logical_unique_key": list(contract.unique_key),
        "persisted_parquet_validated": True,
    }
    if pipeline_run_id is not None:
        expected["pipeline_run_id"] = pipeline_run_id
    expected.update(metrics)
    if any(manifest.get(field) != value for field, value in expected.items()):
        raise M04VerificationError(
            f"{contract.name} dataset manifest does not match independent Parquet reread"
        )


def _json_string_list(value: object, label: str) -> list[str]:
    if not isinstance(value, str):
        raise M04VerificationError(f"{label} must be a JSON string array")
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as error:
        raise M04VerificationError(f"{label} is malformed JSON") from error
    if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
        raise M04VerificationError(f"{label} must be a JSON string array")
    return parsed


def _schema_fingerprint(fields: Sequence[str]) -> str:
    return (
        "sha256:"
        + hashlib.sha256(
            json.dumps(tuple(fields), separators=(",", ":")).encode("utf-8")
        ).hexdigest()
    )


def _hdfs_path_from_uri(config: HDFSConfig, uri: str) -> str:
    prefix = config.uri.rstrip("/")
    if not uri.startswith(prefix + "/"):
        raise M04VerificationError(f"URI is outside the configured HDFS endpoint: {uri}")
    return uri.removeprefix(prefix)


def _physical_schema(dataframe: DataFrame) -> dict[str, str]:
    return {field.name: field.dataType.simpleString() for field in dataframe.schema.fields}


def _check_schema(
    dataframe: DataFrame,
    fields: Sequence[str],
    types: Sequence[object],
    label: str,
    *,
    partition_fields: Sequence[str] = (),
) -> None:
    # Spark appends discovered partition fields after the file's physical fields.
    partitioned_fields = tuple(field for field in fields if field not in partition_fields) + tuple(
        partition_fields
    )
    if tuple(dataframe.columns) not in (tuple(fields), partitioned_fields):
        raise M04VerificationError(
            f"{label} fields differ: expected {tuple(fields)}, received {tuple(dataframe.columns)}"
        )
    for name, expected in zip(fields, types, strict=True):
        field = dataframe.schema[name]
        if field.dataType != expected:
            raise M04VerificationError(
                f"{label} field {field.name} type differs: expected {expected}, received {field.dataType}"
            )


def _null_count(dataframe: DataFrame, fields: Sequence[str]) -> int:
    from pyspark.sql import functions as F

    row = dataframe.agg(
        F.sum(
            sum(
                (F.when(F.col(field).isNull(), F.lit(1)).otherwise(F.lit(0)) for field in fields),
                F.lit(0),
            )
        ).alias("null_count")
    ).first()
    return int(row["null_count"] or 0)


def _duplicate_count(dataframe: DataFrame, fields: Sequence[str]) -> int:
    return dataframe.groupBy(*fields).count().where("count > 1").count()


def _total(dataframe: DataFrame, field: str) -> int:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    row = dataframe.agg(F.sum(F.col(field).cast(DecimalType(38, 0))).alias("total")).first()
    return int(row["total"] or 0)


def _sales_metrics(
    dataframe: DataFrame, total_field: str, fields: Sequence[str]
) -> dict[str, object]:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    row = dataframe.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct("series_id").alias("series_count"),
        F.countDistinct("day_key").alias("day_count"),
        F.sum(F.col(total_field).cast(DecimalType(38, 0))).alias("unit_sales_total"),
        F.sum(F.when(F.col(total_field) == 0, F.lit(1)).otherwise(F.lit(0))).alias(
            "zero_sales_count"
        ),
        F.sum(
            sum(
                (F.when(F.col(field).isNull(), F.lit(1)).otherwise(F.lit(0)) for field in fields),
                F.lit(0),
            )
        ).alias("null_count"),
    ).first()
    return {
        "row_count": int(row["row_count"] or 0),
        "series_count": int(row["series_count"] or 0),
        "day_count": int(row["day_count"] or 0),
        "unit_sales_total": int(row["unit_sales_total"] or 0),
        "zero_sales_count": int(row["zero_sales_count"] or 0),
        "null_count": int(row["null_count"] or 0),
    }


def _require_sales_daily_scope(
    dataframe: DataFrame,
    metrics: Mapping[str, object],
    *,
    expected_series_count: int,
    expected_day_count: int,
    expected_row_count: int,
    selected_days: Sequence[str],
) -> None:
    if (
        metrics["row_count"] != expected_row_count
        or metrics["series_count"] != expected_series_count
        or metrics["day_count"] != expected_day_count
        or metrics["null_count"]
        or _duplicate_count(dataframe, SALES_DAILY.unique_key)
    ):
        raise M04VerificationError("SALES_DAILY row, coverage, null, or key checks failed")
    from pyspark.sql import functions as F

    unexpected_days = (
        dataframe.select("day_key")
        .distinct()
        .where(~F.col("day_key").isin(list(selected_days)))
        .count()
    )
    incomplete_days = (
        dataframe.groupBy("day_key").count().where(F.col("count") != expected_series_count).count()
    )
    if unexpected_days or incomplete_days:
        raise M04VerificationError("SALES_DAILY selected day coverage is incomplete")


def _store_reconciliation(
    store_daily: DataFrame, sales_daily: DataFrame, source_snapshot_id: str
) -> int:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    if store_daily.where(F.col("source_snapshot_id") != source_snapshot_id).count():
        raise M04VerificationError("STORE_DAILY_SALES source snapshot identity differs")
    expected = sales_daily.groupBy("store_id", "date").agg(
        F.sum(F.col("unit_sales").cast(DecimalType(38, 0))).alias("expected_total_units"),
        F.countDistinct("series_id").alias("expected_series_count"),
    )
    actual = store_daily.select(
        "store_id",
        "date",
        F.col("total_units").cast(DecimalType(38, 0)).alias("actual_total_units"),
        F.col("observed_series_count").alias("actual_series_count"),
    )
    joined = expected.join(actual, ["store_id", "date"], "full_outer")
    return joined.where(
        F.col("expected_total_units").isNull()
        | F.col("actual_total_units").isNull()
        | (F.col("expected_total_units") != F.col("actual_total_units"))
        | (F.col("expected_series_count") != F.col("actual_series_count"))
    ).count()


def _category_reconciliation(
    category_monthly: DataFrame,
    sales_daily: DataFrame,
    source_snapshot_id: str,
) -> int:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    if category_monthly.where(F.col("source_snapshot_id") != source_snapshot_id).count():
        raise M04VerificationError("category_monthly_sales source snapshot identity differs")
    expected = (
        sales_daily.withColumn("month_start", F.trunc(F.col("date"), "month"))
        .groupBy("cat_id", "month_start")
        .agg(F.sum(F.col("unit_sales").cast(DecimalType(38, 0))).alias("expected_total_units"))
    )
    actual = category_monthly.select(
        "cat_id",
        "month_start",
        F.col("total_units").cast(DecimalType(38, 0)).alias("actual_total_units"),
    )
    joined = expected.join(actual, ["cat_id", "month_start"], "full_outer")
    return joined.where(
        F.col("expected_total_units").isNull()
        | F.col("actual_total_units").isNull()
        | (F.col("expected_total_units") != F.col("actual_total_units"))
    ).count()


__all__ = [
    "M04VerificationError",
    "M04VerificationResult",
    "audit_hdfs_outputs",
    "run_m04_verification",
]

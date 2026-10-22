"""Independent distributed reconciliation for one M04 J02-to-J03 run."""

from __future__ import annotations

import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

from warehouse_bigdata.common.config import HDFSConfig
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.contracts.datasets import SALES_DAILY, SALES_ENRICHED, STORE_DAILY_SALES
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
    _check_schema(calendar, _CALENDAR_FIELDS, (StringType(),) * len(_CALENDAR_FIELDS), "raw calendar")
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
        *[
            F.sum(F.col(day).cast("decimal(38,0)")).alias(f"total_{day}")
            for day in selected_days
        ],
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
    raw_series_key_mismatches = raw_series_keys.join(
        daily_series_keys,
        (raw_series_keys.raw_series_id == daily_series_keys.daily_series_id)
        & (raw_series_keys.item_id == daily_series_keys.item_id)
        & (raw_series_keys.store_id == daily_series_keys.store_id),
        "full_outer",
    ).where(
        F.col("raw_series_id").isNull() | F.col("daily_series_id").isNull()
    ).count()
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
    )
    price_field = sales_enriched.schema["sell_price"]
    if not price_field.nullable:
        raise M04VerificationError("SALES_ENRICHED sell_price must remain nullable")
    enriched_metrics = _sales_metrics(sales_enriched, "unit_sales", SALES_DAILY.field_names)
    enriched_nulls = _null_count(
        sales_enriched,
        (*SALES_DAILY.field_names, "price_missing", "wday", "month", "year", "snap_applicable", "is_zero_sale"),
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
    expected_prices = sales_daily.select(
        "item_id",
        "store_id",
        "wm_yr_wk",
        "day_key",
    ).alias("sales_daily").join(
        price_lookup,
        (F.col("sales_daily.store_id") == F.col("price_lookup.store_id"))
        & (F.col("sales_daily.item_id") == F.col("price_lookup.item_id"))
        & (F.col("sales_daily.wm_yr_wk") == F.col("price_lookup.price_wm_yr_wk")),
        "left",
    ).select(
        F.col("sales_daily.item_id").alias("item_id"),
        F.col("sales_daily.store_id").alias("store_id"),
        F.col("sales_daily.wm_yr_wk").alias("wm_yr_wk"),
        F.col("sales_daily.day_key").alias("day_key"),
        F.col("price_lookup.expected_sell_price").alias("expected_sell_price"),
        F.lit(1).alias("expected_present"),
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
            | (
                F.col("expected_sell_price").isNull() != F.col("actual_price_missing")
            )
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
        raise M04VerificationError("SALES_ENRICHED cardinality, nullability, key, or total reconciliation failed")
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
            "row_count": store_daily.count(),
            "unit_sales_total": _total(store_daily, "total_units"),
            "unique_key_duplicates": _duplicate_count(
                store_daily, ("source_snapshot_id", "store_id", "date")
            ),
        },
        "category_monthly_sales": {
            "row_count": category_monthly.count(),
            "unit_sales_total": _total(category_monthly, "total_units"),
            "unique_key_duplicates": _duplicate_count(
                category_monthly, ("source_snapshot_id", "cat_id", "month_start")
            ),
        },
    }
    phase_started = time.perf_counter()
    storage = audit_hdfs_outputs(
        client,
        {name: output_paths[name] for name in output_paths},
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
) -> dict[str, object]:
    """Measure committed Parquet files without reading their rows into the driver."""
    return {name: _audit_hdfs_path(client, path) for name, path in paths.items()}


def _audit_hdfs_path(client: HdfsCliClient, path: str) -> dict[str, object]:
    files: list[tuple[str, int]] = []
    partitions: set[str] = set()
    pending = [path]
    while pending:
        current = pending.pop()
        for child in client.list_children(current):
            name = child.rsplit("/", 1)[-1]
            if name.startswith("_") or name.startswith("."):
                continue
            if name.endswith(".parquet"):
                files.append((child, client.stat(child).size_bytes))
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
    sizes = sorted(size for _, size in files)
    return {
        "path": path,
        "file_count": len(files),
        "total_bytes": sum(sizes),
        "size_distribution": {
            "min_bytes": sizes[0],
            "max_bytes": sizes[-1],
            "p50_bytes": sizes[(len(sizes) - 1) // 2],
            "p95_bytes": sizes[min(len(sizes) - 1, int((len(sizes) - 1) * 0.95))],
        },
        "partition_layout": sorted(partitions),
        "files": [{"path": file_path, "bytes": size} for file_path, size in files],
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
    if not isinstance(source_snapshot_id, str) or not _IDENTIFIER_PATTERN.fullmatch(source_snapshot_id):
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
    if type(expected_day_count) is not int or not 1 <= expected_day_count <= REFERENCE_EVALUATION_DAYS:
        raise M04VerificationError("expected_day_count must be within the evaluation scope")


def _hdfs_uri(config: HDFSConfig, path: str) -> str:
    return config.uri.rstrip("/") + path


def _read_parquet(spark: SparkSession, uri: str) -> DataFrame:
    try:
        return spark.read.option("pathGlobFilter", "*.parquet").parquet(uri)
    except Exception as error:
        raise M04VerificationError(f"independent Parquet reread failed: {uri}") from error


def _check_schema(dataframe: DataFrame, fields: Sequence[str], types: Sequence[object], label: str) -> None:
    if tuple(dataframe.columns) != tuple(fields):
        raise M04VerificationError(
            f"{label} fields differ: expected {tuple(fields)}, received {tuple(dataframe.columns)}"
        )
    for field, expected in zip(dataframe.schema.fields, types, strict=True):
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


def _sales_metrics(dataframe: DataFrame, total_field: str, fields: Sequence[str]) -> dict[str, object]:
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
    incomplete_days = dataframe.groupBy("day_key").count().where(F.col("count") != expected_series_count).count()
    if unexpected_days or incomplete_days:
        raise M04VerificationError("SALES_DAILY selected day coverage is incomplete")


def _store_reconciliation(store_daily: DataFrame, sales_daily: DataFrame, source_snapshot_id: str) -> int:
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
    expected = sales_daily.withColumn("month_start", F.trunc(F.col("date"), "month")).groupBy(
        "cat_id", "month_start"
    ).agg(F.sum(F.col("unit_sales").cast(DecimalType(38, 0))).alias("expected_total_units"))
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

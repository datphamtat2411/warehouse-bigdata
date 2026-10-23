"""Distributed J04 smoke readback with a bounded, independent Python oracle."""

from __future__ import annotations

import hashlib
import json
import math
import subprocess
from datetime import date, timedelta
from functools import reduce
from operator import or_
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HDFS = "hdfs://hdfs-namenode:8020"
MASTER = "spark://spark-master:7077"
SERIES_COUNT = 30_490
SOURCE_COUNT = SERIES_COUNT * 30
FEATURE_COUNT = SERIES_COUNT * 2
FIRST_DATE = date(2011, 1, 29)
SERIES_KEY = ["item_id", "store_id"]
# Freeze v1.0 independently of T01 and the publisher's schema helper.
FEATURE_SCHEMA = (
    ("source_snapshot_id", "string"),
    ("feature_version", "string"),
    ("feature_role", "string"),
    ("item_id", "string"),
    ("store_id", "string"),
    ("dept_id", "string"),
    ("target_date", "date"),
    ("as_of_date", "date"),
    ("lag_1", "bigint"),
    ("lag_7", "bigint"),
    ("lag_28", "bigint"),
    ("rolling_mean_7", "double"),
    ("rolling_mean_28", "double"),
    ("day_of_week", "int"),
    ("month", "int"),
    ("is_weekend", "boolean"),
    ("event_flag", "boolean"),
    ("price_missing", "boolean"),
    ("last_known_price", "double"),
    ("label", "bigint"),
)


def owner(code, *args):
    return command(
        [
            "docker",
            "compose",
            "-f",
            str(ROOT / "infrastructure/compose.yaml"),
            "--profile",
            "spark-submit",
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "--pull",
            "never",
            "--user",
            "1000:100",
            "--entrypoint",
            "/opt/warehouse-app/.venv/bin/python",
            "spark-submit",
            "-c",
            code,
            *args,
        ]
    )


def command(args):
    result = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, timeout=1200)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


def submit(source_version, snapshot, upstream, version, run):
    return command(
        [
            "bash",
            "scripts/spark-submit.sh",
            "--conf",
            "spark.sql.shuffle.partitions=16",
            "--conf",
            "spark.sql.adaptive.enabled=true",
            "--",
            "warehouse_bigdata/jobs/build_forecast_features.py",
            "--source-version",
            source_version,
            "--source-snapshot-id",
            snapshot,
            "--upstream-pipeline-run-id",
            upstream,
            "--feature-version",
            version,
            "--pipeline-run-id",
            run,
            "--hdfs-uri",
            HDFS,
            "--spark-master",
            MASTER,
            "--event-log-dir",
            f"{HDFS}/data/warehouse/staging/pipeline_run_id={run}/events",
        ]
    )


def independent_probe(source_version, snapshot, upstream, version, run, *, full=True):
    name = "warehouse-j04-readback-" + run[-16:]
    output = command(
        [
            "docker",
            "compose",
            "-f",
            str(ROOT / "infrastructure/compose.yaml"),
            "--profile",
            "spark-submit",
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "--pull",
            "never",
            "--name",
            name,
            "--env",
            f"_WAREHOUSE_SPARK_DRIVER_HOST={name}",
            "--volume",
            f"{Path(__file__).resolve()}:/opt/warehouse-app/src/feature_snapshot_probe.py:ro",
            "spark-submit",
            "--conf",
            "spark.sql.shuffle.partitions=16",
            "--",
            "feature_snapshot_probe.py",
            source_version,
            snapshot,
            upstream,
            version,
            run,
            "full" if full else "integrity",
        ]
    )
    return json.loads(
        next(
            line.removeprefix("J04_READBACK=")
            for line in output.splitlines()
            if line.startswith("J04_READBACK=")
        )
    )


def _require(document, expected, label):
    differences = {
        key: {"expected": value, "actual": document.get(key)}
        for key, value in expected.items()
        if document.get(key) != value
    }
    assert not differences, f"{label}: {differences}"


def _no_rows(frame, message):
    # Only bounded diagnostics may reach the driver, even on a corrupt artifact.
    sample = frame.limit(4).collect()
    assert not sample, f"{message}; samples={[row.asDict() for row in sample]}"


def _publication_evidence(client, config, source_version, snapshot, upstream, version, run):
    from warehouse_bigdata.common.hdfs_paths import (
        render_curated_sales_enriched_path,
        render_forecast_features_path,
        render_run_manifest_path,
        render_staging_path,
    )
    from warehouse_bigdata.common.hdfs_publication import (
        file_integrity,
        validate_feature_publication_evidence,
    )

    def read(path):
        return json.loads(client.read_remote(path))

    source_path = render_curated_sales_enriched_path(config.base_path, snapshot)
    path = render_forecast_features_path(config.base_path, snapshot, version)
    identity = {"source_version": source_version, "source_snapshot_id": snapshot}
    source_marker = read(render_run_manifest_path(config.base_path, upstream))
    _require(
        source_marker,
        {
            **identity,
            "pipeline_run_id": upstream,
            "status": "VALIDATED",
            "row_count": SOURCE_COUNT,
            "producer": "warehouse-bigdata:enrichment-analytics-snapshot-v1",
        },
        "Selected M04 smoke J03 completion",
    )
    outputs = dict(
        zip(
            json.loads(source_marker["output_dataset_names"]),
            json.loads(source_marker["output_dataset_uris"]),
            strict=True,
        )
    )
    assert outputs.get("sales_enriched") == config.uri + source_path, outputs
    source_sidecar = read(source_path + "/_dataset_manifest.json")
    _require(
        source_sidecar,
        {
            **identity,
            "pipeline_run_id": upstream,
            "status": "VALIDATED",
            "row_count": SOURCE_COUNT,
            "artifact_uri": config.uri + source_path,
        },
        "Persisted source lineage",
    )
    source_staging = render_staging_path(config.base_path, upstream)
    source_quality = read(source_staging + "/quality_report.json")
    _require(
        source_quality,
        {**identity, "pipeline_run_id": upstream, "status": "VALIDATED", "failure_details": []},
        "Selected J03 quality",
    )
    _require(
        source_quality["outputs"]["sales_enriched"],
        {
            **identity,
            "pipeline_run_id": upstream,
            "status": "VALIDATED",
            "row_count": SOURCE_COUNT,
            "persisted_parquet_validated": True,
            "candidate_only": False,
            "schema_fingerprint": source_sidecar["schema_fingerprint"],
        },
        "Selected J03 persisted source quality",
    )
    _require(
        read(source_staging + "/input_binding.json"),
        {**identity, "pipeline_run_id": upstream},
        "Selected J03 input binding",
    )
    marker_bytes = client.read_remote(render_run_manifest_path(config.base_path, run))
    marker = json.loads(marker_bytes)
    _require(
        marker,
        {
            **identity,
            "pipeline_run_id": run,
            "status": "VALIDATED",
            "row_count": FEATURE_COUNT,
            "producer": "warehouse-bigdata:feature-snapshot",
        },
        "J04 completion",
    )
    validate_feature_publication_evidence(client, config, marker, path)
    sidecar_bytes = client.read_remote(path + "/_dataset_manifest.json")
    sidecar = json.loads(sidecar_bytes)
    coverage = json.loads(marker["coverage_and_exclusion_counts"])
    producer = coverage["producing_pipeline_run_id"]
    _require(
        sidecar,
        {
            **identity,
            "feature_version": version,
            "upstream_pipeline_run_id": upstream,
            "pipeline_run_id": producer,
            "status": "VALIDATED",
            "row_count": FEATURE_COUNT,
            "artifact_uri": config.uri + path,
            "provenance": "derived_leakage_controlled_features",
        },
        "Published feature lineage",
    )
    schema = [
        {"name": name, "type": kind, "nullable": name in ("last_known_price", "label")}
        for name, kind in FEATURE_SCHEMA
    ]
    assert sidecar["schema"] == schema, "Frozen logical feature schema differs"
    binding = read(render_staging_path(config.base_path, run) + "/input_binding.json")
    _require(
        binding,
        {**identity, "upstream_pipeline_run_id": upstream, "feature_version": version},
        "J04 input binding",
    )
    _require(
        binding["upstream_evidence"],
        {
            **identity,
            "pipeline_run_id": upstream,
            "row_count": SOURCE_COUNT,
            "upstream_pipeline_run_id": source_quality["input"]["manifest_pipeline_run_id"],
        },
        "J04 recorded J03 evidence",
    )
    return (
        source_path,
        path,
        {
            "row_count": marker["row_count"],
            "producer": producer,
            "lineage": {
                **identity,
                "upstream_pipeline_run_id": upstream,
                "feature_version": version,
            },
            "file_integrity": file_integrity(client, path),
            "sidecar_sha256": hashlib.sha256(sidecar_bytes).hexdigest(),
            "completion_sha256": hashlib.sha256(marker_bytes).hexdigest(),
        },
    )


def _verify_source(source):
    from pyspark.sql import functions as F

    from warehouse_bigdata.contracts.datasets import SALES_ENRICHED

    invalid = reduce(
        or_,
        [
            *[F.col(name).isNull() for name in SALES_ENRICHED.required_field_names],
            ~F.col("day_index").between(1, 30),
            ~F.col("day_key").eqNullSafe(F.concat(F.lit("d_"), F.col("day_index"))),
            ~F.col("date").eqNullSafe(F.date_add(F.lit(FIRST_DATE), F.col("day_index") - 1)),
            F.col("unit_sales") < 0,
            ~F.col("price_missing").eqNullSafe(F.col("sell_price").isNull()),
            ~F.col("is_zero_sale").eqNullSafe(F.col("unit_sales") == 0),
            F.col("wday") != F.pmod(F.dayofweek("date"), F.lit(7)) + 1,
            F.col("month") != F.month("date"),
            F.col("year") != F.year("date"),
        ],
    )
    summary = (
        source.agg(
            F.count("*").alias("row_count"),
            F.countDistinct("day_index").alias("days"),
            F.min("day_index").alias("first_day"),
            F.max("day_index").alias("last_day"),
            F.count(F.when(invalid, 1)).alias("invalid_rows"),
        )
        .first()
        .asDict()
    )
    _require(
        summary,
        {"row_count": SOURCE_COUNT, "days": 30, "first_day": 1, "last_day": 30, "invalid_rows": 0},
        "Persisted source must be exactly the d_1–d_30 smoke scope",
    )
    series = source.groupBy(*SERIES_KEY).agg(
        F.count("*").alias("observations"),
        F.countDistinct("date").alias("dates"),
        F.countDistinct("day_index").alias("days"),
        F.countDistinct("dept_id").alias("departments"),
        F.max((F.col("unit_sales") == 0).cast("int")).alias("has_zero"),
        F.max(F.col("price_missing").cast("int")).alias("has_missing_price"),
        F.min(F.when(F.col("sell_price").isNotNull(), F.col("day_index"))).alias("first_price_day"),
    )
    scope = (
        series.agg(
            F.count("*").alias("series_count"),
            F.count(
                F.when(
                    (F.col("observations") != 30)
                    | (F.col("dates") != 30)
                    | (F.col("days") != 30)
                    | (F.col("departments") != 1),
                    1,
                )
            ).alias("invalid_series"),
        )
        .first()
        .asDict()
    )
    _require(
        scope, {"series_count": SERIES_COUNT, "invalid_series": 0}, "Source series scope/grain"
    )
    return series, {**summary, **scope}


def _verify_features(source, features, snapshot, version):
    from pyspark.sql import functions as F

    actual_schema = tuple((field.name, field.dataType.simpleString()) for field in features.schema)
    assert actual_schema == FEATURE_SCHEMA, f"Frozen physical feature schema: {actual_schema}"
    targets = [FIRST_DATE + timedelta(days=day - 1) for day in (29, 30)]
    weekday = F.pmod(F.dayofweek("target_date") + 5, F.lit(7)) + 1
    invalid = reduce(
        or_,
        [
            *[F.col(name).isNull() for name, _ in FEATURE_SCHEMA if name != "last_known_price"],
            F.col("source_snapshot_id") != snapshot,
            F.col("feature_version") != version,
            F.col("feature_role") != "train",
            ~F.col("target_date").isin(targets),
            F.col("as_of_date") != F.date_sub("target_date", 1),
            F.col("day_of_week") != weekday,
            F.col("month") != F.month("target_date"),
            F.col("is_weekend") != weekday.isin(6, 7),
            F.col("event_flag"),
            ~F.col("price_missing").eqNullSafe(F.col("last_known_price").isNull()),
            *[F.col(name) < 0 for name in ("lag_1", "lag_7", "lag_28", "label")],
            *[
                (F.col(name) < 0) | F.isnan(name) | (F.abs(F.col(name)) > 1.7976931348623157e308)
                for name in ("rolling_mean_7", "rolling_mean_28")
            ],
            F.col("last_known_price").isNotNull()
            & (
                (F.col("last_known_price") <= 0)
                | F.isnan("last_known_price")
                | (F.abs(F.col("last_known_price")) > 1.7976931348623157e308)
            ),
        ],
    )
    summary = (
        features.agg(
            F.count("*").alias("row_count"),
            F.count(F.when(invalid, 1)).alias("invalid_rows"),
            *[
                F.count(F.when(F.col("target_date") == target, 1)).alias(f"d_{day}")
                for day, target in zip((29, 30), targets, strict=True)
            ],
            F.count(F.when(F.col("price_missing"), 1)).alias("price_missing_rows"),
        )
        .first()
        .asDict()
    )
    _require(
        summary,
        {"row_count": FEATURE_COUNT, "invalid_rows": 0, "d_29": SERIES_COUNT, "d_30": SERIES_COUNT},
        "Published smoke feature scope/invariants",
    )
    scoped_key = [
        "source_snapshot_id",
        "feature_version",
        "feature_role",
        *SERIES_KEY,
        "target_date",
    ]
    _no_rows(features.groupBy(*scoped_key).count().where("count != 1"), "Duplicate scoped keys")
    _no_rows(
        features.groupBy(*SERIES_KEY)
        .agg(
            F.count("*").alias("rows"),
            F.countDistinct("target_date").alias("dates"),
        )
        .where("rows != 2 OR dates != 2"),
        "Each eligible series must have two target dates",
    )
    observed = source.where(F.col("day_index").isin(29, 30)).select(
        *SERIES_KEY,
        F.col("date").alias("target_date"),
        F.col("dept_id").alias("source_dept"),
        "unit_sales",
    )
    # Full outer reconciliation also catches missing eligible series/targets and extras.
    joined = features.join(observed, [*SERIES_KEY, "target_date"], "full")
    _no_rows(
        joined.where(
            F.col("feature_version").isNull()
            | F.col("unit_sales").isNull()
            | ~F.col("dept_id").eqNullSafe(F.col("source_dept"))
            | ~F.col("label").eqNullSafe(F.col("unit_sales"))
        ),
        "Eligible key coverage or observed label/department mismatch",
    )
    return summary


def _python_oracle(source, features, series, snapshot, version):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    # One representative from each observed combination (at most eight), including
    # zero-sale, source missing-price and as-of missing-price cases where available.
    categorized = series.withColumn(
        "asof_missing", F.col("first_price_day").isNull() | (F.col("first_price_day") > 28)
    )
    categories = ["has_zero", "has_missing_price", "asof_missing"]
    sample = (
        categorized.withColumn(
            "_rank", F.row_number().over(Window.partitionBy(*categories).orderBy(*SERIES_KEY))
        )
        .where("_rank = 1")
        .orderBy(*SERIES_KEY)
        .limit(8)
        .select(*SERIES_KEY)
    )
    histories = (
        source.join(F.broadcast(sample), SERIES_KEY, "inner")
        .orderBy(*SERIES_KEY, "date")
        .limit(241)
        .collect()
    )
    assert 0 < len(histories) <= 240, "Oracle source collection exceeded eight 30-day series"
    grouped = {}
    for row in histories:
        grouped.setdefault((row.item_id, row.store_id), {})[row.date] = row
    assert len(histories) == 30 * len(grouped) and len(grouped) <= 8
    selected = source.sparkSession.createDataFrame(sorted(grouped), SERIES_KEY)
    actual = features.join(F.broadcast(selected), SERIES_KEY, "inner").limit(17).collect()
    assert len(actual) == len(grouped) * 2 <= 16, "Oracle feature sample coverage differs"
    expected = {}
    for key, history in grouped.items():
        assert set(history) == {FIRST_DATE + timedelta(days=i) for i in range(30)}, key
        for day in (29, 30):
            target = FIRST_DATE + timedelta(days=day - 1)
            row = history[target]
            prior = [history[target - timedelta(days=i)] for i in range(1, 29)]
            known = [
                history[d].sell_price
                for d in sorted(history)
                if d < target and history[d].sell_price is not None
            ]
            price = known[-1] if known else None
            expected[*key, target] = {
                "source_snapshot_id": snapshot,
                "feature_version": version,
                "feature_role": "train",
                "item_id": row.item_id,
                "store_id": row.store_id,
                "dept_id": row.dept_id,
                "target_date": target,
                "as_of_date": target - timedelta(days=1),
                "lag_1": prior[0].unit_sales,
                "lag_7": prior[6].unit_sales,
                "lag_28": prior[27].unit_sales,
                "rolling_mean_7": sum(r.unit_sales for r in prior[:7]) / 7,
                "rolling_mean_28": sum(r.unit_sales for r in prior) / 28,
                "day_of_week": target.isoweekday(),
                "month": target.month,
                "is_weekend": target.isoweekday() in (6, 7),
                "event_flag": False,
                "price_missing": price is None,
                "last_known_price": price,
                "label": row.unit_sales,
            }
    for row in actual:
        key = (row.item_id, row.store_id, row.target_date)
        wanted = expected.pop(key)
        for field, value in row.asDict().items():
            correct = (
                math.isclose(value, wanted[field], rel_tol=1e-12, abs_tol=1e-12)
                if field in ("rolling_mean_7", "rolling_mean_28")
                else value == wanted[field]
            )
            assert correct, f"Oracle {key} {field}: expected {wanted[field]!r}, got {value!r}"
    assert not expected, f"Oracle missing targets: {list(expected)}"
    return {
        "series": [list(key) for key in sorted(grouped)],
        "source_rows": len(histories),
        "feature_rows": len(actual),
        "has_zero_sales": any(row.unit_sales == 0 for row in histories),
        "has_source_price_missing": any(row.sell_price is None for row in histories),
        "has_asof_price_missing": any(row.price_missing for row in actual),
    }


def probe(source_version, snapshot, upstream, version, run, mode):
    """Validate the full persisted snapshot once; bind later probes by exact bytes."""
    from pyspark.sql import SparkSession

    from warehouse_bigdata.common.config import load_hdfs_config
    from warehouse_bigdata.common.spark_hdfs_client import SparkHdfsCliClient

    config = load_hdfs_config()
    client = SparkHdfsCliClient()
    spark = SparkSession.builder.master(MASTER).appName("j04-independent-readback").getOrCreate()
    try:
        source_path, path, evidence = _publication_evidence(
            client, config, source_version, snapshot, upstream, version, run
        )
        evidence["application_id"] = spark.sparkContext.applicationId
        if mode == "full":

            def read(path):
                return (
                    spark.read.option("ignoreCorruptFiles", "false")
                    .option("ignoreMissingFiles", "false")
                    .parquet(config.uri + path)
                )

            source, features = read(source_path), read(path)
            series, evidence["source_scope"] = _verify_source(source)
            evidence["feature_scope"] = _verify_features(source, features, snapshot, version)
            evidence["oracle"] = _python_oracle(source, features, series, snapshot, version)
        else:
            assert mode == "integrity", f"Unknown probe mode: {mode}"
        return evidence
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys

    print("J04_READBACK=" + json.dumps(probe(*sys.argv[1:]), sort_keys=True))

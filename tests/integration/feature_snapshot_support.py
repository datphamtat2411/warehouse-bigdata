"""Bounded, independently submitted J04 readback and fixture oracle."""

from __future__ import annotations

import json
import subprocess
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HDFS = "hdfs://hdfs-namenode:8020"
MASTER = "spark://spark-master:7077"


def owner(code, *args):
    return command([
        "docker", "compose", "-f", str(ROOT / "infrastructure/compose.yaml"),
        "--profile", "spark-submit", "run", "--rm", "--no-deps", "-T", "--pull", "never",
        "--user", "1000:100", "--entrypoint", "/opt/warehouse-app/.venv/bin/python",
        "spark-submit", "-c", code, *args,
    ])


def command(args):
    result = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, timeout=1200)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


def submit(source_version, snapshot, upstream, version, run):
    return command([
        "bash", "scripts/spark-submit.sh", "--conf", "spark.sql.shuffle.partitions=16",
        "--conf", "spark.sql.adaptive.enabled=true", "--",
        "warehouse_bigdata/jobs/build_forecast_features.py",
        "--source-version", source_version, "--source-snapshot-id", snapshot,
        "--upstream-pipeline-run-id", upstream, "--feature-version", version,
        "--pipeline-run-id", run, "--hdfs-uri", HDFS, "--spark-master", MASTER,
        "--event-log-dir", f"{HDFS}/data/warehouse/staging/pipeline_run_id={run}/events",
    ])


def independent_probe(snapshot, version, run):
    name = "warehouse-j04-readback-" + run[-16:]
    output = command([
        "docker", "compose", "-f", str(ROOT / "infrastructure/compose.yaml"),
        "--profile", "spark-submit", "run", "--rm", "--no-deps", "-T", "--pull", "never",
        "--name", name, "--env", f"_WAREHOUSE_SPARK_DRIVER_HOST={name}",
        "--volume", f"{Path(__file__).resolve()}:/opt/warehouse-app/src/feature_snapshot_probe.py:ro",
        "spark-submit", "--conf", "spark.sql.shuffle.partitions=16", "--",
        "feature_snapshot_probe.py", snapshot, version, run,
    ])
    return json.loads(next(line.removeprefix("J04_READBACK=") for line in output.splitlines()
                           if line.startswith("J04_READBACK=")))


def probe(snapshot, version, run):
    """Full bounded fixture comparison to a Python date-window oracle, without T01."""
    from pyspark.sql import SparkSession
    from warehouse_bigdata.common.config import load_hdfs_config
    from warehouse_bigdata.common.hdfs_paths import render_forecast_features_path
    from warehouse_bigdata.common.spark_hdfs_client import SparkHdfsCliClient
    from warehouse_bigdata.forecasting.feature_snapshot import file_integrity

    config = load_hdfs_config()
    client = SparkHdfsCliClient()
    spark = SparkSession.builder.master(MASTER).appName("j04-independent-readback").getOrCreate()
    try:
        marker = json.loads(client.read_remote(f"/data/warehouse/manifests/runs/{run}.json"))
        assert marker["status"] == "VALIDATED"
        path = render_forecast_features_path(config.base_path, snapshot, version)
        source_path = f"/data/warehouse/curated/sales_enriched/source_snapshot_id={snapshot}"
        source = spark.read.parquet(HDFS + source_path).limit(10001).collect()
        assert 0 < len(source) <= 10000, "Explicit live fixture must be bounded to 10000 observations"
        source_map = {(r.item_id, r.store_id, r.date): r for r in source}
        expected = {}
        for row in source:
            if row.day_index < 29:
                continue
            prior = [source_map.get((row.item_id, row.store_id, row.date - timedelta(days=i)))
                     for i in range(1, 29)]
            if any(r is None for r in prior):
                continue
            role = "train" if row.day_index <= 1885 else (
                "validation" if row.day_index <= 1913 else "test")
            known = [r for r in source if r.item_id == row.item_id and r.store_id == row.store_id
                     and r.date < row.date and r.sell_price is not None]
            price = max(known, key=lambda r: r.date).sell_price if known else None
            expected[row.item_id, row.store_id, row.date] = {
                "source_snapshot_id": snapshot, "feature_version": version, "feature_role": role,
                "item_id": row.item_id, "store_id": row.store_id, "dept_id": row.dept_id,
                "target_date": row.date, "as_of_date": row.date - timedelta(days=1),
                "lag_1": prior[0].unit_sales, "lag_7": prior[6].unit_sales,
                "lag_28": prior[27].unit_sales,
                "rolling_mean_7": sum(r.unit_sales for r in prior[:7]) / 7,
                "rolling_mean_28": sum(r.unit_sales for r in prior) / 28,
                "day_of_week": row.date.isoweekday(), "month": row.date.month,
                "is_weekend": row.date.isoweekday() in (6, 7), "event_flag": False,
                "price_missing": price is None, "last_known_price": price, "label": row.unit_sales,
            }
        actual = spark.read.option("ignoreCorruptFiles", "false").option(
            "ignoreMissingFiles", "false").parquet(HDFS + path).limit(10001).collect()
        assert len(actual) == len(expected) > 0
        for row in actual:
            assert row.asDict() == expected.pop((row.item_id, row.store_id, row.target_date))
        assert not expected
        return {"application_id": spark.sparkContext.applicationId, "row_count": len(actual),
                "file_integrity": file_integrity(client, path),
                "producer": json.loads(client.read_remote(path + "/_dataset_manifest.json"))[
                    "pipeline_run_id"]}
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys
    print("J04_READBACK=" + json.dumps(probe(*sys.argv[1:]), sort_keys=True))

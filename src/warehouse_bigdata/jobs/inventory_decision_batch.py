"""Read selected M5 Small artifacts and emit a bounded forward decision summary."""

import argparse
import json

from warehouse_bigdata.common.config import load_hdfs_config
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.common.hdfs_paths import validate_pipeline_run_id
from warehouse_bigdata.common.scope_evidence import SCOPED_METADATA_LIMIT
from warehouse_bigdata.common.small_artifacts import load_small_artifacts, validate_small_artifacts
from warehouse_bigdata.common.small_preflight import validate_small_publications
from warehouse_bigdata.optimization.decision_batch import build_decision_batch
from warehouse_bigdata.optimization.decision_inputs import plain, read_decision_inputs


def run(spark, *, decision_run_id, client, config):
    inputs = read_decision_inputs(spark, client=client, config=config)
    result = build_decision_batch(inputs, decision_run_id=decision_run_id)
    metadata = result.decision_manifest_inputs
    return dict(decision_run_id=decision_run_id, scenario_id=metadata["scenario_id"],
        issue_date=metadata["issue_date"], dataset_scope="demo_sample",
        selection_digest=inputs.selector["selection_digest"], coverage=plain(inputs.coverage),
        row_counts={name: spec["row_count"] for m in (
            result.scenario_manifest_inputs, metadata) for name, spec in m["datasets"].items()},
        configuration_digest=metadata["configuration_digest"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--decision-run-id", required=True)
    parser.add_argument("--spark-master", default="spark://spark-master:7077")
    args = parser.parse_args()
    validate_pipeline_run_id(args.decision_run_id)
    config = load_hdfs_config()
    client = HdfsCliClient(metadata_limit=SCOPED_METADATA_LIMIT)
    selector = load_small_artifacts()
    validate_small_artifacts(selector, through="j06")
    validate_small_publications(selector, through="j06", client=client, config=config)
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.master(args.spark_master)
        .appName("warehouse-inventory-decision-" + args.decision_run_id)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        print("INVENTORY_DECISION_BATCH=" + json.dumps(run(spark,
            decision_run_id=args.decision_run_id, client=client, config=config), sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

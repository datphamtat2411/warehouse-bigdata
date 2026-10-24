"""Read-only verification of explicitly identified sealed J07 bundles."""

import argparse
import json

from warehouse_bigdata.common.config import load_hdfs_config
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.common.hdfs_paths import (
    render_decision_output_path,
    render_evaluation_output_path,
)
from warehouse_bigdata.common.scope_evidence import SCOPED_METADATA_LIMIT
from warehouse_bigdata.optimization.inventory_artifacts import verify_inventory_bundle


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    identities = parser.add_mutually_exclusive_group(required=True)
    identities.add_argument("--decision-run-id")
    identities.add_argument("--evaluation-run-id")
    parser.add_argument("--spark-master", default="spark://spark-master:7077")
    args = parser.parse_args()
    config = load_hdfs_config()
    renderer, identity = (
        (render_decision_output_path, args.decision_run_id)
        if args.decision_run_id
        else (render_evaluation_output_path, args.evaluation_run_id)
    )
    path = renderer(config.base_path, identity)
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master(args.spark_master)
        .appName("warehouse-verify-inventory-" + identity)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        manifest = verify_inventory_bundle(
            spark,
            client=HdfsCliClient(metadata_limit=SCOPED_METADATA_LIMIT),
            config=config,
            path=path,
        )
        print(
            json.dumps(
                dict(
                    status=manifest["status"],
                    artifact_uri=manifest["artifact_uri"],
                    artifact_checksum=manifest["artifact_checksum"],
                ),
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

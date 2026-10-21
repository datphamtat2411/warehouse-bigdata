"""Minimal Spark Standalone read verification for one immutable M5 raw path."""

from __future__ import annotations

import argparse
import json
import re
from urllib.parse import urlsplit

from warehouse_bigdata.common.config import ConfigurationError, load_hdfs_config, load_spark_config
from warehouse_bigdata.contracts.manifests import SOURCE_MANIFEST, VALIDATED
from warehouse_bigdata.contracts.validation import ContractValidationError, validate_manifest

_SOURCE_VERSION = re.compile(r"\Asha256:[0-9a-f]{64}\Z")
RECORD_PREFIX = "WAREHOUSE_M5_RAW "


def validate_request(raw_uri: str, source_version: str, hdfs_uri: str) -> None:
    """Reject local paths or a different HDFS endpoint before importing PySpark."""
    if not _SOURCE_VERSION.fullmatch(source_version):
        raise ConfigurationError("source_version must be a content-derived SHA-256 identity.")
    supplied = urlsplit(raw_uri)
    expected = urlsplit(hdfs_uri)
    if (
        supplied.scheme.lower() != "hdfs"
        or not supplied.netloc
        or supplied.query
        or supplied.fragment
        or not supplied.path.startswith("/")
        or (supplied.scheme, supplied.netloc) != (expected.scheme, expected.netloc)
    ):
        raise ConfigurationError("raw_uri must be an absolute URI on the configured HDFS endpoint.")
    physical_version = source_version.replace(":", "-")
    if supplied.path.rstrip("/").rsplit("/", 1)[-1] != f"source_version={physical_version}":
        raise ConfigurationError("raw_uri must identify the supplied source_version raw snapshot.")


def run_verification(raw_uri: str, source_version: str) -> dict[str, object]:
    """Read actual HDFS metadata and the sales header through Spark Standalone."""
    hdfs_config = load_hdfs_config()
    spark_config = load_spark_config()
    if not spark_config.master_url.startswith("spark://"):
        raise ConfigurationError("M5 raw verification requires a Spark Standalone master URL.")
    validate_request(raw_uri, source_version, hdfs_config.uri)

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master(spark_config.master_url)
        .appName("warehouse-m5-raw-hdfs-verification")
        .getOrCreate()
    )
    try:
        context = spark.sparkContext
        if context.master != spark_config.master_url or not context.master.startswith("spark://"):
            raise RuntimeError("Spark verification did not run on the configured Standalone master.")
        hadoop = context._jsc.hadoopConfiguration()
        configuration = {
            key: hadoop.get(key)
            for key in (
                "fs.defaultFS",
                "dfs.replication",
                "dfs.client.use.datanode.hostname",
            )
        }
        expected_configuration = {
            "fs.defaultFS": hdfs_config.uri,
            "dfs.replication": "2",
            "dfs.client.use.datanode.hostname": "true",
        }
        if configuration != expected_configuration:
            raise RuntimeError(f"effective Hadoop configuration mismatch: {configuration!r}")

        manifest_rows = spark.read.text(f"{raw_uri.rstrip('/')}/source_manifests.json").collect()
        manifest_payload = "\n".join(row.value for row in manifest_rows)
        try:
            source_manifests = json.loads(manifest_payload)
        except (UnicodeError, json.JSONDecodeError) as error:
            raise RuntimeError("stored source manifest is not valid UTF-8 JSON") from error
        if not isinstance(source_manifests, list) or not source_manifests:
            raise RuntimeError("stored source manifest must be a non-empty JSON array")

        filenames = []
        for manifest in source_manifests:
            if not isinstance(manifest, dict):
                raise RuntimeError("stored source manifest contains a non-object record")
            try:
                validate_manifest(
                    SOURCE_MANIFEST,
                    manifest,
                    lineage_context={"required_status": VALIDATED, "source_version": source_version},
                )
            except ContractValidationError as error:
                raise RuntimeError(f"stored source manifest failed Contract v1 validation: {error}") from error
            filename = manifest["source_file"]
            if not isinstance(filename, str) or filename in filenames:
                raise RuntimeError("stored source manifest contains duplicate source files")
            expected_uri = f"{raw_uri.rstrip('/')}/{filename}"
            if manifest["artifact_uri"] != expected_uri:
                raise RuntimeError(f"stored source manifest URI mismatch for {filename}")
            filenames.append(filename)

        header_rows = (
            spark.read.text(f"{raw_uri.rstrip('/')}/sales_train_evaluation.csv")
            .limit(1)
            .collect()
        )
        if not header_rows:
            raise RuntimeError("sales_train_evaluation.csv has no readable header")
        sales_header = header_rows[0].value
        if not sales_header.startswith("id,item_id,dept_id,cat_id,store_id,state_id,d_1"):
            raise RuntimeError("sales_train_evaluation.csv header is not the M5 sales header")
        return {
            "status": VALIDATED,
            "raw_uri": raw_uri,
            "source_version": source_version,
            "source_manifest_files": sorted(filenames),
            "sales_header": sales_header,
            "hdfs_configuration": configuration,
            "spark_master": context.master,
        }
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-uri", required=True, metavar="HDFS_URI")
    parser.add_argument("--source-version", required=True, metavar="SHA256_ID")
    args = parser.parse_args()
    result = run_verification(args.raw_uri, args.source_version)
    print(RECORD_PREFIX + json.dumps(result, ensure_ascii=True, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()

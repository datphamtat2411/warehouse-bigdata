"""J08 orchestration; existing sealed artifacts only, no upstream generation."""

from pathlib import Path

from warehouse_bigdata.common.config import (
    load_clickhouse_admin_config,
    load_clickhouse_publisher_config,
    load_hdfs_config,
    load_paths_config,
)
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.common.scope_evidence import SCOPED_METADATA_LIMIT
from warehouse_bigdata.common.small_artifacts import load_small_artifacts
from warehouse_bigdata.publication.batch import (
    ClickHouseServing,
    preflight_publication_inputs,
    publish_offline_reload,
    validate_offline_gate,
)


def run_publish_clickhouse(
    *, selector_path=None, decision_run_id, decision_pipeline_run_id,
    policy_evaluation_run_id, policy_evaluation_pipeline_run_id, forecast_evaluation_run_id,
    batch_rows=2048, offline_reload=False, confirm_offline_reload=False,
):
    """Validate the explicit small selection, then replace and approve offline."""
    validate_offline_gate(offline_reload, confirm_offline_reload)
    selector = load_small_artifacts(selector_path)
    hdfs = load_hdfs_config()
    paths = load_paths_config()
    publisher_config = load_clickhouse_publisher_config()
    admin_config = load_clickhouse_admin_config()
    if (publisher_config.host, publisher_config.port, publisher_config.database) != (
        admin_config.host, admin_config.port, admin_config.database
    ):
        raise ValueError("publisher and admin must select the same ClickHouse coordinator/database")
    import clickhouse_connect
    try:
        from pyspark.sql import SparkSession
    except ImportError as error:
        raise RuntimeError("J07 sealed-bundle verification requires the existing pinned Spark runtime") from error

    def connect(config):
        return clickhouse_connect.get_client(
            host=config.host, port=config.port, database=config.database,
            username=config.user, password=config.password,
        )

    client = HdfsCliClient(metadata_limit=SCOPED_METADATA_LIMIT)
    # Existing J07 verification is bounded to 200 rows/dataset and requires Spark;
    # this session only reads selected sealed bundles, never recomputes artifacts.
    spark = (SparkSession.builder.master("local[1]").appName("warehouse-j08-sealed-readback")
             .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC")
             .config("spark.hadoop.fs.defaultFS", hdfs.uri).getOrCreate())
    try:
        selection = preflight_publication_inputs(
            selector, decision_run_id=decision_run_id, decision_pipeline_run_id=decision_pipeline_run_id,
            policy_evaluation_run_id=policy_evaluation_run_id,
            policy_evaluation_pipeline_run_id=policy_evaluation_pipeline_run_id,
            forecast_evaluation_run_id=forecast_evaluation_run_id, client=client, config=hdfs, spark=spark,
        )
    finally:
        spark.stop()
    publisher = connect(publisher_config)
    try:
        admin = connect(admin_config)
        try:
            return publish_offline_reload(
                selection, client=client,
                serving=ClickHouseServing(publisher, admin, publisher_config.database),
                scratch_dir=paths.scratch_dir,
                application_path=Path("artifacts/approved_publication.json"),
                batch_rows=batch_rows, offline_reload=offline_reload,
                confirm_offline_reload=confirm_offline_reload, base_path=hdfs.base_path,
            )
        finally:
            admin.close()
    finally:
        publisher.close()

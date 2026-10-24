"""Bounded historical policy evaluation and explicit J07 artifact staging."""

from warehouse_bigdata.optimization.decision_inputs import (
    read_historical_inputs,
    read_historical_labels,
)
from warehouse_bigdata.optimization.policy_evaluation import (
    evaluate_frozen_policies,
    freeze_policy_decisions,
)


def run_inventory_policy_evaluation(
    spark, *, client, config, evaluation_run_id, proposed_decision_run_id, baseline_decision_run_id
):
    inputs = read_historical_inputs(spark, client=client, config=config)
    frozen = freeze_policy_decisions(
        inputs,
        proposed_decision_run_id=proposed_decision_run_id,
        baseline_decision_run_id=baseline_decision_run_id,
    )
    labels = read_historical_labels(spark, frozen=frozen, client=client, config=config)
    return evaluate_frozen_policies(frozen, labels, evaluation_run_id=evaluation_run_id)


def persist(
    spark,
    *,
    client,
    config,
    pipeline_run_id,
    evaluation_run_id,
    proposed_decision_run_id,
    baseline_decision_run_id,
    code_revision,
    effective_configuration,
    expected_args=None,
):
    from warehouse_bigdata.jobs.inventory_decision_batch import validate_cli_binding
    from warehouse_bigdata.optimization.decision_inputs import plain
    from warehouse_bigdata.optimization.inventory_artifacts import persist_policy_evaluation_bundle

    inputs = read_historical_inputs(spark, client=client, config=config)
    frozen = freeze_policy_decisions(
        inputs,
        proposed_decision_run_id=proposed_decision_run_id,
        baseline_decision_run_id=baseline_decision_run_id,
    )
    labels = list(read_historical_labels(spark, frozen=frozen, client=client, config=config))
    result = evaluate_frozen_policies(frozen, labels, evaluation_run_id=evaluation_run_id)
    if expected_args is not None:
        from warehouse_bigdata.common.hdfs_namespace import HdfsNamespaceError

        if not frozen.inventory_rows:
            raise HdfsNamespaceError("policy publication requires one frozen scenario identity")
        metadata = {
            **plain(result.metadata),
            "scenario_id": frozen.inventory_rows[0]["scenario_id"],
            "proposed_decision_run_id": proposed_decision_run_id,
            "baseline_decision_run_id": baseline_decision_run_id,
        }
        validate_cli_binding(
            expected_args, inputs, metadata, effective_configuration, kind="evaluation"
        )
    return persist_policy_evaluation_bundle(
        spark,
        client=client,
        config=config,
        pipeline_run_id=pipeline_run_id,
        frozen=frozen,
        labels=labels,
        result=result,
        code_revision=code_revision,
        effective_configuration=effective_configuration,
    )


def main():
    import json

    from pyspark.sql import SparkSession

    from warehouse_bigdata.common.config import load_hdfs_config
    from warehouse_bigdata.common.hdfs_client import HdfsCliClient
    from warehouse_bigdata.common.scope_evidence import SCOPED_METADATA_LIMIT
    from warehouse_bigdata.jobs.inventory_decision_batch import cli_configuration, inventory_parser

    args = inventory_parser("evaluation").parse_args()
    config = load_hdfs_config()
    effective = cli_configuration(args, config)
    client = HdfsCliClient(metadata_limit=SCOPED_METADATA_LIMIT)
    builder = (
        SparkSession.builder.master(args.spark_master)
        .appName("warehouse-inventory-policy-evaluation-" + args.pipeline_run_id)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if args.event_log_dir:
        builder = builder.config("spark.eventLog.enabled", "true").config(
            "spark.eventLog.dir", args.event_log_dir
        )
    spark = builder.getOrCreate()
    try:
        manifest = persist(
            spark,
            client=client,
            config=config,
            pipeline_run_id=args.pipeline_run_id,
            evaluation_run_id=args.evaluation_run_id,
            proposed_decision_run_id=args.proposed_decision_run_id,
            baseline_decision_run_id=args.baseline_decision_run_id,
            code_revision=args.code_revision,
            effective_configuration=effective,
            expected_args=args,
        )
        print("INVENTORY_POLICY_EVALUATION=" + json.dumps(manifest, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

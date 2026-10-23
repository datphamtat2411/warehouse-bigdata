"""Read selected M5 Small artifacts and stage a validated J07 decision bundle."""

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


def persist(spark, *, pipeline_run_id, decision_run_id, client, config,
            code_revision, effective_configuration, expected_args=None):
    from warehouse_bigdata.optimization.inventory_artifacts import persist_decision_bundle

    inputs = read_decision_inputs(spark, client=client, config=config)
    result = build_decision_batch(inputs, decision_run_id=decision_run_id)
    if expected_args is not None:
        validate_cli_binding(expected_args, inputs, result.decision_manifest_inputs,
                             effective_configuration, kind='decision')
    return persist_decision_bundle(spark, client=client, config=config, pipeline_run_id=pipeline_run_id,
        inputs=inputs, result=result, code_revision=code_revision,
        effective_configuration=effective_configuration)


def inventory_parser(kind):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('pipeline-run-id', 'source-version', 'source-snapshot-id', 'forecast-run-id',
                 'model-run-id', 'feature-version', 'selection-digest', 'scenario-id', 'issue-date',
                 'hdfs-uri', 'spark-master', 'selector-source', 'code-revision', 'configuration-digest',
                 'j02-pipeline-run-id', 'j03-pipeline-run-id', 'j04-pipeline-run-id',
                 'j05-pipeline-run-id', 'j06-pipeline-run-id'):
        parser.add_argument('--' + name, required=True)
    for name in ('validation-evaluation-run-id', 'test-evaluation-run-id',
                 'validation-pipeline-run-id', 'test-pipeline-run-id', 'event-log-dir'):
        parser.add_argument('--' + name)
    if kind == 'decision':
        for name in ('decision-run-id', 'policy-id', 'policy-version'):
            parser.add_argument('--' + name, required=True)
    else:
        for name in ('evaluation-run-id', 'proposed-decision-run-id', 'baseline-decision-run-id'):
            parser.add_argument('--' + name, required=True)
        parser.add_argument('--run-type', required=True, choices=('test',))
    return parser


def cli_configuration(args, config):
    import os
    from warehouse_bigdata.common.hdfs_namespace import HdfsNamespaceError
    from warehouse_bigdata.common.hdfs_publication import document_digest

    effective = {k: getattr(args, k) for k in ('hdfs_uri', 'spark_master', 'selector_source', 'event_log_dir')}
    if (args.hdfs_uri != config.uri or args.selector_source != os.environ.get('WAREHOUSE_SMALL_ARTIFACTS')
            or document_digest(effective) != args.configuration_digest):
        raise HdfsNamespaceError('J07 effective configuration or exact upstream selector differs')
    return effective


def validate_cli_binding(args, inputs, metadata, effective, *, kind):
    from warehouse_bigdata.optimization.inventory_artifacts import validate_inventory_recovery
    from warehouse_bigdata.optimization.decision_inputs import plain
    binding = {**plain(metadata), 'kind': kind, 'pipeline_run_id': args.pipeline_run_id,
        'upstream_identities': plain(inputs.selector), 'effective_configuration': effective,
        'configuration_digest': args.configuration_digest, 'code_revision': args.code_revision}
    validate_inventory_recovery(args, binding)


def main():
    parser = inventory_parser('decision')
    args = parser.parse_args()
    validate_pipeline_run_id(args.decision_run_id)
    config = load_hdfs_config()
    effective = cli_configuration(args, config)
    client = HdfsCliClient(metadata_limit=SCOPED_METADATA_LIMIT)
    selector = load_small_artifacts()
    validate_small_artifacts(selector, through="j06")
    validate_small_publications(selector, through="j06", client=client, config=config)
    from pyspark.sql import SparkSession

    builder = (SparkSession.builder.master(args.spark_master)
        .appName("warehouse-inventory-decision-" + args.decision_run_id)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC"))
    if args.event_log_dir:
        builder = builder.config('spark.eventLog.enabled', 'true').config('spark.eventLog.dir', args.event_log_dir)
    spark = builder.getOrCreate()
    try:
        print("INVENTORY_DECISION_BATCH=" + json.dumps(persist(spark,
            pipeline_run_id=args.pipeline_run_id, decision_run_id=args.decision_run_id,
            client=client, config=config, code_revision=args.code_revision,
            effective_configuration=effective, expected_args=args), sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

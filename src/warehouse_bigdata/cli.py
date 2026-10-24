"""Command-line interface for the warehouse-bigdata project."""

import argparse
import sys
from collections.abc import Sequence
from importlib import metadata
from pathlib import Path

from warehouse_bigdata.common.spark_submit import run_spark_submit
from warehouse_bigdata.jobs.ingest_m5 import run_ingest_m5
from warehouse_bigdata.jobs.ingest_m5_raw import run_ingest_m5_raw
from warehouse_bigdata.jobs.profile_m5_source import run_profile_m5_source


def main(argv: Sequence[str] | None = None) -> int:
    """Parse command-line arguments and return a process status code."""
    parser = argparse.ArgumentParser(
        prog="warehouse-bigdata",
        description="Batch-oriented big-data inventory decision-support prototype.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {metadata.version('warehouse-bigdata')}",
    )
    commands = parser.add_subparsers(dest="command")
    submit = commands.add_parser(
        "spark-submit",
        help="Submit a baked Python application to the existing Spark Standalone cluster.",
    )
    submit.add_argument("--conf", action="append", default=[], metavar="KEY=VALUE")
    submit.add_argument(
        "application", nargs=argparse.REMAINDER, metavar="-- APPLICATION.py [ARGS...]"
    )
    profile = commands.add_parser(
        "profile-m5-source",
        help="Profile already-acquired local M5 CSV files and emit source manifests.",
    )
    profile.add_argument("--source-dir", required=True, type=Path, metavar="PATH")
    profile.add_argument("--source-url", required=True, metavar="URL")
    profile.add_argument("--downloaded-at-utc", metavar="TIMESTAMP")
    workflow = commands.add_parser(
        "ingest-m5",
        help="Profile an extracted M5 package, ingest immutable raw files, and publish its run manifest.",
    )
    workflow.add_argument("--source-dir", required=True, type=Path, metavar="PATH")
    workflow.add_argument("--source-url", required=True, metavar="URL")
    workflow.add_argument("--downloaded-at-utc", metavar="TIMESTAMP")
    workflow.add_argument("--pipeline-run-id", metavar="ID")
    ingest = commands.add_parser(
        "ingest-m5-raw",
        help="Stage and validate profiled M5 CSV files in immutable HDFS raw storage.",
    )
    ingest.add_argument("--source-dir", required=True, type=Path, metavar="PATH")
    ingest.add_argument("--profile-path", required=True, type=Path, metavar="PATH")
    ingest.add_argument("--manifest-path", required=True, type=Path, metavar="PATH")
    ingest.add_argument("--pipeline-run-id", required=True, metavar="ID")
    publish = commands.add_parser("publish-clickhouse", help="Validate and replace M5 Small serving data offline.")
    publish.add_argument("--selector", type=Path, help="Canonical selector; defaults to WAREHOUSE_SMALL_ARTIFACTS.")
    for name in ("decision-run-id", "decision-pipeline-run-id", "policy-evaluation-run-id",
                 "policy-evaluation-pipeline-run-id", "forecast-evaluation-run-id"):
        publish.add_argument("--" + name, required=True)
    publish.add_argument("--batch-rows", type=int, default=2048)
    publish.add_argument("--offline-reload", action="store_true", required=True)
    publish.add_argument("--confirm-offline-reload", action="store_true", required=True)
    verify = commands.add_parser('verify-clickhouse-distributed', help='Read-only approved M5 Small verification.')
    verify.add_argument('--publication', type=Path, default=Path('artifacts/approved_publication.json'))
    verify.add_argument('--evidence-dir', type=Path, default=Path('artifacts/verification'))
    arguments = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    if arguments.command == 'verify-clickhouse-distributed':
        from warehouse_bigdata.jobs.verify_clickhouse_distributed import run_verification

        try:
            print(run_verification(arguments.publication, arguments.evidence_dir))
            return 0
        except (OSError, ValueError, RuntimeError) as error:
            verify.error(str(error))
    if arguments.command == "publish-clickhouse":
        from warehouse_bigdata.jobs.publish_clickhouse import run_publish_clickhouse

        try:
            result = run_publish_clickhouse(
                selector_path=arguments.selector, decision_run_id=arguments.decision_run_id,
                decision_pipeline_run_id=arguments.decision_pipeline_run_id,
                policy_evaluation_run_id=arguments.policy_evaluation_run_id,
                policy_evaluation_pipeline_run_id=arguments.policy_evaluation_pipeline_run_id,
                forecast_evaluation_run_id=arguments.forecast_evaluation_run_id,
                batch_rows=arguments.batch_rows, offline_reload=arguments.offline_reload,
                confirm_offline_reload=arguments.confirm_offline_reload,
            )
            print(f"{result.status} publication_id={result.publication_id}")
            return 0
        except (OSError, ValueError, RuntimeError) as error:
            publish.error(str(error))
    if arguments.command == "spark-submit":
        if len(arguments.application) < 2 or arguments.application[0] != "--":
            submit.error("a required -- APPLICATION.py [APPLICATION_ARGS...] boundary is missing")
        return run_spark_submit(arguments.application[1], arguments.application[2:], arguments.conf)
    if arguments.command == "profile-m5-source":
        try:
            return run_profile_m5_source(
                arguments.source_dir,
                arguments.source_url,
                arguments.downloaded_at_utc,
            )
        except (OSError, ValueError) as error:
            profile.error(str(error))
    if arguments.command == "ingest-m5":
        try:
            return run_ingest_m5(
                arguments.source_dir,
                arguments.source_url,
                arguments.downloaded_at_utc,
                arguments.pipeline_run_id,
            )
        except (OSError, ValueError, RuntimeError) as error:
            workflow.error(str(error))
    if arguments.command == "ingest-m5-raw":
        try:
            return run_ingest_m5_raw(
                arguments.source_dir,
                arguments.profile_path,
                arguments.manifest_path,
                arguments.pipeline_run_id,
            )
        except (OSError, ValueError, RuntimeError) as error:
            ingest.error(str(error))
    return 0

"""Command-line interface for the warehouse-bigdata project."""

import argparse
import sys
from collections.abc import Sequence
from importlib import metadata

from warehouse_bigdata.common.spark_submit import run_spark_submit


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
    arguments = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    if arguments.command == "spark-submit":
        if len(arguments.application) < 2 or arguments.application[0] != "--":
            submit.error("a required -- APPLICATION.py [APPLICATION_ARGS...] boundary is missing")
        return run_spark_submit(arguments.application[1], arguments.application[2:], arguments.conf)
    return 0

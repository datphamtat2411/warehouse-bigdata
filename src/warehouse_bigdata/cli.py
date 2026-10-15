"""Command-line interface for the warehouse-bigdata project."""

import argparse
from collections.abc import Sequence
from importlib import metadata


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
    parser.parse_args(argv)
    return 0

"""T02-submitted entry point for the bounded synthetic M02 T03 workload."""

import argparse

from warehouse_bigdata.common.spark_verification import emit_record, run_verification


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--fixture-root", required=True)
    parser.add_argument("--probe-sha256", required=True)
    args = parser.parse_args()
    result = run_verification(
        run_id=args.run_id, fixture_root=args.fixture_root, probe_sha256=args.probe_sha256
    )
    emit_record("result", args.run_id, **{key: value for key, value in result.items() if key != "run_id"})


if __name__ == "__main__":
    main()

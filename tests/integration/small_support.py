"""Canonical read-only M5 Small probe; missing artifacts never trigger regeneration."""

import json

from tests.integration.feature_snapshot_support import container_command
from warehouse_bigdata.common.small_artifacts import (
    STAGES,
    load_small_artifacts,
    validate_small_artifacts,
)


def readback(tmp_path, *, through="test", selector=None):
    selector = load_small_artifacts() if selector is None else selector
    validate_small_artifacts(selector, through=through)
    records = []

    def record(line):
        if line.startswith(b"SMALL_READBACK="):
            records.append(json.loads(line.removeprefix(b"SMALL_READBACK=")))

    container_command(
        [
            "--conf",
            "spark.sql.shuffle.partitions=8",
            "--",
            "warehouse_bigdata/jobs/verify_small_artifacts.py",
            "--through",
            through,
            "--selector-json",
            json.dumps(selector),
        ],
        operation="readback",
        spark=True,
        on_stdout_line=record,
    )
    assert len(records) == 1, "Missing unique independent readback result; inspect runtime evidence"
    assert records[0].get("verified_through") == through, (
        f"Independent readback must verify through {through}"
    )
    assert records[0].get("verified_stages") == list(STAGES[: STAGES.index(through) + 1])
    (tmp_path / "small-readback.json").write_text(
        json.dumps(records[0], sort_keys=True, indent=2) + "\n"
    )
    return records[0]

"""One explicitly authorized serial small workflow; no benchmark matrix."""

import os
from pathlib import Path
from uuid import uuid4

import pytest

from tests.integration.small_support import readback
from warehouse_bigdata.common.small_artifacts import load_small_artifacts, validate_small_artifacts
from warehouse_bigdata.common.small_demo import run_live

pytestmark = [pytest.mark.integration, pytest.mark.small_acceptance]


def test_small_distributed_acceptance_with_actual_measurements(tmp_path):
    stages = os.environ.get("WAREHOUSE_SMALL_STAGES", "").split()
    if not stages:
        pytest.fail(
            "Set WAREHOUSE_SMALL_STAGES to explicitly authorized serial stages; existing upstream artifacts are reused"
        )
    selector = load_small_artifacts(allow_unresolved="j02" in stages)
    validate_small_artifacts(selector, through="test", allow_unresolved="j02" in stages)
    report = run_live(
        selector,
        stages=stages,
        authorize=True,
        evidence_dir=Path("artifacts/small-demo") / uuid4().hex,
    )
    assert report["status"] == "EXECUTED_PENDING_INDEPENDENT_READBACK"
    for measured in report["stages"].values():
        assert measured["elapsed_seconds"] > 0
        assert set(measured["execution"]["workers"]) == {"spark-worker-1", "spark-worker-2"}
    readback_report = readback(tmp_path, through="test", selector=selector)
    assert readback_report["status"] == "VALIDATED"
    assert readback_report["verified_through"] == "test"
    assert readback_report["model"]["reloaded"] is True
    for artifact in ("features", "forecast", "validation", "test"):
        assert artifact in readback_report

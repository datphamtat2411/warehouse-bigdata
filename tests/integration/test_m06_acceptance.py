"""One explicit M06 distributed Spark/HDFS acceptance entry point."""

import pytest

from tests.integration.m06_support import run_acceptance

pytestmark = [pytest.mark.integration, pytest.mark.small_acceptance]


def test_m06_distributed_acceptance_and_operational_handoff():
    report = run_acceptance(authorize=True)
    assert report["status"] == "PASSED"

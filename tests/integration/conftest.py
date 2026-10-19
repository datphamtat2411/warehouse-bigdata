"""Explicit runtime selection and pre-fixture maintenance-window gating."""

from collections.abc import Iterator

import pytest

from tests.integration.hdfs_support import (
    HDFSVerification,
    disruption_blocker,
    verification_run,
)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-hdfs-disruptions",
        action="store_true",
        default=False,
        help=(
            "Confirm an exclusive maintenance window for serial HDFS "
            "restart/recreation/interruption tests."
        ),
    )


def parallel_execution(config: pytest.Config) -> bool:
    return bool(getattr(config, "workerinput", None) or config.getoption("numprocesses", default=0))


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    enabled = config.getoption("--run-hdfs-disruptions")
    for item in items:
        if item.get_closest_marker("hdfs_disruptive") is None:
            continue
        blocker = disruption_blocker(enabled, parallel_execution(config))
        if not enabled:
            item.add_marker(pytest.mark.skip(reason=blocker))
        elif blocker:
            raise pytest.UsageError(blocker)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo) -> Iterator[None]:
    cluster = getattr(item, "funcargs", {}).get("hdfs")
    if call.when == "call" and call.excinfo is not None and isinstance(cluster, HDFSVerification):
        cluster.test_failure = call.excinfo.value
        call.excinfo.value.add_note(f"T03 evidence: {cluster.evidence.directory}")
    yield


@pytest.fixture
def hdfs(request: pytest.FixtureRequest) -> Iterator[HDFSVerification]:
    with verification_run(
        request.node.name,
        disruptions=request.config.getoption("--run-hdfs-disruptions"),
        parallel=parallel_execution(request.config),
    ) as cluster:
        yield cluster

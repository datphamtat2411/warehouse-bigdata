"""Pre-fixture opt-in policies for live M05 acceptance, without cluster access."""

from types import SimpleNamespace

import pytest

from tests.integration.conftest import pytest_collection_modifyitems


@pytest.mark.parametrize(
    ("marker", "option"),
    [
        ("m05_published", "--run-m05-published"),
        ("m05_lifecycle", "--run-m05-lifecycle"),
        ("m5_small", "--run-small-integration"),
        ("small_acceptance", "--run-small-acceptance"),
    ],
)
@pytest.mark.parametrize(
    "enabled_flag",
    [
        None,
        "--run-m05-published",
        "--run-m05-lifecycle",
        "--run-small-integration",
        "--run-small-acceptance",
    ],
)
@pytest.mark.parametrize("parallel_mode", [None, "controller", "worker"])
def test_m05_collection_requires_its_own_flag_and_serial_execution(
    marker: str, option: str, enabled_flag: str | None, parallel_mode: str | None
) -> None:
    marks = []
    # Only the marker API is needed; filenames and function names must not select a policy.
    item = SimpleNamespace(
        get_closest_marker=lambda name: object() if name == marker else None,
        add_marker=marks.append,
    )
    config = SimpleNamespace(
        getoption=lambda name, default=None: (
            (2 if parallel_mode == "controller" else 0)
            if name == "numprocesses"
            else name == enabled_flag
        ),
        workerinput={"workerid": "gw0"} if parallel_mode == "worker" else None,
    )

    if enabled_flag == option and parallel_mode is not None:
        with pytest.raises(pytest.UsageError, match="Live small-data verification requires serial"):
            pytest_collection_modifyitems(config, [item])
        return

    pytest_collection_modifyitems(config, [item])

    if enabled_flag == option:
        assert marks == []
    else:
        assert len(marks) == 1
        assert marks[0].name == "skip"
        assert option in marks[0].kwargs["reason"]


@pytest.mark.parametrize(
    ("function_name", "marker"),
    [
        ("test_published_feature_snapshot", "m05_published"),
        ("test_live_feature_snapshot_lifecycle", "m05_lifecycle"),
    ],
)
def test_m05_acceptance_functions_carry_registered_policy_markers(
    function_name: str, marker: str, pytestconfig: pytest.Config
) -> None:
    from tests.integration import test_feature_snapshot as acceptance

    assert acceptance.pytestmark.name == "integration"
    assert {mark.name for mark in getattr(acceptance, function_name).pytestmark} == {marker}
    registered = {entry.split(":", maxsplit=1)[0] for entry in pytestconfig.getini("markers")}
    assert marker in registered


def test_m05_flags_do_not_gate_unmarked_tests() -> None:
    marks = []
    item = SimpleNamespace(get_closest_marker=lambda name: None, add_marker=marks.append)
    config = SimpleNamespace(getoption=lambda name, default=None: True)

    pytest_collection_modifyitems(config, [item])

    assert marks == []

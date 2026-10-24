"""Render-scoped resources and real approval gate failures without infrastructure."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from tests.unit.test_application_read_repository import context
from warehouse_bigdata.application import read_repository, runtime
from warehouse_bigdata.common.hdfs_client import HdfsCommandError
from warehouse_bigdata.contracts.validation import ContractValidationError


@pytest.mark.parametrize(
    "failure", ["missing_local", "absent_pointer", "fence", "mismatch", "invalid", "unreachable"]
)
def test_real_loader_failures_cannot_reuse_old_state(tmp_path, monkeypatch, failure):
    path = tmp_path / "approved_publication.json"
    payload = json.dumps(
        {
            "publication_id": "synthetic",
            "manifest_uri": "/warehouse/manifests/publications/synthetic.json",
        }
    ).encode()
    if failure != "missing_local":
        path.write_bytes(payload)
    monkeypatch.setattr(
        runtime, "load_paths_config", lambda: SimpleNamespace(artifacts_dir=tmp_path)
    )
    monkeypatch.setattr(
        read_repository, "load_hdfs_config", lambda: SimpleNamespace(base_path="/warehouse")
    )
    calls = []

    class Hdfs:
        def exists(self, name):
            calls.append(name)
            if failure == "unreachable":
                raise HdfsCommandError("secret endpoint")
            return failure == "fence"

        def read_remote(self, name):
            calls.append(name)
            if failure == "absent_pointer":
                raise HdfsCommandError("missing pointer")
            if name.endswith("current.json"):
                return b"{}" if failure == "mismatch" else payload
            return b"{}"  # Malformed manifest must be rejected by the existing loader.

    monkeypatch.setattr(read_repository, "HdfsCliClient", Hdfs)
    expected_error = {
        "missing_local": FileNotFoundError,
        "absent_pointer": HdfsCommandError,
        "fence": ValueError,
        "mismatch": ValueError,
        "invalid": ContractValidationError,
        "unreachable": HdfsCommandError,
    }[failure]
    for _ in range(2):
        with pytest.raises(expected_error):
            runtime.load_render_context()
    if failure != "missing_local":
        assert sum(name.endswith("unavailable.json") for name in calls) == 2


@pytest.mark.parametrize("mode", ["success", "page_error", "stop", "init_error"])
def test_repository_is_pinned_and_client_always_closed(monkeypatch, mode):
    client = Mock(database="warehouse")
    factory = Mock(return_value=client)
    monkeypatch.setattr(runtime.clickhouse_connect, "get_client", factory)
    monkeypatch.setattr(
        runtime,
        "load_clickhouse_config",
        lambda: SimpleNamespace(
            host="host", port=8123, database="warehouse", user="reader", password="secret"
        ),
    )
    ctx = context()
    if mode == "init_error":
        monkeypatch.setattr(
            runtime, "ClickHouseReadRepository", Mock(side_effect=ValueError("invalid"))
        )

    class Stop(BaseException):
        pass

    def run():
        with runtime.open_render_repository(ctx) as repository:
            assert repository.context is ctx
            assert repository.client is client
            if mode == "page_error":
                raise RuntimeError("page")
            if mode == "stop":
                raise Stop()

    if mode == "success":
        run()
    else:
        with pytest.raises(
            {"page_error": RuntimeError, "stop": Stop, "init_error": ValueError}[mode]
        ):
            run()
    client.close.assert_called_once_with()
    kwargs = factory.call_args.kwargs
    assert 0 < kwargs["connect_timeout"] <= 30
    assert 0 < kwargs["send_receive_timeout"] <= 120


def test_client_acquisition_failure_propagates(monkeypatch):
    monkeypatch.setattr(runtime, "load_clickhouse_config", Mock(side_effect=ValueError("config")))
    factory = Mock()
    monkeypatch.setattr(runtime.clickhouse_connect, "get_client", factory)
    with pytest.raises(ValueError):
        with runtime.open_render_repository(context()):
            pytest.fail("must not enter")
    factory.assert_not_called()

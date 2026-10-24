"""Phase timing distinguishes real CLI subprocesses from WebHDFS requests."""

import io
import logging
import sys
from textwrap import dedent
from urllib.error import HTTPError

import pytest

from warehouse_bigdata.common import hdfs_client, validation_timing
from warehouse_bigdata.common.hdfs_client import HdfsCliClient, HdfsCommandError
from warehouse_bigdata.common.validation_timing import record_validation_io, validation_phase


def completed(caplog, phase):
    return next(
        record.getMessage()
        for record in caplog.records
        if f"validation_phase={phase} status=" in record.getMessage()
        and "elapsed_seconds=" in record.getMessage()
    )


def test_nested_phase_counts_are_inclusive_and_fresh_with_deterministic_elapsed(
    monkeypatch, caplog
):
    clock = iter([10.0, 11.0, 12.5, 15.0, 20.0, 21.0])
    monkeypatch.setattr(validation_timing, "perf_counter", lambda: next(clock))
    with caplog.at_level(logging.INFO):
        record_validation_io("hadoop_cli", "outside")
        with validation_phase("outer"):
            record_validation_io("hadoop_cli", "dfs -getfacl")
            with validation_phase("inner"):
                record_validation_io("webhdfs", "GETFILESTATUS")
                record_validation_io("hadoop_cli", "dfs -checksum")
        with validation_phase("fresh"):
            pass
    outer, inner, fresh = (completed(caplog, phase) for phase in ("outer", "inner", "fresh"))
    assert "elapsed_seconds=5.000" in outer
    assert "hadoop_cli_subprocess_count=2 webhdfs_request_count=1" in outer
    assert "elapsed_seconds=1.500" in inner
    assert "hadoop_cli_subprocess_count=1 webhdfs_request_count=1" in inner
    assert "elapsed_seconds=1.000" in fresh
    assert "hadoop_cli_subprocess_count=0 webhdfs_request_count=0" in fresh
    assert "counts=inclusive" in outer
    assert 'hadoop_cli_operations={"dfs -checksum":1,"dfs -getfacl":1}' in outer
    assert 'webhdfs_operations={"GETFILESTATUS":1}' in inner
    assert "outside" not in caplog.text


def test_failed_phase_reports_work_and_restores_context_without_swallowing_error(caplog):
    with caplog.at_level(logging.INFO):
        with pytest.raises(PermissionError, match="denied"), validation_phase("failed"):
            record_validation_io("webhdfs", "GETFILESTATUS")
            raise PermissionError("denied")
        record_validation_io("hadoop_cli", "outside")
        with validation_phase("later"):
            pass
    assert "status=FAILED" in completed(caplog, "failed")
    assert "webhdfs_request_count=1" in completed(caplog, "failed")
    assert "hadoop_cli_subprocess_count=0 webhdfs_request_count=0" in completed(caplog, "later")


@pytest.fixture
def cli(tmp_path):
    stub = tmp_path / "hdfs.py"
    stub.write_text(
        dedent(
            """
            import sys
            import time
            args = sys.argv[1:]
            if args[1:] == ['getconf', '-confKey', 'fs.defaultFS']:
                print('hdfs://namenode:8020')
            elif args[1:] == ['getconf', '-confKey', 'dfs.namenode.http-address']:
                print('namenode:9870')
            elif args[1:] == ['groups']:
                print('spark : users')
            elif args[1:3] == ['dfs', '-getfacl']:
                print('user::r--\\ngroup::r--\\nother::r--')
            elif args[1:3] == ['dfs', '-cat'] and args[-1] != '/slow':
                print('payload')
            elif args[1:3] == ['dfs', '-put']:
                sys.stdin.buffer.read()
            elif args[-1] == '/slow':
                time.sleep(30)
            else:
                sys.exit(1)
            """
        )
    )
    return HdfsCliClient(command_prefix=(sys.executable, str(stub)))


def test_client_counts_actual_streaming_and_nonstreaming_processes_and_http_separately(
    cli, monkeypatch, caplog
):
    requests = []

    class Opener:
        def open(self, request, **kwargs):
            requests.append(request.full_url)
            return io.BytesIO(b'{"FileStatus":{"owner":"hadoop","permission":"555"}}')

    monkeypatch.setattr(hdfs_client, "build_opener", lambda *args: Opener())
    with caplog.at_level(logging.INFO), validation_phase("transport"):
        cli.get_acl("/safe")
        cli.path_status("/safe")
        cli.path_status("/safe")
        assert cli.read_remote("/safe") == b"payload\n"
        cli.upload_bytes(b"small fixture", "/candidate")
        with pytest.raises(HdfsCommandError, match="failed"):
            cli._run(("dfs", "-unknown"))
    report = completed(caplog, "transport")
    assert "hadoop_cli_subprocess_count=7 webhdfs_request_count=2" in report
    assert '"dfs -cat":1' in report
    assert '"dfs -put":1' in report
    assert '"getconf":2' in report
    assert '"groups":1' in report
    assert 'webhdfs_operations={"GETFILESTATUS":2}' in report
    assert len(requests) == 2


@pytest.mark.parametrize("streaming", [False, True])
def test_started_subprocess_timeouts_are_counted(cli, caplog, streaming):
    cli.command_timeout = 0.2
    with caplog.at_level(logging.INFO), validation_phase("timeout"):
        with pytest.raises(HdfsCommandError, match="timed out"):
            if streaming:
                cli.read_remote("/slow")
            else:
                cli._run(("dfs", "-test", "/slow"))
    assert "hadoop_cli_subprocess_count=1 webhdfs_request_count=0" in completed(caplog, "timeout")


def test_spawn_failures_do_not_claim_a_started_subprocess(caplog):
    client = HdfsCliClient(hdfs_command="/nonexistent/warehouse-test-hdfs")
    with caplog.at_level(logging.INFO), validation_phase("unavailable"):
        for call in (
            lambda: client.get_acl("/safe"),
            lambda: client.read_remote("/safe"),
            lambda: client.upload_bytes(b"fixture", "/candidate"),
        ):
            with pytest.raises(HdfsCommandError, match="unavailable"):
                call()
    assert "hadoop_cli_subprocess_count=0 webhdfs_request_count=0" in completed(
        caplog, "unavailable"
    )


def test_denied_webhdfs_request_is_counted_and_still_fails_closed(cli, monkeypatch, caplog):
    cli._http_endpoint = "http://namenode:9870/webhdfs/v1"
    cli._hdfs_identity = "spark"

    class Denied:
        def open(self, request, **kwargs):
            raise HTTPError(request.full_url, 403, "denied", {}, None)

    monkeypatch.setattr(hdfs_client, "build_opener", lambda *args: Denied())
    with caplog.at_level(logging.INFO), validation_phase("denied"):
        with pytest.raises(HdfsCommandError, match="denied/unavailable"):
            cli.path_status("/safe")
    assert "hadoop_cli_subprocess_count=0 webhdfs_request_count=1" in completed(caplog, "denied")

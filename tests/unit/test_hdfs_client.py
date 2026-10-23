"""Focused HDFS CLI adapter and integrity parser checks."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from textwrap import dedent
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from warehouse_bigdata.common import hdfs_client
from warehouse_bigdata.common.hdfs_client import (
    HdfsCliClient,
    HdfsCommandError,
    parse_datanode_report,
    parse_fsck,
)


def test_file_upload_and_remote_hash_are_streamed_in_bounded_chunks(tmp_path: Path) -> None:
    stub = tmp_path / "hdfs-stub.py"
    stub.write_text(
        dedent(
            """
            import os
            import sys

            args = sys.argv[1:]
            if args[:3] == ["hdfs", "dfs", "-put"]:
                with open(os.environ["HDFS_STUB_OUTPUT"], "wb") as output:
                    while chunk := sys.stdin.buffer.read(2):
                        output.write(chunk)
            elif args[:3] == ["hdfs", "dfs", "-cat"]:
                sys.stdout.buffer.write(open(os.environ["HDFS_STUB_OUTPUT"], "rb").read())
            else:
                raise SystemExit(f"unexpected command: {args!r}")
            """
        ),
        encoding="utf-8",
    )
    source = tmp_path / "source.csv"
    source.write_bytes(b"bounded transfer payload\x00" * 11)
    remote = tmp_path / "remote.bin"
    client = HdfsCliClient(
        command_prefix=(sys.executable, str(stub)),
        env={"HDFS_STUB_OUTPUT": str(remote)},
        transfer_chunk_size=2,
    )

    client.upload_file(source, "/staging/source.csv")
    evidence = client.hash_remote("/staging/source.csv")

    assert remote.read_bytes() == source.read_bytes()
    assert evidence.size_bytes == source.stat().st_size
    assert evidence.sha256 == "sha256:" + hashlib.sha256(source.read_bytes()).hexdigest()


REPORT = """Configured Capacity: 1000000 (976.56 KB)
Live datanodes (2):

Name: 10.0.0.1:9866 (hdfs-datanode-1)
Hostname: hdfs-datanode-1
Decommission Status : Normal
Last contact: Mon Oct 19 10:00:00 UTC 2026
Last Block Report: Mon Oct 19 09:59:59 UTC 2026

Name: 10.0.0.2:9866 (hdfs-datanode-2)
Hostname: hdfs-datanode-2
Decommission Status : Normal
Last contact: Mon Oct 19 10:00:01 UTC 2026
Last Block Report: Mon Oct 19 10:00:00 UTC 2026

Dead datanodes (0):
"""


def _fsck(locations: tuple[str, ...] = ("10.0.0.1:9866", "10.0.0.2:9866")) -> str:
    storage = ", ".join(
        f"DatanodeInfoWithStorage[{address},DS-{index},DISK]"
        for index, address in enumerate(locations)
    )
    return (
        "/raw/file.csv 7 bytes, replicated: replication=2, 1 block(s): OK\n"
        "0. BP-1:blk_1_1 len=7 Live_repl=2 "
        f"[{storage}]\n"
        "Status: HEALTHY\nMissing blocks: 0\nCorrupt blocks: 0\n"
        "Under-replicated blocks: 0\nMissing replicas: 0 (0.0 %)\n"
    )


def test_report_and_fsck_parsers_require_current_two_node_placement() -> None:
    nodes = parse_datanode_report(REPORT)
    addresses = {node.address for node in nodes.values()}
    blocks = parse_fsck(_fsck(), addresses, expected_replication=2)

    assert set(nodes) == {"hdfs-datanode-1", "hdfs-datanode-2"}
    assert addresses == {"10.0.0.1:9866", "10.0.0.2:9866"}
    assert blocks[0].block_id == "BP-1:blk_1_1"
    assert blocks[0].length == 7
    assert set(blocks[0].locations) == addresses


@pytest.mark.parametrize(
    "report",
    [
        REPORT.replace("Live datanodes (2)", "Live datanodes (1)"),
        REPORT.replace("Decommission Status : Normal", "Decommission Status : Decommissioned", 1),
        REPORT.replace("10.0.0.2:9866", "10.0.0.1:9866"),
    ],
)
def test_invalid_live_registration_is_rejected(report: str) -> None:
    with pytest.raises(HdfsCommandError):
        parse_datanode_report(report)


@pytest.mark.parametrize(
    "text",
    [
        _fsck(("10.0.0.1:9866",)),
        _fsck(("10.0.0.1:9866", "10.0.0.1:9866")),
        _fsck().replace("Status: HEALTHY", "Status: CORRUPT"),
        _fsck().replace("len=7", "len=0"),
    ],
)
def test_incomplete_or_corrupt_fsck_placement_is_rejected(text: str) -> None:
    with pytest.raises(HdfsCommandError):
        parse_fsck(text, {"10.0.0.1:9866", "10.0.0.2:9866"}, expected_replication=2)


def test_metadata_output_is_bounded(tmp_path: Path) -> None:
    stub = tmp_path / "oversized.py"
    stub.write_text(
        "import sys\nsys.stdout.write('x' * 100)\n",
        encoding="utf-8",
    )
    client = HdfsCliClient(
        command_prefix=(sys.executable, str(stub)),
        metadata_limit=16,
    )

    with pytest.raises(HdfsCommandError, match="bounded"):
        client.getconf("fs.defaultFS")


def test_acl_commands_are_non_recursive_and_bounded(tmp_path: Path) -> None:
    stub = tmp_path / "acl-stub.py"
    log = tmp_path / "commands.jsonl"
    stub.write_text(
        dedent(
            """
            import json
            import os
            import sys

            args = sys.argv[1:]
            with open(os.environ["HDFS_STUB_LOG"], "a") as stream:
                stream.write(json.dumps(args) + "\\n")
            if args[:3] == ["hdfs", "dfs", "-getfacl"]:
                print("# owner: hadoop\\nuser:spark:rwx\\ndefault:user:spark:rwx")
            elif args[:3] == ["hdfs", "dfs", "-setfacl"]:
                pass
            else:
                raise SystemExit(f"unexpected command: {args!r}")
            """
        ),
        encoding="utf-8",
    )
    client = HdfsCliClient(
        command_prefix=(sys.executable, str(stub)),
        env={"HDFS_STUB_LOG": str(log)},
    )

    client.set_acl(
        "/data/warehouse/staging/pipeline_run_id=run-001",
        "user:spark:rwx,mask::rwx,default:user:spark:rwx,default:mask::rwx",
    )
    assert "user:spark:rwx" in client.get_acl("/data/warehouse/staging/pipeline_run_id=run-001")

    commands = [json.loads(line) for line in log.read_text().splitlines()]
    assert commands == [
        [
            "hdfs",
            "dfs",
            "-setfacl",
            "-m",
            "user:spark:rwx,mask::rwx,default:user:spark:rwx,default:mask::rwx",
            "/data/warehouse/staging/pipeline_run_id=run-001",
        ],
        [
            "hdfs",
            "dfs",
            "-getfacl",
            "/data/warehouse/staging/pipeline_run_id=run-001",
        ],
    ]


def test_whoami_uses_the_local_simple_authentication_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(hdfs_client.os, "getuid", lambda: 185)
    monkeypatch.setattr(
        hdfs_client.pwd,
        "getpwuid",
        lambda uid: SimpleNamespace(pw_name="spark"),
    )

    assert HdfsCliClient().whoami() == "spark"


def test_batched_checksums_bind_every_record_to_exact_requested_path(monkeypatch):
    client = HdfsCliClient()
    run = Mock(
        return_value=SimpleNamespace(
            stdout=(
                b"/data/b\tMD5-of-0MD5-of-512CRC32C\t" + b"ab" * 28 + b"\n"
                b"/data/a\tMD5-of-0MD5-of-512CRC32C\t" + b"cd" * 28 + b"\n"
            )
        )
    )
    monkeypatch.setattr(client, "_run", run)
    assert client.checksums(["/data/a", "/data/b"]) == {
        "/data/a": "MD5-of-0MD5-of-512CRC32C\t" + "cd" * 28,
        "/data/b": "MD5-of-0MD5-of-512CRC32C\t" + "ab" * 28,
    }
    run.assert_called_once_with(("dfs", "-checksum", "/data/a", "/data/b"))


CHECKSUM_RECORD = b"/data/a\tMD5-of-0MD5-of-512CRC32C\t" + b"aa" * 28 + b"\n"


@pytest.mark.parametrize(
    "output",
    [
        b"",
        b"/data/a\tNONE\n",
        b"/data/a\tMD5\tnot-hex\n",
        b"/data/a\tMD5\ta\n",
        b"/data/a\tMD5\taa\textra\n",
        CHECKSUM_RECORD.replace(b"/data/a", b"/wrong"),
        CHECKSUM_RECORD * 2,
        CHECKSUM_RECORD + CHECKSUM_RECORD.replace(b"/data/a", b"/data/extra"),
        CHECKSUM_RECORD.replace(b"aa" * 28, b"aa" * 16),
        CHECKSUM_RECORD.replace(b"CRC32C", b"UNKNOWN"),
        b"\xff\n",
        CHECKSUM_RECORD + b"\n",
    ],
)
def test_checksum_parser_rejects_incomplete_duplicate_and_malformed_output(monkeypatch, output):
    client = HdfsCliClient()
    monkeypatch.setattr(client, "_run", lambda args: SimpleNamespace(stdout=output))
    with pytest.raises(HdfsCommandError):
        client.checksums(["/data/a"])


@pytest.mark.parametrize(
    "paths",
    [
        ["/a", "/a"],
        ["/a*"],
        ["/a\n"],
        ["relative"],
        ["/a/../b"],
        ["/a//b"],
    ],
)
def test_checksum_inputs_fail_before_subprocess(monkeypatch, paths):
    client = HdfsCliClient()
    run = Mock()
    monkeypatch.setattr(client, "_run", run)
    with pytest.raises(HdfsCommandError):
        client.checksums(paths)
    run.assert_not_called()


def test_checksum_batches_are_bounded_and_fresh(monkeypatch):
    client = HdfsCliClient()
    calls = []

    def run(args):
        calls.append(args)
        return SimpleNamespace(
            stdout="".join(
                path + "\tMD5-of-0MD5-of-512CRC32C\t" + "aa" * 28 + "\n" for path in args[2:]
            ).encode()
        )

    monkeypatch.setattr(client, "_run", run)
    paths = [f"/data/{i}" for i in range(130)]
    assert len(client.checksums(paths)) == 130
    assert [len(call) - 2 for call in calls] == [64, 64, 2]
    client.checksum(paths[0])
    assert len(calls) == 4


def test_checksum_batches_respect_argument_bytes_and_propagate_partial_failure(monkeypatch):
    client = HdfsCliClient()
    calls = []

    def run(args):
        calls.append(args)
        assert sum(len(path.encode()) + 1 for path in args[2:]) <= 16 * 1024
        if len(calls) == 2:
            raise HdfsCommandError("one requested file is inaccessible")
        return SimpleNamespace(
            stdout=b"".join(CHECKSUM_RECORD.replace(b"/data/a", path.encode()) for path in args[2:])
        )

    monkeypatch.setattr(client, "_run", run)
    with pytest.raises(HdfsCommandError, match="inaccessible"):
        client.checksums(["/" + str(index) + "x" * 1000 for index in range(20)])
    assert len(calls) == 2


@pytest.mark.parametrize(
    "algorithm,checksum",
    [
        ("MD5-of-0MD5-of-0CRC32", "00000000000000000000000070bc8f4b72a86921468bf8e8441dce51"),
        ("COMPOSITE-CRC32C", "aabb0011"),
    ],
)
def test_checksum_accepts_hadoop_empty_file_and_composite_encodings(
    monkeypatch, algorithm, checksum
):
    client = HdfsCliClient()
    monkeypatch.setattr(
        client,
        "_run",
        lambda args: SimpleNamespace(stdout=f"/data/a\t{algorithm}\t{checksum}\n".encode()),
    )
    assert client.checksum("/data/a") == algorithm + "\t" + checksum


@pytest.mark.parametrize(
    "statuses",
    [
        [{"pathSuffix": "../escape", "type": "FILE", "length": 1}],
        [{"pathSuffix": "a/b", "type": "FILE", "length": 1}],
        [{"pathSuffix": "a", "type": "FILE", "length": -1}],
        [{"pathSuffix": "a", "type": "FILE", "length": True}],
        [{"pathSuffix": "a", "type": "UNKNOWN", "length": 1}],
        [{"pathSuffix": "a", "type": "FILE", "length": 1}] * 2,
        [{}],
        None,
    ],
)
def test_list_status_rejects_malformed_or_duplicate_metadata(monkeypatch, statuses):
    client = HdfsCliClient()
    monkeypatch.setattr(
        client, "_webhdfs", lambda *args: {"FileStatuses": {"FileStatus": statuses}}
    )
    with pytest.raises(HdfsCommandError):
        client.list_status("/data")


def test_list_status_returns_namenode_types_and_lengths_without_cli(monkeypatch):
    client = HdfsCliClient()
    web = Mock(
        return_value={
            "FileStatuses": {
                "FileStatus": [
                    {"pathSuffix": "part.parquet", "type": "FILE", "length": 123},
                ]
            }
        }
    )
    monkeypatch.setattr(client, "_webhdfs", web)
    assert client.list_status("/data") == (
        {"path": "/data/part.parquet", "type": "FILE", "length": 123},
    )
    web.assert_called_once_with("/data", "LISTSTATUS")

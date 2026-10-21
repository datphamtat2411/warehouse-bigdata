"""Focused HDFS CLI adapter and integrity parser checks."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path
from textwrap import dedent

import pytest

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

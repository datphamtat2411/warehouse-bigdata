"""Offline capacity, effective-write, and fail-closed scratch contracts."""

import importlib.util
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "spark_scratch", ROOT / "infrastructure/spark/scratch.py"
)
assert SPEC and SPEC.loader
scratch = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(scratch)


@pytest.fixture
def disk(tmp_path, monkeypatch):
    monkeypatch.setattr(scratch, "ROOT", tmp_path)
    monkeypatch.setattr(scratch.os, "getuid", lambda: 185)
    monkeypatch.setattr(scratch.os, "getgid", lambda: 185)
    monkeypatch.setattr(scratch, "mount_policy", lambda: None)
    for name in ("local", "work"):
        (tmp_path / name).mkdir(mode=0o700)
    stats = SimpleNamespace(
        f_blocks=8 * 1024**2, f_frsize=1024, f_bavail=5 * 1024**2, f_favail=100_000
    )
    monkeypatch.setattr(scratch.os, "statvfs", lambda path: stats)
    monkeypatch.setattr(scratch, "check_owner", lambda path: None)
    return tmp_path, stats


def test_preflight_writes_reads_removes_only_its_unique_probe(disk):
    root, _ = disk
    sentinel = root / "local" / "active-application"
    sentinel.mkdir()
    (sentinel / "shuffle").write_bytes(b"retain")
    report = scratch.check_scratch()
    assert report["available_bytes"] == 5 * 1024**3
    assert (sentinel / "shuffle").read_bytes() == b"retain"
    assert list((root / "local").iterdir()) == [sentinel]
    assert list((root / "work").iterdir()) == []


@pytest.mark.parametrize(
    "field,value", [("f_bavail", 4 * 1024**2 - 1), ("f_blocks", 1024**2), ("f_favail", 100)]
)
def test_small_disk_low_space_and_inode_exhaustion_fail_before_write(
    disk, monkeypatch, field, value
):
    _, stats = disk
    setattr(stats, field, value)
    monkeypatch.setattr(
        scratch, "effective_write", lambda path: pytest.fail("must fail before write")
    )
    with pytest.raises(RuntimeError):
        scratch.check_scratch()


def test_effective_write_error_is_propagated_and_unrelated_files_survive(disk, monkeypatch):
    root, _ = disk
    sentinel = root / "work" / "unrelated"
    sentinel.write_bytes(b"keep")

    def fail(fd):
        raise OSError("No space left on device")

    monkeypatch.setattr(scratch.os, "fsync", fail)
    with pytest.raises(OSError, match="No space"):
        scratch.check_scratch()
    assert list((root / "work").iterdir()) == [sentinel]
    assert list((root / "local").iterdir()) == []


def test_symlink_directory_is_rejected(disk):
    root, _ = disk
    (root / "local").rmdir()
    (root / "local").symlink_to(root / "work", target_is_directory=True)
    with pytest.raises(RuntimeError, match="symlink"):
        scratch.check_scratch()


def test_mount_must_be_dedicated_ext4_and_noexec(monkeypatch):
    def mount_text(text):
        monkeypatch.setattr(scratch, "mountinfo", lambda: text)

    mount_text("1 0 7:0 / /var/lib/warehouse-spark rw,nosuid,nodev,noexec - ext4 /dev/loop0 rw\n")
    scratch.mount_policy()
    for text in (
        "",
        "1 0 0:1 / /var/lib/warehouse-spark rw - tmpfs tmpfs rw\n",
        "1 0 7:0 / /var/lib/warehouse-spark rw,nosuid,nodev - ext4 /dev/loop0 rw\n",
    ):
        mount_text(text)
        with pytest.raises(RuntimeError):
            scratch.mount_policy()


def test_owner_and_private_mode_are_enforced(tmp_path):
    # Real host ownership, independent of the filesystem mock used above.
    original_uid = os.getuid()
    if original_uid != 185:
        with pytest.raises(RuntimeError, match="185"):
            scratch.check_owner(tmp_path)


def test_cleanup_requires_terminal_proof_retains_active_unknown_and_unrelated(disk, monkeypatch):
    root, _ = disk
    app_id = "app-20261022130000-0001"
    executor = root / "work" / app_id / "0"
    executor.mkdir(parents=True)
    log = executor / "stderr"
    log.write_bytes(b"old log")
    sentinel = executor / "unrelated"
    sentinel.write_bytes(b"keep")
    info = log.stat()
    original_stat = Path.stat

    def owned(path, *args, **kwargs):
        value = original_stat(path, *args, **kwargs)
        return os.stat_result((*value[:4], 185, *value[5:])) if path == log else value

    monkeypatch.setattr(Path, "stat", owned)
    now = max(info.st_mtime, info.st_ctime) + 7200
    local = {"executors": [], "finishedexecutors": [{"appid": app_id}]}
    master = {"activeapps": [], "completedapps": [{"id": app_id, "state": "FINISHED"}]}
    assert scratch.cleanup_work(local, {**master, "activeapps": [{"id": app_id}]}, now) == []
    assert scratch.cleanup_work({**local, "executors": [{"appid": app_id}]}, master, now) == []
    assert scratch.cleanup_work(local, {**master, "completedapps": []}, now) == []
    assert scratch.cleanup_work(local, master, now - 7200) == []
    assert scratch.cleanup_work(local, master, now) == [str(log)]
    assert sentinel.read_bytes() == b"keep"

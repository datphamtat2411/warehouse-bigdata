"""Worker-local disk admission checks; never sweep application directories."""

import json
import os
import re
import stat
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path("/var/lib/warehouse-spark")
GIB = 1024**3
MIN_FREE = 4 * GIB
HEALTH_FREE = 512 * 1024**2
APP_ID = re.compile(r"app-[0-9]{14}-[0-9]+\Z")


def cleanup_work(local: dict, master: dict, now: float | None = None) -> list[str]:
    """Remove only old executor stdout/stderr for positively terminal applications.

    Spark itself removes its registered per-application local dirs after ApplicationFinished
    AND the last executor exits. Never infer an orphan is dead from age or a missing UI entry.
    """
    now = time.time() if now is None else now
    active = {app["id"] for app in master.get("activeapps", [])}
    active.update(executor["appid"] for executor in local.get("executors", []))
    terminal = {
        app["id"]
        for app in master.get("completedapps", [])
        if app.get("state") in {"FINISHED", "FAILED", "KILLED"}
    }
    finished = {executor["appid"] for executor in local.get("finishedexecutors", [])}
    removed = []
    for app_id in sorted((terminal & finished) - active):
        if len(removed) >= 4:
            break
        if not APP_ID.fullmatch(app_id):
            continue
        app = ROOT / "work" / app_id
        if app.is_symlink() or not app.is_dir():
            continue
        for executor in app.iterdir():
            if not executor.name.isdecimal() or executor.is_symlink() or not executor.is_dir():
                continue
            for name in ("stdout", "stderr"):
                file = executor / name
                if file.is_symlink() or not file.is_file():
                    continue
                info = file.stat()
                if info.st_uid == 185 and now - max(info.st_mtime, info.st_ctime) >= 3600:
                    file.unlink()
                    removed.append(str(file))
                    if len(removed) == 4:
                        return removed
    return removed


def mountinfo() -> str:
    return Path("/proc/self/mountinfo").read_text()


def mount_policy() -> None:
    mounts = [line.split() for line in mountinfo().splitlines() if line.split()[4] == str(ROOT)]
    if len(mounts) != 1:
        raise RuntimeError("Scratch must be a dedicated mounted filesystem; provision it first")
    fields = mounts[0]
    if fields[fields.index("-") + 1] != "ext4" or not {"rw", "nosuid", "nodev", "noexec"} <= set(
        fields[5].split(",")
    ):
        raise RuntimeError("Scratch requires disk-backed ext4 with rw,nosuid,nodev,noexec")


def check_owner(path: Path) -> None:
    info = path.stat()
    if (info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode)) != (185, 185, 0o700):
        raise RuntimeError(f"Scratch {path} must be owned by 185:185 with mode 0700")


def effective_write(path: Path) -> None:
    # A private name per probe: no glob/rm of active, old, or unrelated app files.
    with tempfile.TemporaryDirectory(prefix=".write-probe-", dir=path) as directory:
        probe = Path(directory) / "bytes"
        payload = b"warehouse-spark-scratch\n" * 4096
        with probe.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        if probe.read_bytes() != payload:
            raise RuntimeError(f"Scratch read-back mismatch: {path}")
        probe.unlink()


def check_scratch(min_free: int = MIN_FREE) -> dict:
    if os.getuid() != 185 or os.getgid() != 185:
        raise RuntimeError("Scratch checks and Spark must run as UID/GID 185:185")
    mount_policy()
    stats = os.statvfs(ROOT)
    capacity = stats.f_blocks * stats.f_frsize
    available = stats.f_bavail * stats.f_frsize
    if not 7 * GIB <= capacity <= 8 * GIB:
        raise RuntimeError(
            f"Expected dedicated 8 GiB scratch image (ext4 overhead allowed): {capacity}"
        )
    if available < min_free or stats.f_favail < 10_000:
        raise RuntimeError(
            f"Insufficient Spark scratch: available={available}, required={min_free}, "
            f"free_inodes={stats.f_favail}; retain active apps, inspect completed-app retention"
        )
    for path in (ROOT / "local", ROOT / "work"):
        if path.is_symlink():
            raise RuntimeError(f"Scratch must not be a symlink: {path}")
        path.mkdir(mode=0o700, exist_ok=True)
        check_owner(path)
        if path.stat().st_dev != ROOT.stat().st_dev:
            raise RuntimeError(f"Scratch directory is on another filesystem: {path}")
        effective_write(path)
    return {
        "root": str(ROOT),
        "capacity_bytes": capacity,
        "available_bytes": available,
        "required_bytes": min_free,
        "free_inodes": stats.f_favail,
        "uid": os.getuid(),
    }


def main() -> None:
    args = sys.argv[1:]
    if args == ["preflight"] or args == ["health"]:
        print(json.dumps(check_scratch(HEALTH_FREE if args == ["health"] else MIN_FREE)))
    elif args and args[0] == "org.apache.spark.deploy.worker.Worker":
        check_scratch()
        if os.environ.get("SPARK_LOCAL_DIRS") != str(ROOT / "local") or os.environ.get(
            "SPARK_WORKER_DIR"
        ) != str(ROOT / "work"):
            raise RuntimeError("Worker scratch environment does not match the mounted disk")
        os.execv("/opt/spark/bin/spark-class", ["/opt/spark/bin/spark-class", *args])
    elif args and args[0] == "org.apache.spark.deploy.master.Master":
        os.execv("/opt/spark/bin/spark-class", ["/opt/spark/bin/spark-class", *args])
    else:
        raise RuntimeError("Expected preflight, health, or an allowlisted Spark daemon")


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError) as error:
        sys.exit(f"Spark scratch check failed: {error}")

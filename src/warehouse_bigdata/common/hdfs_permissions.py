"""Scoped HDFS ACL preparation for run-owned Spark directories."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

from warehouse_bigdata.common.config import HDFSConfig
from warehouse_bigdata.common.hdfs_paths import HdfsPathError, render_staging_path

SPARK_HDFS_USER = "spark"
HDFS_SPARK_ACL_SPEC = (
    "user:spark:rwx,mask::rwx,default:user:spark:rwx,default:mask::rwx"
)
_REQUIRED_ACL_ENTRIES = frozenset(
    {
        "user:spark:rwx",
        "mask::rwx",
        "default:user:spark:rwx",
        "default:mask::rwx",
    }
)


class HdfsPermissionError(RuntimeError):
    """Raised when scoped Spark HDFS access cannot be established."""


def prepare_spark_run_permissions(
    client: Any,
    hdfs_config: HDFSConfig,
    *,
    pipeline_run_id: str,
    event_log_dir: str | None = None,
    verify_effective_access: bool = True,
) -> tuple[str, ...]:
    """Prepare and verify ACLs for one run's staging and optional event directory.

    The caller that owns a pre-created run directory may use
    ``verify_effective_access=False`` to apply the ACL. Spark callers use the
    default to verify their own identity and perform a real bounded file probe.
    No parent or unrelated HDFS path is modified.
    """
    try:
        staging_path = render_staging_path(hdfs_config.base_path, pipeline_run_id)
    except HdfsPathError as error:
        raise HdfsPermissionError(f"invalid Spark run staging identity: {error}") from error

    if verify_effective_access:
        _require_spark_identity(client)

    paths = [staging_path]
    if event_log_dir is not None:
        event_path = _event_path(hdfs_config, event_log_dir)
        _require_run_owned_path(event_path, staging_path)
        if event_path not in paths:
            paths.append(event_path)

    for path in paths:
        _require_directory(client, path)
        _ensure_acl(client, path)

    if verify_effective_access:
        for path in paths:
            _verify_effective_write_access(client, path)
    return tuple(paths)


def _event_path(hdfs_config: HDFSConfig, event_log_dir: str) -> str:
    if not isinstance(event_log_dir, str) or any(
        character in event_log_dir for character in "\0\r\n"
    ):
        raise HdfsPermissionError("event_log_dir must be an absolute HDFS URI")
    try:
        actual = urlsplit(event_log_dir)
        expected = urlsplit(hdfs_config.uri)
    except ValueError as error:
        raise HdfsPermissionError("event_log_dir must be a valid HDFS URI") from error
    if (
        actual.scheme != "hdfs"
        or actual.netloc != expected.netloc
        or actual.query
        or actual.fragment
    ):
        raise HdfsPermissionError(
            "event_log_dir must use the configured HDFS endpoint and an absolute path"
        )
    if (
        not actual.path.startswith("/")
        or any(part in {"", ".", ".."} for part in actual.path.split("/")[1:])
    ):
        raise HdfsPermissionError(
            "event_log_dir must use an absolute path with safe HDFS path components"
        )
    return actual.path


def _require_run_owned_path(path: str, staging_path: str) -> None:
    try:
        PurePosixPath(path).relative_to(PurePosixPath(staging_path))
    except ValueError as error:
        raise HdfsPermissionError(
            f"HDFS permission target is outside run-owned staging: {path}"
        ) from error


def _require_spark_identity(client: Any) -> None:
    try:
        identity = client.whoami().strip()
    except Exception as error:
        raise HdfsPermissionError(f"cannot establish HDFS Spark identity: {error}") from error
    if identity != SPARK_HDFS_USER:
        raise HdfsPermissionError(
            f"Spark HDFS permission preparation must run as {SPARK_HDFS_USER!r}, got {identity!r}"
        )


def _require_directory(client: Any, path: str) -> None:
    try:
        if not client.exists(path):
            raise HdfsPermissionError(
                f"required run-owned HDFS directory is missing: {path}; "
                "create it as the HDFS owner and prepare its ACL before Spark starts"
            )
        if not client.is_directory(path):
            raise HdfsPermissionError(f"HDFS Spark permission target is not a directory: {path}")
    except HdfsPermissionError:
        raise
    except Exception as error:
        raise HdfsPermissionError(f"cannot inspect HDFS permission target {path}: {error}") from error


def _ensure_acl(client: Any, path: str) -> None:
    try:
        acl = client.get_acl(path)
    except Exception as error:
        raise HdfsPermissionError(f"cannot inspect ACL for {path}: {error}") from error
    if not _has_required_acl(acl):
        try:
            client.set_acl(path, HDFS_SPARK_ACL_SPEC)
        except Exception as error:
            raise HdfsPermissionError(
                f"ACL setup failed for {path}; required Spark ACL was not established: {error}"
            ) from error
        try:
            acl = client.get_acl(path)
        except Exception as error:
            raise HdfsPermissionError(f"cannot verify ACL for {path}: {error}") from error
    if not _has_required_acl(acl):
        raise HdfsPermissionError(
            f"ACL setup failed for {path}; required Spark access/default inheritance is absent"
        )


def _has_required_acl(acl: str) -> bool:
    entries = {
        line.strip().split()[0]
        for line in acl.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    return _REQUIRED_ACL_ENTRIES <= entries


def _verify_effective_write_access(client: Any, path: str) -> None:
    probe = f"{path}/.warehouse-spark-acl-probe-{uuid4().hex}"
    try:
        client.upload_bytes(b"", probe)
    except Exception as error:
        raise HdfsPermissionError(
            f"effective Spark write access is unavailable for {path}: {error}"
        ) from error
    try:
        client.remove(probe)
    except Exception as error:
        raise HdfsPermissionError(
            f"effective Spark write access probe cleanup failed for {path}: {error}"
        ) from error

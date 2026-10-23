"""Explicit owner-provisioned M04 namespace; Spark never owns publication parents."""

from __future__ import annotations

import argparse
import json
from pathlib import PurePosixPath
from uuid import uuid4

from warehouse_bigdata.common.config import HDFSConfig, load_hdfs_config
from warehouse_bigdata.common.hdfs_client import HdfsCliClient

HDFS_OWNER = "hadoop"
DATASET_NAMES = ("sales_daily", "sales_enriched", "store_daily_sales", "category_monthly_sales")


class HdfsNamespaceError(RuntimeError):
    """An exact publication prerequisite or immutable publication gate failed."""


def publication_parents(config: HDFSConfig) -> tuple[str, ...]:
    """Contract v1 curated, run-manifest, and feature publication parents."""
    base = config.base_path
    return (*(f"{base}/curated/{name}" for name in DATASET_NAMES), f"{base}/manifests/runs",
            f"{base}/features/demand_forecasting")


def require_owner(client) -> None:
    identity = client.effective_user()
    if identity != HDFS_OWNER:
        raise HdfsNamespaceError(
            f"authorized HDFS owner {HDFS_OWNER} required; identity={identity}"
        )


def _failure(client, path: str, permission: str, detail: str) -> HdfsNamespaceError:
    return HdfsNamespaceError(
        f"HDFS prerequisite failed: directory={path} identity={client.effective_user()} "
        f"requires {permission}: {detail}; run bash scripts/provision-hdfs-namespace.sh setup "
        "as the authorized Hadoop owner"
    )


def _shared_directories(config: HDFSConfig) -> tuple[str, ...]:
    base = config.base_path
    return (
        f"{base}/curated",
        *publication_parents(config)[:4],
        f"{base}/manifests",
        publication_parents(config)[4],
        f"{base}/features",
        publication_parents(config)[5],
        f"{base}/staging",
    )


def prepare_feature_publication_parent(client, config: HDFSConfig, final: str) -> None:
    """Owner-create only the requested source parent; never repair an unsafe existing one."""
    require_owner(client)
    parent = str(PurePosixPath(final).parent)
    shared = config.base_path + "/features/demand_forecasting"
    if str(PurePosixPath(parent).parent) != shared:
        raise HdfsNamespaceError("feature publication parent is outside the canonical namespace")
    if client.path_status(parent) is None:
        try:
            client.mkdir_exclusive(parent)
        except Exception:
            if client.path_status(parent) is None:
                raise
    status = _directory(client, parent, "rwx")
    _protected(client, parent, status, "rwx")
    if not client.can_access(parent, "rwx"):
        raise _failure(client, parent, "rwx", "owner access denied")


def _directory(client, path: str, permission: str) -> dict:
    try:
        status = client.path_status(path)
        if status is None:
            raise _failure(client, path, permission, "directory is missing")
        if status["type"] != "DIRECTORY":
            raise _failure(client, path, permission, "path is not a directory")
        return status
    except HdfsNamespaceError:
        raise
    except Exception as error:
        raise _failure(client, path, permission, str(error)) from error


def _protected(client, path: str, status: dict, permission: str) -> None:
    acl = client.get_acl(path)
    entries = [
        line.strip() for line in acl.splitlines() if not line.startswith("#") and line.strip()
    ]
    if (
        status["owner"] != HDFS_OWNER
        or int(status["permission"], 8) & 0o022
        or any(line.startswith(("default:", "mask:", "user:spark:")) for line in entries)
        or any(line.startswith("user:") and not line.startswith("user::") for line in entries)
        or any(line.startswith("group:") and not line.startswith("group::") for line in entries)
    ):
        raise _failure(
            client, path, permission, "requires hadoop ownership and no shared write/default ACL"
        )


def provision_publication_namespace(client, config: HDFSConfig) -> tuple[str, ...]:
    """Idempotently provision only shared parents, never raw data or snapshot descendants."""
    require_owner(client)
    base = _directory(client, config.base_path, "rwx")
    _protected(client, config.base_path, base, "rwx")
    # Inspect all existing targets before making any changes.
    for path in _shared_directories(config):
        status = client.path_status(path)
        if status is not None and (status["type"] != "DIRECTORY" or status["owner"] != HDFS_OWNER):
            raise _failure(client, path, "rwx", "existing target must be a hadoop-owned directory")
    for path in _shared_directories(config):
        if client.path_status(path) is None:
            client.mkdir_exclusive(path)
        status = client.path_status(path)
        acl = client.get_acl(path)
        if "default:" in acl or "mask:" in acl:
            client.clear_acl(path)
        if status["permission"] != "755":
            client.set_permission(path, "755")
    validate_publication_namespace(client, config)
    return publication_parents(config)


def validate_publication_namespace(client, config: HDFSConfig, *, probe_writes=False) -> dict:
    """Check exact parents and parent-directory isolation, never climb to a writable ancestor."""
    identity = client.effective_user()
    if identity not in {HDFS_OWNER, "spark"}:
        raise HdfsNamespaceError(f"unexpected publication HDFS identity={identity}")
    permission = "rwx" if identity == HDFS_OWNER else "r-x"
    ancestors = tuple(reversed(PurePosixPath(config.base_path).parents)) + (
        PurePosixPath(config.base_path),
    )
    for path in (*map(str, ancestors), *_shared_directories(config)):
        try:
            status = _directory(client, path, permission)
            _protected(client, path, status, permission)
            if not client.can_access(path, permission):
                raise _failure(client, path, permission, "NameNode effective access check denied")
            if identity == "spark" and client.can_access(path, "-w-"):
                raise _failure(
                    client,
                    path,
                    "r-x",
                    "Spark must not have parent WRITE (rename/delete authority)",
                )
        except HdfsNamespaceError:
            raise
        except Exception as error:
            raise _failure(client, path, permission, str(error)) from error
    if probe_writes:
        require_owner(client)
        for parent in publication_parents(config):
            first = parent + "/.warehouse-namespace-probe-" + uuid4().hex
            second = first + "-renamed"
            try:
                client.upload_bytes(b"namespace-readiness\n", first)
                client.rename_exclusive(first, second)
                client.remove(second)
            except Exception as error:
                raise _failure(
                    client, parent, "rwx", f"create/rename/delete probe failed: {error}"
                ) from error
            finally:
                for owned in (first, second):
                    if client.exists(owned):
                        client.remove(owned)
    return {
        "status": "READY",
        "hdfs_user": identity,
        "publication_parents": publication_parents(config),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("setup", "check"))
    args = parser.parse_args()
    config = load_hdfs_config()
    client = HdfsCliClient()
    require_owner(client)
    if client.safe_mode() != "Safe mode is OFF":
        raise HdfsNamespaceError("NameNode is not write-ready: safe mode must be OFF")
    client.report()
    if args.action == "setup":
        provision_publication_namespace(client, config)
    print(json.dumps(validate_publication_namespace(client, config, probe_writes=True)))


if __name__ == "__main__":
    main()

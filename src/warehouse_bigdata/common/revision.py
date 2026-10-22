"""Reproducible revision identity for the installed project package."""

from __future__ import annotations

import hashlib
import os
import re
from functools import lru_cache
from importlib import resources
from importlib.abc import Traversable

_REVISION_PATTERN = re.compile(r"\Asha256:[0-9a-f]{64}\Z")
_REVISION_ENV = "WAREHOUSE_CODE_REVISION"
_IGNORED_PARTS = frozenset({"__pycache__", ".pytest_cache", ".ruff_cache", ".mypy_cache"})


def get_code_revision() -> str:
    """Return the immutable build revision without consulting repository metadata.

    A build may inject the digest of its packaged source through
    ``WAREHOUSE_CODE_REVISION``. Source-only execution uses the same deterministic
    content hash, which keeps local tests and packaged Spark workers equivalent.
    """
    configured = os.environ.get(_REVISION_ENV)
    if configured is not None:
        if not _REVISION_PATTERN.fullmatch(configured):
            raise ValueError(f"{_REVISION_ENV} must be a SHA-256 package revision")
        return configured
    return _package_revision()


@lru_cache(maxsize=1)
def _package_revision() -> str:
    """Hash package files in stable path order; never inspect ``.git``."""
    package = resources.files("warehouse_bigdata")
    digest = hashlib.sha256()
    for relative, path in _package_files(package):
        payload = path.read_bytes()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(len(payload)).encode("ascii"))
        digest.update(b"\0")
        digest.update(payload)
        digest.update(b"\0")
    return "sha256:" + digest.hexdigest()


def _package_files(root: Traversable, prefix: str = "") -> tuple[tuple[str, Traversable], ...]:
    files: list[tuple[str, Traversable]] = []
    for path in sorted(root.iterdir(), key=lambda item: item.name):
        relative = f"{prefix}/{path.name}" if prefix else path.name
        if path.is_dir():
            if path.name not in _IGNORED_PARTS:
                files.extend(_package_files(path, relative))
        elif path.is_file() and path.name not in {".DS_Store"} and path.suffix not in {".pyc", ".pyo"}:
            files.append((relative, path))
    return tuple(files)

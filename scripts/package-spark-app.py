#!/usr/bin/env python3
"""Create the pure-Python executor ZIP from the driver's copied project sources."""

import argparse
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

_CACHES = frozenset({"__pycache__", ".pytest_cache", ".ruff_cache", ".mypy_cache"})


def package_app(source_root: Path, output: Path) -> None:
    """Keep stable archive paths, metadata, and ordering; omit interpreter caches."""
    package = source_root / "warehouse_bigdata"
    if not (package / "__init__.py").is_file():
        raise ValueError("source root must contain warehouse_bigdata/__init__.py")
    with ZipFile(output, "w", compression=ZIP_DEFLATED) as archive:
        for path in sorted(package.rglob("*")):
            relative = path.relative_to(source_root)
            if (
                not path.is_file()
                or path.is_symlink()
                or _CACHES.intersection(relative.parts)
                or path.suffix in {".pyc", ".pyo"}
            ):
                continue
            entry = ZipInfo(relative.as_posix(), date_time=(1980, 1, 1, 0, 0, 0))
            entry.compress_type = ZIP_DEFLATED
            entry.external_attr = 0o100644 << 16
            archive.writestr(entry, path.read_bytes())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source_root", type=Path)
    parser.add_argument("output", type=Path)
    arguments = parser.parse_args()
    try:
        package_app(arguments.source_root, arguments.output)
    except (OSError, ValueError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()

"""One-file HDFS spooling and bounded Arrow batches; no dataset collection."""

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
import hashlib
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory

DEFAULT_BATCH_ROWS = 2048
MAX_BATCH_ROWS = 10000
MAX_FILE_BYTES = 512 * 1024 * 1024
MAX_FILES = 1024


@dataclass(frozen=True, slots=True)
class ParquetFile:
    """An exact selected file, its sealed size and optional streamed SHA-256."""

    path: str
    size_bytes: int
    sha256: str | None = None
    checksum: str | None = None


def iter_parquet_batches(
    client, files: Sequence[ParquetFile], *, scratch_dir: Path,
    batch_rows: int = DEFAULT_BATCH_ROWS,
) -> Iterator:
    """Yield batches from only selected files; clean scratch even on early close."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if type(batch_rows) is not int or not 1 <= batch_rows <= MAX_BATCH_ROWS:
        raise ValueError(f"batch_rows must be between 1 and {MAX_BATCH_ROWS}")
    if not 0 < len(files) <= MAX_FILES or len({f.path for f in files}) != len(files):
        raise ValueError("selected Parquet files must be nonempty, unique, and bounded")
    for item in files:
        path = PurePosixPath(item.path)
        if (not path.is_absolute() or str(path) != item.path or ".." in path.parts
                or not item.path.endswith(".parquet")
                or type(item.size_bytes) is not int or not 0 < item.size_bytes <= MAX_FILE_BYTES):
            raise ValueError("unsafe or oversized selected Parquet file")
    Path(scratch_dir).mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="j08-parquet-", dir=scratch_dir) as directory:
        local = Path(directory) / "part.parquet"
        for item in files:
            if item.checksum and client.checksum(item.path) != item.checksum:
                raise ValueError("selected Parquet checksum changed")
            client.download_file(item.path, local, max_bytes=item.size_bytes)
            try:
                if local.stat().st_size != item.size_bytes:
                    raise ValueError("selected Parquet size changed")
                if item.sha256:
                    with local.open("rb") as handle:
                        actual = "sha256:" + hashlib.file_digest(handle, "sha256").hexdigest()
                    if actual != item.sha256:
                        raise ValueError("selected Parquet SHA-256 changed")
                try:
                    with pq.ParquetFile(local) as parquet:
                        for batch in parquet.iter_batches(batch_size=batch_rows, use_threads=False):
                            yield batch
                except (pa.ArrowException, OSError) as error:
                    raise ValueError(f"malformed selected Parquet: {item.path}") from error
                if item.checksum and client.checksum(item.path) != item.checksum:
                    raise ValueError("selected Parquet checksum changed during read")
            finally:
                local.unlink(missing_ok=True)

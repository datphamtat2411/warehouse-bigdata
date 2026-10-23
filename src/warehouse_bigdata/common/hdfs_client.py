"""Small, bounded Hadoop CLI adapter used by raw ingestion.

The adapter intentionally depends on the Hadoop client already present in the
prepared runtime.  ``command_prefix`` lets callers place that client behind a
network-attached container without adding a Python HDFS connector.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import pwd
import re
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import BinaryIO
from urllib.error import HTTPError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import ProxyHandler, Request, build_opener

DEFAULT_METADATA_LIMIT = 64 * 1024
DEFAULT_TRANSFER_CHUNK_SIZE = 1024 * 1024
DEFAULT_COMMAND_TIMEOUT = 120.0
EXPECTED_DATANODES = frozenset({"hdfs-datanode-1", "hdfs-datanode-2"})
EXPECTED_REPLICATION = 2


class HdfsCommandError(RuntimeError):
    """Raised for an unavailable, failed, malformed, or unsafe HDFS command."""


@dataclass(frozen=True, slots=True)
class RemoteFileHash:
    """Byte count and streamed SHA-256 for one remote file."""

    size_bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class RemoteFileStat:
    """HDFS stat values needed by the raw-ingestion integrity gate."""

    size_bytes: int
    replication: int


@dataclass(frozen=True, slots=True)
class DataNodeReport:
    """One current live DataNode record from ``dfsadmin -report``."""

    hostname: str
    address: str
    last_contact: str
    last_block_report: str


@dataclass(frozen=True, slots=True)
class BlockPlacement:
    """One non-empty HDFS block and its current transfer locations."""

    block_id: str
    length: int
    locations: tuple[str, ...]


def parse_datanode_report(text: str) -> dict[str, DataNodeReport]:
    """Parse and strictly validate the two live project DataNodes."""
    live = re.search(r"(?m)^Live datanodes \((\d+)\):\s*$", text)
    if live is None or live.group(1) != str(len(EXPECTED_DATANODES)):
        raise HdfsCommandError("Expected exactly two live DataNode registrations.")

    section = re.split(
        r"(?m)^(?:Dead|Decommissioning|Entering maintenance|In maintenance) datanodes",
        text[live.end() :],
        maxsplit=1,
    )[0]
    records = re.split(r"(?m)^Name:\s*", section)[1:]
    nodes: dict[str, DataNodeReport] = {}
    addresses: set[str] = set()
    for record in records:
        lines = record.splitlines()
        if not lines:
            continue
        fields = {
            key.strip(): value.strip()
            for key, value in re.findall(r"(?m)^([^:\n]+?)\s*:\s*([^\n]*)$", record)
        }
        address = lines[0].split()[0]
        hostname = fields.get("Hostname", "")
        if hostname not in EXPECTED_DATANODES or hostname in nodes:
            raise HdfsCommandError(f"Unexpected or duplicate live hostname: {hostname!r}.")
        if not address.endswith(":9866") or address in addresses:
            raise HdfsCommandError(f"Unexpected or duplicate DataNode address: {address!r}.")
        if fields.get("Decommission Status") != "Normal":
            raise HdfsCommandError(f"DataNode {hostname!r} is not in normal service.")
        nodes[hostname] = DataNodeReport(
            hostname=hostname,
            address=address,
            last_contact=fields.get("Last contact", ""),
            last_block_report=fields.get("Last Block Report", ""),
        )
        addresses.add(address)

    if set(nodes) != EXPECTED_DATANODES or len(addresses) != len(EXPECTED_DATANODES):
        raise HdfsCommandError("Both expected DataNodes must be live with distinct addresses.")
    return nodes


def parse_fsck(
    text: str,
    addresses: set[str],
    *,
    expected_replication: int = EXPECTED_REPLICATION,
) -> tuple[BlockPlacement, ...]:
    """Parse one file's fsck output and require complete two-node placement."""
    if len(addresses) != expected_replication:
        raise HdfsCommandError("Fsck validation requires two distinct live DataNode addresses.")
    if not re.search(r"(?m)^Status:\s+HEALTHY\s*$", text):
        raise HdfsCommandError("HDFS fsck is not HEALTHY.")
    if re.search(r"\b(?:MISSING|CORRUPT)\b", text):
        raise HdfsCommandError("HDFS fsck reports a missing or corrupt block.")
    for count in re.findall(
        r"(?im)^\s*(?:Missing blocks|Missing replicas|Corrupt blocks|"
        r"Blocks with corrupt replicas|Under[- ]replicated blocks|Mis[- ]replicated blocks)"
        r"\s*:\s*(\d+)",
        text,
    ):
        if int(count) != 0:
            raise HdfsCommandError("HDFS fsck reports incomplete replication.")

    declared = re.findall(r"replication=(\d+),\s*(\d+)\s+block\(s\):", text)
    if len(declared) != 1 or int(declared[0][0]) != expected_replication:
        raise HdfsCommandError("HDFS fsck does not declare the configured replication factor.")
    declared_count = int(declared[0][1])

    pattern = (
        r"(?m)^\s*\d+\.\s+((?:BP-[^\s:]+:)?blk_\d+_\d+)\s+len=(\d+)"
        r"\s+Live_repl=(\d+)\s+\[(.*)\]\s*$"
    )
    storage = r"DatanodeInfoWithStorage\[([^,\]]+),[^,\]]+,[^\]]+\]"
    blocks: list[BlockPlacement] = []
    for block_id, length, replicas, raw_locations in re.findall(pattern, text):
        locations = tuple(re.findall(storage, raw_locations))
        if re.sub(storage, "", raw_locations).strip(" ,"):
            raise HdfsCommandError(f"Unrecognized location in HDFS block {block_id}.")
        if (
            int(replicas) != expected_replication
            or len(locations) != expected_replication
            or len(set(locations)) != expected_replication
            or set(locations) != addresses
        ):
            raise HdfsCommandError(f"HDFS block {block_id} lacks exact current placement.")
        block_length = int(length)
        if block_length <= 0:
            raise HdfsCommandError(f"HDFS block {block_id} is empty.")
        blocks.append(BlockPlacement(block_id, block_length, locations))

    if (
        len(blocks) != declared_count
        or len({block.block_id for block in blocks}) != len(blocks)
        or (declared_count > 0 and not blocks)
    ):
        raise HdfsCommandError("HDFS fsck block records are incomplete or duplicated.")
    return tuple(blocks)


class HdfsCliClient:
    """Run bounded Hadoop CLI operations through an optional command prefix."""

    def __init__(
        self,
        command_prefix: Sequence[str] = (),
        *,
        hdfs_command: str = "hdfs",
        env: Mapping[str, str] | None = None,
        command_timeout: float = DEFAULT_COMMAND_TIMEOUT,
        transfer_chunk_size: int = DEFAULT_TRANSFER_CHUNK_SIZE,
        metadata_limit: int = DEFAULT_METADATA_LIMIT,
    ) -> None:
        if command_timeout <= 0:
            raise ValueError("command_timeout must be positive.")
        if transfer_chunk_size <= 0:
            raise ValueError("transfer_chunk_size must be positive.")
        if metadata_limit <= 0:
            raise ValueError("metadata_limit must be positive.")
        self.command_prefix = tuple(command_prefix)
        self.hdfs_command = hdfs_command
        self.environment = dict(os.environ)
        if env is not None:
            self.environment.update(env)
        self.command_timeout = command_timeout
        self.transfer_chunk_size = transfer_chunk_size
        self.metadata_limit = metadata_limit
        self._http_endpoint: str | None = None
        self._hdfs_identity: str | None = None

    def _argv(self, *arguments: str) -> list[str]:
        return [*self.command_prefix, self.hdfs_command, *arguments]

    def _run(
        self,
        arguments: Sequence[str],
        *,
        check: bool = True,
        timeout: float | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        argv = self._argv(*arguments)
        try:
            completed = subprocess.run(
                argv,
                capture_output=True,
                timeout=self.command_timeout if timeout is None else timeout,
                check=False,
                env=self.environment,
            )
        except subprocess.TimeoutExpired as error:
            raise HdfsCommandError(f"HDFS command timed out: {' '.join(argv)}") from error
        except OSError as error:
            raise HdfsCommandError(f"HDFS command unavailable: {argv[0]!r}: {error}") from error

        if (
            len(completed.stdout) > self.metadata_limit
            or len(completed.stderr) > self.metadata_limit
        ):
            raise HdfsCommandError("HDFS metadata command output exceeded the bounded limit.")
        if check and completed.returncode != 0:
            detail = completed.stderr.decode(errors="replace")[-2000:]
            raise HdfsCommandError(
                f"HDFS command failed (exit {completed.returncode}): {' '.join(argv)}\n{detail}"
            )
        return completed

    def _stream_upload(self, reader: BinaryIO, remote_path: str) -> None:
        argv = self._argv("dfs", "-put", "-", remote_path)
        try:
            process = subprocess.Popen(
                argv,
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=self.environment,
            )
        except OSError as error:
            raise HdfsCommandError(f"HDFS upload command unavailable: {error}") from error

        try:
            if process.stdin is None:
                raise HdfsCommandError("HDFS upload process has no stdin.")
            while chunk := reader.read(self.transfer_chunk_size):
                process.stdin.write(chunk)
            process.stdin.close()
            process.stdin = None
            returncode = process.wait(timeout=self.command_timeout)
        except BrokenPipeError as error:
            process.kill()
            process.wait()
            raise HdfsCommandError(f"HDFS upload failed for {remote_path!r}.") from error
        except subprocess.TimeoutExpired as error:
            process.kill()
            process.wait()
            raise HdfsCommandError(f"HDFS upload timed out for {remote_path!r}.") from error
        except BaseException:
            if process.poll() is None:
                process.kill()
                process.wait()
            raise
        if returncode != 0:
            raise HdfsCommandError(f"HDFS upload failed (exit {returncode}) for {remote_path!r}.")

    def _stream_remote(self, remote_path: str, *, collect: bool = False) -> RemoteFileHash | bytes:
        argv = self._argv("dfs", "-cat", remote_path)
        try:
            process = subprocess.Popen(
                argv,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                env=self.environment,
            )
        except OSError as error:
            raise HdfsCommandError(f"HDFS read command unavailable: {error}") from error

        digest = hashlib.sha256()
        payload = bytearray()
        size = 0
        try:
            if process.stdout is None:
                raise HdfsCommandError("HDFS read process has no stdout.")
            while chunk := process.stdout.read(self.transfer_chunk_size):
                size += len(chunk)
                if collect:
                    if size > self.metadata_limit:
                        process.kill()
                        process.wait()
                        raise HdfsCommandError("HDFS metadata read exceeded the bounded limit.")
                    payload.extend(chunk)
                else:
                    digest.update(chunk)
            process.stdout.close()
            returncode = process.wait(timeout=self.command_timeout)
        except subprocess.TimeoutExpired as error:
            process.kill()
            process.wait()
            raise HdfsCommandError(f"HDFS read timed out for {remote_path!r}.") from error
        except BaseException:
            if process.poll() is None:
                process.kill()
                process.wait()
            raise
        if returncode != 0:
            raise HdfsCommandError(f"HDFS read failed (exit {returncode}) for {remote_path!r}.")
        if collect:
            return bytes(payload)
        return RemoteFileHash(size, f"sha256:{digest.hexdigest()}")

    def upload_file(self, local_path: Path, remote_path: str) -> None:
        """Upload a local file through stdin without loading it into memory."""
        try:
            with Path(local_path).open("rb") as reader:
                self._stream_upload(reader, remote_path)
        except OSError as error:
            raise HdfsCommandError(f"Cannot read local upload {local_path}: {error}") from error

    def upload_bytes(self, payload: bytes, remote_path: str) -> None:
        """Upload bounded metadata through the same non-overwriting stdin path."""
        self._stream_upload(io.BytesIO(payload), remote_path)

    def hash_remote(self, remote_path: str) -> RemoteFileHash:
        """Hash remote bytes through a fresh streamed ``hdfs dfs -cat`` process."""
        result = self._stream_remote(remote_path)
        assert isinstance(result, RemoteFileHash)
        return result

    def read_remote(self, remote_path: str) -> bytes:
        """Read one bounded metadata file through a fresh Hadoop client."""
        result = self._stream_remote(remote_path, collect=True)
        assert isinstance(result, bytes)
        return result

    def stat(self, remote_path: str) -> RemoteFileStat:
        """Return byte size and replication from HDFS stat output."""
        output = self._run(("dfs", "-stat", "%b %r", remote_path)).stdout.decode().strip()
        fields = output.split()
        if len(fields) < 2 or not fields[0].isdecimal() or not fields[1].isdecimal():
            raise HdfsCommandError(f"Malformed HDFS stat output for {remote_path!r}.")
        return RemoteFileStat(int(fields[0]), int(fields[1]))

    def checksum(self, remote_path: str) -> str:
        """Return a fresh actual checksum, rejecting unbound or malformed records."""
        return self.checksums((remote_path,))[remote_path]

    def checksums(self, remote_paths: Sequence[str]) -> dict[str, str]:
        """Read every actual file checksum in bounded multi-path Hadoop CLI batches.

        Hadoop 3.4.3 Display.Checksum emits exactly path TAB algorithm TAB hex.
        MD5MD5CRC includes a 12-byte parameter header and 16-byte digest (56 hex);
        composite CRC is four bytes (8 hex), not the MD5 encoding.
        Never accept NONE, wildcard expansion, partial success, or an unbound record.
        No results are cached, including between preclaim and postseal verification.
        """
        paths = tuple(remote_paths)
        if len(paths) > 1024 or len(set(paths)) != len(paths):
            raise HdfsCommandError("Checksum paths exceed the bound or contain duplicates.")
        argument_limit = min(16 * 1024, self.metadata_limit - 256)
        for path in paths:
            if (
                not path.startswith("/")
                or path == "/"
                or str(PurePosixPath(path)) != path
                or ".." in PurePosixPath(path).parts
                or any(ord(character) <= 32 or character in "*?[]{}\\" for character in path)
                or len(path.encode()) + 1 > argument_limit
            ):
                raise HdfsCommandError(f"Unsafe or oversized checksum path: {path!r}.")
        result: dict[str, str] = {}
        offset = 0
        while offset < len(paths):
            batch: list[str] = []
            size = 0
            while offset < len(paths) and len(batch) < 64:
                path = paths[offset]
                cost = len(path.encode()) + 1
                if size + cost > argument_limit:
                    break
                batch.append(path)
                size += cost
                offset += 1
            try:
                lines = self._run(("dfs", "-checksum", *batch)).stdout.decode("utf-8").splitlines()
            except UnicodeDecodeError as error:
                raise HdfsCommandError("Malformed HDFS checksum encoding.") from error
            expected = set(batch)
            found: dict[str, str] = {}
            for line in lines:
                fields = line.split("\t")
                if (
                    len(fields) != 3
                    or fields[0] not in expected
                    or fields[0] in found
                    or not (
                        (
                            re.fullmatch(r"MD5-of-\d+MD5-of-\d+CRC32C?", fields[1])
                            and re.fullmatch(r"[0-9a-fA-F]{56}", fields[2])
                        )
                        or (
                            re.fullmatch(r"COMPOSITE-CRC32C?", fields[1])
                            and re.fullmatch(r"[0-9a-fA-F]{8}", fields[2])
                        )
                    )
                ):
                    raise HdfsCommandError(
                        "Malformed, unexpected, or duplicate HDFS checksum record."
                    )
                found[fields[0]] = "\t".join(fields[1:])
            if set(found) != expected:
                raise HdfsCommandError("HDFS checksum response is missing requested files.")
            result.update(found)
        return result

    def getconf(self, key: str) -> str:
        """Read one effective Hadoop configuration value."""
        output = self._run(("getconf", "-confKey", key)).stdout.decode().strip()
        if not output or "\n" in output:
            raise HdfsCommandError(f"Malformed Hadoop configuration output for {key!r}.")
        return output

    def whoami(self) -> str:
        """Return the local username used by HDFS simple authentication."""
        try:
            identity = pwd.getpwuid(os.getuid()).pw_name
        except (KeyError, OSError) as error:
            raise HdfsCommandError("Cannot resolve the local HDFS client identity.") from error
        if not identity or any(character in identity for character in "\0\r\n"):
            raise HdfsCommandError("Malformed local HDFS client identity.")
        return identity

    def effective_user(self) -> str:
        """Resolve the Hadoop client's actual UGI, including command-prefix runtimes."""
        if self._hdfs_identity is None:
            output = self._run(("groups",)).stdout.decode().strip()
            identity, separator, _groups = output.partition(" :")
            if not separator or not re.fullmatch(r"[A-Za-z0-9_.-]+", identity):
                raise HdfsCommandError(f"Cannot resolve effective HDFS identity: {output!r}")
            self._hdfs_identity = identity
        return self._hdfs_identity

    def _webhdfs(self, path: str, operation: str, *, method: str = "GET", **parameters):
        """Use bounded NameNode-only metadata RPCs on the configured internal endpoint."""
        if self._http_endpoint is None:
            rpc = urlsplit(self.getconf("fs.defaultFS"))
            http = urlsplit("//" + self.getconf("dfs.namenode.http-address"))
            if (
                rpc.scheme != "hdfs"
                or not rpc.hostname
                or http.port is None
                or http.hostname not in {rpc.hostname, "0.0.0.0", "::"}
            ):
                raise HdfsCommandError("WebHDFS endpoint differs from the configured NameNode")
            self._http_endpoint = f"http://{rpc.hostname}:{http.port}/webhdfs/v1"
        query = urlencode({"op": operation, "user.name": self.effective_user(), **parameters})
        request = Request(self._http_endpoint + quote(path, safe="/") + "?" + query, method=method)
        try:
            with build_opener(ProxyHandler({})).open(
                request, timeout=min(self.command_timeout, 20)
            ) as response:
                payload = response.read(self.metadata_limit + 1)
            if len(payload) > self.metadata_limit:
                raise HdfsCommandError("WebHDFS metadata response exceeded the bounded limit")
            return json.loads(payload) if payload else None
        except HTTPError:
            raise
        except (OSError, ValueError) as error:
            raise HdfsCommandError(f"WebHDFS {operation} failed for {path}: {error}") from error

    def path_status(self, path: str) -> dict | None:
        """Return exact-path metadata; distinguish absent paths from access denial."""
        try:
            return self._webhdfs(path, "GETFILESTATUS")["FileStatus"]
        except HTTPError as error:
            if error.code == 404:
                return None
            raise HdfsCommandError(
                f"HDFS metadata denied/unavailable: path={path} identity={self.effective_user()} "
                f"requires --x on ancestors (HTTP {error.code})"
            ) from error

    def list_status(self, path: str) -> tuple[dict, ...]:
        """List bounded immediate children with fresh NameNode types and byte lengths."""
        try:
            statuses = self._webhdfs(path, "LISTSTATUS")["FileStatuses"]["FileStatus"]
        except HTTPError as error:
            raise HdfsCommandError(
                f"HDFS listing denied/unavailable: path={path} (HTTP {error.code})"
            ) from error
        except (KeyError, TypeError) as error:
            raise HdfsCommandError(f"Malformed HDFS listing for {path}.") from error
        if not isinstance(statuses, list) or len(statuses) > 1024:
            raise HdfsCommandError(f"Malformed or oversized HDFS listing for {path}.")
        result = []
        seen = set()
        for status in statuses:
            if not isinstance(status, dict):
                raise HdfsCommandError(f"Malformed HDFS child status for {path}.")
            name, kind, length = (status.get(key) for key in ("pathSuffix", "type", "length"))
            if (
                not isinstance(name, str)
                or not name
                or name in {".", ".."}
                or "/" in name
                or any(ord(character) < 32 for character in name)
                or name in seen
                or not isinstance(kind, str)
                or kind not in {"DIRECTORY", "FILE", "SYMLINK"}
                or type(length) is not int
                or length < 0
            ):
                raise HdfsCommandError(f"Malformed or duplicate HDFS child status for {path}.")
            seen.add(name)
            result.append({"path": path.rstrip("/") + "/" + name, "type": kind, "length": length})
        return tuple(result)

    def can_access(self, path: str, permission: str) -> bool:
        """Ask the NameNode to check effective FsAction, including ancestor traversal."""
        try:
            self._webhdfs(path, "CHECKACCESS", fsaction=permission)
        except HTTPError as error:
            if error.code == 403:
                return False
            raise HdfsCommandError(
                f"HDFS access check failed for {path}: HTTP {error.code}"
            ) from error
        return True

    def rename_exclusive(self, source: str, destination: str) -> None:
        """Atomic rename2(NONE): never overwrite or nest inside an existing directory."""
        try:
            result = self._webhdfs(
                source, "RENAME", method="PUT", destination=destination, renameoptions="NONE"
            )
            if isinstance(result, dict) and result.get("boolean") is False:
                raise HdfsCommandError(
                    f"exclusive HDFS rename returned false: {source} -> {destination}"
                )
        except HTTPError as error:
            raise HdfsCommandError(
                f"exclusive HDFS rename failed: {source} -> {destination} (HTTP {error.code})"
            ) from error

    def clear_acl(self, path: str, *, recursive: bool = False) -> None:
        """Remove extended/default ACLs only on an explicitly selected subtree."""
        self._run(("dfs", "-setfacl", *(("-R",) if recursive else ()), "-b", path))

    def set_permission(self, path: str, mode: str, *, recursive: bool = False) -> None:
        """Set mode on an explicitly selected namespace parent or unpublished candidate."""
        self._run(("dfs", "-chmod", *(("-R",) if recursive else ()), mode, path))

    def set_owner(self, path: str, owner: str, *, recursive: bool = False) -> None:
        """Transfer only an explicitly selected unpublished candidate to its publisher."""
        self._run(("dfs", "-chown", *(("-R",) if recursive else ()), owner, path))

    def set_acl(self, remote_path: str, acl_spec: str) -> None:
        """Apply one explicit, non-recursive ACL specification to a path."""
        if not acl_spec or any(character in acl_spec for character in "\0\r\n"):
            raise HdfsCommandError("Unsafe HDFS ACL specification.")
        self._run(("dfs", "-setfacl", "-m", acl_spec, remote_path))

    def get_acl(self, remote_path: str) -> str:
        """Read one bounded HDFS ACL listing without traversing descendants."""
        return self._run(("dfs", "-getfacl", remote_path)).stdout.decode(errors="replace")

    def safe_mode(self) -> str:
        """Return the NameNode safe-mode status text."""
        output = self._run(("dfsadmin", "-safemode", "get")).stdout.decode().strip()
        if not output:
            raise HdfsCommandError("Empty HDFS safe-mode response.")
        return output

    def report(self) -> dict[str, DataNodeReport]:
        """Return exactly the two current live DataNode records."""
        output = self._run(("dfsadmin", "-report")).stdout.decode(errors="replace")
        return parse_datanode_report(output)

    def fsck(
        self,
        remote_path: str,
        addresses: set[str],
        *,
        expected_replication: int = EXPECTED_REPLICATION,
    ) -> tuple[BlockPlacement, ...]:
        """Audit every block and current location for one remote file."""
        output = self._run(("fsck", remote_path, "-files", "-blocks", "-locations")).stdout.decode(
            errors="replace"
        )
        return parse_fsck(output, addresses, expected_replication=expected_replication)

    def exists(self, remote_path: str) -> bool:
        """Check existence while distinguishing a normal false result from failure."""
        completed = self._run(("dfs", "-test", "-e", remote_path), check=False)
        if completed.returncode == 0:
            return True
        if completed.returncode == 1:
            return False
        detail = completed.stderr.decode(errors="replace")[-2000:]
        raise HdfsCommandError(
            f"HDFS existence check failed (exit {completed.returncode}) for {remote_path!r}: {detail}"
        )

    def is_directory(self, remote_path: str) -> bool:
        """Check whether an existing HDFS path is a directory."""
        completed = self._run(("dfs", "-test", "-d", remote_path), check=False)
        if completed.returncode == 0:
            return True
        if completed.returncode == 1:
            return False
        detail = completed.stderr.decode(errors="replace")[-2000:]
        raise HdfsCommandError(
            f"HDFS directory check failed (exit {completed.returncode}) for {remote_path!r}: {detail}"
        )

    def mkdir_parents(self, remote_path: str) -> None:
        """Create only a non-owned parent hierarchy."""
        self._run(("dfs", "-mkdir", "-p", remote_path))

    def mkdir_exclusive(self, remote_path: str) -> None:
        """Create one run-owned directory without ``-p`` or overwrite behavior."""
        self._run(("dfs", "-mkdir", remote_path))

    def list_children(self, remote_path: str) -> tuple[str, ...]:
        """List immediate child paths with bounded output."""
        output = self._run(("dfs", "-ls", "-C", remote_path)).stdout.decode().splitlines()
        return tuple(path.strip() for path in output if path.strip())

    def rename(self, source_path: str, destination_path: str) -> None:
        """Atomically rename a complete candidate without an overwrite flag."""
        self._run(("dfs", "-mv", source_path, destination_path))

    def remove(self, remote_path: str) -> None:
        """Remove one exact non-root path; callers must already prove ownership."""
        parts = remote_path.split("/")
        if (
            not remote_path.startswith("/")
            or remote_path in {"", "/"}
            or any(part in {"", ".", ".."} for part in parts[1:])
            or any(character in remote_path for character in "*?[")
        ):
            raise HdfsCommandError(f"Unsafe HDFS cleanup path: {remote_path!r}.")
        self._run(("dfs", "-rm", "-r", "-skipTrash", remote_path))

    # Names used by callers that describe the operation rather than the Hadoop CLI verb.
    stream_hash = hash_remote
    configuration = getconf
    fsck_placement = fsck
    cleanup_owned = remove

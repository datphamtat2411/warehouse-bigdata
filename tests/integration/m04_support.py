"""Shared infrastructure/evidence helpers; legacy real-M5 runners are retired.

No source ingestion, reduced-history execution, full acceptance, or benchmark matrix
lives here. Real-M5 callers use the canonical small-artifact selector.
"""

import hashlib
import json
import re
from pathlib import Path, PurePosixPath

from tests.integration.hdfs_support import HDFSVerification, VerificationError, require
from tests.integration.spark_support import MAX_M04_EVENT_BYTES, summarize_m04_event_log
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.common.hdfs_paths import HdfsPathError, render_staging_path
from warehouse_bigdata.common.hdfs_permissions import prepare_spark_run_permissions
from warehouse_bigdata.processing.m04_verification import audit_hdfs_outputs

ROOT = Path(__file__).resolve().parents[2]


def hdfs_client(hdfs: HDFSVerification, *, metadata_limit=MAX_M04_EVENT_BYTES):
    return HdfsCliClient(
        command_prefix=("docker", "exec", "-i", hdfs.client, "timeout", "--kill-after=5s", "120s"),
        metadata_limit=metadata_limit,
    )


def _validate_event_directory_path(config, path, *, expected_pipeline_run_id):
    try:
        staging = render_staging_path(config.base_path, expected_pipeline_run_id)
    except HdfsPathError as error:
        raise VerificationError("invalid expected pipeline run ID") from error
    if (
        not isinstance(path, str)
        or not path.startswith("/")
        or any(c in path for c in "\0\r\n")
        or any(part in {"", ".", ".."} for part in path.split("/")[1:])
    ):
        raise VerificationError("event directory has unsafe absolute path components")
    if str(PurePosixPath(path).parent) != staging or PurePosixPath(path).name not in {
        "events",
        "events-retry-j02",
        "events-retry-j03",
    }:
        raise VerificationError(
            "event directory must be a direct child of expected run-owned staging path"
        )
    return staging


def _create_event_directory(client, config, path, *, expected_pipeline_run_id):
    staging = _validate_event_directory_path(
        config, path, expected_pipeline_run_id=expected_pipeline_run_id
    )
    if not client.exists(staging):
        client.mkdir_parents(str(PurePosixPath(staging).parent))
        client.mkdir_exclusive(staging)
    elif not client.is_directory(staging):
        raise VerificationError("Run staging path is not a directory")
    if not client.exists(path):
        client.mkdir_exclusive(path)
    elif not client.is_directory(path):
        raise VerificationError("Event log path is not a directory")
    prepare_spark_run_permissions(
        client,
        config,
        pipeline_run_id=expected_pipeline_run_id,
        event_log_dir=config.uri + path,
        verify_effective_access=False,
    )


def _submit(hdfs, application, arguments, conf, output_dir):
    command = [
        "timeout",
        "--foreground",
        "--signal=TERM",
        "--kill-after=45s",
        "1800s",
        "bash",
        str(ROOT / "scripts/spark-submit.sh"),
    ]
    for key, value in conf.items():
        command += ["--conf", f"{key}={value}"]
    command += ["--", application, *arguments]
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "command.json").write_text(json.dumps(command, indent=2) + "\n")
    completed = hdfs.runner.run(command, timeout=1860, check=False)
    (output_dir / "stdout").write_bytes(completed.stdout)
    (output_dir / "stderr").write_bytes(completed.stderr)
    require(
        completed.returncode == 0,
        f"Spark application failed ({application}, exit {completed.returncode})\n"
        f"stdout (last 2000 characters):\n{completed.stdout.decode(errors='replace')[-2000:]}\n"
        f"stderr (last 2000 characters):\n{completed.stderr.decode(errors='replace')[-2000:]}\nFull logs: {output_dir}",
    )
    return completed.stdout


def _collect_event_summary(client, event_dir, *, run_id, application_name, output_dir):
    paths = client.list_children(event_dir)
    require(len(paths) == 1, "Expected one non-rolling event log")
    path = paths[0]
    require(
        re.fullmatch(r"app-\d{14}-\d+(?:\.inprogress)?", path.rsplit("/", 1)[-1]),
        "Unexpected event log name",
    )
    size = client.stat(path).size_bytes
    require(0 < size <= MAX_M04_EVENT_BYTES, "Event log exceeds the evidence bound")
    payload = client.read_remote(path)
    require(len(payload) == size, "Event log changed during independent retrieval")
    (output_dir / "event-log.jsonl").write_bytes(payload)
    summary = summarize_m04_event_log(payload, run_id=run_id, application_name=application_name)
    summary["event_log"] = dict(path=path, bytes=size, sha256=hashlib.sha256(payload).hexdigest())
    (output_dir / "execution.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def _job_status(stdout):
    statuses = [
        line.removeprefix("status=")
        for line in stdout.decode().splitlines()
        if line.startswith("status=")
    ]
    require(
        len(statuses) == 1 and re.fullmatch(r"[A-Z][A-Z0-9_]*", statuses[0]),
        "Expected one structured status",
    )
    return statuses[0]


def _immutable_snapshot(client, output_paths, metadata_paths):
    outputs = audit_hdfs_outputs(client, output_paths, include_hashes=True)
    metadata = {}
    for path in metadata_paths:
        digest = client.hash_remote(path)
        metadata[path] = dict(bytes=digest.size_bytes, sha256=digest.sha256)
    payload = json.dumps(
        dict(outputs=outputs, metadata=metadata), sort_keys=True, separators=(",", ":")
    ).encode()
    return dict(
        outputs=outputs,
        metadata=metadata,
        snapshot_sha256="sha256:" + hashlib.sha256(payload).hexdigest(),
    )

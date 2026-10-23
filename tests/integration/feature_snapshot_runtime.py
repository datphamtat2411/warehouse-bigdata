"""Bounded commands and streamed Spark evidence for J04 acceptance only."""

from __future__ import annotations

import json
import os
import selectors
import signal
import subprocess
import threading
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OWNER_LABEL = "io.warehouse-bigdata.j04-test.owner"
LOG_BYTES = 256 * 1024
EVENT_LINE_BYTES = 1024 * 1024
BUDGETS = {
    "metadata": 180,
    "readback": 900,
    "events": 300,
    "initial": 2400,
    "reuse": 1800,
    "publication": 600,
    "recovery": 600,
}


class CommandError(AssertionError):
    """A bounded operation failed; includes its retained diagnostic tail."""


def _terminate(process):
    # Kill the entire session, even if the direct child has already exited.
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=5)


def cleanup_container(owned):
    """Only a name AND matching random ownership label authorize removal."""
    name, token = owned
    inspect = [
        "docker",
        "inspect",
        "--type",
        "container",
        "--format",
        '{{ index .Config.Labels "' + OWNER_LABEL + '" }}',
        name,
    ]
    # Recheck delayed creates after terminating an attached Compose client.
    for attempt in range(3):
        result = subprocess.run(inspect, capture_output=True, timeout=10, check=False)
        if result.returncode:
            if b"No such" not in result.stderr:
                raise CommandError(f"Cannot inspect owned container {name}: {result.stderr!r}")
            if attempt < 2:
                time.sleep(0.2)
            continue
        if result.stdout.decode().strip() != token:
            raise CommandError(f"Container ownership mismatch; refusing cleanup: {name}")
        subprocess.run(
            ["docker", "stop", "--time", "10", name], capture_output=True, timeout=15, check=False
        )
        removed = subprocess.run(
            ["docker", "rm", "--force", name], capture_output=True, timeout=10, check=False
        )
        if removed.returncode and b"No such" not in removed.stderr:
            raise CommandError(f"Cannot remove owned container {name}: {removed.stderr!r}")


def _drain(process, deadline, tail, on_stdout_line):
    pending = bytearray()
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ, True)
        selector.register(process.stderr, selectors.EVENT_READ, False)
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError
            for key, _ in selector.select(min(remaining, 0.2)):
                chunk = os.read(key.fileobj.fileno(), 16 * 1024)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                tail.extend(chunk)
                del tail[:-LOG_BYTES]
                if on_stdout_line is not None and key.data:
                    pending.extend(chunk)
                    while b"\n" in pending:
                        line, _, remainder = pending.partition(b"\n")
                        if len(line) > EVENT_LINE_BYTES:
                            raise CommandError("Oversized stdout/event line")
                        on_stdout_line(bytes(line))
                        pending = bytearray(remainder)
                    if len(pending) > EVENT_LINE_BYTES:
                        raise CommandError("Oversized stdout/event line")
        if pending and on_stdout_line is not None:
            on_stdout_line(bytes(pending))
    process.wait(timeout=max(0.001, deadline - time.monotonic()))


def command(args, *, operation, timeout=None, owned=None, allowed_codes=(0,), on_stdout_line=None):
    """Drain continuously with a fixed-size tail; always reap children and own containers.

    SIGTERM and Ctrl-C follow the same cleanup path as exceptions. HDFS evidence
    is never removed. SIGKILL/host failure cannot execute Python cleanup.
    """
    budget = BUDGETS[operation] if timeout is None else timeout
    tail = bytearray()
    process = None
    previous = None
    interrupted = None
    if threading.current_thread() is threading.main_thread():
        previous = signal.getsignal(signal.SIGTERM)

        def cancel(signum, frame):
            raise KeyboardInterrupt("J04 operation received SIGTERM")

        signal.signal(signal.SIGTERM, cancel)
    try:
        process = subprocess.Popen(
            args, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True
        )
        try:
            _drain(process, time.monotonic() + budget, tail, on_stdout_line)
        except (TimeoutError, subprocess.TimeoutExpired) as error:
            raise CommandError(
                f"{operation} timed out after {budget:g}s; terminating process group {process.pid}; "
                f"owned_container={owned}; preserve HDFS staging for owner recovery. "
                f"Diagnostic tail (max {LOG_BYTES} bytes):\n{tail.decode(errors='replace')}"
            ) from error
        if process.returncode not in allowed_codes:
            raise CommandError(
                f"{operation} exited {process.returncode}; owned_container={owned}; "
                f"preserve HDFS evidence. Diagnostic tail:\n{tail.decode(errors='replace')}"
            )
        return subprocess.CompletedProcess(args, process.returncode, tail.decode(errors="replace"))
    except BaseException as error:
        interrupted = error
        raise
    finally:
        # Avoid a second TERM interrupting cleanup itself.
        if previous is not None:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        try:
            if process is not None:
                _terminate(process)
                process.stdout.close()
                process.stderr.close()
            if owned is not None:
                cleanup_container(owned)
        except Exception as error:
            if interrupted is None:
                raise
            interrupted.add_note(f"J04 cleanup failed: {error}")
        finally:
            if previous is not None:
                signal.signal(signal.SIGTERM, previous)


class TaskEvidence:
    def __init__(self, application_id):
        self.application_id = application_id
        self.counts, self.metrics = Counter(), Counter()
        self.applications = set()
        self.event_count = 0

    def add(self, line):
        assert len(line) <= EVENT_LINE_BYTES, "Oversized Spark event line"
        try:
            event = json.loads(line)
        except (ValueError, TypeError) as error:
            raise AssertionError("Invalid/truncated Spark event evidence") from error
        assert isinstance(event, dict), "Invalid Spark event object"
        self.event_count += 1
        if event.get("Event") == "SparkListenerApplicationStart":
            self.applications.add(event.get("App ID"))
        if (
            event.get("Event") == "SparkListenerTaskEnd"
            and event.get("Task End Reason", {}).get("Reason") == "Success"
        ):
            info = event["Task Info"]
            if info.get("Failed", False) or info.get("Killed", False):
                return
            self.counts[info["Host"]] += 1
            task = event.get("Task Metrics", {})
            for key in ("Memory Bytes Spilled", "Disk Bytes Spilled", "JVM GC Time"):
                self.metrics[key] += task.get(key, 0)
            self.metrics["Input Bytes"] += task.get("Input Metrics", {}).get("Bytes Read", 0)
            self.metrics["Output Bytes"] += task.get("Output Metrics", {}).get("Bytes Written", 0)

    def result(self):
        assert self.applications == {self.application_id}, (
            f"Spark application evidence mismatch: {self.applications}"
        )
        assert all(self.counts[h] > 0 for h in ("spark-worker-1", "spark-worker-2")), (
            f"Task evidence must include both workers: {self.counts}"
        )
        return {
            "application_id": self.application_id,
            "events": self.event_count,
            "worker_tasks": dict(self.counts),
            "task_metrics": dict(self.metrics),
        }


def task_evidence(lines, application_id):
    """One pass over an iterator, keeping only counters and one bounded event."""
    tracker = TaskEvidence(application_id)
    for line in lines:
        tracker.add(line)
    return tracker.result()


def stream_task_evidence(client, directory, application_id):
    from pathlib import PurePosixPath

    files = client.list_status(directory)
    assert 0 < len(files) <= 64, f"Missing/unbounded producer event evidence: {directory}"
    paths = []
    for status in files:
        path = status["path"]
        assert (
            status["type"] == "FILE"
            and str(PurePosixPath(path).parent) == directory
            and not path.endswith(".inprogress")
        ), f"Incomplete/unsupported event evidence: {path}"
        paths.append(path)
    assert len(set(paths)) == len(paths), "Duplicate event paths"
    tracker = TaskEvidence(application_id)
    command(
        [*client.command_prefix, client.hdfs_command, "dfs", "-text", *sorted(paths)],
        operation="events",
        on_stdout_line=tracker.add,
    )
    checksums = client.checksums(sorted(paths))
    assert set(checksums) == set(paths) and all(checksums.values()), "Missing event checksums"
    return {**tracker.result(), "paths": sorted(paths), "file_checksums": checksums}

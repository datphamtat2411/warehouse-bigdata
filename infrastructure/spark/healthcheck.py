"""Bounded, non-submitting Spark 3.5 Standalone health and registration probes."""

import json
import os
import runpy
import socket
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import ProxyHandler, build_opener

MASTER_HOST = "spark-master"
MASTER_URL = "spark://spark-master:7077"
WORKERS = ("spark-worker-1", "spark-worker-2")
PROBE_SECONDS = 6.0
IO_SECONDS = 1.0
HEARTBEAT_SECONDS = 30
MAX_RESPONSE_BYTES = 1024 * 1024
# These are internal bridge requests; host proxy settings must not redirect them.
urlopen = build_opener(ProxyHandler({})).open


class ProbeError(RuntimeError):
    """A daemon is unreachable, malformed, or not currently registered as expected."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ProbeError(message)


def remaining(deadline: float) -> float:
    seconds = deadline - time.monotonic()
    require(seconds > 0, "Probe deadline exceeded")
    return min(seconds, IO_SECONDS)


def read_json(host: str, port: int, deadline: float) -> dict:
    url = f"http://{host}:{port}/json/"
    try:
        with urlopen(url, timeout=remaining(deadline)) as response:
            payload = response.read(MAX_RESPONSE_BYTES + 1)
        require(len(payload) <= MAX_RESPONSE_BYTES, f"Oversized response from {url}")
        result = json.loads(payload)
    except (OSError, URLError, ValueError) as error:
        raise ProbeError(f"HTTP probe failed for {url}: {error}") from error
    require(isinstance(result, dict), f"Expected a JSON object from {url}")
    return result


def check_tcp(host: str, port: int, deadline: float) -> None:
    try:
        with socket.create_connection((host, port), timeout=remaining(deadline)):
            pass
    except OSError as error:
        raise ProbeError(f"TCP probe failed for {host}:{port}: {error}") from error


def check_master(deadline: float | None = None) -> dict:
    if deadline is None:
        deadline = time.monotonic() + PROBE_SECONDS
    check_tcp(MASTER_HOST, 7077, deadline)
    master = read_json(MASTER_HOST, 8080, deadline)
    require(master.get("url") == MASTER_URL, "Unexpected Master URL")
    require(master.get("status") == "ALIVE", "Master is not ALIVE")
    return master


def live_workers(master: dict) -> list[dict]:
    workers = master.get("workers")
    require(isinstance(workers, list), "Master workers must be a list")
    for worker in workers:
        require(isinstance(worker, dict), "Malformed Master Worker entry")
        require(worker.get("state") in ("ALIVE", "DEAD"), "Invalid Worker state")
    return [worker for worker in workers if worker["state"] == "ALIVE"]


def exact_integer(value: object, expected: int) -> bool:
    return type(value) is int and value == expected


def check_worker(host: str, master: dict | None = None, deadline: float | None = None) -> dict:
    require(host in WORKERS, "Expected an allowlisted Worker hostname")
    if deadline is None:
        deadline = time.monotonic() + PROBE_SECONDS
    if master is None:
        master = check_master(deadline)
    # The local UI must be reachable now; cached Master registrations are insufficient.
    local = read_json(host, 8081, deadline)
    check_tcp(host, 7078, deadline)
    worker_id = local.get("id")
    require(isinstance(worker_id, str) and bool(worker_id), f"Missing current ID for {host}")
    require(local.get("masterurl") == MASTER_URL, f"{host} reports a different Master")
    matches = [worker for worker in live_workers(master) if worker.get("id") == worker_id]
    require(len(matches) == 1, f"{host} current ID is not uniquely registered ALIVE")
    worker = matches[0]
    require(worker.get("host") == host, f"Incorrect advertised hostname for {host}")
    require(exact_integer(worker.get("port"), 7078), f"Incorrect RPC port for {host}")
    require(
        worker.get("webuiaddress") == f"http://{host}:8081",
        f"Incorrect advertised UI for {host}",
    )
    for field, expected in (("cores", 1), ("memory", 1024)):
        require(exact_integer(worker.get(field), expected), f"Incorrect registered {field}: {host}")
        require(exact_integer(local.get(field), expected), f"Incorrect local {field}: {host}")
    heartbeat = worker.get("lastheartbeat")
    require(type(heartbeat) is int, f"Invalid heartbeat for {host}")
    age = time.time() - heartbeat / 1000
    require(-5 <= age <= HEARTBEAT_SECONDS, f"Stale or future heartbeat for {host}: {age:.1f}s")
    return local


def check_ready(deadline: float | None = None) -> None:
    if deadline is None:
        deadline = time.monotonic() + PROBE_SECONDS
    master = check_master(deadline)
    workers = live_workers(master)
    require(len(workers) == 2, "Expected exactly two ALIVE Workers")
    require(
        sorted(str(worker.get("host")) for worker in workers) == sorted(WORKERS),
        "Expected exactly the two configured Worker hosts",
    )
    ids = [worker.get("id") for worker in workers]
    require(all(isinstance(value, str) and value for value in ids), "Invalid registered Worker ID")
    require(len(set(ids)) == 2, "Duplicate registered Worker IDs")
    for field, expected in (("aliveworkers", 2), ("cores", 2), ("memory", 2048)):
        require(exact_integer(master.get(field), expected), f"Incorrect Master total {field}")
    for host in WORKERS:
        check_worker(host, master, deadline)


def wait_ready(seconds: int = 180) -> None:
    require(type(seconds) is int and 1 <= seconds <= 600, "Wait must be between 1 and 600 seconds")
    deadline = time.monotonic() + seconds
    last_error = "No readiness check completed"
    while time.monotonic() < deadline:
        try:
            check_ready(deadline)
            return
        except ProbeError as error:
            last_error = str(error)
        time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
    raise ProbeError(f"Readiness deadline exceeded after {seconds}s: {last_error}")


def main(arguments: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if arguments is None else arguments
    try:
        require(bool(arguments), "Expected master, worker, ready, or wait-ready [seconds]")
        mode, *extra = arguments
        require(mode in ("master", "worker", "ready", "wait-ready"), "Unknown probe mode")
        require(len(extra) <= (1 if mode == "wait-ready" else 0), "Unexpected probe arguments")
        if mode == "master":
            check_master()
        elif mode == "worker":
            scratch = runpy.run_path(str(Path(__file__).with_name("scratch.py")))
            scratch["check_scratch"](scratch["HEALTH_FREE"])
            deadline = time.monotonic() + PROBE_SECONDS
            master = check_master(deadline)
            local = check_worker(os.environ.get("SPARK_WORKER_HOST", ""), master, deadline)
            scratch["cleanup_work"](local, master)
        elif mode == "ready":
            check_ready()
        else:
            wait_ready(int(extra[0]) if extra else 180)
    except (RuntimeError, OSError, ValueError) as error:
        print(f"Spark probe failed: {error}", file=sys.stderr)
        return 1
    print(f"Spark {mode} probe passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())

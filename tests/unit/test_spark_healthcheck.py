"""Standalone registration checks without Docker, Spark, or network access."""

import copy
import importlib.util
import io
import json
from pathlib import Path
from urllib.error import URLError

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "spark_healthcheck", ROOT / "infrastructure/spark/healthcheck.py"
)
assert SPEC and SPEC.loader
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)
NOW = 1_790_000_000.0
HOSTS = ("spark-worker-1", "spark-worker-2")


@pytest.fixture
def cluster(monkeypatch: pytest.MonkeyPatch) -> dict:
    workers = [
        {
            "id": f"worker-current-{index}",
            "host": host,
            "port": 7078,
            "webuiaddress": f"http://{host}:8081",
            "cores": 1,
            "memory": 1024,
            "state": "ALIVE",
            "lastheartbeat": int(NOW * 1000) - 1000,
        }
        for index, host in enumerate(HOSTS, 1)
    ]
    master = {
        "url": "spark://spark-master:7077",
        "status": "ALIVE",
        "workers": workers,
        "aliveworkers": 2,
        "cores": 2,
        "memory": 2048,
    }
    local = {
        host: {
            "id": worker["id"],
            "masterurl": master["url"],
            "cores": 1,
            "memory": 1024,
        }
        for host, worker in zip(HOSTS, workers, strict=True)
    }
    calls = []

    def read_json(host: str, port: int, deadline: float) -> dict:
        calls.append(("http", host, port))
        return copy.deepcopy(master if port == 8080 else local[host])

    def tcp(host: str, port: int, deadline: float) -> None:
        calls.append(("tcp", host, port))

    monkeypatch.setattr(probe, "read_json", read_json)
    monkeypatch.setattr(probe, "check_tcp", tcp)
    monkeypatch.setattr(probe.time, "time", lambda: NOW)
    return {"master": master, "local": local, "calls": calls}


def test_ready_requires_both_current_workers_and_their_rpc_listeners(cluster: dict) -> None:
    probe.check_ready()
    for host in HOSTS:
        assert ("http", host, 8081) in cluster["calls"]
        assert ("tcp", host, 7078) in cluster["calls"]
    assert ("tcp", "spark-master", 7077) in cluster["calls"]


def test_master_health_does_not_require_any_workers(cluster: dict) -> None:
    cluster["master"].update(workers=[], aliveworkers=0, cores=0, memory=0)
    probe.check_master()
    assert not any(call[1] in HOSTS for call in cluster["calls"])


def test_worker_health_only_requires_its_own_current_registration(cluster: dict) -> None:
    cluster["master"]["workers"].pop()
    probe.check_worker(HOSTS[0])
    assert not any(call[1] == HOSTS[1] for call in cluster["calls"])


def test_retained_dead_registration_is_ignored(cluster: dict) -> None:
    dead = {**cluster["master"]["workers"][0], "state": "DEAD", "lastheartbeat": 0}
    cluster["master"]["workers"].append(dead)
    probe.check_ready()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", "worker-old-id"),
        ("id", ""),
        ("host", "unexpected-worker"),
        ("port", 7079),
        ("webuiaddress", "http://unexpected-worker:8081"),
        ("cores", 2),
        ("cores", True),
        ("memory", 2048),
        ("state", "DEAD"),
        ("lastheartbeat", int((NOW - 31) * 1000)),
        ("lastheartbeat", int((NOW + 10) * 1000)),
        ("lastheartbeat", "not-a-timestamp"),
    ],
)
def test_incorrect_or_stale_registration_fails(cluster: dict, field: str, value: object) -> None:
    cluster["master"]["workers"][0][field] = value
    with pytest.raises(probe.ProbeError):
        probe.check_ready()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", "worker-prior-process"),
        ("masterurl", "spark://other:7077"),
        ("cores", 2),
        ("memory", 512),
    ],
)
def test_current_local_report_must_match_master(cluster: dict, field: str, value: object) -> None:
    cluster["local"][HOSTS[0]][field] = value
    with pytest.raises(probe.ProbeError):
        probe.check_worker(HOSTS[0])


@pytest.mark.parametrize("failure", ["missing", "extra", "duplicate-id", "duplicate-host"])
def test_exact_expected_worker_membership(cluster: dict, failure: str) -> None:
    workers = cluster["master"]["workers"]
    if failure == "missing":
        workers.pop()
    elif failure == "extra":
        workers.append({**workers[0], "id": "extra", "host": "spark-worker-3"})
    elif failure == "duplicate-id":
        workers[1]["id"] = workers[0]["id"]
        cluster["local"][HOSTS[1]]["id"] = workers[0]["id"]
    else:
        workers[1]["host"] = workers[0]["host"]
    with pytest.raises(probe.ProbeError):
        probe.check_ready()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("url", "spark://other:7077"),
        ("status", "STANDBY"),
        ("workers", None),
        ("workers", [None]),
        ("aliveworkers", 1),
        ("cores", 3),
        ("memory", 4096),
    ],
)
def test_malformed_or_inconsistent_master_fails(cluster: dict, field: str, value: object) -> None:
    cluster["master"][field] = value
    with pytest.raises(probe.ProbeError):
        probe.check_ready()


def test_stopped_worker_fails_even_with_fresh_cached_registration(
    cluster: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = probe.read_json

    def stopped(host: str, port: int, deadline: float) -> dict:
        if host == HOSTS[1]:
            raise probe.ProbeError("connection refused")
        return original(host, port, deadline)

    monkeypatch.setattr(probe, "read_json", stopped)
    with pytest.raises(probe.ProbeError, match="connection refused"):
        probe.check_ready()


@pytest.mark.parametrize("payload", [b"not json", b"[]", b"null", b'{"workers":'])
def test_bad_http_payload_is_a_probe_failure(
    monkeypatch: pytest.MonkeyPatch, payload: bytes
) -> None:
    monkeypatch.setattr(probe, "urlopen", lambda *args, **kwargs: io.BytesIO(payload))
    with pytest.raises(probe.ProbeError):
        probe.read_json("spark-master", 8080, probe.time.monotonic() + 5)


def test_http_is_direct_bounded_and_parses_json(monkeypatch: pytest.MonkeyPatch) -> None:
    def open_url(url: str, timeout: float) -> io.BytesIO:
        assert url == "http://spark-master:8080/json/"
        assert 0 < timeout <= 1
        return io.BytesIO(json.dumps({"status": "ALIVE"}).encode())

    monkeypatch.setattr(probe, "urlopen", open_url)
    assert probe.read_json("spark-master", 8080, probe.time.monotonic() + 5) == {"status": "ALIVE"}


@pytest.mark.parametrize("exception", [URLError("refused"), TimeoutError("timed out")])
def test_failed_http_is_a_probe_failure(
    monkeypatch: pytest.MonkeyPatch, exception: Exception
) -> None:
    def fail(*args, **kwargs):
        raise exception

    monkeypatch.setattr(probe, "urlopen", fail)
    with pytest.raises(probe.ProbeError):
        probe.read_json("spark-master", 8080, probe.time.monotonic() + 5)


def test_tcp_timeout_is_bounded_and_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(address: tuple, timeout: float):
        assert address == (HOSTS[0], 7078)
        assert 0 < timeout <= 1
        raise TimeoutError("timed out")

    monkeypatch.setattr(probe.socket, "create_connection", fail)
    with pytest.raises(probe.ProbeError):
        probe.check_tcp(HOSTS[0], 7078, probe.time.monotonic() + 5)


def test_expired_deadline_does_not_attempt_io(monkeypatch: pytest.MonkeyPatch) -> None:
    def unexpected(*args, **kwargs):
        pytest.fail("expired deadline attempted network I/O")

    monkeypatch.setattr(probe, "urlopen", unexpected)
    monkeypatch.setattr(probe.socket, "create_connection", unexpected)
    for operation, port in [(probe.read_json, 8080), (probe.check_tcp, 7077)]:
        with pytest.raises(probe.ProbeError, match="deadline"):
            operation("spark-master", port, probe.time.monotonic() - 1)


def test_wait_has_one_monotonic_deadline_and_finite_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [100.0]
    deadlines = []

    def unavailable(deadline: float) -> None:
        deadlines.append(deadline)
        raise probe.ProbeError("worker unavailable")

    monkeypatch.setattr(probe.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        probe.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    monkeypatch.setattr(probe, "check_ready", unavailable)
    with pytest.raises(probe.ProbeError, match="worker unavailable"):
        probe.wait_ready(3)
    assert clock[0] == 103
    assert 1 <= len(deadlines) <= 4
    assert set(deadlines) == {103.0}


@pytest.mark.parametrize("seconds", [0, -1, 601])
def test_invalid_wait_bounds_fail_without_checking(seconds: int) -> None:
    with pytest.raises(probe.ProbeError, match="1.*600"):
        probe.wait_ready(seconds)


@pytest.mark.parametrize("arguments", [[], ["submit"], ["ready", "extra"], ["wait-ready", "bad"]])
def test_cli_rejects_invalid_commands(arguments: list[str]) -> None:
    assert probe.main(arguments) != 0


def test_worker_health_propagates_scratch_capacity_failure(cluster, monkeypatch, capsys):
    def full_disk(min_free):
        raise RuntimeError("Insufficient Spark scratch")

    monkeypatch.setattr(
        probe.runpy,
        "run_path",
        lambda path: {"check_scratch": full_disk, "HEALTH_FREE": 512 * 1024**2},
    )
    assert probe.main(["worker"]) == 1
    assert "Insufficient Spark scratch" in capsys.readouterr().err

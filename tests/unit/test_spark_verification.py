"""Offline M02 T03 contracts; collection never imports PySpark or contacts Docker."""

import copy
import hashlib
import json
import subprocess
import sys
from types import SimpleNamespace

import pytest

from tests.integration import spark_support as support
from tests.integration.hdfs_support import Evidence, HDFSVerification, VerificationError
from warehouse_bigdata.common import spark_verification as workload
from warehouse_bigdata.common.config import ConfigurationError

RUN = "t03-20261020T000000-spark-0123456789abcdef0123456789abcdef"
FIXTURE = "hdfs://hdfs-namenode:8020/t03-integration-0123456789abcdef0123456789abcdef/spark"
APP = "app-20261020000000-0001"
HOSTS = ("spark-worker-1", "spark-worker-2")
ENV = {
    "WAREHOUSE_SPARK_MASTER_URL": "spark://spark-master:7077",
    "WAREHOUSE_HDFS_URI": "hdfs://hdfs-namenode:8020",
}
SHA = "a" * 64


@pytest.mark.parametrize(
    "change",
    [
        {"WAREHOUSE_SPARK_MASTER_URL": "local[*]"},
        {"WAREHOUSE_SPARK_MASTER_URL": "spark://other:7077"},
        {"WAREHOUSE_SPARK_MASTER_URL": "spark://spark-master:7077/"},
        {"WAREHOUSE_HDFS_URI": "hdfs://other:8020"},
        {"WAREHOUSE_HDFS_URI": "file:///tmp"},
        {"WAREHOUSE_HDFS_URI": "hdfs://hdfs-namenode:8020/"},
    ],
)
def test_only_the_exact_standalone_and_hdfs_endpoints_are_accepted(change) -> None:
    with pytest.raises(ConfigurationError):
        workload.validate_request(RUN, FIXTURE, SHA, {**ENV, **change})


@pytest.mark.parametrize(
    "fixture",
    [
        "file:///tmp/spark",
        "hdfs://other:8020" + FIXTURE.split(":8020")[1],
        "hdfs://hdfs-namenode:8020/data/warehouse/spark",
        FIXTURE + "/..",
        FIXTURE + "/output",
        FIXTURE.replace("/spark", "//spark"),
        FIXTURE.replace("/spark", "/%73park"),
        FIXTURE + "?x=1",
        FIXTURE + "#fragment",
        FIXTURE.replace("hdfs://", "hdfs://user@"),
    ],
)
def test_paths_are_exact_owned_fully_qualified_subtrees(fixture) -> None:
    with pytest.raises(ConfigurationError):
        workload.validate_request(RUN, fixture, SHA, ENV)


def test_invalid_identity_or_probe_digest_is_rejected() -> None:
    workload.validate_request(RUN, FIXTURE, SHA, ENV)
    for run, digest in (("../unsafe", SHA), ("", SHA), (RUN, "x" * 64), (RUN, "abc")):
        with pytest.raises(ConfigurationError):
            workload.validate_request(run, FIXTURE, digest, ENV)


def test_existing_hdfs_harness_pytest_run_identity_is_accepted(tmp_path) -> None:
    evidence = Evidence(tmp_path, "test_distributed_spark_hdfs_round_trip")
    workload.validate_request(evidence.run_id, FIXTURE, SHA, ENV)


def test_local_is_rejected_before_any_pyspark_import(monkeypatch) -> None:
    monkeypatch.setenv("WAREHOUSE_SPARK_MASTER_URL", "local[*]")
    monkeypatch.setenv("WAREHOUSE_HDFS_URI", ENV["WAREHOUSE_HDFS_URI"])
    with pytest.raises(ConfigurationError, match="Standalone"):
        workload.run_verification(run_id=RUN, fixture_root=FIXTURE, probe_sha256=SHA)


def test_event_and_barrier_settings_are_bounded_and_hdfs_backed() -> None:
    settings = workload.spark_settings(FIXTURE)
    assert settings["spark.eventLog.enabled"] == "true"
    assert settings["spark.eventLog.dir"] == FIXTURE + "/events"
    assert settings["spark.eventLog.compress"] == "false"
    assert settings["spark.eventLog.rolling.enabled"] == "false"
    assert settings["spark.barrier.sync.timeout"] == "45s"
    assert settings["spark.scheduler.barrier.maxConcurrentTasksCheck.maxFailures"] == "5"
    assert settings["spark.scheduler.barrier.maxConcurrentTasksCheck.interval"] == "2s"
    assert settings["spark.task.cpus"] == "1"
    assert settings["spark.ui.enabled"] == "false"


def test_standalone_and_event_logging_are_explicit_before_context_creation(monkeypatch) -> None:
    configured = {}

    class Builder:
        def master(self, value):
            configured["spark.master"] = value
            return self

        def appName(self, value):
            configured["spark.app.name"] = value
            return self

        def config(self, key, value):
            configured[key] = value
            return self

        def getOrCreate(self):
            assert configured["spark.master"] == ENV["WAREHOUSE_SPARK_MASTER_URL"]
            assert configured["spark.eventLog.enabled"] == "true"
            assert configured["spark.eventLog.dir"] == FIXTURE + "/events"
            raise RuntimeError("stopped at context creation")

    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("_WAREHOUSE_SPARK_DRIVER_HOST", "warehouse-spark-submit-" + "b" * 32)
    monkeypatch.setattr(workload, "source_fingerprint", lambda: {})
    monkeypatch.setitem(
        sys.modules,
        "pyspark",
        SimpleNamespace(SparkContext=SimpleNamespace(_active_spark_context=None)),
    )
    monkeypatch.setitem(
        sys.modules,
        "pyspark.sql",
        SimpleNamespace(
            SparkSession=SimpleNamespace(builder=Builder()), functions=object(), types=object()
        ),
    )
    with pytest.raises(RuntimeError, match="stopped at context creation"):
        workload.run_verification(run_id=RUN, fixture_root=FIXTURE, probe_sha256=SHA)


def test_fixture_has_fixed_arithmetic_rows_and_independent_golden_totals() -> None:
    files = support.synthetic_files()
    assert list(files) == [f"part-{index:02d}.jsonl" for index in range(4)]
    rows = [json.loads(line) for payload in files.values() for line in payload.splitlines()]
    assert len(rows) == 1024
    assert all(len(payload.splitlines()) == 256 for payload in files.values())
    assert rows[0] == {"record_id": 0, "bucket": 0, "quantity": 1, "unit_price_cents": 100}
    assert rows[-1] == {"record_id": 1023, "bucket": 7, "quantity": 4, "unit_price_cents": 121}
    assert all(type(value) is int for row in rows for value in row.values())
    golden = support.expected_rows()
    assert [row["bucket"] for row in golden] == list(range(8))
    assert [row["record_count"] for row in golden] == [85, 85, 86, 85, 85, 86, 85, 85]
    assert [row["quantity_total"] for row in golden] == [252, 253, 256, 257, 258, 259, 257, 253]
    assert sum(row["record_count"] for row in golden) == 682
    # Separate tiny hand-computed monetary example exercises the integer contract.
    assert rows[1]["quantity"] * rows[1]["unit_price_cents"] == 214


def actual_result() -> dict:
    return {
        "run_id": RUN,
        "application_id": APP,
        "input_count": 1024,
        "rows": support.expected_rows(),
        "schema": {
            "bucket": "int",
            "record_count": "bigint",
            "quantity_total": "bigint",
            "amount_cents": "bigint",
        },
        "row_count": 8,
        "unique_keys": 8,
        "null_count": 0,
        "contributing_records": 682,
    }


def test_actual_parquet_semantics_are_compared_with_the_host_oracle() -> None:
    result = actual_result()
    support.assert_correctness(result)
    result["rows"].reverse()  # Physical partition/file ordering is not semantic ordering.
    support.assert_correctness(result)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda r: r["rows"][0].update(amount_cents=-1),
        lambda r: r["rows"][0].update(bucket=1),
        lambda r: r["rows"][0].update(record_count=True),
        lambda r: r["rows"][0].update(quantity_total=None),
        lambda r: r.update(input_count=1023),
        lambda r: r.update(row_count=7),
        lambda r: r.update(unique_keys=7),
        lambda r: r.update(null_count=1),
        lambda r: r.update(contributing_records=681),
        lambda r: r["schema"].update(amount_cents="double"),
    ],
)
def test_oracle_schema_key_null_and_count_mismatches_fail(mutation) -> None:
    result = actual_result()
    mutation(result)
    with pytest.raises(VerificationError):
        support.assert_correctness(result)


def cluster_snapshot() -> dict:
    return {
        "captured_at_ms": 1000000,
        "master": {
            "url": ENV["WAREHOUSE_SPARK_MASTER_URL"],
            "status": "ALIVE",
            "workers": [
                {
                    "id": f"worker-{i}",
                    "host": host,
                    "state": "ALIVE",
                    "port": 7078,
                    "cores": 1,
                    "memory": 1024,
                    "lastheartbeat": 999000,
                }
                for i, host in enumerate(HOSTS)
            ],
            "activeapps": [{"id": APP, "name": f"warehouse-spark-hdfs-{RUN}", "state": "RUNNING"}],
        },
        "workers": {
            host: {
                "id": f"worker-{i}",
                "masterurl": ENV["WAREHOUSE_SPARK_MASTER_URL"],
                "cores": 1,
                "memory": 1024,
                "executors": [
                    {
                        "id": i,
                        "appid": APP,
                        "memory": 512,
                        "appdesc": {"name": f"warehouse-spark-hdfs-{RUN}", "cores": 2},
                    }
                ],
            }
            for i, host in enumerate(HOSTS)
        },
    }


def probes() -> list[dict]:
    identities = [
        {
            "host": host,
            "partition_id": i,
            "task_attempt_id": 10 + i,
            "task_attempt_number": 0,
            "stage_id": 0,
            "stage_attempt_number": 0,
            "rows_consumed": 512,
            "bytes": 7,
            "sha256": hashlib.sha256(b"fixture").hexdigest(),
            "endpoints": list(workload.HDFS_ENDPOINTS),
        }
        for i, host in enumerate(HOSTS)
    ]
    return [{**identity, "peers": copy.deepcopy(identities)} for identity in identities]


def test_probes_require_two_nonempty_partitions_shared_participation_and_real_bytes() -> None:
    result = {
        "probes": probes(),
        "driver_probe": {
            "bytes": 7,
            "sha256": hashlib.sha256(b"fixture").hexdigest(),
            "endpoints": list(workload.HDFS_ENDPOINTS),
        },
    }
    support.assert_probes(result, b"fixture")
    result["probes"][1]["rows_consumed"] = 0
    with pytest.raises(VerificationError):
        support.assert_probes(result, b"fixture")


@pytest.mark.parametrize(
    "field", ["host", "task_attempt_id", "partition_id", "peers", "sha256", "endpoints"]
)
def test_duplicate_or_unverifiable_barrier_participation_is_rejected(field) -> None:
    result = {
        "probes": probes(),
        "driver_probe": {
            "bytes": 7,
            "sha256": hashlib.sha256(b"fixture").hexdigest(),
            "endpoints": list(workload.HDFS_ENDPOINTS),
        },
    }
    result["probes"][1][field] = result["probes"][0][field] if field != "peers" else []
    if field in ("sha256", "endpoints"):
        result["probes"][1][field] = "incorrect"
    with pytest.raises(VerificationError):
        support.assert_probes(result, b"fixture")


def events() -> list[dict]:
    properties = {
        **workload.spark_settings(FIXTURE),
        "spark.master": ENV["WAREHOUSE_SPARK_MASTER_URL"],
        "spark.app.name": f"warehouse-spark-hdfs-{RUN}",
        "spark.app.id": APP,
        "spark.default.parallelism": "2",
        "spark.sql.shuffle.partitions": "2",
        "spark.sql.adaptive.enabled": "false",
        "spark.cores.max": "2",
        "spark.executor.cores": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",
        "spark.dynamicAllocation.enabled": "false",
    }
    log = [
        {"Event": "SparkListenerLogStart", "Spark Version": "3.5.7"},
        {
            "Event": "SparkListenerApplicationStart",
            "App ID": APP,
            "App Name": f"warehouse-spark-hdfs-{RUN}",
            "Timestamp": 1,
        },
        {"Event": "SparkListenerEnvironmentUpdate", "Spark Properties": properties},
        *[
            {
                "Event": "SparkListenerExecutorAdded",
                "Executor ID": str(i),
                "Executor Info": {"Host": host, "Total Cores": 1},
            }
            for i, host in enumerate(HOSTS)
        ],
    ]
    for stage, phase in enumerate(("input-probe", "transform-write", "parquet-reread")):
        group = {"spark.jobGroup.id": f"{RUN}:{phase}"}
        info = {
            "Stage ID": stage,
            "Stage Attempt ID": 0,
            "Number of Tasks": 2,
            "Stage Name": phase,
            "Submission Time": 10,
            "Completion Time": 30,
            "RDD Info": [{"Barrier": stage == 0}],
        }
        log.extend(
            [
                {
                    "Event": "SparkListenerJobStart",
                    "Job ID": stage,
                    "Stage IDs": [stage],
                    "Stage Infos": [info],
                    "Properties": group,
                },
                {"Event": "SparkListenerStageSubmitted", "Stage Info": info, "Properties": group},
            ]
        )
        for i, host in enumerate(HOSTS):
            log.append(
                {
                    "Event": "SparkListenerTaskEnd",
                    "Stage ID": stage,
                    "Stage Attempt ID": 0,
                    "Task End Reason": {"Reason": "Success"},
                    "Task Info": {
                        "Task ID": 10 + stage * 2 + i,
                        "Index": i,
                        "Attempt": 0,
                        "Partition ID": i,
                        "Executor ID": str(i),
                        "Host": host,
                        "Launch Time": 11,
                        "Finish Time": 25,
                        "Failed": False,
                        "Killed": False,
                    },
                    "Task Metrics": {
                        "Executor Run Time": 12,
                        "JVM GC Time": 1,
                        "Memory Bytes Spilled": 0,
                        "Disk Bytes Spilled": 0,
                        "Input Metrics": {"Bytes Read": 100},
                        "Shuffle Read Metrics": {"Remote Bytes Read": 10, "Local Bytes Read": 2},
                        "Shuffle Write Metrics": {"Shuffle Bytes Written": 50},
                    },
                }
            )
        log.extend(
            [
                {"Event": "SparkListenerStageCompleted", "Stage Info": info},
                {
                    "Event": "SparkListenerJobEnd",
                    "Job ID": stage,
                    "Job Result": {"Result": "JobSucceeded"},
                },
            ]
        )
    return [*log, {"Event": "SparkListenerApplicationEnd", "Timestamp": 100}]


def summarize(log=None, **overrides) -> dict:
    payload = (
        b"\n".join(json.dumps(event).encode() for event in (events() if log is None else log))
        + b"\n"
    )
    return support.summarize_event_log(
        payload,
        **{
            "run_id": RUN,
            "application_id": APP,
            "fixture_root": FIXTURE,
            "probes": probes(),
            "active_cluster": cluster_snapshot(),
            **overrides,
        },
    )


def test_event_log_correlates_successful_tasks_workers_groups_and_metrics() -> None:
    summary = summarize()
    assert summary["application_id"] == APP
    assert summary["successful_tasks"] == 6
    assert summary["failed_attempts"] == []
    assert set(summary["workers"]) == set(HOSTS)
    assert summary["executors"]["0"]["successful_tasks"] == 3
    assert summary["executors"]["0"]["input_bytes"] == 300
    assert summary["executors"]["0"]["shuffle_read_bytes"] == 36
    assert summary["executors"]["1"]["executor_run_ms"] == 36
    assert summary["execution_metrics"]["successful_tasks"] == 6
    assert summary["execution_metrics"]["shuffle_read_bytes"] == 72
    assert summary["execution_metrics"]["memory_spill_bytes"] == 0
    assert summary["execution_metrics"]["disk_spill_bytes"] == 0
    assert summary["execution_metrics"]["successful_task_hosts"] == list(HOSTS)


@pytest.mark.parametrize(
    "missing",
    [
        "SparkListenerLogStart",
        "SparkListenerApplicationStart",
        "SparkListenerApplicationEnd",
        "SparkListenerEnvironmentUpdate",
        "SparkListenerStageCompleted",
        "SparkListenerJobEnd",
    ],
)
def test_missing_or_incomplete_application_evidence_fails(missing) -> None:
    with pytest.raises(VerificationError):
        summarize([event for event in events() if event["Event"] != missing])


@pytest.mark.parametrize("field,value", [("App ID", "app-other"), ("App Name", "wrong-run")])
def test_mismatched_application_identity_fails(field, value) -> None:
    log = events()
    log[1][field] = value
    with pytest.raises(VerificationError):
        summarize(log)


def test_local_or_incorrect_effective_settings_cannot_pass() -> None:
    for key, value in (
        ("spark.master", "local[*]"),
        ("spark.eventLog.dir", "file:///tmp"),
        ("spark.sql.adaptive.enabled", "true"),
        ("spark.executor.cores", "2"),
    ):
        log = events()
        log[2]["Spark Properties"][key] = value
        with pytest.raises(VerificationError):
            summarize(log)


def test_registration_and_idle_executors_are_not_execution_proof() -> None:
    log = [event for event in events() if event["Event"] != "SparkListenerTaskEnd"]
    with pytest.raises(VerificationError):
        summarize(log)


def test_missing_successful_task_metrics_are_not_reported_as_measured_zeros() -> None:
    log = events()
    task = next(event for event in log if event["Event"] == "SparkListenerTaskEnd")
    del task["Task Metrics"]
    with pytest.raises(VerificationError, match="metrics"):
        summarize(log)


def test_one_worker_success_and_failed_attempts_on_the_other_do_not_pass() -> None:
    log = events()
    for event in log:
        if event["Event"] == "SparkListenerTaskEnd" and event["Task Info"]["Host"] == HOSTS[1]:
            event["Task End Reason"] = {"Reason": "ExceptionFailure"}
            event["Task Info"]["Failed"] = True
    with pytest.raises(VerificationError):
        summarize(log)


def test_failed_retried_attempts_are_preserved_but_never_counted_as_success() -> None:
    log = events()
    failed = copy.deepcopy(next(event for event in log if event["Event"] == "SparkListenerTaskEnd"))
    failed["Task Info"].update({"Task ID": 9, "Failed": True})
    failed["Task End Reason"] = {"Reason": "ExceptionFailure"}
    log.insert(7, failed)
    summary = summarize(log)
    assert summary["successful_tasks"] == 6
    assert [attempt["task_id"] for attempt in summary["failed_attempts"]] == [9]
    wrong = probes()
    wrong[0]["task_attempt_id"] = 9
    with pytest.raises(VerificationError):
        summarize(log, probes=wrong)


def test_probe_task_ids_stage_attempts_and_executor_placement_must_match() -> None:
    for field, value in (
        ("task_attempt_id", 999),
        ("stage_id", 1),
        ("stage_attempt_number", 1),
        ("host", HOSTS[1]),
        ("partition_id", 1),
    ):
        wrong = probes()
        wrong[0][field] = value
        with pytest.raises(VerificationError):
            summarize(probes=wrong)
    stale = cluster_snapshot()
    stale["workers"][HOSTS[0]]["id"] = "stale-worker"
    with pytest.raises(VerificationError):
        summarize(active_cluster=stale)


def test_probe_tasks_must_belong_to_an_actual_two_task_barrier_stage() -> None:
    log = events()
    for event in log:
        if event["Event"] in ("SparkListenerStageSubmitted", "SparkListenerStageCompleted"):
            event["Stage Info"]["RDD Info"] = []
    with pytest.raises(VerificationError, match="barrier"):
        summarize(log)


def test_malformed_truncated_oversized_and_duplicate_event_evidence_is_rejected() -> None:
    kwargs = {
        "run_id": RUN,
        "application_id": APP,
        "fixture_root": FIXTURE,
        "probes": probes(),
        "active_cluster": cluster_snapshot(),
    }
    for payload in (b"", b'{"Event":', b"x" * (support.MAX_EVENT_BYTES + 1)):
        with pytest.raises(VerificationError):
            support.summarize_event_log(payload, **kwargs)
    with pytest.raises(VerificationError):
        summarize([*events(), events()[1]])


def test_structured_records_require_matching_run_and_unique_complete_results() -> None:
    records = [{"kind": kind, "run_id": RUN} for kind in ("startup", "application", "result")]

    def encode(values):
        return b"ordinary Spark output\n" + b"\n".join(
            workload.RECORD_PREFIX.encode() + json.dumps(value).encode() for value in values
        )

    assert set(support.read_records(encode(records), RUN)) == {"startup", "application", "result"}
    for wrong in (
        records[:-1],
        [*records, records[-1]],
        [{**record, "run_id": "other"} for record in records],
    ):
        with pytest.raises(VerificationError):
            support.read_records(encode(wrong), RUN)
    assert "startup" in support.read_records(encode(records[:1]), RUN, complete=False)


def test_stale_baked_archive_runtime_sources_and_xml_are_detected() -> None:
    expected, xml = (
        {"common/spark_verification.py": SHA},
        {"core-site.xml": SHA, "hdfs-site.xml": SHA},
    )
    startup = {
        "sources": expected,
        "baked_sources": expected,
        "archive_sources": expected,
        "archive_sha256": SHA,
        "xml_sha256": xml,
    }
    support.assert_sources(startup, expected, xml)
    for field in ("sources", "baked_sources", "archive_sources", "xml_sha256"):
        with pytest.raises(VerificationError, match="stale|source|XML"):
            support.assert_sources({**startup, field: {}}, expected, xml)


def test_prerequisites_resolve_both_opt_in_spark_profiles(monkeypatch) -> None:
    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    image = "warehouse-bigdata-spark:3.5.7-python3.11.17"
    driver_image = "warehouse-bigdata-spark-submit:3.5.7-python3.11.17"
    mounts = tuple(
        (str(support.ROOT / "infrastructure/hadoop" / name), f"/opt/hadoop/etc/hadoop/{name}")
        for name in ("core-site.xml", "hdfs-site.xml")
    )
    containers = [
        {
            "Id": service,
            "Image": "sha256:shared-runtime",
            "Config": {
                "Image": image,
                "User": "185:185",
                "Hostname": service,
                "Labels": {
                    "com.docker.compose.project": "warehouse-bigdata",
                    "com.docker.compose.service": service,
                },
            },
            "State": {"Running": True, "Paused": False, "Restarting": False},
            "NetworkSettings": {"Networks": {"warehouse-bigdata": {}}},
            "Mounts": [
                {"Destination": target, "Source": source, "Type": "bind", "RW": False}
                for source, target in mounts
            ],
        }
        for service in support.SPARK_SERVICES
    ]
    calls = []

    def compose(*args):
        calls.append(args)
        if args[-3:] == ("config", "--format", "json"):
            # Compose filters profile-owned services out of its default config.
            profiles = {args[index + 1] for index, value in enumerate(args) if value == "--profile"}
            services = (
                {
                    **{service: {"image": image} for service in support.SPARK_SERVICES},
                    "spark-submit": {"image": driver_image},
                }
                if profiles == {"spark", "spark-submit"}
                else {}
            )
            return SimpleNamespace(stdout=json.dumps({"services": services}).encode())
        assert args == ("--profile", "spark", "ps", "--all", "--quiet", *support.SPARK_SERVICES)
        return SimpleNamespace(stdout="\n".join(support.SPARK_SERVICES).encode())

    def run(args, **kwargs):
        if args[0] == "bash":
            assert args[-1] == "check"
            return SimpleNamespace(stdout=b"")
        if args[:2] == ["docker", "inspect"]:
            return SimpleNamespace(stdout=json.dumps(containers).encode())
        assert args == ["docker", "image", "inspect", driver_image]
        return SimpleNamespace(stdout=b'[{"Id":"sha256:prepared-driver"}]')

    path = FIXTURE.removeprefix(workload.HDFS_URI).removesuffix("/spark")
    hdfs = SimpleNamespace(
        parallel=False,
        fixture=SimpleNamespace(owned=True, created_path=path, path=path),
        runner=SimpleNamespace(run=run),
        compose=compose,
        spec=SimpleNamespace(network="warehouse-bigdata", xml_mounts=mounts),
        config=workload.load_hdfs_config(ENV),
        evidence=SimpleNamespace(run_id=RUN, write=lambda *args: None),
    )
    monkeypatch.setattr(support, "snapshot_cluster", lambda *args: cluster_snapshot())
    result = support.prerequisites(hdfs)
    assert set(result["containers"]) == set(support.SPARK_SERVICES)
    assert result["driver_image"]["Id"] == "sha256:prepared-driver"
    assert calls[0] == (
        "--profile",
        "spark",
        "--profile",
        "spark-submit",
        "config",
        "--format",
        "json",
    )


def test_executor_probe_uses_a_fresh_bounded_java_hadoop_client(monkeypatch) -> None:
    calls, endpoints = [], []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    monkeypatch.setattr(
        workload.socket,
        "create_connection",
        lambda endpoint, timeout: endpoints.append((endpoint, timeout)) or Connection(),
    )

    def run(args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(args, 0, b"fixture", b"")

    monkeypatch.setattr(workload.subprocess, "run", run)
    digest = hashlib.sha256(b"fixture").hexdigest()
    for _ in range(2):
        assert workload.probe_hdfs(FIXTURE + "/input/part-00.jsonl", digest)["bytes"] == 7
    assert len(calls) == 2
    args, options = calls[0]
    assert "-Xmx64m" in args
    assert "org.apache.hadoop.fs.FsShell" in args
    assert args[-2:] == ["-cat", FIXTURE + "/input/part-00.jsonl"]
    assert options["timeout"] == 20
    assert len(endpoints) == 6
    assert all(timeout == 2 for _, timeout in endpoints)
    with pytest.raises(RuntimeError, match="digest|SHA"):
        workload.probe_hdfs(FIXTURE + "/input/part-00.jsonl", SHA)


def test_probe_subprocess_timeout_is_a_failure(monkeypatch) -> None:
    monkeypatch.setattr(workload, "check_hdfs_endpoints", lambda: list(workload.HDFS_ENDPOINTS))

    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(args, 20)

    monkeypatch.setattr(workload.subprocess, "run", timeout)
    with pytest.raises(RuntimeError, match="timed out"):
        workload.probe_hdfs(FIXTURE + "/input/part-00.jsonl", SHA)


def test_submission_deadlines_leave_t02_in_charge_of_cancellation() -> None:
    calls, evidence = [], []
    hdfs = SimpleNamespace(
        runner=SimpleNamespace(
            run=lambda args, **kwargs: (
                calls.append((args, kwargs)) or subprocess.CompletedProcess(args, 124, b"", b"")
            )
        ),
        evidence=SimpleNamespace(write=lambda name, value: evidence.append((name, value))),
    )
    result = support.submit_verification(hdfs, ["--run-id", RUN])
    args, options = calls[0]
    assert args[:5] == ["timeout", "--foreground", "--signal=TERM", "--kill-after=45s", "300s"]
    assert options == {"timeout": 360, "check": False}
    assert "local[*]" not in args
    assert "warehouse_bigdata/jobs/verify_spark_hdfs.py" in args
    assert evidence[0][1]["argv"] == args
    assert evidence[0][1]["sha256"] == hashlib.sha256(json.dumps(args).encode()).hexdigest()
    with pytest.raises(VerificationError, match="timed out"):
        support.assert_submission_status(result.returncode)


def test_driver_removal_requires_the_reported_exact_t02_identity() -> None:
    token = "b" * 32
    name = f"warehouse-spark-submit-{token}"
    info = {
        "Name": f"/{name}",
        "Config": {"Labels": {support.DRIVER_LABEL: token}},
        "State": {"Running": False, "Status": "exited"},
    }
    support.assert_driver_identity(info, name)
    with pytest.raises(VerificationError):
        support.assert_driver_identity(info, "warehouse-spark-submit-" + "c" * 32)


@pytest.mark.parametrize("missing_field", [False, True])
def test_termination_requires_no_active_executor_by_application_id_and_complete_state(
    monkeypatch, missing_field
) -> None:
    snapshot = cluster_snapshot()
    snapshot["master"]["activeapps"] = []
    # Application identity must be checked independently of its display name.
    for worker in snapshot["workers"].values():
        worker["executors"][0]["appdesc"]["name"] = "different-display-name"
    if missing_field:
        del snapshot["workers"][HOSTS[0]]["executors"]
    ticks = iter(range(1000))
    monkeypatch.setattr(support.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(support.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(support, "snapshot_cluster", lambda *args, **kwargs: snapshot)
    hdfs = SimpleNamespace(
        runner=SimpleNamespace(
            run=lambda *args, **kwargs: subprocess.CompletedProcess(
                args, 1, b"", b"No such container"
            )
        ),
        evidence=SimpleNamespace(run_id=RUN, write=lambda *args: None),
    )
    preflight = {
        "registration": cluster_snapshot(),
        "containers": {"spark-master": {"Id": "master"}},
    }
    with pytest.raises(VerificationError):
        support.confirm_termination(
            hdfs, {"driver_host": "warehouse-spark-submit-" + "b" * 32}, APP, preflight
        )


def test_live_scenario_is_integration_marked_without_service_calls() -> None:
    from tests.integration import test_spark_hdfs

    assert test_spark_hdfs.pytestmark.name == "integration"


@pytest.mark.parametrize("termination,evidence", [(False, True), (True, False), (False, False)])
def test_incomplete_finalization_retains_exact_fixture_and_preserves_original(
    termination, evidence
) -> None:
    original = RuntimeError("original correctness failure")
    retained, errors = [], []
    hdfs = SimpleNamespace(retain_fixture=retained.append, errors=errors)
    support.finish_safely(
        hdfs, terminated=termination, evidence_complete=evidence, original=original
    )
    assert len(retained) == 1
    assert "termination" in retained[0] or "evidence" in retained[0]


def test_finalization_success_does_not_retain_a_fixture() -> None:
    hdfs = SimpleNamespace(retain_fixture=lambda reason: pytest.fail(reason), errors=[])
    support.finish_safely(hdfs, terminated=True, evidence_complete=True, original=None)


def test_host_timeout_runs_finalization_and_retains_possible_write_destinations(
    tmp_path, monkeypatch
) -> None:
    run = HDFSVerification(
        Evidence(tmp_path, "offline-spark-timeout"), disruptions=False, parallel=False
    )
    run.fixture.create(lambda path: None)
    calls = []
    original = RuntimeError("360s host submission timed out")
    startup = {
        "kind": "startup",
        "run_id": run.evidence.run_id,
        "driver_host": "warehouse-spark-submit-" + "b" * 32,
    }
    monkeypatch.setattr(support, "prerequisites", lambda h: {})
    monkeypatch.setattr(run, "storage_snapshot", lambda: {})
    monkeypatch.setattr(run, "read_fixture", lambda label: {})
    monkeypatch.setattr(support, "create_spark_fixture", lambda *args: None)

    def timed_out(h, args):
        (h.evidence.directory / "0001.stdout").write_text(
            workload.RECORD_PREFIX + json.dumps(startup) + "\n"
        )
        raise original

    def still_running(*args):
        calls.append("termination")
        raise VerificationError("application still active")

    def incomplete_log(*args):
        calls.append("event retrieval")
        raise VerificationError("in-progress event log")

    monkeypatch.setattr(support, "submit_verification", timed_out)
    monkeypatch.setattr(support, "confirm_termination", still_running)
    monkeypatch.setattr(support, "collect_event_log", incomplete_log)
    monkeypatch.setattr(
        support, "inspect_data_files", lambda *args: pytest.fail("Possible active writer")
    )
    monkeypatch.setattr(run, "remove_fixture", lambda: pytest.fail("Retained fixture was deleted"))
    with pytest.raises(RuntimeError) as caught:
        support.verify_spark_hdfs(run)
    assert caught.value is original
    assert calls == ["termination", "event retrieval"]
    assert run.fixture.owned
    assert run.retention_reasons
    assert "application still active" in " ".join(original.__notes__)
    run.cleanup()
    assert run.fixture.owned


def test_event_retrieval_rejects_oversize_before_starting_a_byte_read(tmp_path) -> None:
    calls = []

    def hdfs(*args):
        calls.append(args)
        return (
            FIXTURE + "/events/" + APP + "\n" if "-ls" in args else str(support.MAX_EVENT_BYTES + 1)
        )

    run = SimpleNamespace(
        hdfs=hdfs,
        evidence=SimpleNamespace(directory=tmp_path),
        client_command=lambda *args, **kwargs: pytest.fail("Oversized read"),
    )
    with pytest.raises(VerificationError, match="oversized"):
        support.collect_event_log(run, FIXTURE, APP)
    assert len(calls) == 2


def test_in_progress_event_log_is_copied_but_never_accepted(tmp_path) -> None:
    payload = b"\n".join(json.dumps(event).encode() for event in events()) + b"\n"

    def hdfs(*args):
        return FIXTURE + "/events/" + APP + ".inprogress\n" if "-ls" in args else str(len(payload))

    run = SimpleNamespace(
        hdfs=hdfs,
        evidence=SimpleNamespace(directory=tmp_path),
        client_command=lambda *args, **kwargs: payload,
    )
    with pytest.raises(VerificationError, match="in-progress"):
        support.collect_event_log(run, FIXTURE, APP)
    assert (tmp_path / "spark-events.jsonl").read_bytes() == payload

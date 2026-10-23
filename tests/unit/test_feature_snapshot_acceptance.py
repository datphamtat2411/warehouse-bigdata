"""Local/mocked FIX-02 acceptance checks; never require Docker or the M04 artifact."""

import json
import os
import signal
import sys
from datetime import timedelta
from types import SimpleNamespace

import pytest

from tests.integration import feature_snapshot_support as support
from tests.integration.feature_snapshot_oracle import expected_features, verify_oracle
from tests.integration.feature_snapshot_runtime import CommandError, command, task_evidence


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("j04-acceptance-local")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def source_fixture(spark):
    rows = []
    for item in ("ordinary", "late-price", "zero"):
        for day in range(1, 31):
            price = float(day) if day in (2, 20, 29, 30) else None
            if item == "late-price":
                price = 999.0 if day >= 29 else None
            rows.append(
                (
                    item,
                    "CA_1",
                    "dept",
                    day,
                    support.FIRST_DATE + timedelta(days=day - 1),
                    0 if item == "zero" else day * day,
                    price,
                )
            )
    return spark.createDataFrame(
        rows,
        "item_id string, store_id string, dept_id string, day_index int, "
        "date date, unit_sales long, sell_price double",
    )


def test_oracle_hand_calculation_and_point_in_time(spark):
    source = source_fixture(spark)
    expected = expected_features(source, "snapshot", "version")
    rows = {(r.item_id, r.target_date): r for r in expected.collect()}
    assert len(rows) == 6
    d29, d30 = (support.FIRST_DATE + timedelta(days=d - 1) for d in (29, 30))
    row = rows["ordinary", d29]
    assert (row.lag_1, row.lag_7, row.lag_28, row.label) == (784, 484, 1, 841)
    assert row.rolling_mean_7 == sum(d * d for d in range(22, 29)) / 7
    assert row.rolling_mean_28 == sum(d * d for d in range(1, 29)) / 28
    assert (row.last_known_price, row.price_missing) == (20.0, False)
    assert rows["ordinary", d30].last_known_price == 29.0
    assert rows["late-price", d29].last_known_price is None
    assert rows["late-price", d29].price_missing is True
    assert rows["late-price", d30].last_known_price == 999.0
    assert rows["zero", d29].rolling_mean_28 == 0
    assert (
        row.feature_role,
        row.as_of_date,
        row.day_of_week,
        row.month,
        row.is_weekend,
        row.event_flag,
    ) == ("train", d29 - timedelta(days=1), 6, 2, True, False)
    assert verify_oracle(source, expected, "snapshot", "version")["compared_rows"] == 6


@pytest.mark.parametrize(
    "field,value",
    [
        ("lag_1", "0L"),
        ("lag_7", "0L"),
        ("lag_28", "0L"),
        ("rolling_mean_7", "0D"),
        ("rolling_mean_28", "0D"),
        ("label", "NULL"),
        ("last_known_price", "NULL"),
        ("feature_role", "'test'"),
        ("event_flag", "true"),
        ("price_missing", "true"),
        ("as_of_date", "target_date"),
        ("day_of_week", "0"),
        ("month", "1"),
        ("is_weekend", "false"),
        ("rolling_mean_28", "CAST('Infinity' AS DOUBLE)"),
        ("rolling_mean_7", "CAST('Infinity' AS DOUBLE)"),
        ("rolling_mean_7", "CAST('-Infinity' AS DOUBLE)"),
        ("rolling_mean_28", "CAST('NaN' AS DOUBLE)"),
    ],
)
def test_oracle_rejects_semantic_corruption(spark, field, value):
    from pyspark.sql import functions as F

    source = source_fixture(spark)
    features = expected_features(source, "s", "v")
    damaged = features.withColumn(field, F.expr(value).cast(features.schema[field].dataType))
    with pytest.raises(AssertionError, match="Independent oracle"):
        verify_oracle(source, damaged, "s", "v")


def test_oracle_covers_keys_beyond_ten_thousand(spark):
    from pyspark.sql import functions as F

    source = (
        spark.range(5002)
        .crossJoin(spark.range(1, 31).toDF("day_index"))
        .select(
            F.col("id").cast("string").alias("item_id"),
            F.lit("CA_1").alias("store_id"),
            F.lit("dept").alias("dept_id"),
            F.col("day_index").cast("int"),
            F.date_add(F.lit(support.FIRST_DATE), (F.col("day_index") - 1).cast("int")).alias(
                "date"
            ),
            F.col("day_index").alias("unit_sales"),
            F.lit(2.0).alias("sell_price"),
        )
    )
    features = expected_features(source, "s", "v").cache()
    assert features.count() == 10004
    try:
        damaged = features.withColumn(
            "lag_28", F.when(F.col("item_id") == "5001", -1).otherwise(F.col("lag_28"))
        )
        with pytest.raises(AssertionError, match="Independent oracle"):
            verify_oracle(source, damaged, "s", "v")
        changed_keys = features.withColumn(
            "item_id", F.when(F.col("item_id") == "5001", "extra").otherwise(F.col("item_id"))
        )
        with pytest.raises(AssertionError, match="Independent oracle"):
            verify_oracle(source, changed_keys, "s", "v")
    finally:
        features.unpersist()


def events():
    yield {"Event": "SparkListenerApplicationStart", "App ID": "app-test"}
    for host in ("spark-worker-1", "spark-worker-2"):
        yield {
            "Event": "SparkListenerTaskEnd",
            "Task End Reason": {"Reason": "Success"},
            "Task Info": {"Host": host},
            "Task Metrics": {"JVM GC Time": 3},
        }


def test_event_evidence_streams_and_requires_both_workers():
    lines = (json.dumps(event).encode() + b"\n" for event in events())
    result = task_evidence(lines, "app-test")
    assert result["worker_tasks"] == {"spark-worker-1": 1, "spark-worker-2": 1}
    assert result["task_metrics"]["JVM GC Time"] == 6
    with pytest.raises(AssertionError, match="both workers"):
        task_evidence([json.dumps(next(events())).encode()], "app-test")
    with pytest.raises(AssertionError, match="application"):
        task_evidence((json.dumps(e).encode() for e in events()), "another-app")
    with pytest.raises(AssertionError, match="event line"):
        task_evidence([b"x" * (1024 * 1024 + 1)], "app-test")
    with pytest.raises(AssertionError, match="event"):
        task_evidence([b"not json"], "app-test")


def test_remote_event_evidence_is_streamed_and_missing_files_fail(monkeypatch):
    from tests.integration import feature_snapshot_runtime as runtime

    client = SimpleNamespace(
        list_status=lambda path: [{"path": path + "/app", "type": "FILE"}],
        checksums=lambda paths: dict.fromkeys(paths, "fresh"),
        command_prefix=(),
        hdfs_command="hdfs",
    )

    def stream(args, **kwargs):
        assert args == ["hdfs", "dfs", "-text", "/events/app"]
        assert kwargs["operation"] == "events"
        for event in events():
            kwargs["on_stdout_line"](json.dumps(event).encode())

    monkeypatch.setattr(runtime, "command", stream)
    result = runtime.stream_task_evidence(client, "/events", "app-test")
    assert result["file_checksums"] == {"/events/app": "fresh"}
    client.list_status = lambda path: []
    with pytest.raises(AssertionError, match="Missing"):
        runtime.stream_task_evidence(client, "/events", "app-test")


def test_command_output_is_bounded_and_timeout_is_diagnostic():
    result = command(
        [sys.executable, "-c", "print('x' * 500000); print('END')"], operation="metadata", timeout=5
    )
    assert len(result.stdout.encode()) <= 256 * 1024
    assert result.stdout.endswith("END\n")
    with pytest.raises(CommandError, match="readback.*timed out.*0.1s") as error:
        command(
            [sys.executable, "-u", "-c", "import time; print('diagnostic'); time.sleep(30)"],
            operation="readback",
            timeout=0.1,
        )
    assert "diagnostic" in str(error.value)
    assert "process group" in str(error.value)


def test_command_failure_and_partial_line_are_diagnostic():
    with pytest.raises(CommandError, match="publication exited 7.*") as error:
        command(
            [sys.executable, "-c", "import sys; print('publisher failed'); sys.exit(7)"],
            operation="publication",
            timeout=5,
        )
    assert "publisher failed" in str(error.value)
    seen = []
    command(
        [sys.executable, "-c", "import sys; sys.stdout.write('no newline')"],
        operation="metadata",
        timeout=5,
        on_stdout_line=seen.append,
    )
    assert seen == [b"no newline"]


def test_command_cleans_owned_container_on_interrupt(monkeypatch):
    from tests.integration import feature_snapshot_runtime as runtime

    cleaned = []
    monkeypatch.setattr(runtime, "cleanup_container", lambda owned: cleaned.append(owned))
    monkeypatch.setattr(
        runtime, "_drain", lambda *a, **k: (_ for _ in ()).throw(KeyboardInterrupt())
    )
    with pytest.raises(KeyboardInterrupt):
        command(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            operation="publication",
            owned=("test-container", "token"),
        )
    assert cleaned == [("test-container", "token")]


def test_timeout_terminates_spawned_children():
    from pathlib import Path

    child_pids = []
    code = (
        "import subprocess, sys, time; "
        "p = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']); "
        "print(p.pid, flush=True); time.sleep(30)"
    )
    with pytest.raises(CommandError):
        command(
            [sys.executable, "-u", "-c", code],
            operation="initial",
            timeout=0.3,
            on_stdout_line=lambda line: child_pids.append(int(line)),
        )
    assert len(child_pids) == 1
    # A killed child may briefly be a zombie awaiting the system subreaper.
    status = Path(f"/proc/{child_pids[0]}/stat")
    assert not status.exists() or status.read_text().split()[2] == "Z"


def test_sigterm_runs_owned_cleanup(monkeypatch):
    from tests.integration import feature_snapshot_runtime as runtime

    cleaned = []
    monkeypatch.setattr(runtime, "cleanup_container", lambda owned: cleaned.append(owned))
    old_handler = signal.getsignal(signal.SIGTERM)

    def terminate(*args):
        os.kill(os.getpid(), signal.SIGTERM)

    monkeypatch.setattr(runtime, "_drain", terminate)
    with pytest.raises(KeyboardInterrupt, match="SIGTERM"):
        command(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            operation="publication",
            owned=("owned", "token"),
        )
    assert cleaned == [("owned", "token")]
    assert signal.getsignal(signal.SIGTERM) == old_handler


def test_cleanup_refuses_foreign_label(monkeypatch):
    from tests.integration import feature_snapshot_runtime as runtime

    calls = []

    def run(args, **kwargs):
        calls.append(args)
        return SimpleNamespace(returncode=0, stdout=b"foreign\n", stderr=b"")

    monkeypatch.setattr(runtime.subprocess, "run", run)
    with pytest.raises(CommandError, match="ownership"):
        runtime.cleanup_container(("test-container", "token"))
    assert len(calls) == 1  # No stop/rm of someone else's container.


def test_cleanup_stops_only_owned_container(monkeypatch):
    from tests.integration import feature_snapshot_runtime as runtime

    calls = []

    def run(args, **kwargs):
        calls.append(args)
        if args[1] == "inspect":
            if len(calls) == 1:
                return SimpleNamespace(returncode=0, stdout=b"token\n", stderr=b"")
            return SimpleNamespace(returncode=1, stdout=b"", stderr=b"No such container")
        return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    monkeypatch.setattr(runtime.subprocess, "run", run)
    monkeypatch.setattr(runtime.time, "sleep", lambda _: None)
    runtime.cleanup_container(("owned", "token"))
    assert [a[1] for a in calls] == ["inspect", "stop", "rm", "inspect", "inspect"]
    assert all(a[-1] == "owned" for a in calls)


def test_recovery_is_owner_only_and_never_falls_through(monkeypatch):
    calls = []

    def owner(*args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=10, stdout="recovered")

    monkeypatch.setattr(support, "publish", owner)
    monkeypatch.setattr(support, "driver", lambda *a, **k: pytest.fail("Unexpected computation"))
    assert support.submit("s", "snap", "up", "v", "run", recovery=True) == "recovered"
    assert len(calls) == 1
    monkeypatch.setattr(
        support, "publish", lambda *a, **k: SimpleNamespace(returncode=0, stdout="")
    )
    with pytest.raises(AssertionError, match="frozen"):
        support.submit("s", "snap", "up", "v", "run", recovery=True)


def test_incomplete_run_cannot_launch_driver(monkeypatch):
    monkeypatch.setattr(
        support, "publish", lambda *a, **k: SimpleNamespace(returncode=0, stdout="")
    )
    monkeypatch.setattr(
        support,
        "owner",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("existing execution evidence")),
    )
    monkeypatch.setattr(support, "driver", lambda *a, **k: pytest.fail("Unexpected computation"))
    with pytest.raises(AssertionError, match="existing execution evidence"):
        support.submit("s", "snap", "up", "v", "run")


def test_published_test_never_submits(monkeypatch, tmp_path):
    from tests.integration import test_feature_snapshot as acceptance

    selected = dict(
        source_version="s",
        source_snapshot_id="snap",
        j03_pipeline_run_id="up",
        feature_version="small-version",
        j04_pipeline_run_id="small-producer",
    )
    monkeypatch.setattr(acceptance, "load_small_artifacts", lambda: selected)
    monkeypatch.setattr(acceptance, "submit", lambda *a, **k: pytest.fail("Unexpected submission"))
    calls = []

    def probe(*args, **kwargs):
        calls.append(args)
        return {"producer": args[4], "file_integrity": list(range(34))}

    monkeypatch.setattr(acceptance, "independent_probe", probe)
    acceptance.test_published_feature_snapshot(tmp_path)
    evidence = json.loads((tmp_path / "j04-published-evidence.json").read_text())
    assert evidence["live_reuse_exercised"] is False
    assert calls[0][3:5] == ("small-version", "small-producer")
    selected.update(feature_version="one-shot-version", j04_pipeline_run_id="one-shot-producer")
    acceptance.test_published_feature_snapshot(tmp_path)
    assert calls[1][3:5] == ("one-shot-version", "one-shot-producer")


def test_readback_result_is_separate_from_log_tail(monkeypatch):
    def container(args, **kwargs):
        assert kwargs["spark"] is True
        kwargs["on_stdout_line"](b"Spark diagnostic output")
        kwargs["on_stdout_line"](b'J04_READBACK={"verified":true}')
        return SimpleNamespace(stdout="tail can be truncated", returncode=0)

    monkeypatch.setattr(support, "container_command", container)
    assert support.independent_probe("s", "snap", "up", "v", "run") == {"verified": True}


def test_document_digest_and_frozen_schema():
    # Independent hand-encoded canonical payload, including its trailing newline.
    import hashlib

    assert support.document_digest({"b": 2, "a": 1}) == (
        "sha256:" + hashlib.sha256(b'{"a":1,"b":2}\n').hexdigest()
    )
    schema = support.logical_schema()
    assert [f["name"] for f in schema if f["nullable"]] == ["last_known_price", "label"]


def frozen_documents():
    binding = {
        "schema_version": "feature-input-binding-v1",
        "pipeline_run_id": "run",
        "source_version": "source",
        "source_snapshot_id": "snapshot",
        "feature_version": "v",
        "upstream_pipeline_run_id": "upstream",
        "code_revision": "sha256:old-packaged-revision",
        "effective_configuration": {},
        "configuration_digest": support.document_digest({}),
        "artifact_uri": "hdfs://features",
        "upstream_evidence": {
            "source_version": "source",
            "source_snapshot_id": "snapshot",
            "pipeline_run_id": "upstream",
            "row_count": 194100,
            "upstream_pipeline_run_id": "raw-run",
        },
    }
    metrics = {
        "row_count": 191300,
        "observed_count": 194100,
        "candidate_count": 191300,
        "eligible_count": 191300,
        "excluded_count": 0,
        "lookback_only_count": 2800,
        "role_counts": {"train": 185700, "validation": 2800, "test": 2800},
        "persisted_parquet_validated": True,
        "required_field_null_counts": {
            n: 0 for n, _ in support.FEATURE_SCHEMA if n != "last_known_price"
        },
    }
    inventory = [{"path": "part.parquet", "size_bytes": 123, "checksum": "actual"}]
    common = {
        "schema": support.logical_schema(),
        "schema_fingerprint": support.document_digest({"schema": support.logical_schema()}),
        "persisted_parquet_validated": True,
        "status": "VALIDATED",
        "file_integrity": inventory,
        "metrics": metrics,
    }
    sidecar = {
        **binding,
        **common,
        "schema_version": "feature-dataset-manifest-v1",
        "input_binding": binding,
        "row_count": 191300,
    }
    quality = {
        **binding,
        **common,
        "producing_pipeline_run_id": "run",
        "spark_application_id": "app-test",
    }
    coverage = {
        "binding_digest": support.document_digest(binding),
        "quality_digest": support.document_digest(quality),
        "dataset_manifest_digest": support.document_digest(sidecar),
        "producing_pipeline_run_id": "run",
        "publication_complete": True,
        "persisted_parquet_validated": True,
        "file_integrity": inventory,
        "metrics": metrics,
        "feature_version": "v",
        "upstream_pipeline_run_id": "upstream",
        "dataset_manifest_uri": "hdfs://features/_dataset_manifest.json",
    }
    marker = {
        **binding,
        "schema_fingerprint": common["schema_fingerprint"],
        "status": "VALIDATED",
        "producer": "warehouse-bigdata:feature-snapshot",
        "row_count": 191300,
        "coverage_and_exclusion_counts": json.dumps(coverage),
        "required_field_null_counts": json.dumps(metrics["required_field_null_counts"]),
    }
    return marker, sidecar, binding, quality, {"version": 1, "manifest": marker}, inventory


def test_old_code_publication_is_verified_against_its_own_binding():
    documents = frozen_documents()
    support.check_frozen_execution(*documents, run="run", producer="run")


@pytest.mark.parametrize("corruption", ["checksum", "schema", "digest", "code", "request"])
def test_independent_frozen_evidence_rejects_corruption(corruption):
    import copy

    marker, sidecar, binding, quality, request, inventory = copy.deepcopy(frozen_documents())
    if corruption == "checksum":
        inventory = [{**inventory[0], "checksum": "changed-on-hdfs"}]
    elif corruption == "schema":
        sidecar["schema_fingerprint"] = "wrong"
    elif corruption == "digest":
        quality["status"] = "FAILED"
    elif corruption == "code":
        binding["code_revision"] = "sha256:new-revision"
    else:
        request = {"version": 1, "manifest": {}}
    with pytest.raises(AssertionError):
        support.check_frozen_execution(
            marker, sidecar, binding, quality, request, inventory, run="run", producer="run"
        )


def test_inventory_checks_actual_files_and_rejects_missing_checksums():
    class Client:
        def list_status(self, root):
            return [
                {"path": root + "/part.parquet", "type": "FILE", "length": 123},
                {"path": root + "/_dataset_manifest.json", "type": "FILE", "length": 10},
            ]

        def checksums(self, paths):
            assert paths == ["/artifact/part.parquet"]
            return {paths[0]: "fresh-hdfs-checksum"}

    client = Client()
    assert support.artifact_inventory(client, "/artifact") == [
        {"path": "part.parquet", "size_bytes": 123, "checksum": "fresh-hdfs-checksum"}
    ]
    client.checksums = lambda paths: {}
    with pytest.raises(AssertionError, match="checksums"):
        support.artifact_inventory(client, "/artifact")

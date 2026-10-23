"""Read-only J04 acceptance and explicitly owned fresh lifecycle orchestration."""

from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta
from functools import reduce
from operator import or_
from pathlib import Path, PurePosixPath
from uuid import uuid4

if __package__:
    from tests.integration.feature_snapshot_oracle import verify_oracle
    from tests.integration.feature_snapshot_runtime import (
        OWNER_LABEL,
        command,
        stream_task_evidence,
    )
else:  # Mounted standalone verification script; never import packaged T01 helpers.
    from feature_snapshot_oracle import verify_oracle
    from feature_snapshot_runtime import OWNER_LABEL, command, stream_task_evidence

ROOT = Path(__file__).resolve().parents[2]
HDFS = "hdfs://hdfs-namenode:8020"
MASTER = "spark://spark-master:7077"
SERIES_COUNT = 30_490
SOURCE_COUNT = SERIES_COUNT * 30
FEATURE_COUNT = SERIES_COUNT * 2
FIRST_DATE = date(2011, 1, 29)
SERIES_KEY = ["item_id", "store_id"]
# Freeze v1.0 independently of T01 and the publisher's schema helper.
FEATURE_SCHEMA = (
    ("source_snapshot_id", "string"),
    ("feature_version", "string"),
    ("feature_role", "string"),
    ("item_id", "string"),
    ("store_id", "string"),
    ("dept_id", "string"),
    ("target_date", "date"),
    ("as_of_date", "date"),
    ("lag_1", "bigint"),
    ("lag_7", "bigint"),
    ("lag_28", "bigint"),
    ("rolling_mean_7", "double"),
    ("rolling_mean_28", "double"),
    ("day_of_week", "int"),
    ("month", "int"),
    ("is_weekend", "boolean"),
    ("event_flag", "boolean"),
    ("price_missing", "boolean"),
    ("last_known_price", "double"),
    ("label", "bigint"),
)


def container_command(arguments, *, operation, owner_user=False, mounted=False, spark=False,
                      allowed_codes=(0,), on_stdout_line=None):
    token = uuid4().hex
    name = "warehouse-j04-test-" + token
    args = ["docker", "compose", "-f", str(ROOT / "infrastructure/compose.yaml"),
            "--profile", "spark-submit", "run", "--rm", "--no-deps", "-T", "--pull", "never",
            "--name", name, "--label", OWNER_LABEL + "=" + token]
    if owner_user:
        args += ["--user", "1000:100", "--entrypoint", "/opt/warehouse-app/.venv/bin/python"]
    if mounted:
        for filename in ("feature_snapshot_support.py", "feature_snapshot_oracle.py",
                         "feature_snapshot_runtime.py"):
            args += ["--volume", f"{Path(__file__).with_name(filename)}:"
                     f"/opt/warehouse-app/src/{filename}:ro"]
    if spark:
        args += ["--env", f"_WAREHOUSE_SPARK_DRIVER_HOST={name}"]
    return command([*args, "spark-submit", *arguments], operation=operation, owned=(name, token),
                   allowed_codes=allowed_codes, on_stdout_line=on_stdout_line)


def owner(code, *args):
    return container_command(["-c", code, *args], operation="metadata", owner_user=True).stdout


def binding_arguments(source_version, snapshot, upstream, version, run):
    return ["--source-version", source_version, "--source-snapshot-id", snapshot,
            "--upstream-pipeline-run-id", upstream, "--feature-version", version,
            "--pipeline-run-id", run]


def publish(source_version, snapshot, upstream, version, run, *, recover=False):
    return container_command(
        ["-m", "warehouse_bigdata.common.hdfs_publication",
         *binding_arguments(source_version, snapshot, upstream, version, run),
         *(["--recover", "--skip-completed-driver"] if recover else [])],
        operation="recovery" if recover else "publication", owner_user=True,
        allowed_codes=(0, 10) if recover else (0,),
    )


def driver(source_version, snapshot, upstream, version, run, *, reuse=False):
    return container_command(
        ["--conf", "spark.sql.shuffle.partitions=16", "--conf", "spark.sql.adaptive.enabled=true",
         "--", "warehouse_bigdata/jobs/build_forecast_features.py",
         *binding_arguments(source_version, snapshot, upstream, version, run),
         "--hdfs-uri", HDFS, "--spark-master", MASTER, "--event-log-dir",
         f"{HDFS}/data/warehouse/staging/pipeline_run_id={run}/events"],
        operation="reuse" if reuse else "initial", spark=True,
    ).stdout


def submit(source_version, snapshot, upstream, version, run, *, reuse=False, recovery=False):
    """Owner recovery always precedes any driver. Never retry a partial transformation."""
    args = source_version, snapshot, upstream, version, run
    recovered = publish(*args, recover=True)
    if recovered.returncode == 10:
        return recovered.stdout
    assert not recovery, "No frozen publication request; refusing recovery/computation"
    owner("""
import sys
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.common.config import load_hdfs_config
from warehouse_bigdata.common.hdfs_paths import render_staging_path, render_run_manifest_path
c, config = HdfsCliClient(), load_hdfs_config()
run = sys.argv[1]
base = render_staging_path(config.base_path, run)
paths = [base + '/' + name for name in ('input_binding.json', 'quality_report.json',
    'failure_report.json', 'candidates', 'publication/request.json')]
paths.append(render_run_manifest_path(config.base_path, run))
assert not any(c.path_status(p) is not None for p in paths), 'existing execution evidence; preserve it'
""", run)
    output = driver(*args, reuse=reuse)
    return output + publish(*args).stdout


def independent_probe(source_version, snapshot, upstream, version, run, *, full=True):
    records = []

    def record(line):
        if line.startswith(b"J04_READBACK="):
            assert len(line) <= 128 * 1024, "Oversized readback result"
            records.append(json.loads(line.removeprefix(b"J04_READBACK=")))
            assert len(records) == 1, "Duplicate readback results"

    args = [source_version, snapshot, upstream, version, run, "full" if full else "integrity"]
    if full:
        invocation = ["--conf", "spark.sql.shuffle.partitions=16", "--",
                      "feature_snapshot_support.py", *args]
    else:
        invocation = ["/opt/warehouse-app/src/feature_snapshot_support.py", *args]
    container_command(invocation, operation="readback" if full else "metadata",
                      owner_user=not full, mounted=True, spark=full, on_stdout_line=record)
    assert len(records) == 1, "Missing independent readback result"
    return records[0]


def _require(document, expected, label):
    differences = {
        key: {"expected": value, "actual": document.get(key)}
        for key, value in expected.items()
        if document.get(key) != value
    }
    assert not differences, f"{label}: {differences}"


def _no_rows(frame, message):
    # Only bounded diagnostics may reach the driver, even on a corrupt artifact.
    sample = frame.limit(4).collect()
    assert not sample, f"{message}; samples={[row.asDict() for row in sample]}"


def document_digest(document):
    payload = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def logical_schema():
    return [{"name": name, "type": kind, "nullable": name in ("last_known_price", "label")}
            for name, kind in FEATURE_SCHEMA]


def artifact_inventory(client, root):
    """Fresh actual HDFS checksums; never trust the sidecar's recorded inventory."""
    pending, seen, inventory = [root], set(), []
    while pending:
        parent = pending.pop()
        for status in client.list_status(parent):
            path = status["path"]
            pure = PurePosixPath(path)
            assert str(pure) == path and str(pure.parent) == parent and path not in seen, (
                f"Invalid/duplicate inventory child: {path}"
            )
            seen.add(path)
            assert len(seen) <= 1024, "Unbounded artifact inventory"
            relative = str(pure.relative_to(root))
            if relative == "_dataset_manifest.json" and status["type"] == "FILE":
                continue
            if status["type"] == "DIRECTORY":
                assert relative in ("_diagnostics", "_diagnostics/exclusions"), relative
                pending.append(path)
                continue
            assert (status["type"] == "FILE" and not pure.name.startswith(".")
                    and (pure.suffix == ".parquet" or pure.name == "_SUCCESS")
                    and type(status["length"]) is int and status["length"] >= 0), status
            inventory.append({"path": relative, "size_bytes": status["length"]})
    assert any(i["path"].endswith(".parquet") and not i["path"].startswith("_")
               for i in inventory), "Missing feature Parquet"
    paths = [root + "/" + entry["path"] for entry in inventory]
    checksums = client.checksums(paths)
    assert set(checksums) == set(paths) and all(checksums.values()), "Incomplete actual checksums"
    for entry, path in zip(inventory, paths, strict=True):
        entry["checksum"] = checksums[path]
    return sorted(inventory, key=lambda entry: entry["path"])


def check_frozen_execution(marker, sidecar, binding, quality, request, inventory, *, run, producer):
    """Independent metadata verification against the producing revision, not today's code."""
    coverage = json.loads(marker["coverage_and_exclusion_counts"])
    schema = logical_schema()
    fingerprint = document_digest({"schema": schema})
    assert sidecar["schema"] == schema, "Frozen logical schema/nullability differs"
    assert quality["schema"] == schema, "Quality schema/nullability differs"
    _require(marker, {"status": "VALIDATED", "pipeline_run_id": run,
                     "producer": "warehouse-bigdata:feature-snapshot"}, "Run completion")
    assert request == {"version": 1, "manifest": marker}, "Frozen request/completion differs"
    for document, key in ((binding, "binding_digest"), (quality, "quality_digest"),
                          (sidecar, "dataset_manifest_digest")):
        assert document_digest(document) == coverage[key], f"Frozen {key} differs"
    assert binding["configuration_digest"] == document_digest(binding["effective_configuration"]), (
        "Effective configuration fingerprint differs"
    )
    assert binding["code_revision"].startswith("sha256:"), "Missing producing code revision"
    _require(coverage, {"producing_pipeline_run_id": producer, "publication_complete": True,
                       "persisted_parquet_validated": True}, "Frozen publication state")
    for document in (marker, quality, sidecar):
        _require(document, {"schema_fingerprint": fingerprint, "row_count": FEATURE_COUNT}
                 if document is not quality else {"schema_fingerprint": fingerprint},
                 "Independent schema fingerprint/rows")
    for document in (quality, sidecar):
        _require(document, {"status": "VALIDATED", "persisted_parquet_validated": True},
                 "Persisted Parquet validation")
    for document in (binding, quality):
        _require(document, {key: marker[key] for key in ("source_version", "source_snapshot_id",
                 "pipeline_run_id", "code_revision", "configuration_digest")},
                 "Frozen execution binding")
    artifact_binding = {k: v for k, v in sidecar["input_binding"].items() if k != "pipeline_run_id"}
    assert artifact_binding == {k: v for k, v in binding.items() if k != "pipeline_run_id"}, (
        "Strict source/code/configuration artifact binding differs"
    )
    for document in (coverage, quality, sidecar):
        assert document["file_integrity"] == inventory, "Actual artifact file integrity differs"
    assert coverage["metrics"] == quality["metrics"] == sidecar["metrics"], "Quality metrics differ"
    metrics = quality["metrics"]
    _require(metrics, {"row_count": FEATURE_COUNT, "observed_count": SOURCE_COUNT,
             "candidate_count": FEATURE_COUNT, "eligible_count": FEATURE_COUNT, "excluded_count": 0,
             "lookback_only_count": SERIES_COUNT * 28,
             "role_counts": {"train": FEATURE_COUNT, "validation": 0, "test": 0},
             "persisted_parquet_validated": True}, "Full fixture persisted coverage")
    assert metrics["required_field_null_counts"] == {
        name: 0 for name, _ in FEATURE_SCHEMA if name != "last_known_price"
    }, "Persisted historical null counts differ"
    _require(quality, {"pipeline_run_id": run, "producing_pipeline_run_id": producer},
             "Quality producer identity")
    _require(sidecar, {**sidecar["input_binding"], "schema_version": "feature-dataset-manifest-v1"},
             "Dataset embedded binding")
    _require(quality, binding, "Quality full input binding")
    _require(coverage, {"feature_version": binding["feature_version"],
                       "upstream_pipeline_run_id": binding["upstream_pipeline_run_id"],
                       "dataset_manifest_uri": binding["artifact_uri"] + "/_dataset_manifest.json"},
             "Coverage lineage")
    assert json.loads(marker["required_field_null_counts"]) == metrics["required_field_null_counts"]
    return coverage


def _publication_evidence(client, config, source_version, snapshot, upstream, version, run):
    from warehouse_bigdata.common.hdfs_paths import (
        render_curated_sales_enriched_path,
        render_forecast_features_path,
        render_run_manifest_path,
        render_staging_path,
    )
    def read(path):
        return json.loads(client.read_remote(path))

    source_path = render_curated_sales_enriched_path(config.base_path, snapshot)
    path = render_forecast_features_path(config.base_path, snapshot, version)
    identity = {"source_version": source_version, "source_snapshot_id": snapshot}
    source_marker = read(render_run_manifest_path(config.base_path, upstream))
    _require(
        source_marker,
        {
            **identity,
            "pipeline_run_id": upstream,
            "status": "VALIDATED",
            "row_count": SOURCE_COUNT,
            "producer": "warehouse-bigdata:enrichment-analytics-snapshot-v1",
        },
        "Selected M04 smoke J03 completion",
    )
    outputs = dict(
        zip(
            json.loads(source_marker["output_dataset_names"]),
            json.loads(source_marker["output_dataset_uris"]),
            strict=True,
        )
    )
    assert outputs.get("sales_enriched") == config.uri + source_path, outputs
    source_sidecar = read(source_path + "/_dataset_manifest.json")
    _require(
        source_sidecar,
        {
            **identity,
            "pipeline_run_id": upstream,
            "status": "VALIDATED",
            "row_count": SOURCE_COUNT,
            "artifact_uri": config.uri + source_path,
        },
        "Persisted source lineage",
    )
    source_staging = render_staging_path(config.base_path, upstream)
    source_quality = read(source_staging + "/quality_report.json")
    _require(
        source_quality,
        {**identity, "pipeline_run_id": upstream, "status": "VALIDATED", "failure_details": []},
        "Selected J03 quality",
    )
    _require(
        source_quality["outputs"]["sales_enriched"],
        {
            **identity,
            "pipeline_run_id": upstream,
            "status": "VALIDATED",
            "row_count": SOURCE_COUNT,
            "persisted_parquet_validated": True,
            "candidate_only": False,
            "schema_fingerprint": source_sidecar["schema_fingerprint"],
        },
        "Selected J03 persisted source quality",
    )
    _require(
        read(source_staging + "/input_binding.json"),
        {**identity, "pipeline_run_id": upstream},
        "Selected J03 input binding",
    )
    marker_bytes = client.read_remote(render_run_manifest_path(config.base_path, run))
    marker = json.loads(marker_bytes)
    _require(
        marker,
        {
            **identity,
            "pipeline_run_id": run,
            "status": "VALIDATED",
            "row_count": FEATURE_COUNT,
            "producer": "warehouse-bigdata:feature-snapshot",
        },
        "J04 completion",
    )
    _require(marker, {
        "artifact_uri": config.uri + render_run_manifest_path(config.base_path, run),
        "quality_report_uri": config.uri + render_staging_path(config.base_path, run)
        + "/quality_report.json",
    }, "Completion paths")
    assert json.loads(marker["output_dataset_names"]) == ["forecast_features"]
    assert json.loads(marker["output_dataset_uris"]) == [config.uri + path]
    sidecar_bytes = client.read_remote(path + "/_dataset_manifest.json")
    sidecar = json.loads(sidecar_bytes)
    coverage = json.loads(marker["coverage_and_exclusion_counts"])
    producer = coverage["producing_pipeline_run_id"]
    _require(
        sidecar,
        {
            **identity,
            "feature_version": version,
            "upstream_pipeline_run_id": upstream,
            "pipeline_run_id": producer,
            "status": "VALIDATED",
            "row_count": FEATURE_COUNT,
            "artifact_uri": config.uri + path,
            "provenance": "derived_leakage_controlled_features",
        },
        "Published feature lineage",
    )
    assert sidecar["schema"] == logical_schema(), "Frozen logical feature schema differs"
    binding = read(render_staging_path(config.base_path, run) + "/input_binding.json")
    _require(
        binding,
        {**identity, "upstream_pipeline_run_id": upstream, "feature_version": version,
         "artifact_uri": config.uri + path},
        "J04 input binding",
    )
    _require(
        binding["upstream_evidence"],
        {
            **identity,
            "pipeline_run_id": upstream,
            "row_count": SOURCE_COUNT,
            "upstream_pipeline_run_id": source_quality["input"]["manifest_pipeline_run_id"],
        },
        "J04 recorded J03 evidence",
    )
    inventory = artifact_inventory(client, path)
    quality = read(render_staging_path(config.base_path, run) + "/quality_report.json")
    request = read(render_staging_path(config.base_path, run) + "/publication/request.json")
    check_frozen_execution(marker, sidecar, binding, quality, request, inventory,
                           run=run, producer=producer)
    for parent in (render_staging_path(config.base_path, run),
                   render_staging_path(config.base_path, run) + "/publication"):
        status = client.path_status(parent)
        assert status and status["owner"] == "hadoop" and not int(status["permission"], 8) & 0o022, (
            f"Execution parent is not owner-frozen: {parent}"
        )
    for name in ("input_binding.json", "quality_report.json", "publication/request.json"):
        status = client.path_status(render_staging_path(config.base_path, run) + "/" + name)
        assert status and status["owner"] == "hadoop" and not int(status["permission"], 8) & 0o222, (
            f"Execution evidence is not frozen: {name}"
        )
    producer_bytes = client.read_remote(render_run_manifest_path(config.base_path, producer))
    producer_marker = json.loads(producer_bytes)
    _require(producer_marker, {
        **identity, "pipeline_run_id": producer, "status": "VALIDATED",
        "row_count": FEATURE_COUNT, "output_dataset_names": marker["output_dataset_names"],
        "output_dataset_uris": marker["output_dataset_uris"],
    }, "Original producer completion")
    if producer != run:
        producer_base = render_staging_path(config.base_path, producer)
        check_frozen_execution(producer_marker, sidecar, read(producer_base + "/input_binding.json"),
                               read(producer_base + "/quality_report.json"),
                               read(producer_base + "/publication/request.json"), inventory,
                               run=producer, producer=producer)
    else:
        assert producer_marker == marker
    producer_quality = read(render_staging_path(config.base_path, producer) + "/quality_report.json")
    runtime = stream_task_evidence(
        client, render_staging_path(config.base_path, producer) + "/events",
        producer_quality["spark_application_id"],
    )
    execution_runtime = (runtime if run == producer else stream_task_evidence(
        client, render_staging_path(config.base_path, run) + "/events", quality["spark_application_id"],
    ))
    return (
        source_path,
        path,
        {
            "row_count": marker["row_count"],
            "producer": producer,
            "execution_run": run,
            "lineage": {
                **identity,
                "upstream_pipeline_run_id": upstream,
                "feature_version": version,
            },
            "file_integrity": inventory,
            "file_integrity_entries": len(inventory),
            "sidecar_sha256": hashlib.sha256(sidecar_bytes).hexdigest(),
            "completion_sha256": hashlib.sha256(marker_bytes).hexdigest(),
            "producer_completion_sha256": hashlib.sha256(producer_bytes).hexdigest(),
            "binding_digest": document_digest(binding),
            "quality_digest": document_digest(quality),
            "request_digest": document_digest(request),
            "frozen_metadata_sha256": {
                name: hashlib.sha256(client.read_remote(
                    render_staging_path(config.base_path, run) + "/" + name)).hexdigest()
                for name in ("input_binding.json", "quality_report.json", "publication/request.json")
            },
            "code_revision": binding["code_revision"],
            "schema_fingerprint": sidecar["schema_fingerprint"],
            "runtime": runtime,
            "execution_runtime": execution_runtime,
        },
    )


def _verify_source(source):
    from pyspark.sql import functions as F

    from warehouse_bigdata.contracts.datasets import SALES_ENRICHED

    invalid = reduce(
        or_,
        [
            *[F.col(name).isNull() for name in SALES_ENRICHED.required_field_names],
            ~F.col("day_index").between(1, 30),
            ~F.col("day_key").eqNullSafe(F.concat(F.lit("d_"), F.col("day_index"))),
            ~F.col("date").eqNullSafe(F.date_add(F.lit(FIRST_DATE), F.col("day_index") - 1)),
            F.col("unit_sales") < 0,
            ~F.col("price_missing").eqNullSafe(F.col("sell_price").isNull()),
            ~F.col("is_zero_sale").eqNullSafe(F.col("unit_sales") == 0),
            F.col("wday") != F.pmod(F.dayofweek("date"), F.lit(7)) + 1,
            F.col("month") != F.month("date"),
            F.col("year") != F.year("date"),
        ],
    )
    summary = (
        source.agg(
            F.count("*").alias("row_count"),
            F.countDistinct("day_index").alias("days"),
            F.min("day_index").alias("first_day"),
            F.max("day_index").alias("last_day"),
            F.count(F.when(invalid, 1)).alias("invalid_rows"),
        )
        .first()
        .asDict()
    )
    _require(
        summary,
        {"row_count": SOURCE_COUNT, "days": 30, "first_day": 1, "last_day": 30, "invalid_rows": 0},
        "Persisted source must be exactly the d_1–d_30 smoke scope",
    )
    series = source.groupBy(*SERIES_KEY).agg(
        F.count("*").alias("observations"),
        F.countDistinct("date").alias("dates"),
        F.countDistinct("day_index").alias("days"),
        F.countDistinct("dept_id").alias("departments"),
    )
    scope = (
        series.agg(
            F.count("*").alias("series_count"),
            F.count(
                F.when(
                    (F.col("observations") != 30)
                    | (F.col("dates") != 30)
                    | (F.col("days") != 30)
                    | (F.col("departments") != 1),
                    1,
                )
            ).alias("invalid_series"),
        )
        .first()
        .asDict()
    )
    _require(
        scope, {"series_count": SERIES_COUNT, "invalid_series": 0}, "Source series scope/grain"
    )
    return series, {**summary, **scope}


def _verify_features(source, features, snapshot, version):
    from pyspark.sql import functions as F

    actual_schema = tuple((field.name, field.dataType.simpleString()) for field in features.schema)
    assert actual_schema == FEATURE_SCHEMA, f"Frozen physical feature schema: {actual_schema}"
    targets = [FIRST_DATE + timedelta(days=day - 1) for day in (29, 30)]
    weekday = F.pmod(F.dayofweek("target_date") + 5, F.lit(7)) + 1
    invalid = reduce(
        or_,
        [
            *[F.col(name).isNull() for name, _ in FEATURE_SCHEMA if name != "last_known_price"],
            F.col("source_snapshot_id") != snapshot,
            F.col("feature_version") != version,
            F.col("feature_role") != "train",
            ~F.col("target_date").isin(targets),
            F.col("as_of_date") != F.date_sub("target_date", 1),
            F.col("day_of_week") != weekday,
            F.col("month") != F.month("target_date"),
            F.col("is_weekend") != weekday.isin(6, 7),
            F.col("event_flag"),
            ~F.col("price_missing").eqNullSafe(F.col("last_known_price").isNull()),
            *[F.col(name) < 0 for name in ("lag_1", "lag_7", "lag_28", "label")],
            *[
                (F.col(name) < 0) | F.isnan(name) | (F.abs(F.col(name)) > 1.7976931348623157e308)
                for name in ("rolling_mean_7", "rolling_mean_28")
            ],
            F.col("last_known_price").isNotNull()
            & (
                (F.col("last_known_price") <= 0)
                | F.isnan("last_known_price")
                | (F.abs(F.col("last_known_price")) > 1.7976931348623157e308)
            ),
        ],
    )
    summary = (
        features.agg(
            F.count("*").alias("row_count"),
            F.count(F.when(invalid, 1)).alias("invalid_rows"),
            *[
                F.count(F.when(F.col("target_date") == target, 1)).alias(f"d_{day}")
                for day, target in zip((29, 30), targets, strict=True)
            ],
            F.count(F.when(F.col("price_missing"), 1)).alias("price_missing_rows"),
        )
        .first()
        .asDict()
    )
    _require(
        summary,
        {"row_count": FEATURE_COUNT, "invalid_rows": 0, "d_29": SERIES_COUNT, "d_30": SERIES_COUNT},
        "Published smoke feature scope/invariants",
    )
    scoped_key = [
        "source_snapshot_id",
        "feature_version",
        "feature_role",
        *SERIES_KEY,
        "target_date",
    ]
    _no_rows(features.groupBy(*scoped_key).count().where("count != 1"), "Duplicate scoped keys")
    _no_rows(
        features.groupBy(*SERIES_KEY)
        .agg(
            F.count("*").alias("rows"),
            F.countDistinct("target_date").alias("dates"),
        )
        .where("rows != 2 OR dates != 2"),
        "Each eligible series must have two target dates",
    )
    observed = source.where(F.col("day_index").isin(29, 30)).select(
        *SERIES_KEY,
        F.col("date").alias("target_date"),
        F.col("dept_id").alias("source_dept"),
        "unit_sales",
    )
    # Full outer reconciliation also catches missing eligible series/targets and extras.
    joined = features.join(observed, [*SERIES_KEY, "target_date"], "full")
    _no_rows(
        joined.where(
            F.col("feature_version").isNull()
            | F.col("unit_sales").isNull()
            | ~F.col("dept_id").eqNullSafe(F.col("source_dept"))
            | ~F.col("label").eqNullSafe(F.col("unit_sales"))
        ),
        "Eligible key coverage or observed label/department mismatch",
    )
    return summary


def probe(source_version, snapshot, upstream, version, run, mode):
    """Validate the full persisted snapshot once; bind later probes by exact bytes."""
    from warehouse_bigdata.common.config import load_hdfs_config
    from warehouse_bigdata.common.spark_hdfs_client import SparkHdfsCliClient

    config = load_hdfs_config()
    client = SparkHdfsCliClient()
    source_path, path, evidence = _publication_evidence(
        client, config, source_version, snapshot, upstream, version, run
    )
    if mode == "integrity":
        return evidence  # Metadata-only owner process; no Spark application.
    assert mode == "full", f"Unknown probe mode: {mode}"

    from pyspark import StorageLevel
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master(MASTER).appName("j04-independent-readback").getOrCreate()
    retained = []
    try:
        evidence["verification_application_id"] = spark.sparkContext.applicationId

        def read(path):
            frame = (spark.read.option("ignoreCorruptFiles", "false")
                     .option("ignoreMissingFiles", "false").option("mergeSchema", "true")
                     .parquet(config.uri + path).persist(StorageLevel.DISK_ONLY))
            retained.append(frame)
            return frame

        source, features = read(source_path), read(path)
        _, evidence["source_scope"] = _verify_source(source)
        evidence["feature_scope"] = _verify_features(source, features, snapshot, version)
        evidence["oracle"] = verify_oracle(source, features, snapshot, version)
        assert evidence["oracle"]["compared_rows"] == FEATURE_COUNT
        # Detect any concurrent mutation during readback; never repair it.
        assert artifact_inventory(client, path) == evidence["file_integrity"], (
            "Artifact changed during independent verification"
        )
        return evidence
    finally:
        for frame in retained:
            frame.unpersist()
        spark.stop()


if __name__ == "__main__":
    import sys

    print("J04_READBACK=" + json.dumps(probe(*sys.argv[1:]), sort_keys=True))

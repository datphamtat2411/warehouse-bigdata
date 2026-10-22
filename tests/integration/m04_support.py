"""Opt-in M04 live-run coordination and bounded evidence collection."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import time
import uuid
from pathlib import Path, PurePosixPath

from tests.integration.hdfs_support import HDFSVerification, VerificationError, require
from tests.integration.spark_support import MAX_EVENT_BYTES, summarize_m04_event_log
from warehouse_bigdata.common.config import load_hdfs_config, load_spark_config
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.common.hdfs_paths import (
    render_curated_sales_daily_path,
    render_curated_sales_enriched_path,
    render_enrichment_candidate_path,
    render_staging_path,
)
from warehouse_bigdata.contracts.manifests import PIPELINE_RUN_MANIFEST, VALIDATED
from warehouse_bigdata.contracts.validation import validate_manifest
from warehouse_bigdata.ingestion.orchestration import run_m5_ingestion

ROOT = Path(__file__).resolve().parents[2]
REPORT_ROOT = ROOT / "artifacts" / "m04"
PERFORMANCE_ENV = "WAREHOUSE_M04_PERFORMANCE"
FULL_DAY_COUNT = 1_941
CALIBRATION_DAY_COUNT = 128
SERIES_COUNT = 30_490
FULL_ROW_COUNT = 59_181_090


def source_environment() -> tuple[Path, str, str | None]:
    source_value = os.environ.get("WAREHOUSE_M5_SOURCE_DIR")
    source_url = os.environ.get("WAREHOUSE_M5_SOURCE_URL")
    downloaded_at = os.environ.get("WAREHOUSE_M5_DOWNLOADED_AT_UTC")
    require(source_value and source_url, "Set WAREHOUSE_M5_SOURCE_DIR and WAREHOUSE_M5_SOURCE_URL.")
    source = Path(source_value).resolve(strict=True)
    require(source.is_dir(), f"M5 source is not a directory: {source}")
    return source, source_url, downloaded_at


def hdfs_client(hdfs: HDFSVerification, *, metadata_limit: int = MAX_EVENT_BYTES) -> HdfsCliClient:
    return HdfsCliClient(
        command_prefix=(
            "docker",
            "exec",
            "-i",
            hdfs.client,
            "timeout",
            "--kill-after=5s",
            "120s",
        ),
        metadata_limit=metadata_limit,
    )


def prepare_source(tmp_path: Path, hdfs: HDFSVerification) -> dict[str, object]:
    source, source_url, downloaded_at = source_environment()
    hdfs_config = load_hdfs_config()
    client = hdfs_client(hdfs)
    ingestion_run_id = f"m04-ingest-{uuid.uuid4().hex}"
    result = run_m5_ingestion(
        source,
        source_url,
        downloaded_at,
        artifact_root=tmp_path,
        hdfs_config=hdfs_config,
        client=client,
        pipeline_run_id=ingestion_run_id,
    )
    require(result.status == VALIDATED, f"M5 ingestion did not validate: {result.failure_details}")
    require(result.source_version is not None and result.raw_path is not None, "M5 lineage is incomplete.")
    manifests = json.loads(client.read_remote(result.raw_path + "/source_manifests.json"))
    return {
        "source": source,
        "source_url": source_url,
        "hdfs_config": hdfs_config,
        "client": client,
        "ingestion_run_id": ingestion_run_id,
        "source_version": result.source_version,
        "raw_path": result.raw_path,
        "raw_manifests": manifests,
    }


def _event_directory(hdfs_config, pipeline_run_id: str) -> str:
    return str(PurePosixPath(render_staging_path(hdfs_config.base_path, pipeline_run_id)) / "events")


def _create_event_directory(client: HdfsCliClient, path: str) -> None:
    client.mkdir_parents(str(PurePosixPath(path).parent))
    client.mkdir_exclusive(path)


def _submit(
    hdfs: HDFSVerification,
    application: str,
    arguments: list[str],
    conf: dict[str, str],
    output_dir: Path,
) -> bytes:
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
        command.extend(("--conf", f"{key}={value}"))
    command.extend(("--", application, *arguments))
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "command.json").write_text(
        json.dumps(command, indent=2) + "\n", encoding="utf-8"
    )
    completed = hdfs.runner.run(command, timeout=1_860, check=False)
    (output_dir / "stdout").write_bytes(completed.stdout)
    (output_dir / "stderr").write_bytes(completed.stderr)
    require(
        completed.returncode == 0,
        f"Spark application failed ({application}, exit {completed.returncode}): "
        f"{completed.stderr.decode(errors='replace')[-2000:]}",
    )
    return completed.stdout


def _timings(stdout: bytes) -> dict[str, float]:
    for line in stdout.decode(errors="strict").splitlines():
        if not line.startswith("timings="):
            continue
        value = json.loads(line.removeprefix("timings="))
        require(
            isinstance(value, dict)
            and all(isinstance(key, str) and type(item) is float for key, item in value.items()),
            "Spark job emitted invalid bounded timing JSON.",
        )
        return value
    raise VerificationError("Spark job did not emit timing JSON.")


def _verification_record(stdout: bytes) -> dict[str, object]:
    records = [
        json.loads(line.removeprefix("WAREHOUSE_M04 "))
        for line in stdout.decode(errors="strict").splitlines()
        if line.startswith("WAREHOUSE_M04 ")
    ]
    require(len(records) == 1 and isinstance(records[0], dict), "M04 verifier must emit one result.")
    return records[0]


def _collect_event_summary(
    client: HdfsCliClient,
    event_dir: str,
    *,
    run_id: str,
    application_name: str,
    output_dir: Path,
) -> dict[str, object]:
    paths = client.list_children(event_dir)
    require(len(paths) == 1, f"Expected one non-rolling event log in {event_dir}.")
    event_path = paths[0]
    name = event_path.rsplit("/", 1)[-1]
    require(re.fullmatch(r"app-\d{14}-\d+(?:\.inprogress)?", name), "Unexpected event log name.")
    stat = client.stat(event_path)
    require(0 < stat.size_bytes <= MAX_EVENT_BYTES, "Event log is empty or exceeds the evidence bound.")
    payload = client.read_remote(event_path)
    require(len(payload) == stat.size_bytes, "Event log changed during independent retrieval.")
    (output_dir / "event-log.jsonl").write_bytes(payload)
    summary = summarize_m04_event_log(
        payload,
        run_id=run_id,
        application_name=application_name,
    )
    summary["event_log"] = {
        "path": event_path,
        "bytes": stat.size_bytes,
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    (output_dir / "execution.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    return summary


def _read_json(client: HdfsCliClient, path: str) -> dict[str, object]:
    value = json.loads(client.read_remote(path).decode("utf-8"))
    require(isinstance(value, dict), f"Expected JSON object at {path}.")
    return value


def _validate_run_manifest(
    client: HdfsCliClient,
    path: str,
    *,
    pipeline_run_id: str,
    source_version: str,
    source_snapshot_id: str,
) -> dict[str, object]:
    manifest = _read_json(client, path)
    validate_manifest(
        PIPELINE_RUN_MANIFEST,
        manifest,
        lineage_context={
            "required_status": VALIDATED,
            "pipeline_run_id": pipeline_run_id,
            "source_version": source_version,
            "source_snapshot_id": source_snapshot_id,
        },
    )
    return manifest


def _lineage_paths(hdfs_config, source_snapshot_id: str, enrichment_run_id: str) -> dict[str, str]:
    staging = render_staging_path(hdfs_config.base_path, enrichment_run_id)
    return {
        "sales_daily": render_curated_sales_daily_path(hdfs_config.base_path, source_snapshot_id),
        "sales_enriched": render_curated_sales_enriched_path(
            hdfs_config.base_path, source_snapshot_id
        ),
        "store_daily_sales": render_enrichment_candidate_path(staging, "store_daily_sales"),
        "category_monthly_sales": render_enrichment_candidate_path(
            staging, "category_monthly_sales"
        ),
    }


def run_case(
    hdfs: HDFSVerification,
    source: dict[str, object],
    *,
    label: str,
    expected_day_count: int,
    days_per_chunk: int,
    shuffle_partitions: int,
    adaptive: bool,
    cores_max: int,
) -> dict[str, object]:
    hdfs_config = source["hdfs_config"]
    client = source["client"]
    source_version = str(source["source_version"])
    token = uuid.uuid4().hex
    source_snapshot_id = f"m04-{label}-snapshot-{token}"
    normalization_run_id = f"m04-{label}-j02-{token}"
    enrichment_run_id = f"m04-{label}-j03-{token}"
    verification_run_id = f"m04-{label}-verify-{token}"
    case_dir = REPORT_ROOT / label / token
    case_dir.mkdir(parents=True, exist_ok=False)
    conf = {
        "spark.default.parallelism": "2",
        "spark.sql.shuffle.partitions": str(shuffle_partitions),
        "spark.sql.adaptive.enabled": str(adaptive).lower(),
        "spark.cores.max": str(cores_max),
    }
    normalization_events = _event_directory(hdfs_config, normalization_run_id)
    enrichment_events = _event_directory(hdfs_config, enrichment_run_id)
    verification_events = _event_directory(hdfs_config, verification_run_id)
    for event_dir in (normalization_events, enrichment_events, verification_events):
        _create_event_directory(client, event_dir)

    normalization_stdout = _submit(
        hdfs,
        "warehouse_bigdata/jobs/normalize_sales_daily.py",
        [
            "--source-version",
            source_version,
            "--source-snapshot-id",
            source_snapshot_id,
            "--pipeline-run-id",
            normalization_run_id,
            "--hdfs-uri",
            hdfs_config.uri,
            "--spark-master",
            load_spark_config().master_url,
            "--days-per-chunk",
            str(days_per_chunk),
            "--expected-day-count",
            str(expected_day_count),
            "--event-log-dir",
            hdfs_config.uri.rstrip("/") + normalization_events,
        ],
        conf,
        case_dir / "j02",
    )
    normalization_execution = _collect_event_summary(
        hdfs_client(hdfs),
        normalization_events,
        run_id=normalization_run_id,
        application_name=f"warehouse-sales-daily-{normalization_run_id}",
        output_dir=case_dir / "j02",
    )

    enrichment_stdout = _submit(
        hdfs,
        "warehouse_bigdata/jobs/enrich_analytics.py",
        [
            "--source-version",
            source_version,
            "--source-snapshot-id",
            source_snapshot_id,
            "--pipeline-run-id",
            enrichment_run_id,
            "--hdfs-uri",
            hdfs_config.uri,
            "--spark-master",
            load_spark_config().master_url,
            "--event-log-dir",
            hdfs_config.uri.rstrip("/") + enrichment_events,
        ],
        conf,
        case_dir / "j03",
    )
    enrichment_execution = _collect_event_summary(
        hdfs_client(hdfs),
        enrichment_events,
        run_id=enrichment_run_id,
        application_name=f"warehouse-enrich-analytics-{enrichment_run_id}",
        output_dir=case_dir / "j03",
    )

    paths = _lineage_paths(hdfs_config, source_snapshot_id, enrichment_run_id)
    verification_stdout = _submit(
        hdfs,
        "warehouse_bigdata/jobs/verify_m04_outputs.py",
        [
            "--source-version",
            source_version,
            "--source-snapshot-id",
            source_snapshot_id,
            "--normalization-run-id",
            normalization_run_id,
            "--enrichment-run-id",
            enrichment_run_id,
            "--pipeline-run-id",
            verification_run_id,
            "--hdfs-uri",
            hdfs_config.uri,
            "--spark-master",
            load_spark_config().master_url,
            "--expected-series-count",
            str(SERIES_COUNT),
            "--expected-day-count",
            str(expected_day_count),
            "--event-log-dir",
            hdfs_config.uri.rstrip("/") + verification_events,
        ],
        conf,
        case_dir / "verify",
    )
    verification = _verification_record(verification_stdout)
    verification_execution = _collect_event_summary(
        hdfs_client(hdfs),
        verification_events,
        run_id=verification_run_id,
        application_name=f"warehouse-m04-verify-{verification_run_id}",
        output_dir=case_dir / "verify",
    )

    normalization_manifest_path = (
        hdfs_config.base_path + "/manifests/runs/" + normalization_run_id + ".json"
    )
    enrichment_manifest_path = hdfs_config.base_path + "/manifests/runs/" + enrichment_run_id + ".json"
    normalization_manifest = _validate_run_manifest(
        client,
        normalization_manifest_path,
        pipeline_run_id=normalization_run_id,
        source_version=source_version,
        source_snapshot_id=source_snapshot_id,
    )
    enrichment_manifest = _validate_run_manifest(
        client,
        enrichment_manifest_path,
        pipeline_run_id=enrichment_run_id,
        source_version=source_version,
        source_snapshot_id=source_snapshot_id,
    )
    snapshot_manifest = _read_json(client, paths["sales_daily"] + "/_snapshot_manifest.json")
    require(snapshot_manifest.get("status") == VALIDATED, "SALES_DAILY snapshot is not VALIDATED.")
    require(snapshot_manifest.get("source_snapshot_id") == source_snapshot_id, "J02 snapshot lineage mismatch.")
    require(
        snapshot_manifest.get("row_count") == SERIES_COUNT * expected_day_count,
        "J02 snapshot row count differs from the selected scope.",
    )
    require(verification.get("status") == VALIDATED, "Independent M04 reconciliation did not validate.")

    report = {
        "schema_version": "m04-task-06-report-v1",
        "status": VALIDATED,
        "case": label,
        "runtime": {
            "python": platform.python_version(),
            "spark_master": load_spark_config().master_url,
            "hdfs_uri": hdfs_config.uri,
            "host_memory_bytes": _host_memory_bytes(),
        },
        "configuration": {
            "days_per_chunk": days_per_chunk,
            "expected_day_count": expected_day_count,
            "expected_series_count": SERIES_COUNT,
            "expected_row_count": SERIES_COUNT * expected_day_count,
            "spark_sql_shuffle_partitions": shuffle_partitions,
            "spark_sql_adaptive_enabled": adaptive,
            "spark_cores_max": cores_max,
        },
        "digests": {
            "source_version": source_version,
            "raw_file_checksums": {
                str(manifest["source_file"]): manifest.get("artifact_checksum")
                for manifest in source["raw_manifests"]
            },
            "configuration_sha256": hashlib.sha256(
                json.dumps(conf, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        },
        "lineage": {
            "source_version": source_version,
            "source_snapshot_id": source_snapshot_id,
            "normalization_run_id": normalization_run_id,
            "enrichment_run_id": enrichment_run_id,
            "verification_run_id": verification_run_id,
            "raw_path": source["raw_path"],
            "paths": paths,
        },
        "production_quality": {
            "normalization_manifest": normalization_manifest,
            "enrichment_manifest": enrichment_manifest,
            "snapshot_manifest": snapshot_manifest,
        },
        "independent_reconciliation": verification,
        "timings": {
            "normalization": _timings(normalization_stdout),
            "enrichment": _timings(enrichment_stdout),
            "verification": verification.get("timings", {}),
        },
        "worker_metrics": {
            "normalization": normalization_execution,
            "enrichment": enrichment_execution,
            "verification": verification_execution,
        },
        "storage": verification.get("storage", {}),
        "limitations": [
            "Worker and executor evidence describes this local two-worker simulation only.",
            "Observed unit sales are not verified inventory or customer orders.",
            "Timings include cluster scheduling and HDFS state and are not a claimed speedup.",
        ],
        "evidence_directory": str(case_dir),
    }
    _write_report(case_dir, report)
    return report


def run_correctness(hdfs: HDFSVerification, tmp_path: Path) -> dict[str, object]:
    source = prepare_source(tmp_path, hdfs)
    return run_case(
        hdfs,
        source,
        label="correctness-full",
        expected_day_count=FULL_DAY_COUNT,
        days_per_chunk=128,
        shuffle_partitions=32,
        adaptive=True,
        cores_max=2,
    )


def run_benchmark_matrix(hdfs: HDFSVerification, tmp_path: Path) -> dict[str, object]:
    source = prepare_source(tmp_path, hdfs)
    matrix = (
        ("baseline", 128, 32, True, 2),
        ("chunk-64", 64, 32, True, 2),
        ("chunk-256", 256, 32, True, 2),
        ("shuffle-16", 128, 16, True, 2),
        ("shuffle-64", 128, 64, True, 2),
        ("aqe-off", 128, 32, False, 2),
        ("cores-1", 128, 32, True, 1),
    )
    reports = []
    for label, chunk, shuffle, adaptive, cores in matrix:
        started = time.monotonic()
        try:
            report = run_case(
                hdfs,
                source,
                label=f"benchmark-{label}",
                expected_day_count=CALIBRATION_DAY_COUNT,
                days_per_chunk=chunk,
                shuffle_partitions=shuffle,
                adaptive=adaptive,
                cores_max=cores,
            )
            reports.append(
                {
                    "case": label,
                    "status": report["status"],
                    "elapsed_seconds": time.monotonic() - started,
                    "timings": report["timings"],
                    "worker_metrics": report["worker_metrics"],
                    "storage": report["storage"],
                    "configuration": report["configuration"],
                    "digests": report["digests"],
                    "evidence_directory": report["evidence_directory"],
                }
            )
        except Exception as error:
            reports.append(
                {
                    "case": label,
                    "status": "FAILED",
                    "elapsed_seconds": time.monotonic() - started,
                    "error": f"{error.__class__.__name__}: {error}",
                }
            )
    report = {
        "schema_version": "m04-task-06-benchmark-v1",
        "status": "COMPLETED_WITH_FAILURES" if any(item["status"] != VALIDATED for item in reports) else VALIDATED,
        "source_version": source["source_version"],
        "source_snapshot_id_scope": "unique snapshot per benchmark case",
        "workload": {
            "expected_day_count": CALIBRATION_DAY_COUNT,
            "expected_series_count": SERIES_COUNT,
            "expected_row_count": SERIES_COUNT * CALIBRATION_DAY_COUNT,
            "matrix_kind": "baseline plus one-variable alternatives",
            "repeat_count": 1,
        },
        "cases": reports,
        "limitations": [
            "Calibration uses the first 128 evaluation days to bound the 16 GB host workload.",
            "A failed or slower candidate is retained as evidence; no speedup is inferred.",
            "Results are specific to this local Spark/HDFS cluster and resource state.",
        ],
    }
    report_dir = REPORT_ROOT / "benchmark"
    report_dir.mkdir(parents=True, exist_ok=True)
    _write_report(report_dir, report)
    return report


def _host_memory_bytes() -> int | None:
    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
    except (AttributeError, OSError, ValueError):
        return None
    return pages * page_size if isinstance(pages, int) and isinstance(page_size, int) else None


def _write_report(directory: Path, report: dict[str, object]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    lines = [
        "# M04 Task 06 Evidence",
        "",
        f"- Status: `{report.get('status')}`",
        f"- Evidence directory: `{directory}`",
        "",
        "## Correctness",
        "",
        f"- Source version: `{report.get('lineage', {}).get('source_version', report.get('source_version'))}`",
        f"- Source snapshot: `{report.get('lineage', {}).get('source_snapshot_id', 'benchmark cases')}`",
        f"- Expected rows: `{report.get('configuration', report.get('workload', {})).get('expected_row_count')}`",
        "- Production quality gates and independent Parquet reconciliation are recorded in `report.json`.",
        "",
        "## Measurements",
        "",
        "- Driver phase timings, task metrics, executor hosts, shuffle, spill, GC, and storage measurements are recorded in `report.json`.",
        "- Timing comparisons are measurements only; this report makes no speedup claim.",
        "",
        "## Limitations",
        "",
    ]
    limitations = report.get("limitations", [])
    lines.extend(f"- {item}" for item in limitations if isinstance(item, str))
    (directory / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


__all__ = [
    "CALIBRATION_DAY_COUNT",
    "FULL_DAY_COUNT",
    "FULL_ROW_COUNT",
    "PERFORMANCE_ENV",
    "SERIES_COUNT",
    "prepare_source",
    "run_benchmark_matrix",
    "run_correctness",
    "source_environment",
]

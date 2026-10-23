"""Task-locked M06 acceptance coordinator; live execution is always opt-in."""

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from warehouse_bigdata.common.config import load_hdfs_config, load_spark_config
from warehouse_bigdata.common.hdfs_paths import (
    render_curated_sales_daily_path,
    render_curated_sales_enriched_path,
    render_evaluation_output_path,
    render_forecast_features_path,
    render_forecast_output_path,
    render_model_artifact_path,
    render_staging_path,
)
from warehouse_bigdata.common.hdfs_publication import document_digest
from warehouse_bigdata.common.hdfs_runtime import create_hdfs_cli_client
from warehouse_bigdata.common.small_artifacts import (
    STAGES,
    load_small_artifacts,
    validate_small_artifacts,
)
from warehouse_bigdata.common.small_demo import (
    owner_command,
    run_bounded_command,
    run_live,
)
from warehouse_bigdata.common.small_evidence import completed_execution
from warehouse_bigdata.common.small_preflight import validate_small_publications
from warehouse_bigdata.contracts.dataset_scope import ScopeError

ALLOWED_EXECUTION_STAGES = ("j06", "validation", "test")
PRODUCER_STAGES = STAGES


def parse_requested_stages(stages=None):
    values = (
        tuple(os.environ.get("WAREHOUSE_SMALL_STAGES", "").split())
        if stages is None
        else tuple(stages)
    )
    if len(set(values)) != len(values):
        raise ScopeError("M06 execution stages must be unique")
    if any(stage not in ALLOWED_EXECUTION_STAGES for stage in values):
        raise ScopeError("M06 accepts only downstream stages j06, validation, and test")
    if values != tuple(sorted(values, key=STAGES.index)):
        raise ScopeError("M06 execution stages must follow dependency order")
    return values


def report_status(gates, *, blocked=False, failed=False):
    if not gates and not blocked and not failed:
        return "NOT_RUN"
    if blocked:
        return "BLOCKED"
    if failed or not gates or not all(gates.values()):
        return "FAILED"
    return "PASSED"


def _write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, sort_keys=True, indent=2, default=str) + "\n", encoding="utf-8"
    )


def _selected_output_exists(client, config, selector, stage):
    if stage == "j06":
        path = render_forecast_output_path(config.base_path, selector["forecast_run_id"])
        marker = path + "/scoring_manifest.json"
    else:
        path = render_evaluation_output_path(
            config.base_path, selector[stage + "_evaluation_run_id"]
        )
        marker = path + "/evaluation_manifest.json"
    return client.exists(path), marker


def _missing_requested_outputs(client, config, selector, stages):
    missing = []
    for stage in stages:
        exists, marker = _selected_output_exists(client, config, selector, stage)
        if exists and not client.exists(marker):
            raise ScopeError(f"selected {stage} output is incomplete: {marker}")
        if not exists:
            missing.append(stage)
    return missing


def _producer_evidence(client, config, selector, *, expected_master):
    names = {
        "j02": "warehouse-sales-daily-",
        "j03": "warehouse-enrich-analytics-",
        "j04": "warehouse-build-forecast-features-",
        "j05": "warehouse-train-forecast-model-",
        "j06": "warehouse-forecast-daily-",
        "validation": "warehouse-evaluate-forecasts-",
        "test": "warehouse-evaluate-forecasts-",
    }
    evidence = {}
    for stage in PRODUCER_STAGES:
        run = selector[stage + "_pipeline_run_id"]
        evidence[stage] = completed_execution(
            client,
            config,
            run,
            expected_application_name=names[stage] + run,
            expected_master=expected_master,
        )
    return evidence


def _run_readback(root, selector, config, *, expected_master):
    """Launch one fresh, read-only Standalone verifier and retain its raw logs."""
    root.mkdir(parents=True, exist_ok=False)
    verification_run_id = "m06-readback-" + uuid4().hex
    event_log_dir = (
        config.uri + render_staging_path(config.base_path, verification_run_id) + "/events"
    )
    run_bounded_command(
        root,
        owner_command(
            "warehouse_bigdata.common.hdfs_namespace",
            "prepare-run",
            "--pipeline-run-id",
            verification_run_id,
            "--events",
        ),
        "prepare-readback",
    )
    run_bounded_command(
        root,
        [
            "bash",
            "scripts/spark-submit.sh",
            "--conf",
            "spark.sql.shuffle.partitions=8",
            "--",
            "warehouse_bigdata/jobs/verify_small_artifacts.py",
            "--through",
            "test",
            "--selector-json",
            json.dumps(selector, separators=(",", ":")),
            "--verification-run-id",
            verification_run_id,
            "--spark-master",
            expected_master,
            "--event-log-dir",
            event_log_dir,
            "--expected-application-name",
            "warehouse-small-readback-" + verification_run_id,
        ],
        "readback",
        timeout=1800,
    )
    lines = [
        line.removeprefix("SMALL_READBACK=")
        for line in (root / "readback.stdout").read_text(encoding="utf-8").splitlines()
        if line.startswith("SMALL_READBACK=")
    ]
    if len(lines) != 1:
        raise ScopeError("fresh readback did not emit one complete SMALL_READBACK result")
    result = json.loads(lines[0])
    execution = result.get("execution", {})
    if (
        result.get("status") != "VALIDATED"
        or result.get("verified_through") != "test"
        or set(execution.get("workers", {})) != {"spark-worker-1", "spark-worker-2"}
    ):
        raise ScopeError("fresh readback did not validate the complete frozen chain")
    return {
        "verification_run_id": verification_run_id,
        "event_log_dir": event_log_dir,
        "report": result,
    }


def _selected_files(client, root, *, limit=4):
    files = []
    pending = [root]
    seen = set()
    while pending and len(files) < limit:
        parent = pending.pop()
        for status in client.list_status(parent):
            path = status["path"]
            if path in seen:
                raise ScopeError("HDFS placement inventory contains a duplicate path")
            seen.add(path)
            if status["type"] == "DIRECTORY":
                pending.append(path)
            elif status["type"] == "FILE" and status.get("length", 0) > 0:
                files.append(path)
                if len(files) == limit:
                    break
    return files


def _hdfs_integrity(client, config, selector):
    """Record live DataNodes and targeted nonempty-file block placement."""
    nodes = client.report()
    addresses = {node.address for node in nodes.values()}
    roots = {
        "j02": render_curated_sales_daily_path(config.base_path, selector["source_snapshot_id"]),
        "j03": render_curated_sales_enriched_path(config.base_path, selector["source_snapshot_id"]),
        "j04": render_forecast_features_path(
            config.base_path, selector["source_snapshot_id"], selector["feature_version"]
        ),
        "j05": render_model_artifact_path(config.base_path, selector["model_run_id"]),
        "j06": render_forecast_output_path(config.base_path, selector["forecast_run_id"]),
        "validation": render_evaluation_output_path(
            config.base_path, selector["validation_evaluation_run_id"]
        ),
        "test": render_evaluation_output_path(config.base_path, selector["test_evaluation_run_id"]),
    }
    files = {stage: _selected_files(client, path) for stage, path in roots.items()}
    if any(not paths for paths in files.values()):
        raise ScopeError("selected HDFS artifact has no nonempty file for placement audit")
    placements = {}
    for stage, paths in files.items():
        placements[stage] = []
        for path in paths:
            blocks = client.fsck(path, addresses)
            placements[stage].append(
                {
                    "path": path,
                    "blocks": [
                        {
                            "block_id": block.block_id,
                            "length": block.length,
                            "locations": list(block.locations),
                        }
                        for block in blocks
                    ],
                }
            )
    return {
        "datanodes": {
            name: {
                "address": node.address,
                "last_contact": node.last_contact,
                "last_block_report": node.last_block_report,
            }
            for name, node in nodes.items()
        },
        "expected_replication": 2,
        "placements": placements,
    }


def _handoff(selector, config, readback, *, hdfs_integrity):
    report = readback["report"]
    return {
        "serving_owner": "J08; no publication ID or approved pointer assigned by M06",
        "dataset_scope": report.get("dataset_scope"),
        "selection_digest": selector["selection_digest"],
        "lineage": {
            key: selector[key]
            for key in (
                "source_version",
                "source_snapshot_id",
                "j03_pipeline_run_id",
                "j04_pipeline_run_id",
                "model_run_id",
                "forecast_run_id",
                "validation_evaluation_run_id",
                "test_evaluation_run_id",
            )
        },
        "forecast": report.get("forecast", {}),
        "metrics": {phase: report.get(phase, {}) for phase in ("validation", "test")},
        "schemas": {
            "forecast": "Contract v1 forecast_daily",
            "metrics": "Contract v1 forecast_metrics",
        },
        "hdfs_integrity": hdfs_integrity,
        "verification_run_id": readback["verification_run_id"],
    }


def run_acceptance(
    *,
    evidence_dir=None,
    selector=None,
    stages=None,
    authorize=False,
    recover=False,
    through="test",
    client=None,
    config=None,
    spark_master=None,
):
    """Verify the frozen chain and optionally execute only absent downstream stages."""
    acceptance_run_id = uuid4().hex
    root = Path(evidence_dir or Path("artifacts/m06-acceptance") / acceptance_run_id)
    if root.exists():
        raise ScopeError("M06 evidence directory must be unique")
    root.mkdir(parents=True)
    report = {
        "acceptance_run_id": acceptance_run_id,
        "status": "NOT_RUN",
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "gates": {},
        "commands": [],
        "artifacts": {},
    }
    try:
        if authorize is not True:
            raise ScopeError(
                "M06 acceptance requires explicit --run-small-acceptance authorization"
            )
        requested = parse_requested_stages(stages)
        selector = load_small_artifacts() if selector is None else selector
        validate_small_artifacts(selector, through=through)
        if through != "test":
            raise ScopeError(
                "M06 acceptance requires validated full-history J03/J04 inputs through test"
            )
        if config is None:
            config = load_hdfs_config()
        if client is None:
            client = create_hdfs_cli_client(config)
        expected_master = spark_master or load_spark_config().master_url
        if not expected_master.startswith("spark://"):
            raise ScopeError("M06 acceptance requires a configured Spark Standalone master")
        report["selector_checksum"] = document_digest(selector)
        report["requested_stages"] = list(requested)
        validate_small_publications(selector, through="j05", client=client, config=config)
        report["gates"]["canonical_upstream"] = True
        all_missing = _missing_requested_outputs(client, config, selector, ALLOWED_EXECUTION_STAGES)
        missing = [stage for stage in all_missing if stage in requested]
        report["missing_requested_stages"] = missing
        unrequested = [stage for stage in all_missing if stage not in requested]
        if unrequested:
            raise ScopeError("missing downstream output was not explicitly authorized")
        if missing:
            report["execution"] = run_live(
                selector,
                stages=missing,
                authorize=True,
                evidence_dir=root / "execution",
                recover=recover,
            )
        report["gates"]["requested_outputs"] = (
            not missing or report["execution"]["status"] == "EXECUTED_PENDING_INDEPENDENT_READBACK"
        )
        report["producer_execution"] = _producer_evidence(
            client, config, selector, expected_master=expected_master
        )
        report["gates"]["producer_execution"] = True
        readback = _run_readback(
            root / "readback", selector, config, expected_master=expected_master
        )
        report["readback"] = readback
        report["gates"]["persisted_readback"] = True
        report["hdfs_integrity"] = _hdfs_integrity(client, config, selector)
        report["gates"]["hdfs_integrity"] = True
        report["handoff"] = _handoff(
            selector, config, readback, hdfs_integrity=report["hdfs_integrity"]
        )
        report["gates"]["immutable_lineage"] = (
            document_digest(selector) == report["selector_checksum"]
        )
        report["status"] = report_status(report["gates"])
        _write(root / "report.json", report)
        return report
    except ScopeError as error:
        report.update(status="BLOCKED", failure=f"{type(error).__name__}: {error}")
        raise
    except BaseException as error:
        report.update(status="FAILED", failure=f"{type(error).__name__}: {error}")
        raise
    finally:
        report["finished_at"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        _write(root / "report.json", report)

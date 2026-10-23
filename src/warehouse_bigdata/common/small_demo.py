"""Explicit serial M5 Small operations; never starts/recreates infrastructure."""

import argparse
import json
import signal
import subprocess
import time
from pathlib import Path
from uuid import uuid4

from warehouse_bigdata.common.small_artifacts import (
    RUN_KEYS,
    STAGES,
    VERIFY_STAGES,
    load_small_artifacts,
    validate_small_artifacts,
)
from warehouse_bigdata.contracts.dataset_scope import ScopeError, validate_scope

JOBS = dict(
    j02="normalize_sales_daily",
    j03="enrich_analytics",
    j04="build_forecast_features",
    j05="train_forecast_model",
    j06="forecast_daily",
    validation="evaluate_forecasts",
    test="evaluate_forecasts",
)
COMPOSE = ["docker", "compose", "-f", "infrastructure/compose.yaml", "--profile", "spark-submit"]
HDFS = "hdfs://hdfs-namenode:8020"
MASTER = "spark://spark-master:7077"


def owner_command(module, *args):
    return [
        *COMPOSE,
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "--pull",
        "never",
        "--user",
        "1000:100",
        "--entrypoint",
        "/opt/warehouse-app/.venv/bin/python",
        "spark-submit",
        "-m",
        module,
        *args,
    ]


def stage_arguments(selector, stage):
    run = selector[RUN_KEYS[stage]]
    arguments = [
        "--source-version",
        selector["source_version"],
        "--source-snapshot-id",
        selector["source_snapshot_id"],
        "--pipeline-run-id",
        run,
        "--dataset-scope",
        "demo_sample",
        "--hdfs-uri",
        HDFS,
        "--spark-master",
        MASTER,
        "--event-log-dir",
        f"{HDFS}/data/warehouse/staging/pipeline_run_id={run}/events",
    ]
    if stage in {"j04", "j05"}:
        arguments += [
            "--upstream-pipeline-run-id",
            selector["j03_pipeline_run_id" if stage == "j04" else "j04_pipeline_run_id"],
            "--feature-version",
            selector["feature_version"],
        ]
    if stage == "j05":
        arguments += [
            "--model-run-id",
            selector["model_run_id"],
            "--training-cap",
            "10000",
            "--sampling-seed",
            "42",
        ]
    if stage in {"j06", "validation", "test"}:
        for key in (
            "j03_pipeline_run_id",
            "j04_pipeline_run_id",
            "feature_version",
            "model_run_id",
        ):
            arguments += ["--" + key.replace("_", "-"), selector[key]]
        if stage == "j06":
            arguments += [
                "--forecast-run-id",
                selector["forecast_run_id"],
                "--issue-date",
                "2016-05-22",
                "--run-type",
                "forward_demo",
            ]
        else:
            arguments += [
                "--evaluation-run-id",
                selector[stage + "_evaluation_run_id"],
                "--run-type",
                stage,
            ]
            if stage == "test":
                arguments += [
                    "--validation-evaluation-run-id",
                    selector["validation_evaluation_run_id"],
                ]
    return arguments


def run_live(selector, *, stages, authorize, evidence_dir, recover=False):
    """Run only explicitly listed stages once. No ingestion or implicit upstream work."""
    if authorize is not True:
        raise ScopeError(
            "live small-demo execution requires explicit --authorize-live authorization"
        )
    if (
        not stages
        or len(set(stages)) != len(stages)
        or any(stage not in STAGES for stage in stages)
    ):
        raise ScopeError("select unique supported stages in chronological order")
    if list(stages) != sorted(stages, key=STAGES.index):
        raise ScopeError("stages must follow the serial J02–test dependency order")
    validate_small_artifacts(selector, through=stages[-1], allow_unresolved=stages[0] == "j02")
    root = Path(evidence_dir)
    root.mkdir(parents=True, exist_ok=False)
    report = {
        "dataset_scope": "demo_sample",
        "selector": selector,
        "status": "RUNNING",
        "stages": {},
    }

    def command(args, stem, timeout=240):
        (root / (stem + ".command.json")).write_text(json.dumps(args, indent=2) + "\n")
        started = time.perf_counter()
        with (
            (root / (stem + ".stdout")).open("wb") as out,
            (root / (stem + ".stderr")).open("wb") as err,
        ):
            process = subprocess.Popen(args, stdout=out, stderr=err)
            try:
                returncode = process.wait(timeout=timeout)
            except BaseException:
                # Let the existing launcher perform its ownership-scoped cleanup.
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=15)
                raise
        elapsed = time.perf_counter() - started
        if returncode:
            raise ScopeError(f"{stem} failed (exit {returncode}); preserve evidence at {root}")
        return elapsed

    try:
        command(["bash", "scripts/spark-cluster.sh", "check"], "cluster")
        command([*COMPOSE, "ps", "--format", "json"], "containers")
        command(["docker", "stats", "--no-stream", "--format", "{{json .}}"], "resources-before")
        for stage in stages:
            run = selector[RUN_KEYS[stage]]
            command(
                owner_command(
                    "warehouse_bigdata.common.small_preflight",
                    "--stage",
                    stage,
                    "--selector-json",
                    json.dumps(selector, separators=(",", ":")),
                ),
                stage + "-preflight",
            )
            if not recover:
                command(
                    owner_command(
                        "warehouse_bigdata.common.hdfs_namespace",
                        "prepare-run",
                        "--pipeline-run-id",
                        run,
                        "--events",
                    ),
                    stage + "-prepare",
                )
            elapsed = command(
                [
                    "bash",
                    "scripts/spark-submit.sh",
                    "--conf",
                    "spark.sql.shuffle.partitions=8",
                    "--",
                    f"warehouse_bigdata/jobs/{JOBS[stage]}.py",
                    *stage_arguments(selector, stage),
                ],
                stage,
                timeout=1800,
            )
            report["stages"][stage] = {"elapsed_seconds": elapsed, "pipeline_run_id": run}
            command(
                owner_command(
                    "warehouse_bigdata.common.small_evidence",
                    "--pipeline-run-id",
                    run,
                    *(
                        ["--source-snapshot-id", selector["source_snapshot_id"]]
                        if stage == "j02"
                        else []
                    ),
                ),
                stage + "-evidence",
            )
            measured = json.loads((root / (stage + "-evidence.stdout")).read_text())
            report["stages"][stage]["execution"] = measured["execution"]
            if stage == "j02":
                scope = validate_scope(
                    measured["scope"],
                    source_version=selector["source_version"],
                    source_snapshot_id=selector["source_snapshot_id"],
                    expected_digest=selector["selection_digest"],
                )
                if selector["selection_digest"] is None:
                    selector["selection_digest"] = scope["selection_digest"]
            (root / "report.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
        command(["docker", "stats", "--no-stream", "--format", "{{json .}}"], "resources-after")
        report["status"] = "EXECUTED_PENDING_INDEPENDENT_READBACK"
    except BaseException as error:
        report.update(status="FAILED", failure=f"{type(error).__name__}: {error}")
        raise
    finally:
        (root / "report.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("init", "run", "verify"))
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--source-version")
    parser.add_argument("--stages", nargs="+", choices=STAGES)
    parser.add_argument("--authorize-live", action="store_true")
    parser.add_argument("--recover", action="store_true")
    parser.add_argument("--evidence-dir", type=Path)
    parser.add_argument("--through", choices=VERIFY_STAGES, default="test")
    args = parser.parse_args()
    if args.action == "init":
        from warehouse_bigdata.common.hdfs_paths import validate_source_version

        validate_source_version(args.source_version)
        token = uuid4().hex
        selector = dict(
            schema_version="small-artifacts-v1",
            dataset_scope="demo_sample",
            source_version=args.source_version,
            source_snapshot_id="small-" + token,
            selection_digest=None,
            feature_version="historical-v1",
            model_run_id="small-rf-" + token,
            forecast_run_id="small-forward-" + token,
            validation_evaluation_run_id="small-validation-" + token,
            test_evaluation_run_id="small-test-" + token,
            **{key: "small-" + stage + "-" + token for stage, key in RUN_KEYS.items()},
        )
        with args.artifacts.open("x", encoding="utf-8") as stream:
            stream.write(json.dumps(selector, sort_keys=True, indent=2) + "\n")
        print(str(args.artifacts))
        return
    selector = load_small_artifacts(
        args.artifacts, allow_unresolved=args.action == "run" and "j02" in (args.stages or [])
    )
    if args.action == "run":
        directory = args.evidence_dir or Path("artifacts/small-demo/" + uuid4().hex)
        try:
            report = run_live(
                selector,
                stages=args.stages,
                authorize=args.authorize_live,
                evidence_dir=directory,
                recover=args.recover,
            )
        finally:
            if selector.get("selection_digest") is not None:
                args.artifacts.write_text(json.dumps(selector, sort_keys=True, indent=2) + "\n")
        print(json.dumps(report, sort_keys=True))
    else:
        if not args.authorize_live:
            parser.error("distributed read-only verification requires --authorize-live")
        validate_small_artifacts(selector, through=args.through)
        command = [
            "bash",
            "scripts/spark-submit.sh",
            "--conf",
            "spark.sql.shuffle.partitions=8",
            "--",
            "warehouse_bigdata/jobs/verify_small_artifacts.py",
            "--through",
            args.through,
            "--selector-json",
            json.dumps(selector, separators=(",", ":")),
        ]
        raise SystemExit(subprocess.run(command, check=False).returncode)


if __name__ == "__main__":
    main()

"""Read-only small-artifact publication/selection gates before starting a Spark driver."""

import argparse
import json
import logging

from warehouse_bigdata.common.config import load_hdfs_config
from warehouse_bigdata.common.hdfs_client import HdfsCliClient
from warehouse_bigdata.common.hdfs_paths import (
    render_curated_category_monthly_sales_path,
    render_curated_sales_daily_path,
    render_curated_sales_enriched_path,
    render_curated_store_daily_sales_path,
    render_evaluation_output_path,
    render_forecast_features_path,
    render_forecast_output_path,
    render_model_artifact_path,
    render_run_manifest_path,
    render_staging_path,
)
from warehouse_bigdata.common.scope_evidence import (
    SCOPED_METADATA_LIMIT,
    read_scope,
    validate_scope_binding,
)
from warehouse_bigdata.common.small_artifacts import STAGES, validate_small_artifacts
from warehouse_bigdata.common.validation_timing import validation_phase
from warehouse_bigdata.contracts.dataset_scope import ScopeError
from warehouse_bigdata.contracts.manifests import PIPELINE_RUN_MANIFEST, VALIDATED
from warehouse_bigdata.contracts.validation import validate_manifest
from warehouse_bigdata.forecasting.feature_input import read_json, require_sealed_paths

_PIPELINE_OUTPUTS = {
    "j02": {"sales_daily": render_curated_sales_daily_path},
    "j03": {
        "sales_enriched": render_curated_sales_enriched_path,
        "store_daily_sales": render_curated_store_daily_sales_path,
        "category_monthly_sales": render_curated_category_monthly_sales_path,
    },
    "j04": {"forecast_features": render_forecast_features_path},
}
_PRODUCERS = {
    "j02": "warehouse-bigdata:normalization-snapshot",
    "j03": "warehouse-bigdata:enrichment-analytics-snapshot-v1",
    "j04": "warehouse-bigdata:feature-snapshot",
}
_UPSTREAM_THROUGH = {
    "j03": "j02",
    "j04": "j03",
    "j05": "j04",
    "j06": "j05",
    "validation": "j05",
    "test": "j05",
}


def _require(document, expected, label):
    for key, value in expected.items():
        if document.get(key) != value:
            raise ScopeError(f"{label} conflicts with selected artifact: {key}")


def _scope(selector, client, path):
    scope = read_scope(
        client,
        path,
        source_version=selector["source_version"],
        source_snapshot_id=selector["source_snapshot_id"],
        expected_digest=selector["selection_digest"],
    )
    _require(scope, {"dataset_scope": "demo_sample"}, "scope")
    return scope


def _pipeline_publication(selector, stage, client, config):
    run = selector[stage + "_pipeline_run_id"]
    marker = render_run_manifest_path(config.base_path, run)
    paths = {
        name: render(
            config.base_path,
            selector["source_snapshot_id"],
            *([selector["feature_version"]] if stage == "j04" else []),
        )
        for name, render in _PIPELINE_OUTPUTS[stage].items()
    }
    scopes = {name: _scope(selector, client, path) for name, path in paths.items()}
    scope = next(iter(scopes.values()))
    if any(value != scope for value in scopes.values()):
        raise ScopeError(stage + " output selections differ")
    if not client.exists(marker):
        raise ScopeError("selected " + stage + " publication manifest is missing: " + marker)
    manifest = read_json(client, marker)
    identity = {
        "source_version": selector["source_version"],
        "source_snapshot_id": selector["source_snapshot_id"],
        "pipeline_run_id": run,
        "status": VALIDATED,
    }
    validate_manifest(
        PIPELINE_RUN_MANIFEST, manifest, lineage_context={**identity, "required_status": VALIDATED}
    )
    validate_scope_binding(manifest, scope)
    _require(
        manifest,
        {"producer": _PRODUCERS[stage], "artifact_uri": config.uri + marker},
        stage + " publication",
    )
    if json.loads(manifest["output_dataset_names"]) != list(paths) or json.loads(
        manifest["output_dataset_uris"]
    ) != [config.uri + path for path in paths.values()]:
        raise ScopeError(stage + " publication output identity conflicts")
    sidecars = {
        path: path + ("/_snapshot_manifest.json" if stage == "j02" else "/_dataset_manifest.json")
        for path in paths.values()
    }
    sealed_paths = [marker]
    for path, sidecar in sidecars.items():
        sealed_paths.extend((sidecar, path + "/_scope.json"))
    with validation_phase(stage + ".permissions"):
        require_sealed_paths(client, sealed_paths, config.base_path)
    coverage = json.loads(manifest["coverage_and_exclusion_counts"])
    for path, sidecar_path in sidecars.items():
        _require(
            read_json(client, sidecar_path),
            {
                **identity,
                "artifact_uri": config.uri + path,
                "pipeline_run_id": coverage.get("producing_pipeline_run_id")
                if stage == "j04"
                else run,
            },
            stage + " dataset publication",
        )
    if stage == "j04":
        from warehouse_bigdata.common.hdfs_publication import validate_feature_publication_evidence

        path = paths["forecast_features"]
        validate_feature_publication_evidence(client, config, manifest, path)
        binding = read_json(
            client, render_staging_path(config.base_path, run) + "/input_binding.json"
        )
        _require(
            binding,
            {
                "upstream_pipeline_run_id": selector["j03_pipeline_run_id"],
                "feature_version": selector["feature_version"],
                "scope": scope,
            },
            "J04 input binding",
        )
    return scope


def _analytical_publication(selector, stage, client, config):
    if stage == "j06":
        from warehouse_bigdata.forecasting.forecast_artifacts import validate_forecast_evidence

        run_id = selector["forecast_run_id"]
        path = render_forecast_output_path(config.base_path, run_id)
        marker_name, binding_name = "scoring_manifest.json", "scoring.json"
        validate = validate_forecast_evidence
        extra = {"forecast_run_id": run_id, "issue_date": "2016-05-22", "run_type": "forward_demo"}
    else:
        from warehouse_bigdata.forecasting.evaluation_artifacts import validate_evaluation_evidence

        run_id = selector[stage + "_evaluation_run_id"]
        path = render_evaluation_output_path(config.base_path, run_id)
        marker_name, binding_name = "evaluation_manifest.json", "evaluation.json"
        validate = validate_evaluation_evidence
        extra = {
            "evaluation_run_id": run_id,
            "run_type": stage,
            "validation_evaluation_run_id": selector["validation_evaluation_run_id"]
            if stage == "test"
            else None,
        }
    scope = _scope(selector, client, path)
    run = selector[stage + "_pipeline_run_id"]
    manifest = read_json(client, path + "/" + marker_name)
    with validation_phase(stage + ".evidence_and_inventory"):
        validate(client, config, manifest, path, pipeline_run_id=run)
    binding = read_json(client, path + "/evidence/" + binding_name)
    _require(
        binding,
        {
            **{
                key: selector[key]
                for key in (
                    "source_version",
                    "source_snapshot_id",
                    "feature_version",
                    "model_run_id",
                    "j03_pipeline_run_id",
                    "j04_pipeline_run_id",
                )
            },
            "pipeline_run_id": run,
            "scope": scope,
            **extra,
        },
        stage + " input binding",
    )
    quality = read_json(client, render_staging_path(config.base_path, run) + "/quality_report.json")
    with validation_phase(stage + ".permissions"):
        require_sealed_paths(
            client,
            (
                path + "/" + relative
                for relative in (
                    marker_name,
                    "evidence/" + binding_name,
                    "evidence/publication.json",
                    *(item["path"] for item in quality["file_integrity"]),
                )
            ),
            config.base_path,
        )
    return manifest, binding


def validate_small_publications(selector, *, through, client, config):
    """Check every selected publication through a stage, without creating SparkContext."""
    validate_small_artifacts(selector, through=through)
    stages = STAGES[: STAGES.index(through) + 1]
    for stage in stages:
        if stage in _PIPELINE_OUTPUTS:
            with validation_phase(stage + ".publication"):
                _pipeline_publication(selector, stage, client, config)
    if "j05" in stages:
        from warehouse_bigdata.forecasting.model_artifacts import read_validated_model_metadata

        path = render_model_artifact_path(config.base_path, selector["model_run_id"])
        with validation_phase("j05.model_metadata"):
            scope = _scope(selector, client, path)
            metadata = read_validated_model_metadata(
                model_run_id=selector["model_run_id"],
                source_snapshot_id=selector["source_snapshot_id"],
                feature_version=selector["feature_version"],
                hdfs_config=config,
                client=client,
            )
        _require(
            metadata["input_binding"],
            {
                **{
                    key: selector[key]
                    for key in (
                        "source_version",
                        "source_snapshot_id",
                        "model_run_id",
                        "feature_version",
                    )
                },
                "pipeline_run_id": selector["j05_pipeline_run_id"],
                "upstream_pipeline_run_id": selector["j04_pipeline_run_id"],
                "scope": scope,
            },
            "J05 input binding",
        )
        if metadata["manifest"]["training_row_count"] > 10000:
            raise ScopeError("selected small model exceeds the training cap")
    evaluations = {}
    for stage in ("j06", "validation", "test"):
        if stage in stages:
            with validation_phase(stage + ".publication"):
                evaluations[stage] = _analytical_publication(selector, stage, client, config)
    if "test" in stages:
        from warehouse_bigdata.common.hdfs_publication import document_digest
        from warehouse_bigdata.forecasting.evaluation import validate_freeze

        manifest, validation_binding = evaluations["validation"]
        _, test_binding = evaluations["test"]
        _require(
            test_binding,
            {"validation_manifest_digest": document_digest(manifest)},
            "test validation freeze",
        )
        validate_freeze(validation_binding["freeze"], test_binding["freeze"])


def preflight_small_stage(selector, stage, *, client, config):
    validate_small_artifacts(selector, through=stage, allow_unresolved=stage == "j02")
    if stage == "j02":
        from warehouse_bigdata.ingestion.hdfs_raw import validate_hdfs_raw_snapshot

        final = render_curated_sales_daily_path(config.base_path, selector["source_snapshot_id"])
        staging = render_staging_path(config.base_path, selector["j02_pipeline_run_id"])
        published_scope = client.exists(final + "/_scope.json")
        marker = render_run_manifest_path(config.base_path, selector["j02_pipeline_run_id"])
        if published_scope and client.exists(marker):
            scope = _scope(selector, client, final)
            # A null local selector may recover its first publication, but is only
            # initialized after completed event and published-scope readback.
            validate_small_publications(
                {**selector, "selection_digest": scope["selection_digest"]},
                through="j02",
                client=client,
                config=config,
            )
        elif (
            published_scope
            or client.exists(staging + "/_scope.json")
            or selector["selection_digest"] is not None
        ):
            scope = _scope(selector, client, final if published_scope else staging)
            request = staging + "/publication/request.json"
            if client.exists(request):
                manifest = read_json(client, request)["manifest"]
                _require(
                    manifest,
                    {
                        "pipeline_run_id": selector["j02_pipeline_run_id"],
                        "source_version": selector["source_version"],
                        "source_snapshot_id": selector["source_snapshot_id"],
                    },
                    "J02 pending publication",
                )
                validate_scope_binding(manifest, scope)
            elif published_scope:
                raise ScopeError("incomplete J02 publication requires its frozen recovery request")
        validate_hdfs_raw_snapshot(selector["source_version"], client=client, config=config)
    else:
        validate_small_publications(
            selector, through=_UPSTREAM_THROUGH[stage], client=client, config=config
        )
        if stage == "test":
            # Test consumes the validation freeze; forward-demo forecasts are
            # required by complete-chain readback, not by this evaluation job.
            _analytical_publication(selector, "validation", client, config)
    return {"status": VALIDATED, "stage": stage, "selection_digest": selector["selection_digest"]}


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selector-json", required=True)
    parser.add_argument("--stage", choices=STAGES, required=True)
    args = parser.parse_args()
    report = preflight_small_stage(
        json.loads(args.selector_json),
        args.stage,
        client=HdfsCliClient(metadata_limit=SCOPED_METADATA_LIMIT),
        config=load_hdfs_config(),
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()

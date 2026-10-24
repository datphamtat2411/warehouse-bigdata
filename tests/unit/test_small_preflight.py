"""Offline publication identity gates used before small-stage submission."""

import copy
import json
from collections import Counter
from pathlib import PurePosixPath

import pytest

from tests.unit.test_hdfs_namespace import CONFIG
from tests.unit.test_hdfs_publication import processing_candidate
from tests.unit.test_m5_small import evidence
from tests.unit.test_small_workflow import complete_selector
from warehouse_bigdata.common.hdfs_paths import render_curated_sales_daily_path, render_staging_path
from warehouse_bigdata.common.hdfs_publication import finalize_publication
from warehouse_bigdata.contracts.dataset_scope import ScopeError


def published_processing():
    client, _ = processing_candidate("j02")
    finalize_publication(client, CONFIG, pipeline_run_id="small-j02")
    processing_candidate("j03", client=client)
    finalize_publication(client, CONFIG, pipeline_run_id="small-j03")
    return client


@pytest.mark.parametrize("stage", ["j03", "j04", "j05", "j06", "validation", "test"])
def test_wrong_selector_digest_fails_before_any_stage(stage):
    from warehouse_bigdata.common.small_preflight import preflight_small_stage

    selector = {**complete_selector(), "selection_digest": "sha256:" + "b" * 64}
    client = published_processing()
    before = copy.deepcopy((client.entries, client.files))
    with pytest.raises(ScopeError, match="digest"):
        preflight_small_stage(selector, stage, client=client, config=CONFIG)
    assert (client.entries, client.files) == before


@pytest.mark.parametrize("stage", ["j03", "j04"])
def test_processing_preflight_requires_selected_validated_sealed_publication(stage):
    from warehouse_bigdata.common.small_preflight import preflight_small_stage

    selector = complete_selector()
    client = published_processing()
    assert (
        preflight_small_stage(selector, stage, client=client, config=CONFIG)["status"]
        == "VALIDATED"
    )
    upstream = "j02" if stage == "j03" else "j03"
    path = CONFIG.base_path + "/manifests/runs/small-" + upstream + ".json"
    manifest = json.loads(client.files[path])
    for field, wrong in (
        ("status", "FAILED"),
        ("pipeline_run_id", "other-run"),
        ("output_dataset_uris", '["hdfs://wrong/output"]'),
    ):
        client.files[path] = json.dumps({**manifest, field: wrong}).encode()
        with pytest.raises((ValueError, RuntimeError)):
            preflight_small_stage(selector, stage, client=client, config=CONFIG)
    client.files[path] = json.dumps(manifest).encode()
    client.entries[path]["permission"] = "777"
    with pytest.raises((ValueError, RuntimeError), match="sealed"):
        preflight_small_stage(selector, stage, client=client, config=CONFIG)
    del client.entries[path]
    with pytest.raises((ValueError, RuntimeError), match="missing|sealed"):
        preflight_small_stage(selector, stage, client=client, config=CONFIG)


@pytest.mark.parametrize("stage", ["j03", "j04", "j05", "j06", "validation", "test"])
def test_unresolved_digest_is_not_accepted_by_downstream_preflight(stage):
    from warehouse_bigdata.common.small_preflight import preflight_small_stage

    with pytest.raises(ScopeError, match="digest"):
        preflight_small_stage(
            {**complete_selector(), "selection_digest": None}, stage, client=None, config=CONFIG
        )


@pytest.mark.parametrize("location", ["published", "pending"])
def test_frozen_j02_preflight_compares_existing_scope_before_raw_or_driver(monkeypatch, location):
    from warehouse_bigdata.common.scope_evidence import write_scope
    from warehouse_bigdata.common.small_preflight import preflight_small_stage
    from warehouse_bigdata.ingestion import hdfs_raw

    client = published_processing()
    if location == "pending":
        final = render_curated_sales_daily_path(CONFIG.base_path, "small-snapshot")
        client.remove(final)
        client.remove(CONFIG.base_path + "/manifests/runs/small-j02.json")
        write_scope(client, render_staging_path(CONFIG.base_path, "small-j02"), evidence())
    calls = []
    monkeypatch.setattr(
        hdfs_raw, "validate_hdfs_raw_snapshot", lambda *args, **kwargs: calls.append(args)
    )
    selector = {**complete_selector(), "selection_digest": "sha256:" + "b" * 64}
    with pytest.raises(ScopeError, match="digest"):
        preflight_small_stage(selector, "j02", client=client, config=CONFIG)
    assert calls == []


def test_new_j02_preflight_validates_raw_without_initializing_digest(monkeypatch):
    from tests.unit.test_hdfs_namespace import PermissionHdfs
    from warehouse_bigdata.common.small_preflight import preflight_small_stage
    from warehouse_bigdata.ingestion import hdfs_raw

    selector = {**complete_selector(), "selection_digest": None}
    calls = []
    monkeypatch.setattr(
        hdfs_raw, "validate_hdfs_raw_snapshot", lambda *args, **kwargs: calls.append(args)
    )
    result = preflight_small_stage(selector, "j02", client=PermissionHdfs(), config=CONFIG)
    assert result["status"] == "VALIDATED"
    assert calls == [(selector["source_version"],)]
    assert selector["selection_digest"] is None


@pytest.mark.parametrize("stage,through", [("j05", "j04"), ("j06", "j05"), ("validation", "j05")])
def test_ml_preflight_checks_required_upstream_frontier(monkeypatch, stage, through):
    from warehouse_bigdata.common import small_preflight

    calls = []
    monkeypatch.setattr(
        small_preflight,
        "validate_small_publications",
        lambda selector, **kwargs: calls.append(kwargs),
    )
    small_preflight.preflight_small_stage(complete_selector(), stage, client=None, config=CONFIG)
    assert [call["through"] for call in calls] == [through]


@pytest.mark.parametrize("digest", [None, evidence()["selection_digest"]])
def test_j02_preflight_allows_recovery_after_final_rename_before_manifest(monkeypatch, digest):
    from warehouse_bigdata.common.small_preflight import preflight_small_stage
    from warehouse_bigdata.ingestion import hdfs_raw

    client, _ = processing_candidate("j02")
    rename = client.rename_exclusive
    marker = CONFIG.base_path + "/manifests/runs/small-j02.json"

    def interrupted(source, destination):
        if destination == marker:
            raise OSError("completion interrupted")
        rename(source, destination)

    client.rename_exclusive = interrupted
    with pytest.raises(OSError, match="interrupted"):
        finalize_publication(client, CONFIG, pipeline_run_id="small-j02")
    monkeypatch.setattr(hdfs_raw, "validate_hdfs_raw_snapshot", lambda *args, **kwargs: None)
    selector = {**complete_selector(), "selection_digest": digest}
    assert (
        preflight_small_stage(selector, "j02", client=client, config=CONFIG)["status"]
        == "VALIDATED"
    )
    assert selector["selection_digest"] == digest
    client.rename_exclusive = rename
    assert finalize_publication(client, CONFIG, pipeline_run_id="small-j02") == "VALIDATED"


@pytest.mark.parametrize(
    "field",
    [
        "source_version",
        "source_snapshot_id",
        "model_run_id",
        "feature_version",
        "pipeline_run_id",
        "upstream_pipeline_run_id",
        "scope",
    ],
)
def test_model_preflight_binds_verified_metadata_to_the_selector(monkeypatch, field):
    from types import SimpleNamespace

    from warehouse_bigdata.common import small_preflight
    from warehouse_bigdata.forecasting import model_artifacts

    selector = complete_selector()
    scope = evidence()
    client = SimpleNamespace(
        exists=lambda path: True, read_remote=lambda path: json.dumps(scope).encode()
    )
    binding = {
        key: selector[key]
        for key in ("source_version", "source_snapshot_id", "model_run_id", "feature_version")
    }
    binding.update(
        pipeline_run_id=selector["j05_pipeline_run_id"],
        upstream_pipeline_run_id=selector["j04_pipeline_run_id"],
        scope=scope,
    )
    metadata = {"input_binding": binding, "manifest": {"training_row_count": 10000}}
    monkeypatch.setattr(small_preflight, "_pipeline_publication", lambda *args: scope)
    monkeypatch.setattr(model_artifacts, "read_validated_model_metadata", lambda **kwargs: metadata)
    small_preflight.preflight_small_stage(selector, "j06", client=client, config=CONFIG)
    binding[field] = "conflict"
    with pytest.raises(ScopeError, match=field):
        small_preflight.preflight_small_stage(selector, "j06", client=client, config=CONFIG)


@pytest.mark.parametrize("stage", ["j06", "validation"])
@pytest.mark.parametrize(
    "field", ["model_run_id", "feature_version", "j03_pipeline_run_id", "j04_pipeline_run_id"]
)
def test_analytical_readback_requires_the_selected_ml_lineage(stage, field):
    from tests.unit.test_evaluation_artifacts import staged_evaluation
    from tests.unit.test_forecast_artifacts import staged_forecast
    from warehouse_bigdata.common.small_preflight import _analytical_publication

    selector = complete_selector()
    selector.update(
        source_snapshot_id="snapshot",
        selection_digest=evidence("snapshot")["selection_digest"],
        model_run_id="model",
        feature_version="v1",
        j03_pipeline_run_id="j03",
        j04_pipeline_run_id="j04",
    )
    if stage == "j06":
        client, _, _ = staged_forecast(issue_date="2016-05-22", run_type="forward_demo")
        run = "j06"
        selector.update(forecast_run_id="forecast", j06_pipeline_run_id=run)
    else:
        client, _, _, _ = staged_evaluation()
        run = "execution"
        selector.update(validation_evaluation_run_id="eval", validation_pipeline_run_id=run)
    finalize_publication(client, CONFIG, pipeline_run_id=run)
    _analytical_publication(selector, stage, client, CONFIG)
    selector[field] = "conflict"
    with pytest.raises(ScopeError, match=field):
        _analytical_publication(selector, stage, client, CONFIG)


def test_test_preflight_requires_validation_freeze_without_unrelated_forward_output(monkeypatch):
    from tests.unit.test_evaluation_artifacts import staged_evaluation
    from warehouse_bigdata.common import small_preflight
    from warehouse_bigdata.common.hdfs_paths import render_model_artifact_path
    from warehouse_bigdata.common.scope_evidence import write_scope
    from warehouse_bigdata.forecasting import model_artifacts

    client, _, _, binding = staged_evaluation()
    finalize_publication(client, CONFIG, pipeline_run_id="execution")
    selector = {
        **complete_selector(),
        **{
            key: binding[key]
            for key in (
                "source_version",
                "source_snapshot_id",
                "model_run_id",
                "feature_version",
                "j03_pipeline_run_id",
                "j04_pipeline_run_id",
            )
        },
        "selection_digest": binding["scope"]["selection_digest"],
        "validation_evaluation_run_id": "eval",
        "validation_pipeline_run_id": "execution",
    }
    model_path = render_model_artifact_path(CONFIG.base_path, selector["model_run_id"])
    client.mkdir_exclusive(model_path)
    write_scope(client, model_path, binding["scope"])
    metadata = {
        "manifest": {"training_row_count": 10000},
        "input_binding": {
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
            "scope": binding["scope"],
        },
    }
    monkeypatch.setattr(small_preflight, "_pipeline_publication", lambda *args: binding["scope"])
    monkeypatch.setattr(model_artifacts, "read_validated_model_metadata", lambda **kwargs: metadata)
    assert (
        small_preflight.preflight_small_stage(selector, "test", client=client, config=CONFIG)[
            "status"
        ]
        == "VALIDATED"
    )
    selector["validation_evaluation_run_id"] = "missing-freeze"
    with pytest.raises(ScopeError, match="missing"):
        small_preflight.preflight_small_stage(selector, "test", client=client, config=CONFIG)


def test_publication_timing_reports_failed_phase_without_swallowing_error(monkeypatch, caplog):
    import logging

    from warehouse_bigdata.common import small_preflight

    def rejected(*args):
        raise ScopeError("publication rejected")

    monkeypatch.setattr(small_preflight, "_pipeline_publication", rejected)
    with caplog.at_level(logging.INFO), pytest.raises(ScopeError, match="rejected"):
        small_preflight.preflight_small_stage(
            complete_selector(), "j06", client=None, config=CONFIG
        )
    assert "validation_phase=j02.publication status=STARTED" in caplog.text
    assert "validation_phase=j02.publication status=FAILED elapsed_seconds=" in caplog.text


def expected_permission_paths(paths):
    expected = set()
    for path in paths:
        for current in (PurePosixPath(path), *PurePosixPath(path).parents):
            expected.add(str(current))
            if str(current) == CONFIG.base_path:
                break
    return Counter(dict.fromkeys(expected, 1))


def count_acls(client, monkeypatch):
    calls = Counter()
    original = client.get_acl

    def counted(path):
        calls[path] += 1
        return original(path)

    monkeypatch.setattr(client, "get_acl", counted)
    return calls


@pytest.mark.parametrize("stage", ["j02", "j03"])
def test_pipeline_batches_all_required_permission_paths_per_publication(monkeypatch, stage):
    from warehouse_bigdata.common.small_preflight import _PIPELINE_OUTPUTS, _pipeline_publication

    client = published_processing()
    selector = complete_selector()
    marker = CONFIG.base_path + "/manifests/runs/" + selector[stage + "_pipeline_run_id"] + ".json"
    paths = [marker]
    sidecar = "/_snapshot_manifest.json" if stage == "j02" else "/_dataset_manifest.json"
    for render in _PIPELINE_OUTPUTS[stage].values():
        root = render(CONFIG.base_path, selector["source_snapshot_id"])
        paths.extend([root + sidecar, root + "/_scope.json"])
    calls = count_acls(client, monkeypatch)
    _pipeline_publication(selector, stage, client, CONFIG)
    assert calls == expected_permission_paths(paths)
    _pipeline_publication(selector, stage, client, CONFIG)
    assert calls == Counter({p: 2 for p in expected_permission_paths(paths)})


@pytest.mark.parametrize("stage", ["j06", "validation"])
def test_analytical_batches_inventory_and_evidence_without_skipping_duplicate_paths(
    monkeypatch, stage
):
    from tests.unit.test_evaluation_artifacts import staged_evaluation
    from tests.unit.test_forecast_artifacts import staged_forecast
    from warehouse_bigdata.common.small_preflight import _analytical_publication

    if stage == "j06":
        client, _, manifest = staged_forecast(issue_date="2016-05-22", run_type="forward_demo")
        run, run_id, marker, binding = "j06", "forecast", "scoring_manifest.json", "scoring.json"
        selector_keys = {"forecast_run_id": run_id, "j06_pipeline_run_id": run}
    else:
        client, _, manifest, _ = staged_evaluation()
        run, run_id = "execution", "eval"
        marker, binding = "evaluation_manifest.json", "evaluation.json"
        selector_keys = {"validation_evaluation_run_id": run_id, "validation_pipeline_run_id": run}
    finalize_publication(client, CONFIG, pipeline_run_id=run)
    quality = json.loads(
        client.files[render_staging_path(CONFIG.base_path, run) + "/quality_report.json"]
    )
    root = manifest["artifact_uri"].removeprefix(CONFIG.uri)
    paths = [
        root + "/" + relative
        for relative in (
            marker,
            "evidence/" + binding,
            "evidence/publication.json",
            *(item["path"] for item in quality["file_integrity"]),
        )
    ]
    selector = {
        **complete_selector(),
        **selector_keys,
        "source_snapshot_id": "snapshot",
        "selection_digest": evidence("snapshot")["selection_digest"],
        "model_run_id": "model",
        "feature_version": "v1",
        "j03_pipeline_run_id": "j03",
        "j04_pipeline_run_id": "j04",
    }
    calls = count_acls(client, monkeypatch)
    _analytical_publication(selector, stage, client, CONFIG)
    assert calls == expected_permission_paths(paths)
    assert len(paths) > len(set(paths))  # The binding is also an inventory entry.
    client.entries[CONFIG.base_path]["owner"] = "spark"
    with pytest.raises(ValueError, match="sealed"):
        _analytical_publication(selector, stage, client, CONFIG)

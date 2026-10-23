"""One explicit local selector for immutable canonical M5 Small artifacts."""

import json
import os
import re
from pathlib import Path

from warehouse_bigdata.common.hdfs_paths import (
    validate_feature_version,
    validate_pipeline_run_id,
    validate_source_snapshot_id,
    validate_source_version,
)
from warehouse_bigdata.contracts.dataset_scope import ScopeError

STAGES = ("j02", "j03", "j04", "j05", "j06", "validation", "test")
VERIFY_STAGES = STAGES[1:]
RUN_KEYS = {stage: f"{stage}_pipeline_run_id" for stage in STAGES}
STAGE_KEYS = {
    "j02": ("j02_pipeline_run_id",),
    "j03": ("j03_pipeline_run_id",),
    "j04": ("j04_pipeline_run_id", "feature_version"),
    "j05": ("j05_pipeline_run_id", "model_run_id"),
    "j06": ("j06_pipeline_run_id", "forecast_run_id"),
    "validation": ("validation_pipeline_run_id", "validation_evaluation_run_id"),
    "test": ("test_pipeline_run_id", "test_evaluation_run_id"),
}


def load_small_artifacts(path=None, *, allow_unresolved=False):
    value = path or os.environ.get("WAREHOUSE_SMALL_ARTIFACTS")
    if not value:
        raise ScopeError(
            "set WAREHOUSE_SMALL_ARTIFACTS to the local canonical small-artifacts JSON file"
        )
    document = json.loads(Path(value).read_text(encoding="utf-8"))
    return validate_small_artifacts(document, allow_unresolved=allow_unresolved)


def validate_small_artifacts(document, *, through=None, allow_unresolved=False):
    """Validate a selector and require every identity up to an explicit completion stage."""
    required = {
        "schema_version",
        "dataset_scope",
        "source_version",
        "source_snapshot_id",
        "selection_digest",
        "j02_pipeline_run_id",
        "j03_pipeline_run_id",
    }
    if through is not None:
        if through not in STAGES:
            raise ScopeError("unsupported small-artifact completion stage: " + str(through))
        required.update(
            key for stage in STAGES[: STAGES.index(through) + 1] for key in STAGE_KEYS[stage]
        )
    if not isinstance(document, dict):
        raise ScopeError("canonical selector must be a JSON object")
    missing = sorted(required - document.keys())
    if missing:
        raise ScopeError(
            f"canonical selector missing required artifacts through {through or 'j03'}: {', '.join(missing)}"
        )
    if (
        document["schema_version"] != "small-artifacts-v1"
        or document["dataset_scope"] != "demo_sample"
    ):
        raise ScopeError("canonical selector requires small-artifacts-v1 and demo_sample")
    validate_source_version(document["source_version"])
    validate_source_snapshot_id(document["source_snapshot_id"])
    if document["source_snapshot_id"].startswith("m04-"):
        raise ScopeError("legacy M04 snapshot identities cannot select M5 Small")
    digest = document["selection_digest"]
    if not (allow_unresolved and digest is None) and not (
        isinstance(digest, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", digest)
    ):
        raise ScopeError("canonical selector requires the frozen selection_digest")
    for key, value in document.items():
        if key.endswith("_run_id"):
            validate_pipeline_run_id(value)
    if "feature_version" in document:
        validate_feature_version(document["feature_version"])
    if ("feature_version" in document) != ("j04_pipeline_run_id" in document):
        raise ScopeError("feature version and J04 run must be selected together")
    return document

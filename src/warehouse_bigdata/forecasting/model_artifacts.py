"""Internal model-layout inventory, Contract v1 manifest, and owner-only promotion.

The artifact checksum is SHA-256 of canonical JSON ``{files: inventory}`` plus
newline. Inventory entries are sorted relative paths, byte sizes and fresh HDFS
checksums. model_manifest.json and evidence/publication.json are excluded to avoid
self-reference; their canonical digests are bound by the frozen publication request.
The final sealed model_manifest.json is the sole model completion marker.
"""

from __future__ import annotations

import json
import re
from pathlib import PurePosixPath

from warehouse_bigdata.common.hdfs_namespace import (
    HdfsNamespaceError,
    require_owner,
    validate_publication_namespace,
)
from warehouse_bigdata.common.hdfs_paths import (
    render_model_artifact_path,
    render_staging_path,
    validate_feature_version,
    validate_model_run_id,
    validate_pipeline_run_id,
    validate_source_snapshot_id,
)
from warehouse_bigdata.common.hdfs_publication import (
    _envelope,
    _freeze_feature_metadata,
    _json,
    _require_private,
    _seal,
    document_digest,
)
from warehouse_bigdata.common.scope_evidence import bind_scope, read_scope, validate_scope_binding
from warehouse_bigdata.common.validation_timing import validation_phase
from warehouse_bigdata.contracts.manifests import MODEL_MANIFEST, VALIDATED
from warehouse_bigdata.contracts.validation import validate_manifest
from warehouse_bigdata.forecasting.feature_input import require_sealed, require_sealed_paths

_EXCLUDED = {"model_manifest.json", "evidence/publication.json"}
_SIDECARS = {"feature_schema.json", "evidence/training.json", "_scope.json"}
_STAGE = re.compile(r"[0-9]+_[A-Za-z0-9_]+")


def artifact_uri(config, path):
    return config.uri + path


def _layout(relative, directory):
    parts = PurePosixPath(relative).parts
    if len(parts) > 9:
        return False
    if relative in _EXCLUDED | _SIDECARS:
        return not directory
    if relative == "evidence":
        return directory
    if parts[0] == "training_selection_manifest":
        return (len(parts) == 1 and directory) or (
            len(parts) == 2
            and not directory
            and (
                parts[-1] == "_SUCCESS"
                or (parts[-1].startswith("part-") and parts[-1].endswith(".parquet"))
            )
        )
    if parts[0] != "spark_pipeline_model":
        return False
    # Native Spark pipelines, indexers/encoders/imputer and RF stage/tree data only.
    if len(parts) == 1:
        return directory
    parents = parts[1:] if directory else parts[1:-1]
    for index, part in enumerate(parents):
        if part not in {"metadata", "stages", "data", "treesMetadata"}:
            if index == 0 or parents[index - 1] != "stages" or not _STAGE.fullmatch(part):
                return False
    return directory or (parts[-1] == "_SUCCESS" or parts[-1].startswith("part-"))


def model_file_integrity(client, root):
    """Bounded recursive ML inventory; J04's Parquet-only inventory is unchanged."""
    pending, seen, inventory = [root], set(), []
    while pending:
        parent = pending.pop()
        for status in client.list_status(parent):
            path = status["path"]
            if (
                str(PurePosixPath(path).parent) != parent
                or str(PurePosixPath(path)) != path
                or path in seen
                or PurePosixPath(path).name in {".", ".."}
            ):
                raise HdfsNamespaceError("model inventory contains a duplicate/non-child path")
            seen.add(path)
            if len(seen) > 4096:
                raise HdfsNamespaceError("model inventory exceeds bounded layout (4096)")
            relative = str(PurePosixPath(path).relative_to(root))
            directory = status["type"] == "DIRECTORY"
            if status["type"] not in {"DIRECTORY", "FILE"} or not _layout(relative, directory):
                raise HdfsNamespaceError("unexpected model layout: " + relative)
            if directory:
                pending.append(path)
            elif relative not in _EXCLUDED:
                if type(status["length"]) is not int or status["length"] < 0:
                    raise HdfsNamespaceError("invalid model file size")
                inventory.append({"path": relative, "size_bytes": status["length"]})
    names = {item["path"] for item in inventory}
    if (
        not any(n.startswith("spark_pipeline_model/metadata/part-") for n in names)
        or not any(
            n.startswith("training_selection_manifest/part-") and n.endswith(".parquet")
            for n in names
        )
        or not _SIDECARS.issubset(names)
    ):
        raise HdfsNamespaceError("incomplete model bundle layout")
    inventory.sort(key=lambda item: item["path"])
    paths = [root + "/" + item["path"] for item in inventory]
    with validation_phase("j05.model_checksums"):
        checksums = client.checksums(paths)
    if set(checksums) != set(paths) or any(not c for c in checksums.values()):
        raise HdfsNamespaceError("model integrity checksums incomplete")
    for item, path in zip(inventory, paths, strict=True):
        item["checksum"] = checksums[path]
    return inventory


def make_model_manifest(config, *, binding, quality, summary, parameters, created_at, completed_at):
    final = render_model_artifact_path(config.base_path, binding["model_run_id"])
    checksum = document_digest({"files": quality["file_integrity"]})
    fingerprint = quality["schema_fingerprint"]
    coverage = {
        "input_binding_digest": document_digest(binding),
        "publication_evidence_digest": document_digest(quality),
        "inventory_digest_definition": "sha256 of canonical JSON {files: sorted relative paths, sizes, HDFS checksums} plus newline; excludes model_manifest.json and evidence/publication.json",
        "source_version": binding["source_version"],
        "selection": summary,
        "recursive_horizon_days": 7,
        "publication_complete": True,
    }
    manifest = dict(
        manifest_schema_version=MODEL_MANIFEST.version,
        contract_name=MODEL_MANIFEST.name,
        contract_version=MODEL_MANIFEST.version,
        manifest_kind=MODEL_MANIFEST.manifest_kind,
        status=VALIDATED,
        producer="warehouse-bigdata:model-training",
        pipeline_run_id=binding["pipeline_run_id"],
        model_run_id=binding["model_run_id"],
        source_snapshot_id=binding["source_snapshot_id"],
        feature_version=binding["feature_version"],
        artifact_uri=config.uri + final,
        artifact_checksum=checksum,
        schema_fingerprint=fingerprint,
        grain="one global model per model_run_id",
        logical_unique_key="model_run_id",
        row_count=summary["selected_count"],
        required_field_null_counts="{}",
        code_revision=binding["code_revision"],
        configuration_digest=binding["configuration_digest"],
        created_at=created_at,
        completed_at=completed_at,
        coverage_and_exclusion_counts=json.dumps(coverage, sort_keys=True, separators=(",", ":")),
        algorithm="RandomForestRegressor",
        training_start="2011-02-26",
        training_end="2016-03-27",
        validation_start="2016-03-28",
        validation_end="2016-04-24",
        test_start="2016-04-25",
        test_end="2016-05-22",
        forecast_strategy="recursive_7_day",
        model_parameters=json.dumps(parameters, sort_keys=True, separators=(",", ":")),
        seed=42,
        training_row_count=summary["selected_count"],
        training_series_count=summary["selected_series_count"],
        model_artifact_uri=config.uri + final + "/spark_pipeline_model",
        model_artifact_checksum=checksum,
        model_artifact_schema_fingerprint=fingerprint,
    )
    validate_model_request(config, manifest)
    return bind_scope(manifest, binding["scope"])


def validate_model_request(config, manifest):
    validate_manifest(MODEL_MANIFEST, manifest, lineage_context={"required_status": VALIDATED})
    validate_pipeline_run_id(manifest.get("pipeline_run_id"))
    final = render_model_artifact_path(config.base_path, manifest["model_run_id"])
    if (
        manifest["artifact_uri"] != config.uri + final
        or manifest["model_artifact_uri"] != config.uri + final + "/spark_pipeline_model"
        or manifest["artifact_checksum"] != manifest["model_artifact_checksum"]
        or manifest["schema_fingerprint"] != manifest["model_artifact_schema_fingerprint"]
        or manifest["row_count"] != manifest["training_row_count"]
    ):
        raise HdfsNamespaceError("model request paths, counts or integrity fields conflict")
    return [
        (
            render_staging_path(config.base_path, manifest["pipeline_run_id"])
            + "/candidates/model",
            final,
            "model_manifest.json",
        )
    ]


def validate_model_evidence(client, config, manifest, path):
    """Recheck frozen binding and independent fresh inventory on every mutation boundary."""
    validate_model_request(config, manifest)
    staging = render_staging_path(config.base_path, manifest["pipeline_run_id"])
    coverage = json.loads(manifest["coverage_and_exclusion_counts"])
    binding = _json(client, staging + "/input_binding.json")
    scope = read_scope(client, path)
    validate_scope_binding(manifest, scope)
    if binding.get("scope") != scope:
        raise HdfsNamespaceError("model scope differs from frozen training input")
    quality = _json(client, staging + "/quality_report.json")
    if (
        _json(client, path + "/model_manifest.json") != manifest
        or _json(client, path + "/evidence/publication.json") != quality
        or document_digest(binding) != coverage.get("input_binding_digest")
        or document_digest(quality) != coverage.get("publication_evidence_digest")
    ):
        raise HdfsNamespaceError("frozen model evidence digest conflicts with request")
    if (
        binding.get("schema_version") != "model-training-binding-v1"
        or quality.get("input_binding") != binding
        or quality.get("status") != VALIDATED
        or quality.get("reload_verified") is not True
        or coverage.get("publication_complete") is not True
    ):
        raise HdfsNamespaceError("model persisted/reload evidence is not VALIDATED")
    for name in (
        "pipeline_run_id",
        "model_run_id",
        "source_snapshot_id",
        "feature_version",
        "configuration_digest",
        "code_revision",
    ):
        if binding.get(name) != manifest.get(name):
            raise HdfsNamespaceError("model frozen lineage/configuration conflicts: " + name)
    if (
        binding.get("source_version") != coverage.get("source_version")
        or document_digest(binding.get("effective_configuration"))
        != binding["configuration_digest"]
    ):
        raise HdfsNamespaceError("model frozen configuration digest conflicts")
    inventory = model_file_integrity(client, path)
    if (
        inventory != quality.get("file_integrity")
        or document_digest({"files": inventory}) != manifest["model_artifact_checksum"]
        or quality.get("schema_fingerprint") != manifest["schema_fingerprint"]
    ):
        raise HdfsNamespaceError("model file integrity/schema conflicts with persisted validation")
    return inventory


def finalize_model_publication(client, config, *, pipeline_run_id, manifest):
    """Owner claim/seal/exclusive complete-bundle rename, recoverable without refitting."""
    require_owner(client)
    validate_publication_namespace(client, config)
    if manifest.get("pipeline_run_id") != pipeline_run_id:
        raise HdfsNamespaceError("model publication execution mismatch")
    candidate, final, _ = validate_model_request(config, manifest)[0]
    envelope = _envelope(final, pipeline_run_id)
    claimed = envelope + "/candidate"

    def exists(path):
        return client.path_status(path) is not None

    if exists(final):
        if exists(candidate) or exists(claimed):
            raise HdfsNamespaceError("candidate and immutable model both exist")
        require_sealed(client, final + "/model_manifest.json", config.base_path)
        validate_model_evidence(client, config, manifest, final)
        require_sealed(client, final + "/evidence/publication.json", config.base_path)
        for item in model_file_integrity(client, final):
            require_sealed(client, final + "/" + item["path"], config.base_path)
        return "REUSED"
    if exists(candidate) and exists(claimed):
        raise HdfsNamespaceError("concurrent candidate and model publication claim")
    source = claimed if exists(claimed) else candidate
    if (client.path_status(source) or {}).get("type") != "DIRECTORY":
        raise HdfsNamespaceError("complete model candidate is missing")
    validate_model_evidence(client, config, manifest, source)
    staging = render_staging_path(config.base_path, pipeline_run_id)
    _freeze_feature_metadata(client, staging)
    _require_private(client, envelope)
    if not exists(claimed):
        client.rename_exclusive(candidate, claimed)
    _seal(client, claimed)
    validate_model_evidence(client, config, manifest, claimed)
    _seal(client, staging)
    client.rename_exclusive(claimed, final)
    client.remove(envelope)
    return "VALIDATED"


def read_validated_model_metadata(
    *, model_run_id, source_snapshot_id, feature_version, hdfs_config, client
):
    """Load only a completed, sealed and integrity-checked model with matching lineage."""
    from warehouse_bigdata.forecasting.training import NUMERIC_FEATURES, PREDICTORS

    validate_model_run_id(model_run_id)
    validate_source_snapshot_id(source_snapshot_id)
    validate_feature_version(feature_version)
    path = render_model_artifact_path(hdfs_config.base_path, model_run_id)
    require_sealed(client, path + "/model_manifest.json", hdfs_config.base_path)
    manifest = _json(client, path + "/model_manifest.json")
    for key, value in dict(
        model_run_id=model_run_id,
        source_snapshot_id=source_snapshot_id,
        feature_version=feature_version,
    ).items():
        if manifest.get(key) != value:
            raise HdfsNamespaceError("selected model lineage conflicts: " + key)
    with validation_phase("j05.model_evidence_and_inventory"):
        inventory = validate_model_evidence(client, hdfs_config, manifest, path)
    schema = _json(client, path + "/feature_schema.json")
    if (
        schema.get("schema_version") != "model-feature-schema-v1"
        or schema.get("predictor_order") != list(PREDICTORS)
        or schema.get("normalized_predictors")
        != [{"name": n, "type": "double"} for n in NUMERIC_FEATURES]
        or document_digest(schema) != manifest["schema_fingerprint"]
    ):
        raise HdfsNamespaceError("unsupported model input schema")
    with validation_phase("j05.model_permissions_and_acls"):
        require_sealed_paths(
            client, (path + "/" + item["path"] for item in inventory), hdfs_config.base_path
        )
    binding = _json(client, path + "/evidence/training.json")["input_binding"]
    frozen = _json(
        client,
        render_staging_path(hdfs_config.base_path, manifest["pipeline_run_id"])
        + "/input_binding.json",
    )
    if binding != frozen:
        raise HdfsNamespaceError("model training evidence input binding conflicts")
    return {
        "manifest": manifest,
        "feature_schema": schema,
        "input_binding": binding,
        "artifact_path": path,
    }


def validate_scoring_pipeline(model):
    """Require the supported fitted T01 stages, input order and unknown policy."""
    from pyspark.ml.feature import (
        ImputerModel,
        OneHotEncoderModel,
        SQLTransformer,
        StringIndexerModel,
        VectorAssembler,
    )
    from pyspark.ml.regression import RandomForestRegressionModel

    from warehouse_bigdata.forecasting.training import NUMERIC_FEATURES, PREDICTORS

    expected = (
        SQLTransformer,
        StringIndexerModel,
        StringIndexerModel,
        OneHotEncoderModel,
        ImputerModel,
        VectorAssembler,
        RandomForestRegressionModel,
    )
    stages = model.stages
    aliases = ", ".join(f"CAST(`{n}` AS DOUBLE) AS `_rf_{n}`" for n in NUMERIC_FEATURES)
    if len(stages) != 7 or any(
        not isinstance(s, cls) for s, cls in zip(stages, expected, strict=True)
    ):
        raise HdfsNamespaceError("unsupported fitted T01 pipeline stages")
    if (
        stages[0].getStatement() != "SELECT *, " + aliases + " FROM __THIS__"
        or stages[1].getInputCol() != "store_id"
        or stages[1].getOutputCol() != "_rf_store_index"
        or stages[2].getInputCol() != "dept_id"
        or stages[2].getOutputCol() != "_rf_dept_index"
        or any(stages[i].getHandleInvalid() != "keep" for i in (1, 2, 3))
        or stages[3].getInputCols() != ["_rf_store_index", "_rf_dept_index"]
        or stages[3].getOutputCols() != ["_rf_store_vector", "_rf_dept_vector"]
        or stages[3].getDropLast()
        or stages[4].getInputCols() != ["last_known_price"]
        or stages[4].getOutputCols() != ["_rf_price"]
        or stages[4].getStrategy() != "median"
        or stages[5].getInputCols() != list(PREDICTORS)
        or stages[5].getOutputCol() != "features"
        or stages[5].getHandleInvalid() != "error"
        or stages[6].getFeaturesCol() != "features"
        or stages[6].getPredictionCol() != "raw_prediction"
    ):
        raise HdfsNamespaceError("unsupported fitted T01 preprocessing inputs/order/output")


def load_validated_model(
    spark, *, model_run_id, source_snapshot_id, feature_version, hdfs_config, client
):
    from pyspark.ml import PipelineModel

    metadata = read_validated_model_metadata(
        model_run_id=model_run_id,
        source_snapshot_id=source_snapshot_id,
        feature_version=feature_version,
        hdfs_config=hdfs_config,
        client=client,
    )
    model = PipelineModel.load(
        artifact_uri(hdfs_config, metadata["artifact_path"] + "/spark_pipeline_model")
    )
    validate_scoring_pipeline(model)
    return model

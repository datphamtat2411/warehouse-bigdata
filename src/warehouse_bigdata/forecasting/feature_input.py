"""Public, explicit J04 consumer gates without rebuilding features or reading sales."""

from __future__ import annotations

import hashlib
import json
from functools import reduce
from operator import or_
from pathlib import PurePosixPath

from warehouse_bigdata.common.hdfs_paths import (
    render_forecast_features_path,
    render_raw_m5_path,
    render_run_manifest_path,
    render_staging_path,
    validate_feature_version,
    validate_pipeline_run_id,
    validate_source_snapshot_id,
    validate_source_version,
)
from warehouse_bigdata.common.hdfs_publication import (
    document_digest,
    validate_feature_publication_evidence,
)
from warehouse_bigdata.common.scope_evidence import read_scope, validate_scope_binding
from warehouse_bigdata.contracts.datasets import FORECAST_FEATURES
from warehouse_bigdata.contracts.manifests import PIPELINE_RUN_MANIFEST, SOURCE_MANIFEST, VALIDATED
from warehouse_bigdata.contracts.validation import validate_manifest
from warehouse_bigdata.forecasting.feature_validation import (
    FEATURE_TYPES,
    SCOPED_KEY,
    typed_feature_schema,
)
from warehouse_bigdata.forecasting.features import TEST_DAYS, TRAIN_DAYS, VALIDATION_DAYS
from warehouse_bigdata.processing.m5_selection import validate_scope_population


class FeatureInputError(ValueError):
    """A selected feature artifact does not satisfy the supported J04 contract."""


def read_json(client, path):
    document = json.loads(client.read_remote(path))
    if not isinstance(document, dict):
        raise FeatureInputError("evidence must be a JSON object: " + path)
    return document


def require_sealed(client, path, base):
    """Check selected paths and ancestors, including named/default ACL isolation."""
    require_sealed_paths(client, [path], base)


def require_sealed_paths(client, paths, base):
    """Check a read-only batch once per path/ancestor, with no cross-call cache.

    Use only within one validation boundary. Publication mutations must initiate
    a fresh call, so preclaim and postseal checks never reuse old permissions.
    """
    checked = set()
    for path in paths:
        for current in (PurePosixPath(path), *PurePosixPath(path).parents):
            text = str(current)
            if text in checked:
                break
            status = client.path_status(text)
            if (
                status is None
                or status["owner"] != "hadoop"
                or int(status["permission"], 8) & 0o022
            ):
                raise FeatureInputError("published input is not sealed: " + text)
            for line in client.get_acl(text).splitlines():
                line = line.strip()
                if (
                    line.startswith(("default:", "mask:"))
                    or (line.startswith("user:") and not line.startswith("user::"))
                    or (line.startswith("group:") and not line.startswith("group::"))
                ):
                    raise FeatureInputError("published input has shared ACLs: " + text)
            checked.add(text)
            if text == base:
                break


def _reject(frame, message):
    if frame.limit(1).count():
        raise FeatureInputError(message)


def validate_feature_rows(
    frame, *, calendar_mapping, source_snapshot_id, feature_version, expected_count
):
    """Distributed schema, domain, key, calendar and split checks on actual Parquet."""
    from pyspark.sql import functions as F

    fields = FORECAST_FEATURES.field_names
    if len(frame.columns) != len(fields) or {
        f.name: f.dataType.simpleString() for f in frame.schema
    } != dict(zip(fields, FEATURE_TYPES, strict=True)):
        raise FeatureInputError("J04 physical schema differs from the supported feature contract")
    required = (*FORECAST_FEATURES.required_field_names, "label")
    summary = frame.agg(
        F.count("*").alias("n"),
        *[F.count(F.when(F.col(name).isNull(), 1)).alias(name) for name in required],
    ).first()
    if summary.n != expected_count or not summary.n or any(summary[name] for name in required):
        raise FeatureInputError("J04 count or required null evidence conflicts with Parquet")
    weekday = F.pmod(F.dayofweek("target_date") + 5, F.lit(7)) + 1
    invalid = [
        *[F.col(name).rlike(r"^\s*$") for name in fields[:6]],
        F.col("source_snapshot_id") != source_snapshot_id,
        F.col("feature_version") != feature_version,
        ~F.col("feature_role").isin("train", "validation", "test"),
        F.col("as_of_date") != F.date_sub("target_date", 1),
        F.col("day_of_week") != weekday,
        F.col("month") != F.month("target_date"),
        F.col("is_weekend") != weekday.isin(6, 7),
        F.col("event_flag"),
        F.col("price_missing") != F.col("last_known_price").isNull(),
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
    ]
    _reject(frame.where(reduce(or_, invalid)), "invalid J04 feature values or lineage")
    _reject(frame.groupBy(*SCOPED_KEY).count().where("count != 1"), "duplicate scoped J04 keys")
    joined = frame.join(F.broadcast(calendar_mapping), "target_date", "left")
    day = F.col("day_index")
    role = (
        F.when(day.between(*TRAIN_DAYS), "train")
        .when(day.between(*VALIDATION_DAYS), "validation")
        .when(day.between(*TEST_DAYS), "test")
    )
    _reject(
        joined.where(day.isNull() | ~F.col("feature_role").eqNullSafe(role)),
        "J04 calendar/role mismatch",
    )
    if (
        not joined.where((F.col("feature_role") == "train") & day.between(*TRAIN_DAYS))
        .limit(1)
        .count()
    ):
        raise FeatureInputError("no eligible training labels in selected J04 input")


def _read_bound_calendar(spark, client, config, binding):
    """Verify only J04's small bound calendar; never rescan raw observations/prices."""
    from pyspark.sql import functions as F

    upstream = binding.get("upstream_evidence", {})
    raw = render_raw_m5_path(config.base_path, binding["source_version"])
    if (
        upstream.get("raw_uri") != config.uri + raw
        or upstream.get("source_version") != binding["source_version"]
        or upstream.get("source_snapshot_id") != binding["source_snapshot_id"]
        or upstream.get("pipeline_run_id") != binding["upstream_pipeline_run_id"]
    ):
        raise FeatureInputError("J04 frozen upstream calendar binding conflicts")
    require_sealed(client, raw + "/source_manifests.json", config.base_path)
    manifests = json.loads(client.read_remote(raw + "/source_manifests.json"))
    if not isinstance(manifests, list) or document_digest(manifests) != upstream.get(
        "raw_manifest_digest"
    ):
        raise FeatureInputError("J04 bound source manifest digest conflicts")
    selected = [
        m for m in manifests if isinstance(m, dict) and m.get("source_file") == "calendar.csv"
    ]
    if len(selected) != 1:
        raise FeatureInputError("source manifest has no unique calendar binding")
    manifest = selected[0]
    validate_manifest(
        SOURCE_MANIFEST,
        manifest,
        lineage_context={"required_status": VALIDATED, "source_version": binding["source_version"]},
    )
    path = raw + "/calendar.csv"
    require_sealed(client, path, config.base_path)
    if type(manifest["size_bytes"]) is not int or not 0 < manifest["size_bytes"] <= 2 * 1024 * 1024:
        raise FeatureInputError("bound calendar exceeds the small-input byte limit")
    payload = client.read_remote(path)
    if (
        manifest["artifact_uri"] != config.uri + path
        or manifest["size_bytes"] != len(payload)
        or manifest["artifact_checksum"] != "sha256:" + hashlib.sha256(payload).hexdigest()
    ):
        raise FeatureInputError("bound calendar checksum/size/URI conflicts")
    # CSV is read by Spark only after the bounded source binding has passed.
    calendar = spark.read.option("header", "true").option("mode", "FAILFAST").csv(config.uri + path)
    mapping = calendar.select(
        F.to_date("date").alias("target_date"),
        F.regexp_extract("d", r"^d_([1-9][0-9]*)$", 1).cast("int").alias("day_index"),
    )
    if not 1 <= mapping.count() <= 1969:
        raise FeatureInputError("calendar exceeds canonical bounded coverage")
    _reject(
        mapping.where(
            F.col("target_date").isNull()
            | F.col("day_index").isNull()
            | ~F.col("day_index").between(1, 1969)
            | (
                F.col("target_date")
                != F.date_add(F.lit("2011-01-29").cast("date"), F.col("day_index") - 1)
            )
        ),
        "invalid canonical calendar mapping",
    )
    for key in ("target_date", "day_index"):
        _reject(mapping.groupBy(key).count().where("count != 1"), "duplicate calendar mapping")
    return mapping, {
        "source_manifest_digest": document_digest(manifest),
        "calendar_checksum": manifest["artifact_checksum"],
    }


def read_validated_feature_context(
    spark,
    *,
    source_version,
    source_snapshot_id,
    upstream_pipeline_run_id,
    feature_version,
    hdfs_config,
    client,
):
    validate_source_version(source_version)
    validate_source_snapshot_id(source_snapshot_id)
    validate_pipeline_run_id(upstream_pipeline_run_id)
    validate_feature_version(feature_version)
    config = hdfs_config
    artifact = render_forecast_features_path(config.base_path, source_snapshot_id, feature_version)
    marker = render_run_manifest_path(config.base_path, upstream_pipeline_run_id)
    if not client.exists(marker):
        raise FeatureInputError("selected J04 completion manifest is missing")
    manifest = read_json(client, marker)
    validate_manifest(
        PIPELINE_RUN_MANIFEST,
        manifest,
        lineage_context={
            "required_status": VALIDATED,
            "source_version": source_version,
            "source_snapshot_id": source_snapshot_id,
            "pipeline_run_id": upstream_pipeline_run_id,
        },
    )
    if manifest.get("output_dataset_names") != '["forecast_features"]':
        raise FeatureInputError("selected completion is not J04")
    if manifest.get("producer") != "warehouse-bigdata:feature-snapshot":
        raise FeatureInputError("selected completion has an unsupported J04 producer")
    require_sealed(client, marker, config.base_path)
    require_sealed(client, artifact + "/_dataset_manifest.json", config.base_path)
    validate_feature_publication_evidence(client, config, manifest, artifact)
    sidecar = read_json(client, artifact + "/_dataset_manifest.json")
    binding = read_json(
        client,
        render_staging_path(config.base_path, upstream_pipeline_run_id) + "/input_binding.json",
    )
    scope = read_scope(
        client, artifact, source_version=source_version, source_snapshot_id=source_snapshot_id
    )
    validate_scope_binding(manifest, scope)
    if binding.get("scope") != scope or binding.get("upstream_evidence", {}).get("scope") != scope:
        raise FeatureInputError("J04 frozen scope selection differs from J03")
    schema = typed_feature_schema()
    coverage = json.loads(manifest["coverage_and_exclusion_counts"])
    expected_identity = {
        "source_version": source_version,
        "source_snapshot_id": source_snapshot_id,
        "feature_version": feature_version,
        "artifact_uri": config.uri + artifact,
    }
    if (
        any(sidecar.get(k) != v or binding.get(k) != v for k, v in expected_identity.items())
        or binding.get("pipeline_run_id") != upstream_pipeline_run_id
        or sidecar.get("pipeline_run_id") != coverage.get("producing_pipeline_run_id")
        or sidecar.get("status") != VALIDATED
        or sidecar.get("logical_unique_key") != list(SCOPED_KEY)
    ):
        raise FeatureInputError("J04 frozen sidecar lineage/status/key conflicts")
    if (
        binding.get("schema_version") != "feature-input-binding-v1"
        or sidecar.get("schema_version") != "feature-dataset-manifest-v1"
        or sidecar.get("schema") != schema
        or sidecar.get("schema_fingerprint") != document_digest({"schema": schema})
        or manifest.get("schema_fingerprint") != sidecar["schema_fingerprint"]
        or sidecar.get("provenance") != FORECAST_FEATURES.provenance
        or sidecar.get("dataset_version") != FORECAST_FEATURES.version
        or sidecar.get("dataset_name") != FORECAST_FEATURES.name
        or sidecar.get("input_binding")
        != {**binding, "pipeline_run_id": sidecar.get("pipeline_run_id")}
    ):
        raise FeatureInputError("unsupported or conflicting frozen J04 schema/binding/provenance")
    semantics = binding.get("effective_configuration", {}).get("semantics")
    supported = {
        "contract": FORECAST_FEATURES.version,
        "lookback_days": 28,
        "lag_days": [1, 7, 28],
        "rolling_mean_days": [7, 28],
        "train_days": list(TRAIN_DAYS),
        "validation_days": list(VALIDATION_DAYS),
        "test_days": list(TEST_DAYS),
        "events": "disabled_without_verified_advance_availability",
        "price_availability": "observed_date_close_strictly_prior_to_target",
        "missing_observations": "not_imputed",
        "historical_roles": "one_step_observed_labels",
    }
    if (
        semantics != supported
        or document_digest(binding["effective_configuration"]) != binding["configuration_digest"]
    ):
        raise FeatureInputError("unsupported J04 feature semantics/configuration")
    for item in sidecar["file_integrity"]:
        require_sealed(client, artifact + "/" + item["path"], config.base_path)
    mapping, calendar_evidence = _read_bound_calendar(spark, client, config, binding)
    if sidecar.get("row_count") != manifest["row_count"]:
        raise FeatureInputError("J04 completion/sidecar counts conflict")
    return {
        "calendar_mapping": mapping,
        "completion_digest": document_digest(manifest),
        "scope": scope,
        "dataset_manifest_digest": document_digest(sidecar),
        "input_binding_digest": document_digest(binding),
        "calendar_evidence": calendar_evidence,
        "artifact_uri": config.uri + artifact,
        "source_version": source_version,
        "source_snapshot_id": source_snapshot_id,
        "feature_version": feature_version,
        "upstream_pipeline_run_id": upstream_pipeline_run_id,
        "upstream_evidence": binding["upstream_evidence"],
        "manifest": manifest,
        "sidecar": sidecar,
    }


def read_validated_forecast_features_snapshot(
    spark,
    *,
    source_version,
    source_snapshot_id,
    upstream_pipeline_run_id,
    feature_version,
    hdfs_config,
    client,
):
    context = read_validated_feature_context(
        spark,
        source_version=source_version,
        source_snapshot_id=source_snapshot_id,
        upstream_pipeline_run_id=upstream_pipeline_run_id,
        feature_version=feature_version,
        hdfs_config=hdfs_config,
        client=client,
    )
    config = hdfs_config
    manifest, sidecar = context["manifest"], context["sidecar"]
    mapping = context["calendar_mapping"]
    coverage = json.loads(manifest["coverage_and_exclusion_counts"])
    frame = (
        spark.read.option("mergeSchema", "true")
        .option("ignoreCorruptFiles", "false")
        .option("ignoreMissingFiles", "false")
        .parquet(context["artifact_uri"])
    )
    if sidecar.get("row_count") != manifest["row_count"]:
        raise FeatureInputError("J04 completion/sidecar counts conflict")
    validate_feature_rows(
        frame,
        calendar_mapping=mapping,
        source_snapshot_id=source_snapshot_id,
        feature_version=feature_version,
        expected_count=manifest["row_count"],
    )
    validate_scope_population(frame, context["scope"], allow_subset=True)
    metrics = sidecar.get("metrics", {})
    quality = read_json(
        client,
        render_staging_path(config.base_path, upstream_pipeline_run_id) + "/quality_report.json",
    )
    role_counts = {
        row["feature_role"]: row["count"] for row in frame.groupBy("feature_role").count().collect()
    }
    if (
        metrics.get("row_count") != manifest["row_count"]
        or metrics != quality.get("metrics")
        or metrics != coverage.get("metrics")
        or metrics.get("role_counts")
        != {role: role_counts.get(role, 0) for role in ("train", "validation", "test")}
    ):
        raise FeatureInputError("J04 frozen coverage/count evidence conflicts with Parquet")
    return frame, {
        key: value
        for key, value in context.items()
        if key not in {"manifest", "sidecar", "upstream_evidence"}
    }

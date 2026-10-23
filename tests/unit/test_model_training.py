"""M06 training-only selection, preprocessing and native model persistence checks."""

from datetime import timedelta

import pytest
from pyspark.ml import PipelineModel
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from tests.unit.test_historical_features import ORIGIN
from tests.unit.test_historical_features import spark as spark
from warehouse_bigdata.forecasting.feature_input import validate_feature_rows
from warehouse_bigdata.forecasting.feature_validation import SCOPED_KEY, typed_feature_schema
from warehouse_bigdata.forecasting.training import (
    PREDICTORS,
    RF_PARAMETERS,
    TrainingConfig,
    TrainingError,
    allocate_quotas,
    fit_training_pipeline,
    select_training_rows,
)


def feature_frame(spark, days=(29, 30, 31, 32, 1885, 1886, 1914), items=3):
    rows = []
    for item in range(items):
        for day in days:
            target = ORIGIN + timedelta(days=day - 1)
            holdout = day >= 1886
            rows.append(
                dict(
                    source_snapshot_id="snapshot",
                    feature_version="historical_v1",
                    feature_role="test" if day >= 1914 else "validation" if holdout else "train",
                    item_id=f"item_{item}",
                    store_id="POISON" if holdout else "CA_1",
                    dept_id="POISON" if holdout else "dept_1",
                    target_date=target,
                    as_of_date=target - timedelta(days=1),
                    lag_1=item,
                    lag_7=item + 1,
                    lag_28=item + 2,
                    rolling_mean_7=float(item),
                    rolling_mean_28=float(item + 1),
                    day_of_week=target.isoweekday(),
                    month=target.month,
                    is_weekend=target.isoweekday() >= 6,
                    event_flag=False,
                    price_missing=item == 0,
                    last_known_price=None if item == 0 else 99999.0 if holdout else float(item + 1),
                    label=999999 if holdout else item,
                )
            )
    schema = StructType.fromJson(
        {
            "type": "struct",
            "fields": [
                {
                    **field,
                    "metadata": {},
                    "type": "long"
                    if field["type"] == "bigint"
                    else "integer"
                    if field["type"] == "int"
                    else field["type"],
                }
                for field in typed_feature_schema()
            ],
        }
    )
    return spark.createDataFrame(rows, schema)


def calendar(spark, days=(29, 30, 31, 32, 1885, 1886, 1914)):
    return spark.createDataFrame(
        [(ORIGIN + timedelta(days=d - 1), d) for d in days], "target_date date, day_index int"
    )


def config(cap=10000):
    return TrainingConfig(
        cap=cap, resource_justification="tiny offline fixture" if cap != 10000 else None
    )


def test_m06_quotas_reserve_and_largest_remainder_and_sparse_cap():
    assert allocate_quotas([("a", 5), ("b", 3), ("c", 1)], 6) == {"a": 3, "b": 2, "c": 1}
    assert allocate_quotas([("b", 2), ("a", 2)], 3) == {"a": 2, "b": 1}
    with pytest.raises(TrainingError, match="strata"):
        allocate_quotas([("a", 1), ("b", 1)], 1)
    assert TrainingConfig().cap == 10000
    with pytest.raises(TrainingError, match="justification"):
        TrainingConfig(cap=9999)
    with pytest.raises(TrainingError):
        TrainingConfig(cap=150001, resource_justification="oversized")


def test_m06_selection_is_training_only_repartition_stable_and_exact(spark):
    frame = feature_frame(spark)
    first = select_training_rows(
        frame.repartition(1), calendar_mapping=calendar(spark), config=config(13)
    )
    second = select_training_rows(
        frame.repartition(3), calendar_mapping=calendar(spark), config=config(13)
    )
    assert first.keys.exceptAll(second.keys).count() == 0
    assert first.keys.count() == 13
    assert first.summary["available_count"] == 15
    assert first.rows.where("feature_role != 'train' or day_index > 1885").count() == 0
    assert sum(q["quota"] for q in first.summary["strata"]) == 13
    assert first.rows.where("day_index = 1885").count() > 0
    assert first.rows.where("day_index = 29").count() > 0
    poisoned = frame.withColumn(
        "label", F.when(F.col("feature_role") != "train", -100).otherwise(F.col("label"))
    )
    third = select_training_rows(poisoned, calendar_mapping=calendar(spark), config=config(13))
    assert first.keys.exceptAll(third.keys).count() == 0
    with pytest.raises(TrainingError, match="empty|eligible"):
        select_training_rows(
            frame.where("feature_role = 'test'"), calendar_mapping=calendar(spark), config=config()
        )


@pytest.mark.parametrize(
    "mutation",
    [
        "schema",
        "lineage",
        "role",
        "null",
        "negative",
        "infinite",
        "price",
        "flag",
        "event",
        "asof",
        "duplicate",
    ],
)
def test_m06_consumer_rejects_corrupt_rows(spark, mutation):
    frame = feature_frame(spark)
    if mutation == "schema":
        frame = frame.withColumn("lag_1", F.col("lag_1").cast("double"))
    elif mutation == "duplicate":
        frame = frame.unionByName(frame.limit(1))
    else:
        column, value = {
            "lineage": ("source_snapshot_id", F.lit("wrong")),
            "role": ("feature_role", F.lit("test")),
            "null": ("label", F.lit(None).cast("long")),
            "negative": ("label", F.lit(-1).cast("long")),
            "infinite": ("rolling_mean_7", F.lit(float("inf"))),
            "price": ("last_known_price", F.lit(0.0)),
            "flag": ("price_missing", ~F.col("price_missing")),
            "event": ("event_flag", F.lit(True)),
            "asof": ("as_of_date", F.col("target_date")),
        }[mutation]
        frame = frame.withColumn(column, value)
    with pytest.raises(ValueError):
        validate_feature_rows(
            frame,
            calendar_mapping=calendar(spark),
            source_snapshot_id="snapshot",
            feature_version="historical_v1",
            expected_count=21,
        )


def test_m06_consumer_accepts_missing_holdout_and_rejects_empty_train(spark):
    frame = feature_frame(spark, days=(29, 1885))
    validate_feature_rows(
        frame,
        calendar_mapping=calendar(spark),
        source_snapshot_id="snapshot",
        feature_version="historical_v1",
        expected_count=6,
    )
    with pytest.raises(ValueError, match="training|eligible"):
        validate_feature_rows(
            feature_frame(spark, days=(1886, 1914)),
            calendar_mapping=calendar(spark),
            source_snapshot_id="snapshot",
            feature_version="historical_v1",
            expected_count=6,
        )


def test_m06_actual_rf_reload_training_mappings_median_and_fractional_scoring(spark, tmp_path):
    selection = select_training_rows(
        feature_frame(spark), calendar_mapping=calendar(spark), config=config()
    )
    model = fit_training_pipeline(selection.rows, config())
    assert "POISON" not in model.stages[1].labels
    assert model.stages[1].labels == ["CA_1"]
    assert model.stages[3].getDropLast() is False
    assert model.stages[4].surrogateDF.first()["last_known_price"] == 2.0
    assert model.stages[5].getInputCols() == list(PREDICTORS)
    rf = model.stages[-1]
    for name, value in RF_PARAMETERS.items():
        assert rf.getOrDefault(name) == value
    probe = (
        selection.rows.orderBy(*SCOPED_KEY).limit(1).drop("label").withColumn("lag_1", F.lit(0.375))
    )
    probe = probe.withColumn("store_id", F.lit("UNSEEN")).withColumn("dept_id", F.lit("UNSEEN"))
    transformed = model.transform(probe).select("features", "raw_prediction", "_rf_lag_1").first()
    assert transformed["_rf_lag_1"] == 0.375
    assert transformed.features[0] == 0.375
    assert transformed.features[9] == 1.0
    assert transformed.features[10] == 2.0
    path = str(tmp_path / "pipeline")
    model.write().save(path)
    loaded = PipelineModel.load(path)
    assert loaded.transform(probe).select("features", "raw_prediction").first() == (
        transformed.features,
        transformed.raw_prediction,
    )
    with pytest.raises(Exception, match="exists"):
        model.write().save(path)
    with pytest.raises(TrainingError, match="all.missing"):
        fit_training_pipeline(
            selection.rows.withColumn("last_known_price", F.lit(None).cast("double")), config()
        )
    with pytest.raises(TrainingError, match="training"):
        fit_training_pipeline(feature_frame(spark), config())


def test_m06_workflow_stages_complete_bundle_and_preserves_failed_attempt(
    spark, tmp_path, monkeypatch
):
    from tests.unit.test_feature_snapshot import FeatureLocalHdfs
    from tests.unit.test_hdfs_namespace import CONFIG
    from warehouse_bigdata.common.hdfs_paths import render_staging_path
    from warehouse_bigdata.forecasting import training_snapshot as module

    hdfs = FeatureLocalHdfs(tmp_path)
    staging = render_staging_path(CONFIG.base_path, "j05-001")
    hdfs.mkdir_parents(staging)
    frame = feature_frame(spark)
    from tests.unit.test_m5_small import evidence

    frozen_scope = evidence("snapshot")
    monkeypatch.setattr(module, "read_scope", lambda *a, **k: frozen_scope)
    monkeypatch.setattr(
        module,
        "read_validated_forecast_features_snapshot",
        lambda *a, **k: (
            frame,
            {"calendar_mapping": calendar(spark), "verified": True, "scope": frozen_scope},
        ),
    )
    monkeypatch.setattr(module, "artifact_uri", lambda config, path: str(hdfs._local(path)))
    monkeypatch.setattr(module, "packaged_code_revision", lambda: "sha256:" + "c" * 64)
    requests = []
    monkeypatch.setattr(module, "stage_publication", lambda *a, **k: requests.append(a[2]))
    arguments = dict(
        source_version="sha256:" + "a" * 64,
        source_snapshot_id="snapshot",
        upstream_pipeline_run_id="j04-001",
        feature_version="historical_v1",
        model_run_id="rf-001",
        pipeline_run_id="j05-001",
        config=config(),
        hdfs_config=CONFIG,
        client=hdfs,
    )
    result = module.run_model_training(spark, **arguments)
    assert result.status == "STAGED", result.failure_details
    candidate = staging + "/candidates/model"
    for name in (
        "spark_pipeline_model",
        "training_selection_manifest",
        "feature_schema.json",
        "evidence/training.json",
        "evidence/publication.json",
        "model_manifest.json",
    ):
        assert hdfs.exists(candidate + "/" + name)
    assert requests[0]["training_row_count"] == 15
    assert not hdfs.exists(result.artifact_path)
    assert requests[0]["pipeline_run_id"] == "j05-001"
    assert "evaluation_run_id" not in requests[0]
    assert module.run_model_training(spark, **arguments).status == "FAILED"
    assert not hdfs.exists(staging + "/failure_report.json")
    from warehouse_bigdata.forecasting import model_artifacts

    hdfs.rename(candidate, result.artifact_path)
    monkeypatch.setattr(
        model_artifacts, "artifact_uri", lambda config, path: str(hdfs._local(path))
    )
    monkeypatch.setattr(model_artifacts, "require_sealed", lambda *a: None)
    loaded = model_artifacts.load_validated_model(
        spark,
        model_run_id="rf-001",
        source_snapshot_id="snapshot",
        feature_version="historical_v1",
        hdfs_config=CONFIG,
        client=hdfs,
    )
    assert loaded.stages[1].labels == ["CA_1"]
    with pytest.raises(RuntimeError, match="lineage"):
        model_artifacts.load_validated_model(
            spark,
            model_run_id="rf-001",
            source_snapshot_id="wrong",
            feature_version="historical_v1",
            hdfs_config=CONFIG,
            client=hdfs,
        )
    next_staging = render_staging_path(CONFIG.base_path, "j05-failed")
    hdfs.mkdir_parents(next_staging)
    monkeypatch.setattr(
        module,
        "fit_training_pipeline",
        lambda *a, **k: (_ for _ in ()).throw(ValueError("fit failure")),
    )
    failed = module.run_model_training(
        spark, **dict(arguments, pipeline_run_id="j05-failed", model_run_id="rf-failed")
    )
    assert failed.status == "FAILED"
    assert hdfs.exists(failed.failure_report_path)


def test_m06_consumer_requires_completion_before_reading_spark():
    from tests.unit.test_hdfs_namespace import CONFIG, PermissionHdfs
    from warehouse_bigdata.forecasting.feature_input import (
        read_validated_forecast_features_snapshot,
    )

    with pytest.raises(ValueError, match="completion"):
        read_validated_forecast_features_snapshot(
            object(),
            source_version="sha256:" + "a" * 64,
            source_snapshot_id="snapshot",
            upstream_pipeline_run_id="j04-001",
            feature_version="historical_v1",
            hdfs_config=CONFIG,
            client=PermissionHdfs(),
        )


@pytest.mark.parametrize(
    "corruption", ["status", "digest", "schema", "semantics", "unsealed", "calendar"]
)
def test_m06_consumer_rejects_frozen_metadata_before_parquet(spark, corruption):
    import json

    from tests.unit.test_feature_snapshot import (
        RUN,
        SOURCE_SNAPSHOT_ID,
        SOURCE_VERSION,
        VERSION,
        _feature_staged,
    )
    from tests.unit.test_hdfs_namespace import CONFIG, PermissionHdfs
    from warehouse_bigdata.common.hdfs_paths import render_run_manifest_path, render_staging_path
    from warehouse_bigdata.common.hdfs_publication import document_digest, finalize_publication
    from warehouse_bigdata.contracts.datasets import FORECAST_FEATURES
    from warehouse_bigdata.forecasting import feature_input as module
    from warehouse_bigdata.forecasting.feature_snapshot import _effective_configuration

    client = PermissionHdfs()
    candidate, final, marker = _feature_staged(client)
    staging = render_staging_path(CONFIG.base_path, RUN)
    binding = json.loads(client.files[staging + "/input_binding.json"])
    binding["schema_version"] = "feature-input-binding-v1"
    binding["effective_configuration"] = _effective_configuration(spark, CONFIG, 16, 1000000)
    binding["configuration_digest"] = document_digest(binding["effective_configuration"])
    sidecar = json.loads(client.files[candidate + "/_dataset_manifest.json"])
    sidecar.update(
        binding,
        schema_version="feature-dataset-manifest-v1",
        input_binding=binding,
        schema=typed_feature_schema(),
        provenance=FORECAST_FEATURES.provenance,
        schema_fingerprint=document_digest({"schema": typed_feature_schema()}),
        dataset_name=FORECAST_FEATURES.name,
        dataset_version=FORECAST_FEATURES.version,
        logical_unique_key=list(SCOPED_KEY),
        row_count=1,
    )
    quality = dict(sidecar)
    marker["configuration_digest"] = binding["configuration_digest"]
    marker["schema_fingerprint"] = sidecar["schema_fingerprint"]
    marker["row_count"] = 1
    coverage = json.loads(marker["coverage_and_exclusion_counts"])
    coverage.update(
        binding_digest=document_digest(binding),
        quality_digest=document_digest(quality),
        dataset_manifest_digest=document_digest(sidecar),
    )
    marker["coverage_and_exclusion_counts"] = json.dumps(coverage)
    for path, doc in (
        (staging + "/input_binding.json", binding),
        (staging + "/quality_report.json", quality),
        (candidate + "/_dataset_manifest.json", sidecar),
    ):
        client.files[path] = json.dumps(doc).encode()
    client.files[staging + "/publication/request.json"] = json.dumps(
        {"version": 1, "manifest": marker}
    ).encode()
    finalize_publication(client, CONFIG, pipeline_run_id=RUN)
    marker_path = render_run_manifest_path(CONFIG.base_path, RUN)
    if corruption == "status":
        changed = dict(marker, status="FAILED")
        client.files[marker_path] = json.dumps(changed).encode()
    elif corruption == "unsealed":
        client.entries[final]["permission"] = "777"
    elif corruption in {"digest", "schema", "semantics"}:
        path = (
            final + "/_dataset_manifest.json"
            if corruption != "semantics"
            else staging + "/input_binding.json"
        )
        doc = json.loads(client.files[path])
        doc["schema" if corruption == "schema" else "unexpected"] = []
        client.files[path] = json.dumps(doc).encode()
    # The calendar case reaches the actual upstream binding check, before any sales/Parquet read.
    with pytest.raises((ValueError, RuntimeError)):
        module.read_validated_forecast_features_snapshot(
            object(),
            source_version=SOURCE_VERSION,
            source_snapshot_id=SOURCE_SNAPSHOT_ID,
            upstream_pipeline_run_id=RUN,
            feature_version=VERSION,
            hdfs_config=CONFIG,
            client=client,
        )

"""Independent persisted J04 quality gates; tiny fixtures include every split boundary."""

from dataclasses import replace

import pytest
from pyspark import StorageLevel
from pyspark.sql import functions as F

from tests.unit.test_historical_features import _frame, _rows
from tests.unit.test_historical_features import spark as spark
from warehouse_bigdata.forecasting.feature_validation import (
    FeatureValidationError,
    validate_persisted_feature_snapshot,
)
from warehouse_bigdata.forecasting.features import build_historical_features


@pytest.fixture(scope="module")
def context(spark, tmp_path_factory):
    source = _frame(
        spark,
        (
            _rows([*range(1, 33), *range(1857, 1942)], zero=True, prices={})
            + _rows([d for d in range(1, 34) if d != 30], item="gap")
            + _rows(range(25, 32), item="late")
        ),
    )
    result = build_historical_features(
        source, source_snapshot_id="snapshot_1", feature_version="historical_v1"
    )
    for frame in (result.features, result.eligibility, result.coverage):
        frame.persist(StorageLevel.DISK_ONLY)
        frame.count()
    path = str(tmp_path_factory.mktemp("features") / "parquet")
    result.features.write.mode("errorifexists").parquet(path)
    persisted = spark.read.parquet(path)
    exclusions = result.eligibility.where("status != 'ELIGIBLE'")
    yield source, result, persisted, exclusions
    for frame in (result.features, result.eligibility, result.coverage):
        frame.unpersist()


def check(context, *, persisted=None, result=None, exclusions=None):
    source, expected, actual, diagnostics = context
    return validate_persisted_feature_snapshot(
        actual if persisted is None else persisted,
        expected=expected if result is None else result,
        source=source,
        exclusions=diagnostics if exclusions is None else exclusions,
        source_snapshot_id="snapshot_1",
        feature_version="historical_v1",
    )


def test_persisted_boundary_roles_zeros_missing_prices_and_conservation(context):
    metrics = check(context)
    assert metrics["row_count"] > 0
    assert set(metrics["role_counts"]) == {"train", "validation", "test"}
    assert metrics["observed_count"] == (
        metrics["lookback_only_count"] + metrics["candidate_count"]
    )
    assert metrics["candidate_count"] == metrics["row_count"] + metrics["excluded_count"]
    assert {"INSUFFICIENT_HISTORY", "DISCONTINUOUS_HISTORY"} <= set(metrics["reason_counts"])
    assert metrics["required_field_null_counts"]["label"] == 0


@pytest.mark.parametrize(
    "mutation",
    [
        lambda df: df.drop("month"),
        lambda df: df.withColumn("extra", F.lit(1)),
        lambda df: df.withColumn("lag_1", F.col("lag_1").cast("int")),
        lambda df: df.select("*", F.col("month")),
        lambda df: df.withColumn("label", F.lit(None).cast("bigint")),
        lambda df: df.withColumn("feature_version", F.lit("other")),
        lambda df: df.withColumn("feature_role", F.lit("scoring")),
        lambda df: df.withColumn("as_of_date", F.col("target_date")),
        lambda df: df.withColumn("rolling_mean_7", F.lit(float("nan"))),
        lambda df: df.withColumn("event_flag", F.lit(True)),
        lambda df: df.withColumn("price_missing", ~F.col("price_missing")),
        lambda df: df.withColumn("label", F.col("label") + 1),
        lambda df: df.withColumn("dept_id", F.lit("substituted")),
        lambda df: df.withColumn("lag_7", F.col("lag_7") + 1),
        lambda df: df.withColumn("item_id", F.lit("swapped")),
        lambda df: df.limit(1),
        lambda df: df.unionByName(df.limit(1)),
    ],
)
def test_corrupt_persisted_features_fail_closed(context, mutation):
    with pytest.raises(FeatureValidationError):
        check(context, persisted=mutation(context[2]))


def test_diagnostic_rows_and_reason_counts_are_reconciled(context):
    with pytest.raises(FeatureValidationError, match="diagnostic|exclusion"):
        check(context, exclusions=context[3].limit(1))
    corrupt = context[3].withColumn("status", F.lit("INSUFFICIENT_HISTORY"))
    with pytest.raises(FeatureValidationError, match="diagnostic|exclusion"):
        check(context, exclusions=corrupt)
    bad_coverage = context[1].coverage.withColumn("excluded_count", F.lit(999).cast("long"))
    with pytest.raises(FeatureValidationError, match="coverage"):
        check(context, result=replace(context[1], coverage=bad_coverage))


def test_empty_eligible_snapshot_is_not_publishable(context):
    with pytest.raises(FeatureValidationError):
        check(context, persisted=context[2].limit(0))

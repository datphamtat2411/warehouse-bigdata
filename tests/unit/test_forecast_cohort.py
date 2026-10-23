"""J06 frozen training-era SKU selection."""

from datetime import timedelta

import pytest
from pyspark.sql import functions as F

from tests.unit.test_scoring_features import ORIGIN, spark as spark
from warehouse_bigdata.forecasting.forecast_cohort import (
    CohortError, select_forecast_cohort, validate_cohort_selection,
)

END = ORIGIN + timedelta(days=1884)


def sales(spark, items=105):
    return (spark.range(items).select(F.format_string("item_%03d", "id").alias("item_id"),
                                     (F.col("id") % 3).alias("volume"))
            .crossJoin(spark.range(10).select(F.concat(F.lit("CA_"), F.col("id")).alias("store_id")))
            .crossJoin(spark.range(1858, 1886).select(F.col("id").cast("int").alias("day_index")))
            .withColumn("date", F.date_add(F.lit(ORIGIN), F.col("day_index") - 1))
            .withColumn("unit_sales", F.col("volume").cast("long"))
            .withColumn("dept_id", F.lit("dept_1")).withColumn("cat_id", F.lit("cat_1"))
            .withColumn("state_id", F.lit("CA"))
            .withColumn("series_id", F.concat_ws("_", "item_id", "store_id")))


def test_j06_cohort_exact_deterministic_cutoff_and_readback(spark, tmp_path):
    frame = sales(spark)
    first = select_forecast_cohort(frame.repartition(1), training_end=END, seed=42)
    poison = frame.withColumn("date", F.date_add("date", 100)).withColumn("unit_sales", F.lit(-999))
    second = select_forecast_cohort(frame.unionByName(poison).repartition(3), training_end=END, seed=42)
    assert first.scope.count() == 1000
    assert first.scope.select("item_id").distinct().count() == 100
    assert first.scope.exceptAll(second.scope).count() == 0
    assert first.evidence.where("status = 'ELIGIBLE_NOT_SELECTED'").count() == 5
    assert first.evidence.where("training_volume = 0 AND status = 'SELECTED'").count() > 0
    path = str(tmp_path / "selection")
    first.evidence.write.parquet(path)
    validate_cohort_selection(spark.read.parquet(path), first.scope)
    with pytest.raises(CohortError):
        validate_cohort_selection(spark.read.parquet(path).where("status != 'SELECTED'"), first.scope)


def test_j06_cohort_shortfall_and_incomplete_store_universe(spark):
    with pytest.raises(CohortError, match="100"):
        select_forecast_cohort(sales(spark, 99), training_end=END, seed=42)
    with pytest.raises(CohortError, match="ten|10"):
        select_forecast_cohort(sales(spark).where("store_id != 'CA_0'"), training_end=END, seed=42)
    incomplete = sales(spark).where("NOT (item_id = 'item_000' AND day_index = 1885)")
    result = select_forecast_cohort(incomplete, training_end=END, seed=42)
    assert result.evidence.where("item_id = 'item_000' AND status = 'EXCLUDED'").count() == 1

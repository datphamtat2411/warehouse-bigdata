"""Training-only, reproducible 100-SKU by ten-store forecast cohort."""

from dataclasses import dataclass
from datetime import date, timedelta

POLICY = "training_stratified_100_v1"
TRAINING_END = date(2016, 3, 27)  # d_1885, the supported T01 cutoff
KEYS = ["item_id", "store_id"]


class CohortError(ValueError):
    """The fixed cohort cannot be established without silently changing scope."""


@dataclass(frozen=True)
class CohortSelection:
    scope: object
    evidence: object


def select_forecast_cohort(sales, *, training_end, seed):
    """Round-robin occupied department/volume strata, with hash/tie-break ranking.

    Dates are filtered before metadata and sales reductions. Late product/store
    identities cannot alter membership, eligibility, volumes, or stratum ranks.
    """
    from pyspark.sql import Window, functions as F

    if training_end != TRAINING_END or type(seed) is not int or not -(2**31) <= seed < 2**31:
        raise CohortError("cohort requires the T01 training cutoff and a signed 32-bit seed")
    past = sales.where(F.col("date") <= F.lit(training_end))
    stores = past.select("store_id").distinct()
    if stores.count() != 10 or stores.where(F.col("store_id").isNull() | F.col("store_id").rlike(r"^\s*$")).count():
        raise CohortError("training-era universe must contain exactly ten stores")
    recent = F.col("date") >= F.lit(training_end - timedelta(days=27))
    metadata = ["item_id", "store_id", "dept_id", "cat_id", "state_id", "series_id"]
    invalid = F.lit(False)
    for name in metadata:
        invalid = invalid | F.col(name).isNull() | F.col(name).rlike(r"^\s*$")
    invalid = (invalid | ~F.col("state_id").isin("CA", "TX", "WI") |
               F.col("unit_sales").isNull() | (F.col("unit_sales") < 0) |
               F.col("day_index").isNull() |
               (F.col("date") != F.date_add(F.lit(date(2011, 1, 29)), F.col("day_index") - 1)))
    per_store = past.groupBy(*KEYS).agg(
        F.countDistinct(F.struct("dept_id", "cat_id", "state_id", "series_id")).alias("identities"),
        F.min("dept_id").alias("dept_id"), F.min("cat_id").alias("cat_id"),
        F.max(invalid.cast("int")).alias("invalid"),
        F.sum(F.when(recent, 1).otherwise(0)).alias("history_rows"),
        F.countDistinct(F.when(recent, F.col("date"))).alias("history_days"),
        F.sum(F.when(F.col("day_index").between(29, 1885), F.col("unit_sales").cast("double"))
              .otherwise(0.0)).alias("training_volume"))
    stats = per_store.groupBy("item_id").agg(
        F.sort_array(F.collect_set("store_id")).alias("stores"),
        F.countDistinct(F.struct("dept_id", "cat_id")).alias("metadata_count"),
        F.min("dept_id").alias("dept_id"), F.min("cat_id").alias("cat_id"),
        F.sum("training_volume").alias("training_volume"),
        F.min("history_days").alias("minimum_history_days"),
        F.max(F.when((F.col("identities") != 1) | (F.col("invalid") != 0) |
                     (F.col("history_rows") != 28) | (F.col("history_days") != 28), 1)
              .otherwise(0)).alias("invalid_store"))
    stats = stats.withColumn("reason_codes", F.filter(F.array(
        F.when(F.size("stores") != 10, F.lit("INCOMPLETE_STORE_COVERAGE")),
        F.when(F.col("metadata_count") != 1, F.lit("INCONSISTENT_METADATA")),
        F.when(F.col("invalid_store") != 0, F.lit("INVALID_OR_INCOMPLETE_HISTORY"))), lambda x: x.isNotNull()))
    eligible = stats.where(F.size("reason_codes") == 0)
    if eligible.count() < 100:
        raise CohortError("fewer than 100 qualified training-era SKUs")
    tertiles = Window.partitionBy("dept_id").orderBy("training_volume", "item_id")
    eligible = eligible.withColumn("volume_tertile", F.ntile(3).over(tertiles))
    rank = Window.partitionBy("dept_id", "volume_tertile").orderBy(
        F.xxhash64(F.lit(seed), F.col("item_id")), "item_id")
    eligible = eligible.withColumn("stratum_rank", F.row_number().over(rank))
    round_robin = Window.orderBy("stratum_rank", "dept_id", "volume_tertile", "item_id")
    eligible = (eligible.withColumn("selection_rank", F.row_number().over(round_robin))
                .withColumn("status", F.when(F.col("selection_rank") <= 100, "SELECTED")
                            .otherwise("ELIGIBLE_NOT_SELECTED")))
    invalid = (stats.where(F.size("reason_codes") > 0).withColumn("volume_tertile", F.lit(None).cast("int"))
               .withColumn("stratum_rank", F.lit(None).cast("int"))
               .withColumn("selection_rank", F.lit(None).cast("int")).withColumn("status", F.lit("EXCLUDED")))
    evidence = (eligible.unionByName(invalid).withColumn("policy", F.lit(POLICY))
                .withColumn("seed", F.lit(seed)).withColumn("training_end", F.lit(training_end)))
    scope = evidence.where("status = 'SELECTED'").select("item_id", F.explode("stores").alias("store_id"))
    return CohortSelection(scope, evidence)


def validate_cohort_selection(evidence, scope):
    """Reconcile the saved exact membership, not just its row count."""
    from pyspark.sql import functions as F

    chosen = evidence.where("status = 'SELECTED'")
    actual = chosen.select("item_id", F.explode("stores").alias("store_id"))
    if (chosen.count() != 100 or chosen.select("item_id").distinct().count() != 100 or
        actual.count() != 1000 or actual.select("store_id").distinct().count() != 10 or
        actual.groupBy(*KEYS).count().where("count != 1").limit(1).count() or
        actual.exceptAll(scope).unionByName(scope.exceptAll(actual)).limit(1).count()):
        raise CohortError("persisted cohort must retain exact 100 by ten membership")
    if evidence.groupBy("item_id").count().where("count != 1").limit(1).count():
        raise CohortError("duplicate selection evidence")

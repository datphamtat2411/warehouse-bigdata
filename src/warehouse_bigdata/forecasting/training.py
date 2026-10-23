"""Training-only, deterministic bounded selection and native Spark ML preprocessing."""

from __future__ import annotations

from dataclasses import dataclass

from warehouse_bigdata.forecasting.feature_validation import SCOPED_KEY
from warehouse_bigdata.forecasting.features import TRAIN_DAYS

SELECTION_POLICY = "store-department-quarter-zero-volume-tertile-v1"
NUMERIC_FEATURES = (
    "lag_1", "lag_7", "lag_28", "rolling_mean_7", "rolling_mean_28",
    "day_of_week", "month", "is_weekend", "event_flag", "price_missing",
)
PREDICTORS = (*("_rf_" + name for name in NUMERIC_FEATURES),
              "_rf_price", "_rf_store_vector", "_rf_dept_vector")
STRATA = ("store_id", "dept_id", "year_quarter", "zero_label", "volume_tertile")
RF_PARAMETERS = dict(numTrees=20, maxDepth=6, maxBins=32, minInstancesPerNode=5,
                     subsamplingRate=0.8, seed=42, maxMemoryInMB=128,
                     featureSubsetStrategy="auto")
MAX_STRATA = 20000


class TrainingError(ValueError):
    """The locked training protocol or its bounded fit cannot be satisfied."""


@dataclass(frozen=True, slots=True)
class TrainingConfig:
    cap: int = 150000
    sampling_seed: int = 42
    resource_justification: str | None = None
    pilot_evidence_uri: str | None = None

    def __post_init__(self):
        if type(self.cap) is not int or not 1 <= self.cap <= 150000:
            raise TrainingError("training cap must be an integer in [1, 150000]")
        if type(self.sampling_seed) is not int or not -(2**31) <= self.sampling_seed < 2**31:
            raise TrainingError("sampling seed must be a signed 32-bit integer")
        if self.cap != 150000 and not (self.resource_justification or "").strip():
            raise TrainingError("a nondefault cap requires a resource justification")


@dataclass(frozen=True)
class TrainingSelection:
    keys: object
    rows: object
    summary: dict


def allocate_quotas(capacities, cap):
    """Reserve one per occupied stratum, then integer-exact largest remainders."""
    capacities = sorted(capacities)
    if not capacities or any(type(n) is not int or n < 1 for _, n in capacities):
        raise TrainingError("empty or invalid eligible stratum capacities")
    if len(capacities) > MAX_STRATA:
        raise TrainingError("occupied strata exceed the bounded summary limit")
    total = sum(n for _, n in capacities)
    target = min(cap, total)
    if target < len(capacities):
        raise TrainingError("training cap cannot cover occupied strata")
    remaining = target - len(capacities)
    denominator = total - len(capacities)
    quotas = {key: 1 for key, _ in capacities}
    if not denominator:
        return quotas
    remainders = []
    for key, count in capacities:
        numerator = remaining * (count - 1)
        places, remainder = divmod(numerator, denominator)
        quotas[key] += places
        remainders.append((-remainder, key))
    for _, key in sorted(remainders)[:target - sum(quotas.values())]:
        quotas[key] += 1
    return quotas


def select_training_rows(features, *, calendar_mapping, config: TrainingConfig):
    """Rank narrow keys only; no holdout row enters volume statistics or quotas."""
    from pyspark.sql import Window, functions as F

    train = (features.join(F.broadcast(calendar_mapping), "target_date", "inner")
             .where((F.col("feature_role") == "train") & F.col("day_index").between(*TRAIN_DAYS)))
    series = train.groupBy("store_id", "dept_id", "item_id").agg(F.avg("label").alias("mean_sales"))
    volume_order = Window.partitionBy("store_id", "dept_id").orderBy("mean_sales", "item_id", "store_id")
    series = series.withColumn("volume_tertile", F.ntile(3).over(volume_order)).drop("mean_sales")
    narrow = (train.select(*SCOPED_KEY, "dept_id", "label")
              .join(series, ["store_id", "dept_id", "item_id"])
              .withColumn("year_quarter", F.year("target_date") * 10 + F.quarter("target_date"))
              .withColumn("zero_label", F.col("label") == 0).drop("label"))
    counts = narrow.groupBy(*STRATA).count().limit(MAX_STRATA + 1).collect()
    if not counts:
        raise TrainingError("no eligible training labels in d_29–d_1885 (empty train)")
    capacities = [(tuple(row[name] for name in STRATA), row["count"]) for row in counts]
    quotas = allocate_quotas(capacities, config.cap)
    quota_frame = features.sparkSession.createDataFrame(
        [(*key, value) for key, value in sorted(quotas.items())],
        "store_id string, dept_id string, year_quarter int, zero_label boolean, volume_tertile int, quota int",
    )
    ranking = Window.partitionBy(*STRATA).orderBy(
        F.xxhash64(F.lit(config.sampling_seed), *[F.col(name) for name in SCOPED_KEY]),
        *[F.col(name) for name in SCOPED_KEY],
    )
    keys = (narrow.join(F.broadcast(quota_frame), list(STRATA))
            .withColumn("_rank", F.row_number().over(ranking)).where(F.col("_rank") <= F.col("quota"))
            .select(*SCOPED_KEY, "dept_id", "year_quarter", "zero_label", "volume_tertile"))
    rows = train.join(keys.select(*SCOPED_KEY), list(SCOPED_KEY), "inner")
    summary = selection_coverage(rows)
    return TrainingSelection(keys, rows, {
        **summary, "policy": SELECTION_POLICY, "policy_version": 1,
        "seed": config.sampling_seed, "cap": config.cap,
        "available_count": sum(n for _, n in capacities),
        "available_series_count": series.count(),
        "strata": [{**dict(zip(STRATA, key, strict=True)), "available_count": count,
                    "quota": quotas[key]} for key, count in sorted(capacities)],
        "proportion_note": "Minimum-stratum quotas slightly alter population proportions.",
    })


def selection_coverage(rows):
    """Fixed-cardinality summaries; never collect selected observations or series keys."""
    from pyspark.sql import functions as F

    stats = rows.agg(F.count("*").alias("selected_count"),
                     F.countDistinct("item_id", "store_id").alias("selected_series_count"),
                     F.min("target_date").alias("start"), F.max("target_date").alias("end"),
                     F.sum(F.when(F.col("label") == 0, 1).otherwise(0)).alias("zero_count"),
                     F.sum(F.col("price_missing").cast("long")).alias("missing_price_count")).first().asDict()
    for name in ("start", "end"):
        stats[name] = str(stats[name])
    stats["nonzero_count"] = stats["selected_count"] - (stats["zero_count"] or 0)
    stats["observed_price_count"] = stats["selected_count"] - (stats["missing_price_count"] or 0)
    for column, maximum in (("day_of_week", 7), ("month", 12)):
        values = rows.groupBy(column).count().limit(maximum + 1).collect()
        if len(values) > maximum:
            raise TrainingError("unbounded calendar coverage")
        stats[column + "_coverage"] = {str(r[column]): r["count"] for r in values}
    return stats


def build_training_pipeline(config: TrainingConfig):
    """Label-free SQL aliases also accept double-valued fractional recursive lags."""
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import Imputer, OneHotEncoder, SQLTransformer, StringIndexer, VectorAssembler
    from pyspark.ml.regression import RandomForestRegressor

    aliases = ", ".join(f"CAST(`{name}` AS DOUBLE) AS `_rf_{name}`" for name in NUMERIC_FEATURES)
    return Pipeline(stages=[
        SQLTransformer(statement="SELECT *, " + aliases + " FROM __THIS__"),
        StringIndexer(inputCol="store_id", outputCol="_rf_store_index", handleInvalid="keep",
                      stringOrderType="alphabetAsc"),
        StringIndexer(inputCol="dept_id", outputCol="_rf_dept_index", handleInvalid="keep",
                      stringOrderType="alphabetAsc"),
        OneHotEncoder(inputCols=["_rf_store_index", "_rf_dept_index"],
                      outputCols=["_rf_store_vector", "_rf_dept_vector"], handleInvalid="keep", dropLast=False),
        Imputer(inputCols=["last_known_price"], outputCols=["_rf_price"], strategy="median", relativeError=0.0),
        VectorAssembler(inputCols=list(PREDICTORS), outputCol="features", handleInvalid="error"),
        RandomForestRegressor(featuresCol="features", labelCol="label", predictionCol="raw_prediction",
                              **RF_PARAMETERS),
    ])


def fit_training_pipeline(rows, config: TrainingConfig):
    """Refuse fitting outside the selected training scope or without an observed price."""
    from pyspark.sql import functions as F

    if "day_index" not in rows.columns:
        raise TrainingError("canonical training day_index is required")
    if rows.where(F.col("feature_role").isNull() | (F.col("feature_role") != "train") |
                  F.col("day_index").isNull() | ~F.col("day_index").between(*TRAIN_DAYS)).limit(1).count():
        raise TrainingError("all fitted rows must have training labels d_29–d_1885")
    counts = rows.agg(F.count("*").alias("n"), F.count("last_known_price").alias("prices")).first()
    if counts.n == 0 or counts.n > config.cap:
        raise TrainingError("empty or unbounded training sample")
    if counts.prices == 0:
        raise TrainingError("all-missing fit prices cannot train a median imputer")
    return build_training_pipeline(config).fit(rows)

"""Locked, paired chronological evaluation; predictions precede label alignment.

All observation-level computation stays distributed. Only the bound calendar,
cohort hashes and compact coverage summaries may be returned to the driver.
"""

from dataclasses import dataclass
from datetime import date, timedelta
from functools import reduce
import hashlib
from types import MappingProxyType

from warehouse_bigdata.common.hdfs_paths import validate_evaluation_run_id
from warehouse_bigdata.common.hdfs_publication import document_digest
from warehouse_bigdata.contracts.datasets import FORECAST_METRICS
from warehouse_bigdata.forecasting.recursive_forecasting import ForecastError, validate_forecast_daily

ORIGINS = MappingProxyType({"validation": (1885, 1892, 1899, 1906), "test": (1913, 1920, 1927, 1934)})
METHODS = ("random_forest", "seasonal_naive")
KEYS = ["item_id", "store_id", "issue_date", "target_date", "horizon"]
POLICY = MappingProxyType(dict(
    version="chronological-paired-v1", horizon_days=7, lookback_days=28,
    baseline="observed_T_plus_h_minus_7", pairing="valid_method_intersection_and_observed_label_per_row",
    rf_eligibility="T02_complete_seven_horizons", labels="observed_m5_unit_sales_no_imputation",
    price="hold_last_known_at_issue", events="recorded_events_disabled",
    preprocessing="frozen_T01_fitted_pipeline_unknown_keep_price_training_median",
    metrics="MAE=SAE/N;RMSE=sqrt(SSE/N);WAPE=100*SAE/SUM_ACTUAL;zero_denominator=null",
    pooling="horizon_0_row_weighted_all_four_origins", empty="N=0;sums=0;metrics=null",
))


@dataclass(frozen=True)
class EvaluationContext:
    evaluation_run_id: str
    model_run_id: str
    source_snapshot_id: str
    feature_version: str
    run_type: str

    def __post_init__(self):
        validate_evaluation_run_id(self.evaluation_run_id)
        if self.run_type not in ORIGINS:
            raise ForecastError("evaluation run_type must be validation or test")

    @property
    def holdout_id(self):
        return "m5-chronological-paired-v1-" + self.run_type

    @property
    def window(self):
        start = date(2011, 1, 29) + timedelta(days=ORIGINS[self.run_type][0])
        return start, start + timedelta(days=27)

    def forecast_id(self, origin):
        if origin not in ORIGINS[self.run_type]:
            raise ForecastError("origin is outside locked phase")
        token = hashlib.sha256((self.evaluation_run_id + "\0" + str(origin)).encode()).hexdigest()
        return f"eval-rf-{token}-d{origin}"

    @property
    def origin_forecasts(self):
        return {str(d): self.forecast_id(d) for d in ORIGINS[self.run_type]}


def resolve_origins(calendar_mapping, run_type):
    """Verify every origin/outcome date through the authoritative bound calendar."""
    from pyspark.sql import functions as F
    if run_type not in ORIGINS:
        raise ForecastError("unsupported evaluation phase")
    days = ORIGINS[run_type]
    rows = calendar_mapping.where(F.col("day_index").between(days[0], days[-1] + 7)).collect()
    expected = {d: date(2011, 1, 29) + timedelta(days=d-1) for d in range(days[0], days[-1] + 8)}
    if len(rows) != len(expected) or {r.day_index: r.target_date for r in rows} != expected:
        raise ForecastError("bound calendar differs from locked origins/outcome windows")
    return [(d, expected[d]) for d in days]


def freeze_protocol(*, model_metadata, cohort_digest, seed, code_revision, source_binding):
    """Analytical freeze is identical across phases; execution identity is separate."""
    return dict(schema_version="forecast-evaluation-freeze-v1", policy=dict(POLICY),
        origins={k: list(v) for k, v in ORIGINS.items()}, seed=seed,
        cohort_policy="training_stratified_100_v1", cohort_digest=cohort_digest,
        model=model_metadata, code_revision=code_revision, source_binding=source_binding)


def validate_freeze(selected, current):
    if selected != current or selected.get("schema_version") != "forecast-evaluation-freeze-v1":
        raise ForecastError("locked-test freeze conflicts with selected validation protocol")


def cohort_content_digest(frame):
    """Hash bounded cohort evidence content independently of Parquet partitioning."""
    from pyspark.sql import functions as F
    count = frame.count()
    if not 100 <= count <= 3049:
        raise ForecastError("cohort evidence exceeds the canonical SKU bound")
    hashes = frame.select(F.sha2(F.to_json(F.struct(*frame.columns)), 256).alias("hash"))
    return document_digest({"rows": [r.hash for r in hashes.orderBy("hash").collect()]})


@dataclass(frozen=True)
class BaselineResult:
    predictions: object
    exceptions: object


def _bad(value):
    from pyspark.sql import functions as F
    return value.isNull() | F.isnan(value) | (value < 0) | (value > 1.7976931348623157e308)


def seasonal_naive(state, *, issue_date):
    """Read only original as-of state. No recursive state or labels are accepted."""
    from pyspark.sql import functions as F
    grid = (state.withColumn("horizon", F.explode(F.sequence(F.lit(1), F.lit(7))))
        .withColumn("issue_date", F.lit(issue_date))
        .withColumn("target_date", F.date_add("issue_date", F.col("horizon")))
        .withColumn("baseline_source_date", F.date_sub("target_date", 7))
        .withColumn("_entries", F.filter("history", lambda e: e["date"] == F.col("baseline_source_date")))
        .withColumn("_entry", F.get("_entries", 0)))
    reason = (F.when(F.size("_entries") == 0, "MISSING_BASELINE_HISTORY")
        .when(F.size("_entries") != 1, "DUPLICATE_BASELINE_HISTORY")
        .when(~F.col("as_of_date").eqNullSafe(F.lit(issue_date)) |
              ~F.col("_entry.origin").eqNullSafe(F.lit("observed_m5_unit_sales")) |
              F.col("_entry.prediction_issue_date").isNotNull(), "INVALID_BASELINE_PROVENANCE")
        .when(_bad(F.col("_entry.value")), "INVALID_BASELINE_VALUE"))
    grid = grid.withColumn("reason_code", reason)
    predictions = grid.where("reason_code IS NULL").select("source_snapshot_id", "feature_version",
        *KEYS, F.col("_entry.value").alias("predicted_units"), "baseline_source_date",
        F.lit("observed_m5_unit_sales").alias("provenance"))
    exceptions = grid.where("reason_code IS NOT NULL").select(*KEYS,
        F.lit("seasonal_naive").alias("method_id"), F.lit("BASELINE").alias("stage"), "reason_code")
    return BaselineResult(predictions, exceptions)


def expected_keys(scope, origins):
    from pyspark.sql import functions as F
    dates = scope.sparkSession.createDataFrame([(day,) for _, day in origins], "issue_date date")
    return (scope.crossJoin(dates).withColumn("horizon", F.explode(F.sequence(F.lit(1), F.lit(7))))
        .withColumn("target_date", F.date_add("issue_date", F.col("horizon"))).select(*KEYS))


def _reject(frame, message):
    if frame.limit(1).count():
        raise ForecastError(message)


def _unique(frame, keys, name):
    _reject(frame.groupBy(*keys).count().where("count != 1"), "duplicate " + name + " keys")


@dataclass(frozen=True)
class AlignmentResult:
    paired: object
    audit: object
    coverage: object
    exceptions: object


def align_origin(expected, rf, baseline, labels, eligibility, *, run_context):
    """The sole future-label boundary; callers must first persist predictions.

    Wide audit rows retain every candidate, including partially labelled series.
    Missing/invalid outcomes are exclusions; structural keys/lineage fail closed.
    """
    from pyspark.sql import functions as F
    scope = expected.select("item_id", "store_id").distinct()
    baseline_schema = {"source_snapshot_id": "string", "feature_version": "string", "item_id": "string",
        "store_id": "string", "issue_date": "date", "target_date": "date", "horizon": "int",
        "predicted_units": "double", "baseline_source_date": "date", "provenance": "string"}
    if {f.name: f.dataType.simpleString() for f in baseline.schema} != baseline_schema:
        raise ForecastError("baseline physical schema conflicts")
    validate_forecast_daily(rf, run_context=run_context, series_scope=scope)
    _unique(expected, KEYS, "expected")
    _reject(expected.where(~F.col("issue_date").eqNullSafe(F.lit(run_context.issue_date)) |
        F.col("horizon").isNull() | ~F.col("horizon").between(1, 7) |
        ~F.col("target_date").eqNullSafe(F.date_add("issue_date", F.col("horizon")))), "expected dates conflict")
    _unique(baseline, KEYS, "baseline")
    _unique(eligibility, ["item_id", "store_id"], "eligibility")
    _reject(eligibility.where(F.col("status").isNull() | ~F.col("status").isin("ELIGIBLE", "EXCLUDED")),
            "invalid preparation eligibility status")
    _reject(eligibility.select("item_id", "store_id").exceptAll(scope)
        .unionByName(scope.exceptAll(eligibility.select("item_id", "store_id"))), "eligibility scope conflicts")
    for name, value in (("source_snapshot_id", run_context.source_snapshot_id),
                        ("feature_version", run_context.feature_version),
                        ("provenance", "observed_m5_unit_sales")):
        _reject(baseline.where(~F.col(name).eqNullSafe(F.lit(value))), "baseline lineage conflicts")
    _reject(baseline.where(_bad(F.col("predicted_units")) |
        ~F.col("baseline_source_date").eqNullSafe(F.date_sub("target_date", 7))), "invalid baseline values/dates")
    _reject(baseline.select(*KEYS).join(expected, KEYS, "left_anti"), "baseline outside expected scope/dates")
    valid_scope = eligibility.where("status = 'ELIGIBLE'").select("item_id", "store_id")
    _reject(rf.select("item_id", "store_id").join(valid_scope, ["item_id", "store_id"], "left_anti"),
            "RF predictions outside preparation eligibility")
    _reject(baseline.select("item_id", "store_id").join(valid_scope, ["item_id", "store_id"], "left_anti"),
            "baseline predictions outside preparation eligibility")
    # Restrict observed labels to the expected target set before any reductions.
    selected = labels.select("item_id", "store_id", F.col("date").alias("target_date"),
                              F.col("unit_sales").cast("double").alias("actual_units"))
    selected = selected.join(expected.select("item_id", "store_id", "target_date"),
                             ["item_id", "store_id", "target_date"], "left_semi")
    _unique(selected, ["item_id", "store_id", "target_date"], "label")
    selected = selected.withColumn("_label_present", F.lit(True))
    audit = (expected.join(eligibility.select("item_id", "store_id", "status"), ["item_id", "store_id"], "left")
        .join(rf.select("item_id", "store_id", "issue_date", F.col("forecast_date").alias("target_date"),
            "horizon", F.col("predicted_units").alias("rf_prediction"), "forecast_run_id", "model_run_id",
            "source_snapshot_id", "feature_version"), KEYS, "left")
        .join(baseline.select(*KEYS, F.col("predicted_units").alias("baseline_prediction"), "baseline_source_date"), KEYS, "left")
        .join(selected, ["item_id", "store_id", "target_date"], "left")
        .withColumn("eligible", F.coalesce(F.col("status") == "ELIGIBLE", F.lit(False)))
        .withColumn("label_valid", ~_bad(F.col("actual_units")))
        .withColumn("paired", F.col("eligible") & F.col("rf_prediction").isNotNull() &
                    F.col("baseline_prediction").isNotNull() & F.col("label_valid")))
    paired = audit.where("paired").select(*KEYS, "rf_prediction", "baseline_prediction", "actual_units",
        "baseline_source_date", "forecast_run_id", "model_run_id", "source_snapshot_id", "feature_version")
    exceptions = audit.select(*KEYS, F.explode(F.filter(F.array(
        F.when(~F.col("eligible"), F.struct(F.lit("PREPARATION").alias("stage"), F.lit("COMMON_INELIGIBLE").alias("reason_code"))),
        F.when(F.col("eligible") & F.col("rf_prediction").isNull(), F.struct(F.lit("RF").alias("stage"), F.lit("RF_SERIES_EXCLUDED").alias("reason_code"))),
        F.when(F.col("eligible") & F.col("baseline_prediction").isNull(), F.struct(F.lit("BASELINE").alias("stage"), F.lit("BASELINE_UNAVAILABLE").alias("reason_code"))),
        F.when(F.col("_label_present").isNull(), F.struct(F.lit("LABEL").alias("stage"), F.lit("MISSING_LABEL").alias("reason_code"))),
        F.when(F.col("_label_present").isNotNull() & ~F.col("label_valid"), F.struct(F.lit("LABEL").alias("stage"), F.lit("INVALID_LABEL").alias("reason_code")))
    ), lambda e: e.isNotNull())).alias("exception")).select(*KEYS, "exception.*")
    return AlignmentResult(paired, audit, coverage_from_audit(audit), exceptions)


def coverage_from_audit(audit):
    from pyspark.sql import functions as F
    def count(condition):
        return F.sum(F.when(condition, 1).otherwise(0)).cast("long")
    series = audit.groupBy("issue_date", "item_id", "store_id").agg(count(F.col("paired")).alias("pairs"))
    series = series.groupBy("issue_date").agg(
        count(F.col("pairs") == 0).alias("fully_dropped_series"),
        count(F.col("pairs").between(1, 6)).alias("partially_paired_series"),
        count(F.col("pairs") == 7).alias("complete_paired_series"))
    frames = []
    for method, prediction in zip(METHODS, ("rf_prediction", "baseline_prediction")):
        predicted = F.col(prediction).isNotNull()
        frame = audit.groupBy("issue_date").agg(F.count("*").alias("candidate_rows"),
            count(F.col("eligible")).alias("eligible_rows"), count(predicted).alias("predicted_rows"),
            count(predicted & F.col("label_valid")).alias("label_aligned_rows"),
            count(F.col("paired")).alias("paired_rows"),
            count(F.col("rf_prediction").isNotNull() != F.col("baseline_prediction").isNotNull()).alias("prediction_difference_rows"))
        frames.append(frame.withColumn("method_id", F.lit(method))
            .withColumn("dropped_rows", F.col("candidate_rows") - F.col("paired_rows")).join(series, "issue_date"))
    return frames[0].unionByName(frames[1])


def aggregate_metrics(paired, context):
    """Return exactly 48 v1 metric rows plus squared-error sufficient statistics."""
    from pyspark.sql import functions as F
    methods = []
    for method, prediction in zip(METHODS, ("rf_prediction", "baseline_prediction")):
        methods.append(paired.select("horizon", "actual_units", F.lit(method).alias("method_id"),
            F.abs(F.col(prediction) - F.col("actual_units")).alias("ae"),
            F.pow(F.col(prediction) - F.col("actual_units"), 2).alias("se")))
    long = methods[0].unionByName(methods[1])
    expanded = long.unionByName(long.withColumn("horizon", F.lit(0)))
    sums = expanded.groupBy("method_id", "horizon").agg(F.count("*").alias("observation_count"),
        F.sum("ae").alias("sum_absolute_error"), F.sum("se").alias("sum_squared_error"),
        F.sum("actual_units").alias("sum_actual_units"))
    grid = paired.sparkSession.createDataFrame([(m, h) for m in METHODS for h in range(8)],
                                              "method_id string, horizon int")
    stats = grid.join(sums, ["method_id", "horizon"], "left").fillna(0)
    for name in ("sum_absolute_error", "sum_squared_error", "sum_actual_units"):
        _reject(stats.where(_bad(F.col(name))), "nonfinite metric sufficient statistics")
    metrics = stats.withColumn("metric_name", F.explode(F.array(*[F.lit(m) for m in ("MAE", "RMSE", "WAPE")])))
    n, sae, sse, actual = (F.col(n) for n in ("observation_count", "sum_absolute_error", "sum_squared_error", "sum_actual_units"))
    value = (F.when(F.col("metric_name") == "WAPE", F.when(actual > 0, 100.0 * (sae / actual)))
        .when(n > 0, F.when(F.col("metric_name") == "MAE", sae / n).otherwise(F.sqrt(sse / n))))
    metrics = (metrics.withColumn("metric_value", value).withColumn("evaluation_run_id", F.lit(context.evaluation_run_id))
        .withColumn("holdout_id", F.lit(context.holdout_id))
        .withColumn("model_run_id", F.when(F.col("method_id") == "random_forest", context.model_run_id))
        .withColumn("forecast_run_id", F.lit(None).cast("string"))
        .withColumn("evaluation_start", F.lit(context.window[0])).withColumn("evaluation_end", F.lit(context.window[1]))
        .withColumn("run_type", F.lit(context.run_type)).select(*FORECAST_METRICS.field_names))
    _reject(metrics.where(F.col("metric_value").isNotNull() & _bad(F.col("metric_value"))), "nonfinite metrics")
    return metrics, stats


def validate_metrics(metrics, paired, context):
    """Recompute from persisted pairs, including all empty slices and v1 lineage."""
    from pyspark.sql import functions as F
    expected, _ = aggregate_metrics(paired, context)
    if metrics.schema.simpleString() != expected.schema.simpleString() or metrics.count() != 48:
        raise ForecastError("persisted metrics schema/count differs from v1")
    _unique(metrics, list(FORECAST_METRICS.unique_key), "metrics")
    # Floating aggregation order may differ on reread; compare nullness and relative error.
    keys = list(FORECAST_METRICS.unique_key)
    joined = metrics.alias("a").join(expected.alias("b"), keys, "full")
    numeric = {"metric_value", "sum_absolute_error", "sum_actual_units"}
    bad = []
    for name in FORECAST_METRICS.field_names:
        if name in keys:
            continue
        a, b = F.col("a." + name), F.col("b." + name)
        if name in numeric:
            bad.append(~a.eqNullSafe(b) & (a.isNull() | b.isNull() | F.isnan(a) |
                (F.abs(a-b) > 1e-9 * F.greatest(F.lit(1.0), F.abs(b)))))
        else:
            bad.append(~a.eqNullSafe(b))
    _reject(joined.where(reduce(lambda a, b: a | b, bad)), "persisted metrics differ from paired recomputation")


def diagnostics(paired, cohort):
    """Compact dimensional errors and complete-seven-pair total-sales errors."""
    from pyspark.sql import functions as F
    selected = cohort.where("status = 'SELECTED'").select("item_id", "dept_id", "cat_id", "volume_tertile")
    enriched = paired.join(selected, "item_id")
    groups = []
    for dimension in ("store_id", "dept_id", "cat_id", "volume_tertile"):
        for method, prediction in zip(METHODS, ("rf_prediction", "baseline_prediction")):
            groups.append(enriched.groupBy(dimension).agg(F.count("*").alias("observation_count"),
                F.sum(F.abs(F.col(prediction)-F.col("actual_units"))).alias("sum_absolute_error"),
                F.sum(F.pow(F.col(prediction)-F.col("actual_units"), 2)).alias("sum_squared_error"),
                F.sum("actual_units").alias("sum_actual_units"))
                .withColumnRenamed(dimension, "dimension_value").withColumn("dimension_value", F.col("dimension_value").cast("string"))
                .withColumn("dimension", F.lit(dimension)).withColumn("method_id", F.lit(method)))
    totals = (paired.groupBy("item_id", "store_id", "issue_date").agg(F.count("*").alias("pair_count"),
        F.sum("actual_units").alias("actual_7d_units"), F.sum("rf_prediction").alias("rf_7d_units"),
        F.sum("baseline_prediction").alias("baseline_7d_units")).where("pair_count = 7")
        .withColumn("rf_total_error", F.col("rf_7d_units") - F.col("actual_7d_units"))
        .withColumn("baseline_total_error", F.col("baseline_7d_units") - F.col("actual_7d_units")))
    return reduce(lambda a, b: a.unionByName(b), groups), totals

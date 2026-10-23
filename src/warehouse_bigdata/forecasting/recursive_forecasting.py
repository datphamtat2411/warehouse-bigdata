"""Seven Spark-batched inference steps; only predicted doubles advance demand state."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import reduce
from operator import or_

from warehouse_bigdata.contracts.datasets import FORECAST_DAILY

KEYS = ["item_id", "store_id"]
DAILY_TYPES = (
    "string",
    "string",
    "string",
    "string",
    "date",
    "date",
    "int",
    "string",
    "string",
    "double",
    "double",
    "string",
    "timestamp",
)


class ForecastError(ValueError):
    """Structural inference, lineage or persisted-output contract failure."""


@dataclass(frozen=True)
class ForecastRunContext:
    forecast_run_id: str
    model_run_id: str
    source_snapshot_id: str
    feature_version: str
    issue_date: date
    run_type: str
    created_at: datetime


@dataclass(frozen=True)
class RecursiveForecastResult:
    daily: object
    exceptions: object
    coverage: object
    series_scope: object
    retained: tuple

    def release(self):
        for frame in reversed(self.retained):
            frame.unpersist()


def _bad_number(column):
    from pyspark.sql import functions as F

    return column.isNull() | F.isnan(column) | (F.abs(column) > 1.7976931348623157e308)


def validate_forecast_daily(frame, *, run_context, series_scope):
    """Distributed v1 schema, identity, finite values and complete-series validation."""
    from pyspark.sql import functions as F

    ctx = run_context
    if ctx.run_type not in {"validation", "test", "forward_demo"}:
        raise ForecastError("unsupported forecast run_type")
    if (
        tuple(frame.columns) != FORECAST_DAILY.field_names
        or tuple(f.dataType.simpleString() for f in frame.schema) != DAILY_TYPES
    ):
        raise ForecastError("forecast_daily physical schema differs from Contract v1")
    if (
        ctx.created_at.tzinfo is None
        or ctx.created_at.utcoffset() != timedelta(0)
        or frame.sparkSession.conf.get("spark.sql.session.timeZone") not in {"UTC", "Etc/UTC"}
    ):
        raise ForecastError("forecast timestamps require UTC")
    invalid = [F.col(n).isNull() for n in FORECAST_DAILY.required_field_names]
    for name in (
        "forecast_run_id",
        "model_run_id",
        "source_snapshot_id",
        "feature_version",
        "issue_date",
        "run_type",
        "created_at",
    ):
        invalid.append(~F.col(name).eqNullSafe(F.lit(getattr(ctx, name))))
    invalid += [
        ~F.col("horizon").between(1, 7),
        F.col("forecast_date") != F.date_add("issue_date", F.col("horizon")),
        _bad_number(F.col("raw_prediction")),
        _bad_number(F.col("predicted_units")),
        F.col("predicted_units") != F.greatest(F.lit(0.0), F.col("raw_prediction")),
    ]
    for name in KEYS:
        invalid.append(F.col(name).rlike(r"^\s*$"))
    if frame.where(reduce(or_, invalid)).limit(1).count():
        raise ForecastError("invalid forecast values or run lineage")
    if frame.groupBy(*FORECAST_DAILY.unique_key).count().where("count != 1").limit(1).count():
        raise ForecastError("duplicate forecast unique key")
    series = frame.groupBy(*KEYS).agg(
        F.count("*").alias("n"), F.countDistinct("horizon").alias("h")
    )
    if series.where("n != 7 OR h != 7").limit(1).count():
        raise ForecastError("forecast series must have complete seven horizons")
    if series.select(*KEYS).join(series_scope, KEYS, "left_anti").limit(1).count():
        raise ForecastError("forecast contains a series outside the fixed scope")
    return {"row_count": frame.count(), "successful_series": series.count()}


def forecast_recursive(model, prepared_history, *, known_calendar, run_context):
    """Caller owns returned compact caches and must release them after staging.

    Pipeline/schema exceptions fail the run. Series-attributable diagnostics stop
    that series, and a later failure removes all its earlier horizon outputs.
    """
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    from warehouse_bigdata.forecasting.scoring_features import (
        PREDICTED_ORIGIN,
        build_scoring_features,
    )

    ctx = run_context
    cached = []
    checkpoints = []

    def materialize(frame):
        frame = frame.persist(StorageLevel.DISK_ONLY)
        cached.append(frame)
        frame.count()
        return frame

    def bounded_state(frame):
        # Truncate the recursive logical plan, too: caching alone leaves each
        # new feature/eligibility join referencing all preceding horizons.
        checkpoint = frame.localCheckpoint(eager=True)
        checkpoints.append(checkpoint._jdf.logicalPlan().rdd())
        return checkpoint

    def release_state():
        checkpoints.pop(0).unpersist(False)

    def diagnose(eligibility, stage, horizon):
        return eligibility.where("status = 'EXCLUDED'").select(
            *KEYS,
            F.lit(stage).alias("stage"),
            F.lit(horizon).cast("int").alias("horizon"),
            F.explode("reason_codes").alias("reason_code"),
        )

    try:
        scope = materialize(prepared_history.eligibility.select(*KEYS))
        state = bounded_state(prepared_history.state)
        if (
            state.where(
                (F.col("source_snapshot_id") != ctx.source_snapshot_id)
                | (F.col("feature_version") != ctx.feature_version)
            )
            .limit(1)
            .count()
        ):
            raise ForecastError("prepared history lineage conflicts with run")
        exceptions = [materialize(diagnose(prepared_history.eligibility, "PREPARATION", 0))]
        outputs = []
        for horizon in range(1, 8):
            target = ctx.issue_date + timedelta(days=horizon)
            scoring = build_scoring_features(
                state, issue_date=ctx.issue_date, target_date=target, known_calendar=known_calendar
            )
            exceptions.append(materialize(diagnose(scoring.eligibility, "FEATURES", horizon)))
            transformed = model.transform(scoring.features)
            if (
                "raw_prediction" not in transformed.columns
                or transformed.schema["raw_prediction"].dataType.simpleString() != "double"
            ):
                raise ForecastError("fitted pipeline must output double raw_prediction")
            raw = materialize(transformed.select(*KEYS, "raw_prediction"))
            if (
                raw.groupBy(*KEYS).count().where("count != 1").limit(1).count()
                or raw.select(*KEYS).exceptAll(scoring.features.select(*KEYS)).limit(1).count()
                or scoring.features.select(*KEYS).exceptAll(raw.select(*KEYS)).limit(1).count()
            ):
                raise ForecastError("pipeline changed the scoring series keys")
            bad = _bad_number(F.col("raw_prediction"))
            exceptions.append(
                materialize(
                    raw.where(bad).select(
                        *KEYS,
                        F.lit("PREDICTION").alias("stage"),
                        F.lit(horizon).alias("horizon"),
                        F.lit("NONFINITE_PREDICTION").alias("reason_code"),
                    )
                )
            )
            good = raw.where(~bad).withColumn(
                "predicted_units", F.greatest(F.lit(0.0), F.col("raw_prediction"))
            )
            daily = good.select(
                F.lit(ctx.forecast_run_id).alias("forecast_run_id"),
                F.lit(ctx.model_run_id).alias("model_run_id"),
                F.lit(ctx.source_snapshot_id).alias("source_snapshot_id"),
                F.lit(ctx.feature_version).alias("feature_version"),
                F.lit(ctx.issue_date).alias("issue_date"),
                F.lit(target).alias("forecast_date"),
                F.lit(horizon).alias("horizon"),
                *KEYS,
                "raw_prediction",
                "predicted_units",
                F.lit(ctx.run_type).alias("run_type"),
                F.lit(ctx.created_at).cast("timestamp").alias("created_at"),
            )
            outputs.append(materialize(daily))
            if horizon < 7:
                entry = F.struct(
                    F.lit(target).alias("date"),
                    F.col("predicted_units").alias("value"),
                    F.lit(PREDICTED_ORIGIN).alias("origin"),
                    F.lit(ctx.issue_date).alias("prediction_issue_date"),
                )
                next_state = bounded_state(
                    state.join(good.select(*KEYS, "predicted_units"), KEYS)
                    .withColumn("history", F.concat(F.slice("history", 2, 27), F.array(entry)))
                    .select(*state.columns)
                )
                release_state()
                state = next_state
            raw.unpersist()
        all_daily = reduce(lambda a, b: a.unionByName(b), outputs)
        complete = outputs[-1].select(*KEYS)
        daily = materialize(
            all_daily.join(complete, KEYS, "left_semi").select(*FORECAST_DAILY.field_names)
        )
        details = materialize(reduce(lambda a, b: a.unionByName(b), exceptions).distinct())
        successful = daily.select(*KEYS).distinct()
        excluded = details.select(*KEYS).distinct()
        if (
            scope.count() != successful.count() + excluded.count()
            or successful.join(excluded, KEYS).limit(1).count()
            or excluded.join(scope, KEYS, "left_anti").limit(1).count()
        ):
            raise ForecastError("fixed scope coverage does not reconcile with explicit exclusions")
        counts = validate_forecast_daily(daily, run_context=ctx, series_scope=scope)
        coverage = materialize(
            scope.agg(F.count("*").alias("candidate_series"))
            .withColumn("successful_series", F.lit(counts["successful_series"]).cast("long"))
            .withColumn("excluded_series", F.col("candidate_series") - F.col("successful_series"))
            .withColumn("row_count", F.lit(counts["row_count"]).cast("long"))
        )
        retained = (scope, daily, details, coverage)
        for frame in reversed(cached):
            if all(frame is not keep for keep in retained):
                frame.unpersist()
        while checkpoints:
            release_state()
        return RecursiveForecastResult(daily, details, coverage, scope, retained)
    except Exception:
        for frame in reversed(cached):
            frame.unpersist()
        while checkpoints:
            release_state()
        raise

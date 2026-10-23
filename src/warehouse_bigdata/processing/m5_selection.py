"""Spark boundary for bounded metadata selection before wide-to-long expansion."""

from warehouse_bigdata.contracts.dataset_scope import (
    HISTORICAL_DAYS,
    IDENTITY_FIELDS,
    ScopeError,
    select_scope,
    validate_scope,
)


def resolve_scope(sales_wide, **selection):
    # Only 30,490 × six identity strings cross the driver boundary, never sales labels.
    records = sales_wide.select(*IDENTITY_FIELDS).limit(30491).collect()
    if len(records) != 30490:
        raise ScopeError("validated real M5 source must have exactly 30,490 wide series")
    return select_scope([r.asDict() for r in records], **selection)


def filter_selected_wide(sales_wide, scope):
    from pyspark.sql import functions as F

    validate_scope(scope)
    return sales_wide.where(F.col("item_id").isin(scope["selected_item_ids"]))


def validate_scope_population(
    frame, scope, *, wide=False, complete_history=False, allow_subset=False
):
    """Verify actual membership/metadata; daily gates also prove every historical key."""
    from pyspark.sql import functions as F

    validate_scope(scope)
    fields = [name for name in IDENTITY_FIELDS if name in frame.columns]
    if not {"item_id", "store_id"} <= set(fields):
        raise ScopeError("scope population requires item/store keys")
    if "series_id" in frame.columns:
        actual = frame.withColumn("id", F.col("series_id"))
        fields = ["id", *fields]
    else:
        actual = frame
    observed = actual.select(*fields).distinct().limit(scope["selected_series_count"] + 1).collect()
    expected = {tuple(r[n] for n in fields) for r in scope["selected_series"]}
    values = {tuple(r[n] for n in fields) for r in observed}
    if (not values <= expected) or (not allow_subset and values != expected):
        raise ScopeError("persisted population differs from frozen selection")
    if wide and (
        frame.count() != scope["selected_series_count"]
        or any(f"d_{d}" not in frame.columns for d in range(1, HISTORICAL_DAYS + 1))
    ):
        raise ScopeError("filtered wide population/count/day fields differ from scope")
    if complete_history:
        if (
            "date" in frame.columns
            and frame.where(
                F.col("date").isNull()
                | (
                    F.col("date")
                    != F.date_add(F.lit("2011-01-29").cast("date"), F.col("day_index") - 1)
                )
            )
            .limit(1)
            .count()
        ):
            raise ScopeError("scope historical day/date alignment mismatch")
        summary = frame.groupBy("item_id", "store_id").agg(
            F.count("*").alias("rows"),
            F.countDistinct("day_index").alias("days"),
            F.min("day_index").alias("first"),
            F.max("day_index").alias("last"),
        )
        if (
            summary.where(
                f"rows != {HISTORICAL_DAYS} or days != {HISTORICAL_DAYS} or first != 1 or last != {HISTORICAL_DAYS}"
            )
            .limit(1)
            .count()
        ):
            raise ScopeError("scope requires complete, unique d_1 through d_1941 history")

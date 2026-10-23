"""Independent, full-fixture d_1–d_30 oracle; no production feature imports.

Fixed calendar-day conditional aggregates intentionally differ from T01's range
windows. Each group has exactly 30 observations (checked before this oracle).
All expected/actual comparisons stay distributed; only four failure samples or
scalar counts reach the driver.
"""

from datetime import date, timedelta
from functools import reduce
from operator import or_

FIRST_DATE = date(2011, 1, 29)
KEY = [
    "source_snapshot_id",
    "feature_version",
    "feature_role",
    "item_id",
    "store_id",
    "target_date",
]


def no_rows(frame, message):
    sample = frame.limit(4).collect()
    assert not sample, f"{message}; samples={[row.asDict() for row in sample]}"


def expected_features(source, snapshot, version):
    from pyspark.sql import functions as F

    targets = []
    for day in (29, 30):
        target = FIRST_DATE + timedelta(days=day - 1)
        index = F.col("day_index")
        price = F.max(
            F.when(
                (index < day) & F.col("sell_price").isNotNull(),
                F.struct(index, F.col("sell_price")),
            )
        ).getField("sell_price")
        frame = (
            source.groupBy("item_id", "store_id")
            .agg(
                F.max(F.when(index == day, F.col("dept_id"))).alias("dept_id"),
                *[
                    F.max(F.when(index == day - lag, F.col("unit_sales"))).alias(f"lag_{lag}")
                    for lag in (1, 7, 28)
                ],
                *[
                    (
                        F.sum(F.when(index.between(day - window, day - 1), F.col("unit_sales")))
                        / F.lit(window)
                    )
                    .cast("double")
                    .alias(f"rolling_mean_{window}")
                    for window in (7, 28)
                ],
                price.alias("last_known_price"),
                F.max(F.when(index == day, F.col("unit_sales"))).alias("label"),
            )
            .select(
                F.lit(snapshot).alias("source_snapshot_id"),
                F.lit(version).alias("feature_version"),
                F.lit("train").alias("feature_role"),
                "item_id",
                "store_id",
                "dept_id",
                F.lit(target).alias("target_date"),
                F.lit(target - timedelta(days=1)).alias("as_of_date"),
                "lag_1",
                "lag_7",
                "lag_28",
                "rolling_mean_7",
                "rolling_mean_28",
                F.lit(target.isoweekday()).alias("day_of_week"),
                F.lit(target.month).alias("month"),
                F.lit(target.isoweekday() in (6, 7)).alias("is_weekend"),
                F.lit(False).alias("event_flag"),
                F.col("last_known_price").isNull().alias("price_missing"),
                "last_known_price",
                "label",
            )
        )
        targets.append(frame)
    return targets[0].unionByName(targets[1])


def verify_oracle(source, features, snapshot, version):
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    expected = expected_features(source, snapshot, version).persist(StorageLevel.DISK_ONLY)
    try:
        assert features.columns == expected.columns, "Independent oracle fields/order differ"
        no_rows(
            features.groupBy(*KEY).count().where("count != 1"), "Independent oracle duplicate keys"
        )
        wanted = expected.withColumn("_expected", F.lit(True)).alias("e")
        actual = features.withColumn("_actual", F.lit(True)).alias("a")
        joined = actual.join(wanted, KEY, "full")
        differences = [F.col("_actual").isNull(), F.col("_expected").isNull()]
        for name in features.columns:
            if name in KEY:
                continue
            left, right = F.col(f"a.{name}"), F.col(f"e.{name}")
            if name in ("rolling_mean_7", "rolling_mean_28"):
                # Match math.isclose(abs_tol=rel_tol=1e-12), with explicit null/NaN checks.
                correct = (
                    left.isNotNull()
                    & right.isNotNull()
                    & ~F.isnan(left)
                    & (F.abs(left) <= 1.7976931348623157e308)
                    & (
                        F.abs(left - right)
                        <= F.greatest(
                            F.lit(1e-12), F.lit(1e-12) * F.greatest(F.abs(left), F.abs(right))
                        )
                    )
                )
            else:
                correct = left.eqNullSafe(right)
            differences.append(~F.coalesce(correct, F.lit(False)))
        no_rows(
            joined.where(reduce(or_, differences)),
            "Independent oracle key/value/nullability mismatch",
        )
        return {
            "method": "full_distributed_conditional_aggregates",
            "target_days": [29, 30],
            "compared_rows": expected.count(),
            "compared_fields": list(features.columns),
            "rolling_abs_tolerance": 1e-12,
            "rolling_rel_tolerance": 1e-12,
        }
    finally:
        expected.unpersist()

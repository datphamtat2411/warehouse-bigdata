"""Independent complete-history oracle using row offsets on validated daily keys.

Production uses date-range windows and eligibility rules; this oracle relies on the
separately verified complete daily M5 Small history. It also supports tiny unit inputs.
"""

from functools import reduce
from operator import or_

KEY = [
    "source_snapshot_id",
    "feature_version",
    "feature_role",
    "item_id",
    "store_id",
    "target_date",
]


def no_rows(frame, message):
    samples = frame.limit(4).collect()
    assert not samples, f"{message}; samples={[r.asDict() for r in samples]}"


def expected_features(source, snapshot, version):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    window = Window.partitionBy("item_id", "store_id").orderBy("date")
    frame = source
    for lag in (1, 7, 28):
        frame = frame.withColumn(f"lag_{lag}", F.lag("unit_sales", lag).over(window))
    for width in (7, 28):
        frame = frame.withColumn(
            f"rolling_mean_{width}", F.avg("unit_sales").over(window.rowsBetween(-width, -1))
        )
    frame = frame.withColumn(
        "last_known_price",
        F.last("sell_price", ignorenulls=True).over(
            window.rowsBetween(Window.unboundedPreceding, -1)
        ),
    )
    target = F.col("date")
    weekday = F.pmod(F.dayofweek(target) + 5, F.lit(7)) + 1
    return frame.where(F.col("day_index").between(29, 1941)).select(
        F.lit(snapshot).alias("source_snapshot_id"),
        F.lit(version).alias("feature_version"),
        F.when(F.col("day_index") <= 1885, "train")
        .when(F.col("day_index") <= 1913, "validation")
        .otherwise("test")
        .alias("feature_role"),
        "item_id",
        "store_id",
        "dept_id",
        target.alias("target_date"),
        F.date_sub(target, 1).alias("as_of_date"),
        "lag_1",
        "lag_7",
        "lag_28",
        "rolling_mean_7",
        "rolling_mean_28",
        weekday.alias("day_of_week"),
        F.month(target).alias("month"),
        weekday.isin(6, 7).alias("is_weekend"),
        F.lit(False).alias("event_flag"),
        F.col("last_known_price").isNull().alias("price_missing"),
        "last_known_price",
        F.col("unit_sales").alias("label"),
    )


def verify_oracle(source, features, snapshot, version):
    from pyspark.sql import functions as F

    expected = expected_features(source, snapshot, version)
    assert features.columns == expected.columns, "Independent oracle fields/order differ"
    no_rows(features.groupBy(*KEY).count().where("count != 1"), "Independent oracle duplicate keys")
    joined = (
        features.withColumn("_actual", F.lit(True))
        .alias("a")
        .join(expected.withColumn("_expected", F.lit(True)).alias("e"), KEY, "full")
    )
    differences = [F.col("_actual").isNull(), F.col("_expected").isNull()]
    for name in features.columns:
        if name in KEY:
            continue
        left, right = F.col(f"a.{name}"), F.col(f"e.{name}")
        if name in ("rolling_mean_7", "rolling_mean_28"):
            correct = (
                left.isNotNull()
                & right.isNotNull()
                & ~F.isnan(left)
                & ~F.isnan(right)
                & (F.abs(left) <= 1.7976931348623157e308)
                & (F.abs(right) <= 1.7976931348623157e308)
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
        joined.where(reduce(or_, differences)), "Independent oracle key/value/nullability mismatch"
    )
    return dict(
        method="complete_daily_row_offset_windows",
        compared_rows=expected.count(),
        compared_fields=list(features.columns),
        rolling_abs_tolerance=1e-12,
        rolling_rel_tolerance=1e-12,
    )

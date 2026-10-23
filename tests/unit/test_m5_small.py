"""Offline checks for canonical real-data scope using metadata-only synthetic inputs."""

import copy
import hashlib
import json
from datetime import date, timedelta

import pytest

from warehouse_bigdata.contracts.dataset_scope import (
    DEMO_SAMPLE,
    DEPARTMENTS,
    FULL_M5,
    STORES,
    ScopeError,
    require_scope_execution,
    select_scope,
    validate_scope,
)


def identities():
    return [
        dict(
            id=f"{dept}_{item:03}_{store}_evaluation",
            item_id=f"{dept}_{item:03}",
            dept_id=dept,
            cat_id=dept.rsplit("_", 1)[0],
            store_id=store,
            state_id=store[:2],
        )
        for dept in DEPARTMENTS
        for item in range(3)
        for store in STORES
    ]


def evidence(snapshot="small-snapshot"):
    return select_scope(
        identities(), source_version="sha256:" + "a" * 64, source_snapshot_id=snapshot
    )


def test_selection_is_order_independent_balanced_and_label_free():
    source = identities()
    selected = evidence()
    assert selected == select_scope(
        list(reversed(source)),
        source_version=selected["source_version"],
        source_snapshot_id=selected["source_snapshot_id"],
    )
    assert selected["dataset_scope"] == DEMO_SAMPLE
    assert selected["selected_sku_count"] == 10
    assert selected["selected_series_count"] == 100
    assert selected["selected_store_ids"] == list(STORES)
    assert selected["selected_department_ids"] == list(DEPARTMENTS)
    assert selected["historical_day_count"] == 1941
    assert selected["expected_observation_count"] == 194100
    assert selected["excluded_series_count"] == len(source) - 100
    assert sorted(
        sum(r["dept_id"] == d for r in selected["selected_series"]) for d in DEPARTMENTS
    ) == [10, 10, 10, 10, 20, 20, 20]
    poisoned = [{**r, "d_1941": "999999", "validation_score": -99} for r in source]
    assert selected == select_scope(
        poisoned,
        source_version=selected["source_version"],
        source_snapshot_id=selected["source_snapshot_id"],
    )
    payload = {k: v for k, v in selected.items() if k != "selection_digest"}
    canonical = (
        json.dumps(
            payload, sort_keys=True, ensure_ascii=True, separators=(",", ":"), allow_nan=False
        )
        + "\n"
    ).encode()
    assert selected["selection_digest"] == "sha256:" + hashlib.sha256(canonical).hexdigest()


@pytest.mark.parametrize(
    "corruption", ["duplicate", "missing_store", "metadata", "duplicate_id", "unknown_store"]
)
def test_selection_rejects_invalid_identity_universe(corruption):
    source = identities()
    if corruption == "duplicate":
        source.append(source[0])
    elif corruption == "missing_store":
        source.pop()
    elif corruption == "metadata":
        source[0] = {**source[0], "dept_id": "FOODS_2"}
    elif corruption == "duplicate_id":
        source[0] = {**source[0], "id": source[1]["id"]}
    else:
        source[0] = {**source[0], "store_id": "UNKNOWN"}
    with pytest.raises(ScopeError):
        select_scope(source, source_version="sha256:" + "a" * 64, source_snapshot_id="small")


def test_scope_digest_lineage_and_counts_are_fail_closed():
    scope = evidence()
    validate_scope(
        scope, source_version=scope["source_version"], source_snapshot_id="small-snapshot"
    )
    for field, value in (
        ("selected_series_count", 101),
        ("historical_day_count", 30),
        ("expected_observation_count", 914700),
        ("selection_seed", 43),
    ):
        bad = {**scope, field: value}
        with pytest.raises(ScopeError):
            validate_scope(bad)
    with pytest.raises(ScopeError, match="lineage"):
        validate_scope(scope, source_snapshot_id="other")
    bad = copy.deepcopy(scope)
    bad["selected_item_ids"].reverse()
    with pytest.raises(ScopeError):
        validate_scope(bad)


def test_full_scope_needs_two_explicit_opt_ins_and_preserves_reference():
    require_scope_execution(DEMO_SAMPLE)
    with pytest.raises(ScopeError, match="confirm"):
        require_scope_execution(FULL_M5)
    with pytest.raises(ScopeError):
        require_scope_execution(None, confirm_large_workload=True)
    require_scope_execution(FULL_M5, confirm_large_workload=True)
    from warehouse_bigdata.ingestion.m5_source import REFERENCE_DERIVED_OBSERVATIONS
    from warehouse_bigdata.processing.normalization_snapshot import NormalizationSnapshotConfig

    assert NormalizationSnapshotConfig().row_count == 194100
    assert REFERENCE_DERIVED_OBSERVATIONS == 59181090


def test_filtering_precedes_expansion_and_preserves_all_day_columns(monkeypatch):
    from warehouse_bigdata.processing.m5_selection import filter_selected_wide

    class Wide:
        columns = ["item_id", *[f"d_{day}" for day in range(1, 1942)]]

        def where(self, condition):
            assert condition == ("item_id", evidence()["selected_item_ids"])
            return self

    class Column:
        def isin(self, values):
            return ("item_id", values)

    monkeypatch.setattr("pyspark.sql.functions.col", lambda name: Column())
    frame = Wide()
    result = filter_selected_wide(frame, evidence())
    assert result.columns == frame.columns
    assert "unpivot" not in filter_selected_wide.__code__.co_names
    assert len(Wide.columns) - 1 == 1941


def test_frozen_demo_cohort_uses_same_ten_items_without_reselection(spark):
    from warehouse_bigdata.forecasting.forecast_cohort import (
        TRAINING_END,
        select_forecast_cohort,
        validate_cohort_selection,
    )

    scope = evidence()
    rows = [{**r, "date": TRAINING_END} for r in scope["selected_series"]]
    source = spark.createDataFrame(rows)
    selected = select_forecast_cohort(
        source, training_end=TRAINING_END, seed=42, scope_evidence=scope
    )
    validate_cohort_selection(selected.evidence, selected.scope, scope_evidence=scope)
    assert selected.scope.count() == 100
    assert {r.item_id for r in selected.scope.select("item_id").distinct().collect()} == set(
        scope["selected_item_ids"]
    )


def test_scope_population_and_j03_j04_boundaries(spark):
    from pyspark.sql import functions as F

    from warehouse_bigdata.forecasting.features import (
        LOOKBACK_DAYS,
        TEST_DAYS,
        TRAIN_DAYS,
        VALIDATION_DAYS,
    )
    from warehouse_bigdata.processing.m5_selection import (
        filter_selected_wide,
        validate_scope_population,
    )

    scope = evidence()
    frame = spark.createDataFrame(identities())
    filtered = filter_selected_wide(frame, scope)
    assert filtered.count() == 100
    validate_scope_population(filtered, scope)
    with pytest.raises(ScopeError):
        validate_scope_population(filtered.unionByName(filtered.limit(1)), scope, wide=True)
    with pytest.raises(ScopeError):
        validate_scope_population(filtered.where(F.col("store_id") != STORES[0]), scope)
    with pytest.raises(ScopeError, match="complete"):
        validate_scope_population(
            filtered.withColumn("day_index", F.lit(1)).withColumn(
                "date", F.lit("2011-01-29").cast("date")
            ),
            scope,
            complete_history=True,
        )
    assert LOOKBACK_DAYS == 28
    assert (TRAIN_DAYS, VALIDATION_DAYS, TEST_DAYS) == ((29, 1885), (1886, 1913), (1914, 1941))
    assert date(2011, 1, 29) + timedelta(days=1940) == date(2016, 5, 22)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("m5-small-synthetic")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()

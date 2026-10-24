"""Independent persisted-artifact gates for M06 distributed acceptance.

This module is deliberately read-only.  It validates bounded evidence from the
frozen J03/J04/J05/J06/evaluation chain and never fits a model or publishes an
artifact.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from collections.abc import Mapping
from datetime import date, timedelta

from warehouse_bigdata.common.hdfs_publication import document_digest
from warehouse_bigdata.contracts.dataset_scope import ScopeError, validate_scope

EXPECTED_STORES = (
    "CA_1",
    "CA_2",
    "CA_3",
    "CA_4",
    "TX_1",
    "TX_2",
    "TX_3",
    "WI_1",
    "WI_2",
    "WI_3",
)
EXPECTED_ISSUE_DATE = date(2016, 5, 22)
EXPECTED_HISTORICAL_DAYS = 1941
EXPECTED_OBSERVATIONS = 194100
EXPECTED_SERIES = 100
EXPECTED_SKUS = 10
TRAINING_START_DAY = 29
TRAINING_END_DAY = 1885
PREDICTION_TOLERANCE = 1e-9
_MAX_DOUBLE = 1.7976931348623157e308


class AcceptanceError(ScopeError):
    """Raised when persisted M06 evidence cannot be independently accepted."""


def _fail(message: str) -> None:
    raise AcceptanceError(message)


def _mapping(value):
    if hasattr(value, "asDict"):
        return value.asDict(recursive=True)
    if isinstance(value, Mapping):
        return dict(value)
    _fail("acceptance evidence rows must be mappings")


def _date(value, name):
    if isinstance(value, date) and not hasattr(value, "hour"):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            pass
    _fail(f"{name} must be an ISO date")


def _finite_nonnegative(value, name):
    value = _finite_number(value, name)
    if value < 0:
        _fail(f"{name} must be finite and nonnegative")
    return value


def _finite_number(value, name):
    if type(value) not in (int, float) or not math.isfinite(float(value)):
        _fail(f"{name} must be finite")
    return float(value)


def _key(row, fields):
    return tuple(row.get(field) for field in fields)


def validate_canonical_scope(scope, *, expected_original_series_count=30490):
    """Require the immutable ten-SKU/full-history M5 Small population.

    Complete HDFS sidecars are validated with :func:`validate_scope`; the small
    shape fallback keeps this gate usable with tiny deterministic unit fixtures.
    """

    if not isinstance(scope, Mapping) or scope.get("dataset_scope") != "demo_sample":
        _fail("acceptance requires the demo_sample scope")
    expected = {
        "selected_sku_count": EXPECTED_SKUS,
        "selected_series_count": EXPECTED_SERIES,
        "selected_store_ids": list(EXPECTED_STORES),
        "historical_day_count": EXPECTED_HISTORICAL_DAYS,
        "expected_observation_count": EXPECTED_OBSERVATIONS,
        "original_series_count": expected_original_series_count,
    }
    for name, value in expected.items():
        if scope.get(name) != value:
            category = "history" if name == "historical_day_count" else "scope"
            _fail(f"canonical {category} differs: {name}")
    selected = scope.get("selected_series")
    if not isinstance(selected, list) or len(selected) != EXPECTED_SERIES:
        _fail("canonical scope must contain exactly 100 selected series")
    pairs = {
        (row.get("item_id"), row.get("store_id")) for row in selected if isinstance(row, Mapping)
    }
    items = {item for item, _ in pairs}
    stores = {store for _, store in pairs}
    if (
        len(pairs) != EXPECTED_SERIES
        or len(items) != EXPECTED_SKUS
        or stores != set(EXPECTED_STORES)
    ):
        _fail("canonical scope selected-series membership is not 10 items by 10 stores")
    if any(
        sum(item == candidate for item, _ in pairs) != len(EXPECTED_STORES) for candidate in items
    ):
        _fail("canonical scope does not retain every store for every selected item")
    if "excluded_series_count" in scope and scope["excluded_series_count"] != 30390:
        _fail("canonical scope excluded-series count differs")
    # Full sidecars carry all v1 fields and a self-validating digest.
    required = {
        "schema_version",
        "selection_policy",
        "selection_seed",
        "source_version",
        "source_snapshot_id",
        "selected_item_ids",
        "selected_department_ids",
        "available_department_ids",
        "selection_digest",
    }
    if required <= set(scope):
        try:
            validate_scope(dict(scope))
        except ScopeError as error:
            raise AcceptanceError(
                f"canonical scope digest or lineage is invalid: {error}"
            ) from error
    return dict(scope)


def _selection_key(row):
    return _key(
        row,
        (
            "source_snapshot_id",
            "feature_version",
            "feature_role",
            "item_id",
            "store_id",
            "target_date",
        ),
    )


def validate_training_selection(
    rows,
    *,
    selected_feature_keys,
    summary,
    cap=10000,
    seed=42,
):
    """Validate a bounded persisted T01 selection against J04 training keys."""

    selected = [_mapping(row) for row in rows]
    if not selected:
        _fail("training selection must contain at least one row")
    feature_keys = {_selection_key(_mapping(row)) for row in selected_feature_keys}
    actual_keys = [_selection_key(row) for row in selected]
    if len(actual_keys) != len(set(actual_keys)):
        _fail("training selection contains duplicate keys")
    if any(
        row.get("feature_role") != "train"
        or not TRAINING_START_DAY
        <= (_date(row.get("target_date"), "training target_date") - date(2011, 1, 29)).days + 1
        <= TRAINING_END_DAY
        for row in selected
    ):
        _fail("training selection labels must remain in d_29-d_1885")
    if any(None in key for key in actual_keys) or not set(actual_keys) <= feature_keys:
        _fail("training selection keys are not members of selected J04 training rows")
    if not isinstance(summary, Mapping) or summary.get("selected_count") != len(selected):
        _fail("training selection count differs from persisted summary")
    if len(selected) > cap or summary.get("selected_count", 0) > cap:
        _fail("training selection exceeds the 10,000-row cap")
    if summary.get("seed") != seed or seed != 42:
        _fail("training selection seed differs from frozen seed 42")
    return {
        "selected_count": len(selected),
        "selected_series_count": len(
            {(row.get("item_id"), row.get("store_id")) for row in selected}
        ),
        "seed": seed,
        "training_start_day": TRAINING_START_DAY,
        "training_end_day": TRAINING_END_DAY,
    }


def validate_training_selection_frame(selection, features, *, summary, cap=10000, seed=42):
    """Distributed counterpart of :func:`validate_training_selection`."""

    from pyspark.sql import functions as F

    from warehouse_bigdata.forecasting.feature_validation import SCOPED_KEY

    if tuple(selection.columns[: len(SCOPED_KEY)]) != SCOPED_KEY or not set(SCOPED_KEY) <= set(
        selection.columns
    ):
        _fail("training selection schema/key order differs from J04")
    selected_count = selection.count()
    if selected_count <= 0:
        _fail("training selection must contain at least one row")
    if selected_count > cap:
        _fail("training selection exceeds the 10,000-row cap")
    if selection.groupBy(*SCOPED_KEY).count().where("count != 1").limit(1).count():
        _fail("training selection contains duplicate keys")
    if (
        selection.where(
            (F.col("feature_role") != "train")
            | ~F.col("target_date").between("2011-02-26", "2016-03-27")
        )
        .limit(1)
        .count()
    ):
        _fail("training selection labels must remain in d_29–d_1885")
    if selection.join(features.select(*SCOPED_KEY), list(SCOPED_KEY), "left_anti").limit(1).count():
        _fail("training selection keys are not members of J04")
    if not isinstance(summary, Mapping) or summary.get("selected_count") != selected_count:
        _fail("training selection summary differs from persisted keys")
    if summary.get("seed") != seed or seed != 42:
        _fail("training selection summary differs from persisted keys")
    return {
        "selected_count": selected_count,
        "selected_series_count": selection.select("item_id", "store_id").distinct().count(),
        "seed": seed,
    }


def compare_prediction_values(
    actual, reference, *, tolerance=PREDICTION_TOLERANCE, name="prediction"
):
    """Compare persisted and fresh floating predictions using the locked tolerance."""

    if not math.isfinite(float(actual)) or not math.isfinite(float(reference)):
        _fail(f"{name} is not finite")
    bound = tolerance * max(1.0, abs(float(reference)))
    if abs(float(actual) - float(reference)) > bound:
        _fail(f"{name} differs beyond the persisted-readback tolerance")


def validate_model_metadata(
    model,
    *,
    schema,
    training,
    vector_layout,
    imputed_price_median,
):
    """Reconcile persisted T01 metadata with the independently loaded model."""

    if schema.get("predictor_order") != list(model.stages[5].getInputCols()):
        _fail("reloaded model predictor ordering differs from persisted schema")
    if schema.get("vector_layout") != vector_layout:
        _fail("reloaded model vector metadata differs from persisted schema")
    categories = training.get("category_mappings")
    if not isinstance(categories, Mapping):
        _fail("training category mappings are missing")
    actual_categories = {
        "store_id": list(model.stages[1].labels),
        "dept_id": list(model.stages[2].labels),
    }
    if categories != actual_categories:
        _fail("reloaded model categorical mappings differ from training evidence")
    if type(imputed_price_median) not in (int, float) or not math.isfinite(
        float(imputed_price_median)
    ):
        _fail("reloaded model imputation metadata is missing or nonfinite")
    expected_median = training.get("imputed_price_median")
    _finite_number(expected_median, "training imputed_price_median")
    compare_prediction_values(
        imputed_price_median,
        expected_median,
        name="reloaded model price imputation median",
    )


def validate_forecast_rows(
    rows,
    *,
    selector,
    candidate_series,
    excluded_series,
    issue_date=EXPECTED_ISSUE_DATE,
    run_type="forward_demo",
):
    """Validate Contract v1 J06 rows, dates, lineage, and complete-series coverage."""

    records = [_mapping(row) for row in rows]
    if not records:
        _fail("forward-demo forecast is empty")
    keys = ("forecast_run_id", "item_id", "store_id", "forecast_date")
    values = [_key(row, keys) for row in records]
    if any(None in key for key in values) or len(values) != len(set(values)):
        _fail("forecast rows contain duplicate or null Contract v1 keys")
    for row in records:
        if row.get("issue_date") != issue_date and row.get("issue_date") != issue_date.isoformat():
            _fail(f"{run_type} requires issue date {issue_date.isoformat()}")
        if row.get("run_type") != run_type:
            _fail(f"forecast run type is not {run_type}")
        horizon = row.get("horizon")
        if type(horizon) is not int or not 1 <= horizon <= 7:
            _fail("forward-demo forecast horizons must be 1 through 7")
        issue = _date(row.get("issue_date"), "issue_date")
        if _date(row.get("forecast_date"), "forecast_date") != issue + timedelta(days=horizon):
            _fail("forecast date is not consecutive from the issue date")
        raw_prediction = _finite_number(row.get("raw_prediction"), "raw_prediction")
        _finite_nonnegative(row.get("predicted_units"), "predicted_units")
        if float(row["predicted_units"]) != max(0.0, raw_prediction):
            _fail("predicted_units does not reconcile with raw_prediction")
        for name in ("forecast_run_id", "model_run_id", "source_snapshot_id", "feature_version"):
            if selector.get(name) is not None and row.get(name) != selector[name]:
                _fail("forecast lineage differs: " + name)
    series = defaultdict(list)
    for row in records:
        series[(row["item_id"], row["store_id"])].append(row["horizon"])
    if any(sorted(horizons) != list(range(1, 8)) for horizons in series.values()):
        _fail("every successful forecast series must contain exactly seven horizons")
    if (
        type(candidate_series) is not int
        or type(excluded_series) is not int
        or candidate_series < 0
        or excluded_series < 0
    ):
        _fail("forecast coverage counts are invalid")
    if len(series) + excluded_series != candidate_series:
        _fail("forecast successful and excluded series do not conserve the candidate cohort")
    return {
        "row_count": len(records),
        "successful_series": len(series),
        "excluded_series": excluded_series,
        "seven_day_total_units": sum(row["predicted_units"] for row in records),
        "series": sorted(f"{item}/{store}" for item, store in series),
    }


def validate_forecast_coverage(cohort_rows, forecast_rows, exception_rows, *, expected_series=100):
    """Reconcile candidate, successful and explicitly excluded scoring series."""

    cohort = [_mapping(row) for row in cohort_rows]
    successful = {_key(_mapping(row), ("item_id", "store_id")) for row in forecast_rows}
    excluded = {_key(_mapping(row), ("item_id", "store_id")) for row in exception_rows}
    candidates = set()
    for row in cohort:
        if row.get("status") == "SELECTED":
            stores = row.get("stores") or []
            candidates.update((row.get("item_id"), store) for store in stores)
    if (
        len(candidates) != expected_series
        or successful & excluded
        or successful | excluded != candidates
    ):
        _fail("forecast cohort, successful rows, and exclusions do not reconcile")
    return {
        "candidate_series": len(candidates),
        "successful_series": len(successful),
        "excluded_series": len(excluded),
    }


def _metric_value(metric, sae, sse, actual, count):
    if metric == "WAPE":
        return 100.0 * sae / actual if actual > 0 else None
    if not count:
        return None
    return sae / count if metric == "MAE" else math.sqrt(sse / count)


def reference_metrics(paired_rows, *, evaluation_run_id, model_run_id, run_type):
    """Calculate all 48 metric rows from persisted pairs without producer code."""

    records = [_mapping(row) for row in paired_rows]
    if run_type not in {"validation", "test"}:
        _fail("reference metrics require validation or test")
    aggregates = {}
    for method, field in (
        ("random_forest", "rf_prediction"),
        ("seasonal_naive", "baseline_prediction"),
    ):
        for horizon in range(8):
            selected = (
                records
                if horizon == 0
                else [row for row in records if row.get("horizon") == horizon]
            )
            sae = sse = actual = 0.0
            for row in selected:
                prediction = _finite_nonnegative(row.get(field), field)
                label = _finite_nonnegative(row.get("actual_units"), "actual_units")
                error = prediction - label
                sae += abs(error)
                sse += error * error
                actual += label
            aggregates[(method, horizon)] = (len(selected), sae, sse, actual)
    try:
        from warehouse_bigdata.forecasting.evaluation import EvaluationContext

        context = EvaluationContext(
            evaluation_run_id, model_run_id, "acceptance-snapshot", "historical-v1", run_type
        )
        start, end = context.window
        holdout = context.holdout_id
    except Exception:
        start = end = None
        holdout = "m5-chronological-paired-v1-" + run_type
    result = []
    for method in ("random_forest", "seasonal_naive"):
        for metric in ("MAE", "RMSE", "WAPE"):
            for horizon in range(8):
                count, sae, sse, actual = aggregates[(method, horizon)]
                result.append(
                    {
                        "evaluation_run_id": evaluation_run_id,
                        "model_run_id": model_run_id if method == "random_forest" else None,
                        "forecast_run_id": None,
                        "holdout_id": holdout,
                        "method_id": method,
                        "metric_name": metric,
                        "horizon": horizon,
                        "metric_value": _metric_value(metric, sae, sse, actual, count),
                        "observation_count": count,
                        "sum_absolute_error": sae,
                        "sum_actual_units": actual,
                        "evaluation_start": start,
                        "evaluation_end": end,
                        "run_type": run_type,
                    }
                )
    return result


def validate_persisted_metrics(
    persisted_rows, paired_rows, *, evaluation_run_id, model_run_id, run_type
):
    expected = reference_metrics(
        paired_rows,
        evaluation_run_id=evaluation_run_id,
        model_run_id=model_run_id,
        run_type=run_type,
    )
    actual = [_mapping(row) for row in persisted_rows]
    key = ("evaluation_run_id", "holdout_id", "method_id", "metric_name", "horizon")
    if len(actual) != 48 or len({_key(row, key) for row in actual}) != 48:
        _fail("persisted evaluation metrics must contain 48 unique rows")
    expected_by_key = {_key(row, key): row for row in expected}
    actual_by_key = {_key(row, key): row for row in actual}
    if set(actual_by_key) != set(expected_by_key):
        _fail("persisted metric keys differ from the locked paired reference")
    for metric_key, row in actual_by_key.items():
        reference = expected_by_key[metric_key]
        for name in (
            "model_run_id",
            "forecast_run_id",
            "evaluation_start",
            "evaluation_end",
            "run_type",
            "observation_count",
        ):
            if row.get(name) != reference.get(name):
                _fail("persisted metric lineage/statistics differ: " + name)
        if row.get("metric_value") is None or reference.get("metric_value") is None:
            if row.get("metric_value") is not None or reference.get("metric_value") is not None:
                _fail("persisted metric nullness differs from the reference")
        else:
            compare_prediction_values(
                row["metric_value"], reference["metric_value"], name="metric_value"
            )
        for name in ("sum_absolute_error", "sum_actual_units"):
            compare_prediction_values(row[name], reference[name], name=name)
    return {"metric_rows": len(actual), "paired_rows": len(paired_rows)}


def validate_paired_rows(
    rows,
    *,
    expected_count=2800,
    selector=None,
    expected_issue_dates=None,
    expected_forecast_ids=None,
):
    """Validate bounded evaluation pairing and seasonal-naive source dates."""

    records = [_mapping(row) for row in rows]
    if expected_count is not None and len(records) != expected_count:
        _fail("evaluation paired coverage is incomplete")
    keys = ("item_id", "store_id", "issue_date", "target_date", "horizon")
    if len({_key(row, keys) for row in records}) != len(records):
        _fail("evaluation paired keys are duplicated")
    for row in records:
        issue = _date(row.get("issue_date"), "evaluation issue_date")
        target = _date(row.get("target_date"), "evaluation target_date")
        if type(row.get("horizon")) is not int or not 1 <= row["horizon"] <= 7:
            _fail("evaluation horizons must be one through seven")
        if target != issue + timedelta(days=row["horizon"]):
            _fail("evaluation target date does not match horizon")
        if expected_issue_dates is not None and issue not in set(expected_issue_dates):
            _fail("evaluation issue date is outside the locked origins")
        if expected_forecast_ids is not None and row.get(
            "forecast_run_id"
        ) != expected_forecast_ids.get(issue):
            _fail("paired random-forest forecast identity differs from locked origin")
        if selector is not None:
            for name in ("model_run_id", "source_snapshot_id", "feature_version"):
                if row.get(name) != selector[name]:
                    _fail("paired prediction lineage differs: " + name)
        if not row.get("forecast_run_id"):
            _fail("paired random-forest forecast identity is missing")
        source = row.get("baseline_source_date")
        if source is None or _date(source, "baseline_source_date") != target - timedelta(days=7):
            _fail("seasonal-naive source date is not T+h-7")
        for name in ("rf_prediction", "baseline_prediction", "actual_units"):
            _finite_nonnegative(row.get(name), name)
    return {"paired_rows": len(records), "complete": bool(records)}


def validate_evaluation_alignment(
    alignment_rows,
    paired_rows,
    exception_rows,
    expected_rows,
    *,
    selector,
    expected_forecast_ids,
):
    """Reconcile every candidate alignment row with pairs and explicit exclusions."""

    key = ("item_id", "store_id", "issue_date", "target_date", "horizon")
    expected = {_key(_mapping(row), key) for row in expected_rows}
    alignment = [_mapping(row) for row in alignment_rows]
    if len(alignment) != len(expected) or len({_key(row, key) for row in alignment}) != len(
        alignment
    ):
        _fail("evaluation alignment contains duplicate or incomplete candidate keys")
    if {_key(row, key) for row in alignment} != expected:
        _fail("evaluation alignment keys differ from the locked candidate grid")
    paired = [_mapping(row) for row in paired_rows]
    paired_keys = {_key(row, key) for row in paired}
    alignment_by_key = {_key(row, key): row for row in alignment}
    if any(row.get("paired") is not True for row in alignment if _key(row, key) in paired_keys):
        _fail("paired rows include an alignment row that is not marked paired")
    if {
        row_key for row_key, row in alignment_by_key.items() if row.get("paired") is True
    } != paired_keys:
        _fail("alignment paired flags differ from persisted paired rows")

    for row in alignment:
        if type(row.get("eligible")) is not bool or type(row.get("label_valid")) is not bool:
            _fail("evaluation alignment eligibility flags are invalid")
        prediction_present = (
            row.get("rf_prediction") is not None and row.get("baseline_prediction") is not None
        )
        expected_paired = row["eligible"] and prediction_present and row["label_valid"]
        if row.get("paired") is not expected_paired:
            _fail("evaluation alignment paired flag does not reconcile with evidence")
        for name in ("rf_prediction", "baseline_prediction", "actual_units"):
            if row.get(name) is not None:
                _finite_nonnegative(row[name], name)
        baseline_source = row.get("baseline_source_date")
        if row.get("baseline_prediction") is not None:
            if baseline_source is None or _date(baseline_source, "baseline_source_date") != _date(
                row["target_date"], "target_date"
            ) - timedelta(days=7):
                _fail("evaluation alignment baseline date is not T+h-7")
        if row.get("rf_prediction") is not None:
            if row.get("model_run_id") != selector["model_run_id"]:
                _fail("evaluation alignment model lineage differs")
            if row.get("source_snapshot_id") != selector["source_snapshot_id"]:
                _fail("evaluation alignment source lineage differs")
            if row.get("feature_version") != selector["feature_version"]:
                _fail("evaluation alignment feature lineage differs")
            issue = _date(row["issue_date"], "issue_date")
            if row.get("forecast_run_id") != expected_forecast_ids.get(issue):
                _fail("evaluation alignment forecast identity differs")

    exceptions = [_mapping(row) for row in exception_rows]
    exception_keys = set()
    for row in exceptions:
        row_key = _key(row, key)
        if row_key not in expected or row.get("method_id") not in {
            "paired",
            "random_forest",
            "seasonal_naive",
        }:
            _fail("evaluation exception is outside the locked candidate grid")
        if not isinstance(row.get("reason_code"), str) or not row["reason_code"]:
            _fail("evaluation exception reason is missing")
        if row.get("method_id") == "paired":
            exception_keys.add(row_key)
    if exception_keys != expected - paired_keys:
        _fail("evaluation exclusions do not explain every unpaired candidate")
    return {
        "candidate_rows": len(alignment),
        "paired_rows": len(paired),
        "excluded_rows": len(expected - paired_keys),
    }


def validate_evaluation_coverage(rows, alignment_rows, *, expected_issue_dates):
    """Recalculate persisted per-origin coverage from the complete alignment."""

    alignment = [_mapping(row) for row in alignment_rows]
    dates = {_date(value, "evaluation issue_date") for value in expected_issue_dates}
    if {_date(row.get("issue_date"), "evaluation issue_date") for row in alignment} != dates:
        _fail("evaluation alignment origins differ from coverage origins")
    actual = [_mapping(row) for row in rows]
    key = ("issue_date", "method_id")
    if len(actual) != len(dates) * 2 or len({_key(row, key) for row in actual}) != len(actual):
        _fail("evaluation coverage must contain one row per method and origin")

    reference = {}
    for issue in dates:
        origin_rows = [row for row in alignment if _date(row["issue_date"], "issue_date") == issue]
        series = defaultdict(int)
        for row in origin_rows:
            series[(row["item_id"], row["store_id"])] += int(row.get("paired") is True)
        series_counts = {
            "fully_dropped_series": sum(value == 0 for value in series.values()),
            "partially_paired_series": sum(1 <= value <= 6 for value in series.values()),
            "complete_paired_series": sum(value == 7 for value in series.values()),
        }
        for method, prediction in (
            ("random_forest", "rf_prediction"),
            ("seasonal_naive", "baseline_prediction"),
        ):
            candidate_rows = len(origin_rows)
            paired_rows = sum(row.get("paired") is True for row in origin_rows)
            predicted_rows = sum(row.get(prediction) is not None for row in origin_rows)
            label_aligned_rows = sum(
                row.get(prediction) is not None and row.get("label_valid") is True
                for row in origin_rows
            )
            reference[(issue, method)] = {
                "candidate_rows": candidate_rows,
                "eligible_rows": sum(row.get("eligible") is True for row in origin_rows),
                "predicted_rows": predicted_rows,
                "label_aligned_rows": label_aligned_rows,
                "paired_rows": paired_rows,
                "prediction_difference_rows": sum(
                    (row.get("rf_prediction") is not None)
                    != (row.get("baseline_prediction") is not None)
                    for row in origin_rows
                ),
                "dropped_rows": candidate_rows - paired_rows,
                **series_counts,
            }
    for row in actual:
        issue = _date(row.get("issue_date"), "coverage issue_date")
        method = row.get("method_id")
        expected = reference.get((issue, method))
        if expected is None:
            _fail("evaluation coverage contains an unknown origin or method")
        for name, value in expected.items():
            if type(row.get(name)) is not int or row[name] != value:
                _fail("evaluation coverage differs from independently reconstructed alignment")
    return {"coverage_rows": len(actual), "origins": len(dates)}


def _read_json(client, path):
    try:
        value = json.loads(client.read_remote(path))
    except (TypeError, UnicodeError, json.JSONDecodeError) as error:
        raise AcceptanceError(f"invalid acceptance JSON: {path}") from error
    if not isinstance(value, dict):
        _fail("acceptance metadata must be an object: " + path)
    return value


def _model_report(spark, selector, *, client, config, features, scope):
    from warehouse_bigdata.forecasting.model_artifacts import (
        load_validated_model,
        read_validated_model_metadata,
    )

    args = dict(
        model_run_id=selector["model_run_id"],
        source_snapshot_id=selector["source_snapshot_id"],
        feature_version=selector["feature_version"],
        hdfs_config=config,
        client=client,
    )
    metadata = read_validated_model_metadata(**args)
    manifest = metadata["manifest"]
    training_row_count = manifest.get("training_row_count")
    if (
        manifest.get("seed") != 42
        or type(training_row_count) is not int
        or not 0 < training_row_count <= 10000
    ):
        _fail("frozen model training seed/cap differs from M06")
    if (manifest.get("training_start"), manifest.get("training_end")) != (
        "2011-02-26",
        "2016-03-27",
    ):
        _fail("frozen model training window differs from d_29–d_1885")
    if metadata.get("input_binding", {}).get("scope") != scope:
        _fail("model scope differs from canonical selection")
    model_path = metadata["artifact_path"]
    training = _read_json(client, model_path + "/evidence/training.json")
    schema = metadata.get("feature_schema")
    if not isinstance(schema, dict) or schema.get("predictor_order") is None:
        _fail("frozen model feature schema is missing predictor ordering")
    if training.get("input_binding") != metadata.get("input_binding"):
        _fail("training evidence input binding differs from frozen model")
    categories = training.get("category_mappings", {})
    if not isinstance(categories.get("store_id"), list) or not isinstance(
        categories.get("dept_id"), list
    ):
        _fail("training category mappings are missing")
    median = training.get("imputed_price_median")
    if type(median) not in (int, float) or not math.isfinite(float(median)):
        _fail("training imputation metadata is missing or nonfinite")
    selection = spark.read.parquet(config.uri + model_path + "/training_selection_manifest")
    training_features = features.where("feature_role = 'train'")
    selection_report = validate_training_selection_frame(
        selection,
        training_features,
        summary=training.get("selection", {}),
        cap=10000,
        seed=42,
    )
    probes = training_features.limit(1).drop("label")
    if not probes.take(1):
        _fail("model readback has no training probe row")
    model = load_validated_model(spark, **args)
    transformed = model.transform(probes)
    median_row = model.stages[4].surrogateDF.select("last_known_price").first()
    if median_row is None:
        _fail("reloaded model has no price imputation median")
    validate_model_metadata(
        model,
        schema=schema,
        training=training,
        vector_layout=transformed.schema["features"].metadata,
        imputed_price_median=median_row["last_known_price"],
    )
    from pyspark.sql import functions as F

    missing_price = probes.withColumn("last_known_price", F.lit(None).cast("double")).withColumn(
        "price_missing", F.lit(True)
    )
    probe_rows = [
        probes,
        missing_price,
        missing_price.withColumn("store_id", F.concat("store_id", F.lit("__unseen__")))
        .withColumn("dept_id", F.concat("dept_id", F.lit("__unseen__")))
        .withColumn("lag_1", F.col("lag_1") + F.lit(0.375)),
    ]
    outputs = []
    for probe in probe_rows:
        row = model.transform(probe).select("raw_prediction").first()
        if row is None or not math.isfinite(float(row.raw_prediction)):
            _fail("reloaded model probe is nonfinite")
        outputs.append(float(row.raw_prediction))
    return {
        "model_run_id": selector["model_run_id"],
        "reloaded": True,
        "probe_count": len(outputs),
        "probe_cases": ["ordinary", "missing_price", "fractional_unseen"],
        "selection": selection_report,
        "metadata_reconciled": True,
        "schema_fingerprint": document_digest(schema),
    }, model


def _forecast_report(spark, selector, *, client, config, model, scope):
    from warehouse_bigdata.common.hdfs_paths import render_forecast_output_path, render_staging_path
    from warehouse_bigdata.forecasting.feature_input import read_validated_feature_context
    from warehouse_bigdata.forecasting.forecast_artifacts import validate_forecast_evidence
    from warehouse_bigdata.forecasting.scoring_features import (
        build_scoring_features,
        prepare_as_of_history,
    )
    from warehouse_bigdata.processing.enrichment_analytics_snapshot import (
        read_validated_sales_enriched_snapshot,
    )

    path = render_forecast_output_path(config.base_path, selector["forecast_run_id"])
    marker = _read_json(client, path + "/scoring_manifest.json")
    validate_forecast_evidence(
        client, config, marker, path, pipeline_run_id=selector["j06_pipeline_run_id"]
    )
    frame = spark.read.parquet(config.uri + path + "/forecast_daily")
    quality = _read_json(
        client,
        render_staging_path(config.base_path, selector["j06_pipeline_run_id"])
        + "/quality_report.json",
    )
    coverage_counts = {
        "candidate_series": quality.get("candidate_series"),
        "successful_series": quality.get("successful_series"),
        "excluded_series": quality.get("excluded_series"),
    }
    result = validate_forecast_rows(
        frame.collect(),
        selector=selector,
        candidate_series=coverage_counts["candidate_series"],
        excluded_series=coverage_counts["excluded_series"],
    )
    if result["successful_series"] != coverage_counts["successful_series"]:
        _fail("forecast persisted coverage count differs from rows")
    cohort_frame = spark.read.parquet(config.uri + path + "/cohort_selection")
    cohorts = cohort_frame.collect()
    exceptions = spark.read.parquet(config.uri + path + "/coverage_exceptions").collect()
    coverage_result = validate_forecast_coverage(
        cohorts,
        frame.select("item_id", "store_id").distinct().collect(),
        exceptions,
        expected_series=coverage_counts["candidate_series"],
    )
    from pyspark.sql import functions as F

    sales, sales_evidence = read_validated_sales_enriched_snapshot(
        spark,
        source_version=selector["source_version"],
        source_snapshot_id=selector["source_snapshot_id"],
        pipeline_run_id=selector["j03_pipeline_run_id"],
        hdfs_config=config,
        client=client,
    )
    feature_context = read_validated_feature_context(
        spark,
        source_version=selector["source_version"],
        source_snapshot_id=selector["source_snapshot_id"],
        upstream_pipeline_run_id=selector["j04_pipeline_run_id"],
        feature_version=selector["feature_version"],
        hdfs_config=config,
        client=client,
    )
    series_scope = cohort_frame.where("status = 'SELECTED'").select(
        "item_id", F.explode("stores").alias("store_id")
    )
    prepared = prepare_as_of_history(
        sales,
        issue_date=EXPECTED_ISSUE_DATE,
        source_snapshot_id=selector["source_snapshot_id"],
        feature_version=selector["feature_version"],
        series_scope=series_scope,
    )
    scoring = build_scoring_features(
        prepared.state,
        issue_date=EXPECTED_ISSUE_DATE,
        target_date=EXPECTED_ISSUE_DATE + timedelta(days=1),
        known_calendar=feature_context["calendar_mapping"].select(
            F.col("target_date").alias("date")
        ),
    )
    fresh = (
        model.transform(scoring.features).select("item_id", "store_id", "raw_prediction").collect()
    )
    persisted = frame.where("horizon = 1").select("item_id", "store_id", "raw_prediction").collect()
    fresh_by_key = {
        _key(_mapping(row), ("item_id", "store_id")): row.raw_prediction for row in fresh
    }
    persisted_by_key = {
        _key(_mapping(row), ("item_id", "store_id")): row.raw_prediction for row in persisted
    }
    if set(fresh_by_key) != set(persisted_by_key):
        _fail("fresh horizon-one scoring coverage differs from persisted forecast")
    for key, value in fresh_by_key.items():
        compare_prediction_values(
            value, persisted_by_key[key], name="horizon-one raw prediction " + str(key)
        )
    return {
        **result,
        "coverage": coverage_result,
        "marker_digest": document_digest(marker),
        "fresh_horizon_one": {"row_count": len(fresh), "prediction_parity": True},
        "source_lineage": sales_evidence,
    }


def _evaluation_report(spark, selector, *, client, config, scope, phase):
    from pyspark.sql import functions as F

    from warehouse_bigdata.common.hdfs_paths import render_evaluation_output_path
    from warehouse_bigdata.forecasting.evaluation import ORIGINS, EvaluationContext, expected_keys
    from warehouse_bigdata.forecasting.evaluation_artifacts import validate_evaluation_evidence

    path = render_evaluation_output_path(config.base_path, selector[phase + "_evaluation_run_id"])
    manifest = _read_json(client, path + "/evaluation_manifest.json")
    validate_evaluation_evidence(
        client, config, manifest, path, pipeline_run_id=selector[phase + "_pipeline_run_id"]
    )
    binding = _read_json(client, path + "/evidence/evaluation.json")
    context = EvaluationContext(
        selector[phase + "_evaluation_run_id"],
        selector["model_run_id"],
        selector["source_snapshot_id"],
        selector["feature_version"],
        phase,
    )
    if binding.get("origin_forecasts") != context.origin_forecasts or binding.get("freeze", {}).get(
        "origins"
    ) != {key: list(value) for key, value in ORIGINS.items()}:
        _fail("evaluation origins or deterministic forecast IDs differ")
    if binding.get("scope") != scope:
        _fail("evaluation scope differs from canonical selection")
    if phase == "test":
        validation_path = render_evaluation_output_path(
            config.base_path, selector["validation_evaluation_run_id"]
        )
        validation_manifest = _read_json(client, validation_path + "/evaluation_manifest.json")
        if binding.get("validation_manifest_digest") != document_digest(validation_manifest):
            _fail("test evaluation does not select the persisted validation freeze")
    cohort = spark.read.parquet(config.uri + path + "/cohort_selection")
    selected = cohort.where("status = 'SELECTED'").select(
        "item_id", F.explode("stores").alias("store_id")
    )
    origins = [(day, date(2011, 1, 29) + timedelta(days=day - 1)) for day in ORIGINS[phase]]
    expected = expected_keys(selected, origins)
    alignment = spark.read.parquet(config.uri + path + "/alignment")
    if (
        alignment.count() != len(ORIGINS[phase]) * EXPECTED_SERIES * 7
        or alignment.select("item_id", "store_id", "issue_date", "target_date", "horizon")
        .exceptAll(expected)
        .unionByName(
            expected.exceptAll(
                alignment.select("item_id", "store_id", "issue_date", "target_date", "horizon")
            )
        )
        .limit(1)
        .count()
    ):
        _fail("evaluation alignment does not reconstruct the locked candidate keys")
    expected_rows = expected.collect()
    alignment_rows = alignment.collect()
    paired = spark.read.parquet(config.uri + path + "/paired").collect()
    expected_issue_dates = tuple(issue_date for _, issue_date in origins)
    expected_forecast_ids = {issue_date: context.forecast_id(day) for day, issue_date in origins}
    paired_report = validate_paired_rows(
        paired,
        expected_count=None,
        selector=selector,
        expected_issue_dates=expected_issue_dates,
        expected_forecast_ids=expected_forecast_ids,
    )
    exceptions = spark.read.parquet(config.uri + path + "/exceptions").collect()
    alignment_report = validate_evaluation_alignment(
        alignment_rows,
        paired,
        exceptions,
        expected_rows,
        selector=selector,
        expected_forecast_ids=expected_forecast_ids,
    )
    coverage_report = validate_evaluation_coverage(
        spark.read.parquet(config.uri + path + "/coverage").collect(),
        alignment_rows,
        expected_issue_dates=expected_issue_dates,
    )
    metrics = spark.read.parquet(config.uri + path + "/metrics").collect()
    metric_report = validate_persisted_metrics(
        metrics,
        paired,
        evaluation_run_id=selector[phase + "_evaluation_run_id"],
        model_run_id=selector["model_run_id"],
        run_type=phase,
    )
    return {
        "evaluation_run_id": selector[phase + "_evaluation_run_id"],
        "paired_rows": len(paired),
        "candidate_rows": len(expected_rows),
        "alignment": alignment_report,
        "coverage": coverage_report,
        "paired": paired_report,
        **metric_report,
    }


def verify_ml_artifacts(
    spark,
    selector,
    *,
    through="test",
    client,
    config,
    scope,
    features=None,
):
    """Read and independently reconcile persisted ML artifacts through ``through``."""

    from warehouse_bigdata.common.small_artifacts import STAGES

    validate_canonical_scope(scope)
    stages = STAGES[: STAGES.index(through) + 1]
    report = {
        "scope": {key: scope[key] for key in ("dataset_scope", "selection_digest") if key in scope}
    }
    model = None
    if "j04" in stages:
        if features is None:
            from warehouse_bigdata.forecasting.feature_input import (
                read_validated_forecast_features_snapshot,
            )

            features, _ = read_validated_forecast_features_snapshot(
                spark,
                source_version=selector["source_version"],
                source_snapshot_id=selector["source_snapshot_id"],
                upstream_pipeline_run_id=selector["j04_pipeline_run_id"],
                feature_version=selector["feature_version"],
                hdfs_config=config,
                client=client,
            )
        report["features"] = {"row_count": features.count(), "full_history": True}
    if "j05" in stages:
        report["model"], model = _model_report(
            spark, selector, client=client, config=config, features=features, scope=scope
        )
    if "j06" in stages:
        if model is None:
            _fail("forecast readback requires the reloaded model")
        report["forecast"] = _forecast_report(
            spark, selector, client=client, config=config, model=model, scope=scope
        )
    for phase in ("validation", "test"):
        if phase in stages:
            report[phase] = _evaluation_report(
                spark, selector, client=client, config=config, scope=scope, phase=phase
            )
    return report


__all__ = [
    "AcceptanceError",
    "EXPECTED_ISSUE_DATE",
    "PREDICTION_TOLERANCE",
    "compare_prediction_values",
    "reference_metrics",
    "validate_canonical_scope",
    "validate_evaluation_alignment",
    "validate_evaluation_coverage",
    "validate_forecast_coverage",
    "validate_forecast_rows",
    "validate_paired_rows",
    "validate_persisted_metrics",
    "validate_model_metadata",
    "validate_training_selection",
    "validate_training_selection_frame",
    "verify_ml_artifacts",
]

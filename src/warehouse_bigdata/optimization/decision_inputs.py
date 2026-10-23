"""Read-only, bounded binding of the explicitly selected J03/J06 small artifacts."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from itertools import islice
from types import MappingProxyType

from warehouse_bigdata.common.hdfs_paths import render_forecast_output_path
from warehouse_bigdata.common.hdfs_publication import document_digest
from warehouse_bigdata.common.scope_evidence import read_scope
from warehouse_bigdata.common.small_artifacts import load_small_artifacts, validate_small_artifacts
from warehouse_bigdata.common.small_preflight import validate_small_publications
from warehouse_bigdata.contracts.datasets import FORECAST_DAILY
from warehouse_bigdata.contracts.validation import validate_dataset, validate_record
from warehouse_bigdata.forecasting.acceptance import (
    EXPECTED_ISSUE_DATE, EXPECTED_STORES, validate_canonical_scope,
    validate_forecast_coverage, validate_forecast_rows,
)
from warehouse_bigdata.forecasting.feature_input import read_json
from warehouse_bigdata.forecasting.forecast_artifacts import validate_forecast_evidence

from .replenishment import ForecastPrediction
from .scenario_generation import EligiblePair, ObservedDailySale


class DecisionInputError(ValueError):
    """Selected inputs cannot form a complete, bounded forward decision cohort."""


def freeze(value):
    """Copy nested evidence into immutable containers."""
    if isinstance(value, Mapping):
        return MappingProxyType({k: freeze(v) for k, v in sorted(value.items())})
    if isinstance(value, (list, tuple)):
        return tuple(freeze(v) for v in value)
    return value


def plain(value):
    """Convert immutable metadata to JSON-compatible containers."""
    if isinstance(value, Mapping):
        return {k: plain(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [plain(v) for v in value]
    return value


@dataclass(frozen=True, slots=True)
class ExcludedSeries:
    item_id: str
    store_id: str
    reason_codes: tuple[str, ...]
    stage: str = "FORECAST"


@dataclass(frozen=True, slots=True)
class DecisionInputs:
    predictions: tuple[ForecastPrediction, ...]
    observed_sales: tuple[ObservedDailySale, ...]
    eligible_pairs: tuple[EligiblePair, ...]
    exclusions: tuple[ExcludedSeries, ...]
    selector: Mapping[str, object]
    scope: Mapping[str, object]
    coverage: Mapping[str, object]
    upstream_evidence: Mapping[str, object]


def _rows(values, maximum, label):
    rows = list(islice(values, maximum + 1))
    if len(rows) > maximum:
        raise DecisionInputError(f"{label} exceeds {maximum} rows")
    return [r.asDict(recursive=True) if hasattr(r, "asDict") else dict(r) for r in rows]


def _day(value):
    if type(value) is date:
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            pass
    raise DecisionInputError("expected a calendar date")


def _contract_forecast(row):
    result = dict(row)
    for name in ("issue_date", "forecast_date"):
        result[name] = _day(result[name]).isoformat()
    stamp = result.get("created_at")
    if isinstance(stamp, datetime):
        # Spark TimestampType is returned as a naive UTC datetime in a UTC session.
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        result["created_at"] = stamp.isoformat()
    return result


def bind_decision_inputs(*, selector, scope, forecast_rows, history_rows,
                         cohort_rows, exception_rows, coverage_rows, upstream_evidence):
    """Validate bounded materialized inputs; never infer eligibility from missing rows."""
    validate_small_artifacts(dict(selector), through="j06")
    validate_canonical_scope(scope)
    if scope.get("selection_digest") != selector["selection_digest"]:
        raise DecisionInputError("scope selection_digest differs from selector")
    forecasts = _rows(forecast_rows, 700, "forecast")
    history = _rows(history_rows, 700, "history")
    cohort = _rows(cohort_rows, 10, "cohort")
    exceptions = _rows(exception_rows, 10000, "exceptions")
    coverage = _rows(coverage_rows, 1, "coverage")
    population = {(r["item_id"], r["store_id"]) for r in scope["selected_series"]}
    candidates = set()
    for row in cohort:
        stores = row.get("stores")
        if (row.get("status") != "SELECTED"
                or row.get("selection_digest") != selector["selection_digest"]
                or not isinstance(stores, (list, tuple)) or not stores
                or len(stores) != len(set(stores))):
            raise DecisionInputError("cohort selection conflicts with frozen scope")
        for store in stores:
            key = (row.get("item_id"), store)
            if key in candidates:
                raise DecisionInputError("duplicate cohort candidate")
            candidates.add(key)
    if candidates != population:
        raise DecisionInputError("cohort differs from frozen population")
    reasons, exception_keys = {}, set()
    for row in exceptions:
        key = (row.get("item_id"), row.get("store_id"))
        reason, stage, horizon = row.get("reason_code"), row.get("stage"), row.get("horizon")
        if (key not in candidates or not isinstance(reason, str) or not reason.strip()
                or stage not in {"PREPARATION", "FEATURES", "PREDICTION"}
                or type(horizon) is not int
                or horizon not in (range(1) if stage == "PREPARATION" else range(1, 8))):
            raise DecisionInputError("invalid explicit exclusion reason or key")
        diagnostic = (*key, stage, horizon, reason)
        if diagnostic in exception_keys:
            raise DecisionInputError("duplicate exclusion diagnostic")
        exception_keys.add(diagnostic)
        reasons.setdefault(key, set()).add(reason)
    counts = validate_forecast_coverage(cohort, forecasts, exceptions, expected_series=100)
    report = validate_forecast_rows(forecasts, selector=selector,
        candidate_series=counts["candidate_series"], excluded_series=counts["excluded_series"])
    expected_counts = {**counts, "row_count": report["row_count"]}
    if len(coverage) != 1 or any(
        type(coverage[0].get(k)) is not int or coverage[0][k] != v
        for k, v in expected_counts.items()
    ):
        raise DecisionInputError("persisted coverage counts differ from inputs")
    validate_dataset(FORECAST_DAILY, [_contract_forecast(r) for r in forecasts],
                     lineage_context=selector)
    predictions = tuple(sorted((ForecastPrediction(
        item_id=r["item_id"], store_id=r["store_id"], issue_date=_day(r["issue_date"]),
        horizon=r["horizon"], prediction_date=_day(r["forecast_date"]),
        predicted_units=r["predicted_units"], forecast_run_id=r["forecast_run_id"],
        model_run_id=r["model_run_id"], source_snapshot_id=r["source_snapshot_id"],
        feature_version=r["feature_version"],
    ) for r in forecasts), key=lambda r: (r.item_id, r.store_id, r.horizon)))
    successful = {(r.item_id, r.store_id) for r in predictions}
    observations = []
    seen = set()
    days = {EXPECTED_ISSUE_DATE - timedelta(days=d) for d in range(7)}
    for row in history:
        key = (row.get("item_id"), row.get("store_id"))
        day, units = _day(row.get("date")), row.get("unit_sales")
        full_key = (*key, day)
        if (key not in successful or day not in days or full_key in seen
                or row.get("source_snapshot_id") != selector["source_snapshot_id"]
                or type(units) is not int or not 0 <= units <= 2**63 - 1):
            raise DecisionInputError("invalid, duplicate, future or cross-snapshot history")
        seen.add(full_key)
        observations.append(ObservedDailySale(selector["source_snapshot_id"], *key, day, units))
    if len(seen) != len(successful) * 7:
        raise DecisionInputError("each included series requires seven observed dates")
    if not upstream_evidence:
        raise DecisionInputError("upstream evidence digests are required")
    normalized_scope = dict(scope)
    normalized_scope["selected_series"] = sorted(scope["selected_series"],
        key=lambda r: (r["item_id"], r["store_id"]))
    exclusions = tuple(ExcludedSeries(*key, tuple(sorted(codes)))
                       for key, codes in sorted(reasons.items()))
    reason_counts = Counter(code for row in exclusions for code in row.reason_codes)
    return DecisionInputs(predictions,
        tuple(sorted(observations, key=lambda r: (r.item_id, r.store_id, r.date))),
        tuple(EligiblePair(*key) for key in sorted(successful)), exclusions,
        freeze(selector), freeze(normalized_scope),
        freeze({**expected_counts, "reason_counts": dict(sorted(reason_counts.items()))}),
        freeze(upstream_evidence))


def collect_bounded(frame, maximum, label):
    """Check distributed cardinality before collecting any bounded driver dataset."""
    count = frame.count()
    if count > maximum:
        raise DecisionInputError(f"{label} exceeds {maximum} rows")
    rows = _rows(frame.limit(maximum).collect(), maximum, label)
    if len(rows) != count:
        raise DecisionInputError(f"{label} changed during bounded collection")
    return rows


def require_forecast_schema(frame):
    from warehouse_bigdata.forecasting.recursive_forecasting import DAILY_TYPES

    if (tuple(frame.columns) != FORECAST_DAILY.field_names
            or tuple(f.dataType.simpleString() for f in frame.schema) != DAILY_TYPES):
        raise DecisionInputError("forecast_daily physical schema differs from Contract v1")


def read_history(spark, selector, successful, *, client, config,
                 issue_date=EXPECTED_ISSUE_DATE):
    """Filter the validated J03 frame in Spark before collecting T-6..T only."""
    return _read_sales_window(spark, selector, successful, client=client, config=config,
        start=issue_date - timedelta(days=6), end=issue_date, label="history")


def _read_sales_window(spark, selector, successful, *, client, config, start, end, label):
    from pyspark.sql import functions as F
    from warehouse_bigdata.processing.enrichment_analytics_snapshot import (
        read_validated_sales_enriched_snapshot,
    )

    sales, evidence = read_validated_sales_enriched_snapshot(spark,
        source_version=selector["source_version"], source_snapshot_id=selector["source_snapshot_id"],
        pipeline_run_id=selector["j03_pipeline_run_id"], hdfs_config=config, client=client)
    keys = spark.createDataFrame(sorted(successful), "item_id string, store_id string")
    window = sales.join(keys, ["item_id", "store_id"], "left_semi").where(
        F.col("date").between(start, end))
    if "source_snapshot_id" not in window.columns:
        window = window.withColumn("source_snapshot_id", F.lit(evidence["source_snapshot_id"]))
    rows = collect_bounded(window.select("source_snapshot_id", "item_id", "store_id",
                                         "date", "unit_sales"), 700, label)
    return rows, evidence


def read_decision_inputs(spark, *, client, config):
    """Consume WAREHOUSE_SMALL_ARTIFACTS only; no discovery, fallback, or writes."""
    selector = load_small_artifacts()
    validate_small_artifacts(selector, through="j06")
    validate_small_publications(selector, through="j06", client=client, config=config)
    if spark.conf.get("spark.sql.session.timeZone") not in {"UTC", "Etc/UTC"}:
        raise DecisionInputError("forecast timestamp reads require a UTC Spark session")
    path = render_forecast_output_path(config.base_path, selector["forecast_run_id"])
    manifest = read_json(client, path + "/scoring_manifest.json")
    validate_forecast_evidence(client, config, manifest, path,
                               pipeline_run_id=selector["j06_pipeline_run_id"])
    scope = read_scope(client, path, source_version=selector["source_version"],
        source_snapshot_id=selector["source_snapshot_id"], expected_digest=selector["selection_digest"])
    validate_canonical_scope(scope)
    frames = {name: spark.read.parquet(config.uri + path + "/" + name)
              for name in ("forecast_daily", "cohort_selection", "coverage", "coverage_exceptions")}
    require_forecast_schema(frames["forecast_daily"])
    forecasts = collect_bounded(frames["forecast_daily"], 700, "forecast")
    # Validate all forecast/coverage evidence before even opening J03 history.
    cohort = collect_bounded(frames["cohort_selection"], 10, "cohort")
    exceptions = collect_bounded(frames["coverage_exceptions"], 10000, "exceptions")
    coverage = collect_bounded(frames["coverage"], 1, "coverage")
    counts = validate_forecast_coverage(cohort, forecasts, exceptions, expected_series=100)
    validate_forecast_rows(forecasts, selector=selector,
        candidate_series=counts["candidate_series"], excluded_series=counts["excluded_series"])
    if manifest["row_count"] != len(forecasts):
        raise DecisionInputError("forecast manifest row count differs from rows")
    successful = {(r["item_id"], r["store_id"]) for r in forecasts}
    history, evidence = read_history(spark, selector, successful, client=client, config=config)
    expected = dict(source_version=selector["source_version"],
        source_snapshot_id=selector["source_snapshot_id"], pipeline_run_id=selector["j03_pipeline_run_id"])
    if evidence.get("scope") != scope or any(evidence.get(k) != v for k, v in expected.items()):
        raise DecisionInputError("J03 evidence lineage or scope differs from J06")
    return bind_decision_inputs(selector=selector, scope=scope, forecast_rows=forecasts,
        history_rows=history, cohort_rows=cohort, exception_rows=exceptions, coverage_rows=coverage,
        upstream_evidence={"j03": document_digest(evidence),
            "j03_metadata": evidence["metadata_digests"],
            "j06_manifest": document_digest(manifest),
            "j06_scoring": document_digest(read_json(client, path + "/evidence/scoring.json")),
            "j06_publication": document_digest(read_json(client, path + "/evidence/publication.json"))})


HISTORICAL_ISSUE_DATE = date(2011, 1, 29) + timedelta(days=1912)


def _historical_selector(selector, scope, binding):
    from warehouse_bigdata.forecasting.evaluation import EvaluationContext, ORIGINS

    validate_small_artifacts(plain(selector), through="test")
    validate_canonical_scope(plain(scope))
    context = EvaluationContext(selector["test_evaluation_run_id"], selector["model_run_id"],
        selector["source_snapshot_id"], selector["feature_version"], "test")
    if (ORIGINS["test"][0] != 1913
            or binding.get("origin_forecasts") != context.origin_forecasts
            or binding.get("freeze", {}).get("origins") != {k: list(v) for k, v in ORIGINS.items()}
            or binding.get("scope") != plain(scope)
            or scope.get("selection_digest") != selector["selection_digest"]):
        raise DecisionInputError("historical scope, origin forecast identity or freeze conflicts")
    return {**plain(selector), "forecast_run_id": binding["origin_forecasts"]["1913"]}


def bind_historical_inputs(*, selector, scope, binding, forecast_rows, history_rows,
                           eligibility_rows, exception_rows, upstream_evidence):
    """Bind only precommitted d_1913 predictions and history; never accept labels."""
    import math

    selected_artifacts = plain(selector)
    selector = _historical_selector(selector, scope, binding)
    forecasts = _rows(forecast_rows, 700, "forecast")
    history = _rows(history_rows, 700, "history")
    eligibility = _rows(eligibility_rows, 100, "eligibility")
    exceptions = _rows(exception_rows, 10000, "exceptions")
    population = {(p["item_id"], p["store_id"]) for p in scope["selected_series"]}
    statuses, reasons, diagnostics = {}, {}, set()
    for row in eligibility:
        key = (row.get("item_id"), row.get("store_id"))
        codes = row.get("reason_codes")
        if (key not in population or key in statuses
                or row.get("status") not in {"ELIGIBLE", "EXCLUDED"}
                or not isinstance(codes, (list, tuple))
                or any(not isinstance(c, str) or not c.strip() for c in codes)
                or (row["status"] == "ELIGIBLE") != (len(codes) == 0)
                or _day(row.get("issue_date")) != HISTORICAL_ISSUE_DATE
                or any(row.get(n) != selector[n] for n in ("source_snapshot_id", "feature_version"))):
            raise DecisionInputError("invalid historical eligibility or lineage")
        statuses[key] = row["status"]
        if codes:
            reasons[key] = ("FORECAST", set(codes))
    if set(statuses) != population:
        raise DecisionInputError("historical eligibility differs from frozen population")
    for row in exceptions:
        key = (row.get("item_id"), row.get("store_id"))
        stage, horizon, code = row.get("stage"), row.get("horizon"), row.get("reason_code")
        diagnostic = (*key, stage, horizon, code)
        if (key not in population or stage not in {"PREPARATION", "FEATURES", "PREDICTION"}
                or type(horizon) is not int
                or horizon not in (range(1) if stage == "PREPARATION" else range(1, 8))
                or not isinstance(code, str) or not code.strip() or diagnostic in diagnostics):
            raise DecisionInputError("invalid or duplicate historical forecast exception")
        diagnostics.add(diagnostic)
        reasons.setdefault(key, ("FORECAST", set()))[1].add(code)
    for row in forecasts:
        validate_record(FORECAST_DAILY, _contract_forecast(row), lineage_context=selector)
    grouped, seen = {}, set()
    for row in forecasts:
        key = (row["item_id"], row["store_id"])
        h = row["horizon"]
        if (key not in population or statuses[key] != "ELIGIBLE"
                or type(h) is not int or h not in range(1, 8) or (*key, h) in seen
                or _day(row["issue_date"]) != HISTORICAL_ISSUE_DATE
                or _day(row["forecast_date"]) != HISTORICAL_ISSUE_DATE + timedelta(days=h)
                or row["run_type"] != "test"
                or any(row[n] != selector[n] for n in (
                    "forecast_run_id", "model_run_id", "source_snapshot_id", "feature_version"))
                or not math.isfinite(row["raw_prediction"])
                or row["predicted_units"] != max(0.0, row["raw_prediction"])):
            raise DecisionInputError("historical forecast key, date or lineage conflicts")
        seen.add((*key, h))
        grouped.setdefault(key, []).append(row)
    for key in sorted(population):
        if key not in reasons and len(grouped.get(key, ())) != 7:
            reasons[key] = ("FORECAST", {"INCOMPLETE_FORECAST"})
        if len(grouped.get(key, ())) == 7 and key in reasons:
            raise DecisionInputError("successful forecast conflicts with exclusion evidence")
    successful = population - reasons.keys()
    complete_forecasts = [r for key in sorted(successful) for r in grouped[key]]
    if complete_forecasts:
        validate_forecast_rows(complete_forecasts, selector=selector,
            candidate_series=len(population), excluded_series=len(reasons),
            issue_date=HISTORICAL_ISSUE_DATE, run_type="test")
    observations, seen, invalid = {}, set(), set()
    days = {HISTORICAL_ISSUE_DATE - timedelta(days=d) for d in range(7)}
    for row in history:
        key = (row.get("item_id"), row.get("store_id"))
        day, units = _day(row.get("date")), row.get("unit_sales")
        if (key not in population or day not in days or (*key, day) in seen
                or row.get("source_snapshot_id") != selector["source_snapshot_id"]):
            raise DecisionInputError("duplicate, future or conflicting historical observations")
        seen.add((*key, day))
        if type(units) is not int or not 0 <= units <= 2**63 - 1:
            invalid.add(key)
        else:
            observations.setdefault(key, []).append(ObservedDailySale(
                selector["source_snapshot_id"], *key, day, units))
    for key in sorted(successful):
        if key in invalid or len(observations.get(key, ())) != 7:
            reasons[key] = ("HISTORY", {"INVALID_HISTORY" if key in invalid else "INCOMPLETE_HISTORY"})
    included = sorted(population - reasons.keys())
    predictions = tuple(ForecastPrediction(r["item_id"], r["store_id"], HISTORICAL_ISSUE_DATE,
        r["horizon"], _day(r["forecast_date"]), r["predicted_units"], r["forecast_run_id"],
        r["model_run_id"], r["source_snapshot_id"], r["feature_version"])
        for key in included for r in sorted(grouped[key], key=lambda r: r["horizon"]))
    if not upstream_evidence:
        raise DecisionInputError("historical upstream evidence is required")
    return DecisionInputs(predictions,
        tuple(r for key in included for r in sorted(observations[key], key=lambda r: r.date)),
        tuple(EligiblePair(*key) for key in included),
        tuple(ExcludedSeries(*key, tuple(sorted(codes)), stage)
              for key, (stage, codes) in sorted(reasons.items())),
        freeze(selector), freeze(scope), freeze(dict(candidate_series=len(population),
            forecast_eligible_series=len(successful), decision_eligible_series=len(included))),
        freeze({**plain(upstream_evidence), "test_binding": document_digest(binding),
                "selected_artifacts": selected_artifacts}))


def _check_historical_sales_evidence(selector, scope, evidence):
    if (evidence.get("scope") != plain(scope) or any(evidence.get(k) != v for k, v in dict(
            source_version=selector["source_version"], source_snapshot_id=selector["source_snapshot_id"],
            pipeline_run_id=selector["j03_pipeline_run_id"]).items())):
        raise DecisionInputError("historical J03 lineage or scope conflicts")


def read_historical_inputs(spark, *, client, config):
    """Bounded read-only historical path; no paired/alignment/metric dataset reads."""
    from warehouse_bigdata.common.hdfs_paths import render_evaluation_output_path

    selector = load_small_artifacts()
    validate_small_publications(selector, through="test", client=client, config=config)
    if spark.conf.get("spark.sql.session.timeZone") not in {"UTC", "Etc/UTC"}:
        raise DecisionInputError("forecast timestamp reads require a UTC Spark session")
    path = render_evaluation_output_path(config.base_path, selector["test_evaluation_run_id"])
    binding = read_json(client, path + "/evidence/evaluation.json")
    scope = read_scope(client, path, source_version=selector["source_version"],
        source_snapshot_id=selector["source_snapshot_id"], expected_digest=selector["selection_digest"])
    historical = _historical_selector(selector, scope, binding)
    frames = {name: spark.read.parquet(config.uri + path + "/origins/d_1913/" + name)
              for name in ("forecast_daily", "eligibility", "rf_exceptions")}
    require_forecast_schema(frames["forecast_daily"])
    args = dict(selector=selector, scope=scope, binding=binding,
        forecast_rows=collect_bounded(frames["forecast_daily"], 700, "forecast"),
        eligibility_rows=collect_bounded(frames["eligibility"], 100, "eligibility"),
        exception_rows=collect_bounded(frames["rf_exceptions"], 10000, "exceptions"),
        upstream_evidence={"test_binding": document_digest(binding),
            "test_manifest": document_digest(read_json(client, path + "/evaluation_manifest.json"))})
    # Validate prediction artifacts before opening even pre-origin J03 observations.
    bind_historical_inputs(**args, history_rows=[])
    successful = {(r["item_id"], r["store_id"]) for r in args["forecast_rows"]}
    history, evidence = read_history(spark, historical, successful, client=client, config=config,
                                    issue_date=HISTORICAL_ISSUE_DATE)
    _check_historical_sales_evidence(historical, scope, evidence)
    args["upstream_evidence"]["j03"] = document_digest(evidence)
    return bind_historical_inputs(**args, history_rows=history)


def read_historical_labels(spark, *, frozen, client, config):
    """Reveal only T+1..T+7 after verifying the immutable decision freeze."""
    from .policy_evaluation import validate_decision_freeze

    validate_decision_freeze(frozen)
    selector = plain(frozen.inputs.selector)
    validate_small_publications(plain(frozen.inputs.upstream_evidence["selected_artifacts"]),
                               through="test", client=client, config=config)
    keys = {(r.item_id, r.store_id) for r in frozen.requests}
    if not keys:
        return []
    rows, evidence = _read_sales_window(spark, selector, keys, client=client, config=config,
        start=HISTORICAL_ISSUE_DATE + timedelta(days=1),
        end=HISTORICAL_ISSUE_DATE + timedelta(days=7), label="labels")
    _check_historical_sales_evidence(selector, frozen.inputs.scope, evidence)
    if document_digest(evidence) != frozen.inputs.upstream_evidence.get("j03"):
        raise DecisionInputError("historical J03 evidence changed after decision freeze")
    return rows

"""Contract tests for seven-day forecast and evaluation records."""

import math
from copy import deepcopy

import pytest

from tests.fixtures.loader import load_context, load_dataset, load_expected
from warehouse_bigdata.contracts import DATASET_CONTRACTS_BY_NAME
from warehouse_bigdata.contracts.validation import ContractValidationError, validate_dataset


def _assert_code(error: ContractValidationError, code: str) -> None:
    assert any(issue.code == code for issue in error.issues), str(error)


def test_forecast_golden_values_cover_exactly_seven_horizons() -> None:
    context = load_context()
    expected = load_expected("forecast_daily")
    records = load_dataset("forecast_daily")

    validate_dataset(
        DATASET_CONTRACTS_BY_NAME["forecast_daily"],
        records,
        lineage_context=context["lineage"],
    )

    series = {}
    for record in records:
        series.setdefault(f"{record['item_id']}|{record['store_id']}", []).append(record)

    assert len(records) == expected["row_count"]
    assert len(series) == expected["series_count"]
    assert {record["horizon"] for record in series["ITEM_A|CA_1"]} == set(expected["horizons"])
    assert {record["horizon"] for record in series["ITEM_B|CA_2"]} == set(expected["horizons"])
    assert sorted({record["forecast_date"] for record in records}) == expected["forecast_dates"]
    assert {
        key: sum(record["predicted_units"] for record in values) for key, values in series.items()
    } == expected["series_totals"]
    assert all(
        math.isfinite(record["predicted_units"]) and record["predicted_units"] >= 0
        for record in records
    )


def test_missing_forecast_horizon_is_rejected() -> None:
    context = load_context()
    records = [
        record
        for record in deepcopy(load_dataset("forecast_daily"))
        if not (record["item_id"] == "ITEM_A" and record["horizon"] == 7)
    ]

    with pytest.raises(ContractValidationError) as error:
        validate_dataset(
            DATASET_CONTRACTS_BY_NAME["forecast_daily"],
            records,
            lineage_context=context["lineage"],
        )
    _assert_code(error.value, "INCOMPLETE_HORIZON_COVERAGE")


def test_duplicate_forecast_horizon_is_rejected() -> None:
    context = load_context()
    records = deepcopy(load_dataset("forecast_daily"))
    duplicate = next(
        record for record in records if record["item_id"] == "ITEM_A" and record["horizon"] == 2
    )
    duplicate["horizon"] = 1

    with pytest.raises(ContractValidationError) as error:
        validate_dataset(
            DATASET_CONTRACTS_BY_NAME["forecast_daily"],
            records,
            lineage_context=context["lineage"],
        )
    _assert_code(error.value, "DUPLICATE_HORIZON")


@pytest.mark.parametrize("horizon", [0, 8])
def test_forecast_horizon_outside_one_to_seven_is_rejected(horizon: int) -> None:
    context = load_context()
    records = deepcopy(load_dataset("forecast_daily"))
    records[0]["horizon"] = horizon

    with pytest.raises(ContractValidationError) as error:
        validate_dataset(
            DATASET_CONTRACTS_BY_NAME["forecast_daily"],
            records,
            lineage_context=context["lineage"],
        )
    _assert_code(error.value, "DOMAIN_VIOLATION")


def test_forecast_date_must_align_with_issue_date_and_horizon() -> None:
    context = load_context()
    records = deepcopy(load_dataset("forecast_daily"))
    records[0]["forecast_date"] = "2026-01-09"

    with pytest.raises(ContractValidationError) as error:
        validate_dataset(
            DATASET_CONTRACTS_BY_NAME["forecast_daily"],
            records,
            lineage_context=context["lineage"],
        )
    _assert_code(error.value, "TEMPORAL_MISMATCH")


@pytest.mark.parametrize("prediction", [-1.0, math.nan, math.inf])
def test_prediction_must_be_finite_and_nonnegative(prediction: float) -> None:
    context = load_context()
    records = deepcopy(load_dataset("forecast_daily"))
    records[0]["predicted_units"] = prediction

    with pytest.raises(ContractValidationError) as error:
        validate_dataset(
            DATASET_CONTRACTS_BY_NAME["forecast_daily"],
            records,
            lineage_context=context["lineage"],
        )
    _assert_code(error.value, "DOMAIN_VIOLATION")


def test_invalid_forecast_run_type_and_missing_model_lineage_are_rejected() -> None:
    context = load_context()
    records = deepcopy(load_dataset("forecast_daily"))
    records[0]["run_type"] = "historical_backtest"
    with pytest.raises(ContractValidationError) as run_type_error:
        validate_dataset(
            DATASET_CONTRACTS_BY_NAME["forecast_daily"],
            records,
            lineage_context=context["lineage"],
        )
    _assert_code(run_type_error.value, "INVALID_ENUM")

    missing_lineage = deepcopy(load_dataset("forecast_daily")[:1])
    del missing_lineage[0]["model_run_id"]
    with pytest.raises(ContractValidationError) as lineage_error:
        validate_dataset(
            DATASET_CONTRACTS_BY_NAME["forecast_daily"],
            missing_lineage,
            lineage_context=context["lineage"],
        )
    _assert_code(lineage_error.value, "MISSING_LINEAGE")


def test_nullable_metric_value_is_accepted_without_claiming_accuracy() -> None:
    context = load_context()
    expected = load_expected("forecast_metrics")
    records = load_dataset("forecast_metrics")

    validate_dataset(
        DATASET_CONTRACTS_BY_NAME["forecast_metrics"],
        records,
        lineage_context=context["lineage"],
    )
    assert len(records) == expected["row_count"]
    assert records[0]["metric_value"] is None
    assert expected["metric_value_is_null"] is True


def test_forward_demo_cannot_be_used_for_forecast_metrics() -> None:
    context = load_context()
    records = deepcopy(load_dataset("forecast_metrics"))
    records[0]["run_type"] = "forward_demo"

    with pytest.raises(ContractValidationError) as error:
        validate_dataset(
            DATASET_CONTRACTS_BY_NAME["forecast_metrics"],
            records,
            lineage_context=context["lineage"],
        )
    _assert_code(error.value, "RUN_TYPE_MISMATCH")


def test_forward_demo_cannot_be_used_for_policy_evaluation() -> None:
    context = load_context()
    records = deepcopy(load_dataset("inventory_policy_evaluation"))
    records[0]["run_type"] = "forward_demo"

    with pytest.raises(ContractValidationError) as error:
        validate_dataset(
            DATASET_CONTRACTS_BY_NAME["inventory_policy_evaluation"],
            records,
            lineage_context=context["lineage"],
        )
    _assert_code(error.value, "RUN_TYPE_MISMATCH")

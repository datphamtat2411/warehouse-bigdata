"""Contract tests for synthetic source, enrichment, feature, and decision rows."""

from copy import deepcopy

import pytest

from tests.fixtures.fnd06_loader import load_expected, load_fixture
from warehouse_bigdata.contracts import DATASET_CONTRACTS_BY_NAME
from warehouse_bigdata.contracts.validation import (
    ContractValidationError,
    validate_dataset,
    validate_record,
)


def _assert_code(error: ContractValidationError, code: str) -> None:
    assert any(issue.code == code for issue in error.issues), str(error)


def test_valid_synthetic_datasets_are_accepted_by_catalog_contracts() -> None:
    fixture = load_fixture()

    assert fixture["metadata"]["synthetic"] is True
    for dataset_name, records in fixture["datasets"].items():
        validate_dataset(
            DATASET_CONTRACTS_BY_NAME[dataset_name],
            records,
            lineage_context=fixture["lineage"],
        )


def test_sales_golden_values_are_independently_verified() -> None:
    fixture = load_fixture()
    expected = load_expected()["sales_daily"]
    records = fixture["datasets"]["sales_daily"]

    validate_dataset(
        DATASET_CONTRACTS_BY_NAME["sales_daily"],
        records,
        lineage_context=fixture["lineage"],
    )

    keys = {(row["item_id"], row["store_id"], row["date"]) for row in records}
    assert len(records) == expected["row_count"]
    assert len(keys) == expected["logical_key_count"]
    assert sum(row["unit_sales"] for row in records) == expected["unit_sales_total"]
    assert sum(row["unit_sales"] == 0 for row in records) == expected["zero_sales_count"]


def test_enrichment_preserves_zero_sales_and_nullable_price() -> None:
    fixture = load_fixture()
    records = fixture["datasets"]["sales_enriched"]

    validate_dataset(
        DATASET_CONTRACTS_BY_NAME["sales_enriched"],
        records,
        lineage_context=fixture["lineage"],
    )

    assert sum(row["sell_price"] is None for row in records) == 1
    assert sum(row["is_zero_sale"] for row in records) == 2
    missing_price = next(row for row in records if row["sell_price"] is None)
    assert missing_price["price_missing"] is True


def test_store_aggregate_golden_values_are_independently_verified() -> None:
    fixture = load_fixture()
    expected = load_expected()["store_daily_sales"]
    records = fixture["datasets"]["store_daily_sales"]

    validate_dataset(
        DATASET_CONTRACTS_BY_NAME["store_daily_sales"],
        records,
        lineage_context=fixture["lineage"],
    )

    actual = {f"{row['store_id']}|{row['date']}": row["total_units"] for row in records}
    assert len(records) == expected["row_count"]
    assert actual == expected["totals"]


def test_nullable_feature_label_is_accepted() -> None:
    fixture = load_fixture()
    record = fixture["datasets"]["forecast_features"][0]

    validate_record(
        DATASET_CONTRACTS_BY_NAME["forecast_features"],
        record,
        lineage_context=fixture["lineage"],
    )
    assert record["label"] is None


def test_missing_required_sales_field_is_rejected() -> None:
    fixture = load_fixture()
    record = deepcopy(fixture["datasets"]["sales_daily"][0])
    del record["unit_sales"]

    with pytest.raises(ContractValidationError) as error:
        validate_record(
            DATASET_CONTRACTS_BY_NAME["sales_daily"],
            record,
            lineage_context=fixture["lineage"],
        )

    _assert_code(error.value, "MISSING_FIELD")


def test_non_nullable_null_and_wrong_sales_type_are_rejected() -> None:
    fixture = load_fixture()
    contract = DATASET_CONTRACTS_BY_NAME["sales_daily"]

    null_record = deepcopy(fixture["datasets"]["sales_daily"][0])
    null_record["unit_sales"] = None
    with pytest.raises(ContractValidationError) as null_error:
        validate_record(contract, null_record, lineage_context=fixture["lineage"])
    _assert_code(null_error.value, "NULL_NOT_ALLOWED")

    fractional_record = deepcopy(fixture["datasets"]["sales_daily"][0])
    fractional_record["unit_sales"] = 1.5
    with pytest.raises(ContractValidationError) as type_error:
        validate_record(contract, fractional_record, lineage_context=fixture["lineage"])
    _assert_code(type_error.value, "TYPE_MISMATCH")


def test_negative_sales_and_unexpected_field_are_rejected() -> None:
    fixture = load_fixture()
    contract = DATASET_CONTRACTS_BY_NAME["sales_daily"]

    negative_record = deepcopy(fixture["datasets"]["sales_daily"][0])
    negative_record["unit_sales"] = -1
    with pytest.raises(ContractValidationError) as negative_error:
        validate_record(contract, negative_record, lineage_context=fixture["lineage"])
    _assert_code(negative_error.value, "DOMAIN_VIOLATION")

    extra_record = deepcopy(fixture["datasets"]["sales_daily"][0])
    extra_record["unapproved_field"] = "not in catalog"
    with pytest.raises(ContractValidationError) as extra_error:
        validate_record(contract, extra_record, lineage_context=fixture["lineage"])
    _assert_code(extra_error.value, "UNEXPECTED_FIELD")


def test_zero_sales_remains_valid() -> None:
    fixture = load_fixture()
    record = next(row for row in fixture["datasets"]["sales_daily"] if row["unit_sales"] == 0)

    validate_record(
        DATASET_CONTRACTS_BY_NAME["sales_daily"],
        record,
        lineage_context=fixture["lineage"],
    )


def test_duplicate_sales_key_is_rejected() -> None:
    fixture = load_fixture()
    records = deepcopy(fixture["datasets"]["sales_daily"])
    records.append(deepcopy(records[0]))

    with pytest.raises(ContractValidationError) as error:
        validate_dataset(
            DATASET_CONTRACTS_BY_NAME["sales_daily"],
            records,
            lineage_context=fixture["lineage"],
        )

    _assert_code(error.value, "DUPLICATE_KEY")


def test_missing_dataset_lineage_is_rejected() -> None:
    fixture = load_fixture()
    with pytest.raises(ContractValidationError) as error:
        validate_record(
            DATASET_CONTRACTS_BY_NAME["sales_daily"],
            fixture["datasets"]["sales_daily"][0],
        )

    _assert_code(error.value, "MISSING_LINEAGE")


def test_inventory_and_decision_quantity_domains_are_enforced() -> None:
    fixture = load_fixture()
    contract = DATASET_CONTRACTS_BY_NAME["inventory_scenario"]
    record = deepcopy(fixture["datasets"]["inventory_scenario"][0])
    record["opening_stock_units"] = -1

    with pytest.raises(ContractValidationError) as error:
        validate_record(contract, record, lineage_context=fixture["lineage"])
    _assert_code(error.value, "DOMAIN_VIOLATION")

    allocation = deepcopy(fixture["datasets"]["allocation_recommendations"])
    allocation[0]["allocated_units"] = allocation[0]["requested_units"] + 1
    with pytest.raises(ContractValidationError) as allocation_error:
        validate_dataset(
            DATASET_CONTRACTS_BY_NAME["allocation_recommendations"],
            allocation,
            lineage_context=fixture["lineage"],
        )
    _assert_code(allocation_error.value, "CONSTRAINT_VIOLATION")


def test_total_allocation_cannot_exceed_one_central_supply_row() -> None:
    fixture = load_fixture()
    allocation = deepcopy(fixture["datasets"]["allocation_recommendations"])
    allocation[1]["allocated_units"] = 3

    with pytest.raises(ContractValidationError) as error:
        validate_dataset(
            DATASET_CONTRACTS_BY_NAME["allocation_recommendations"],
            allocation,
            lineage_context=fixture["lineage"],
        )

    _assert_code(error.value, "CONSTRAINT_VIOLATION")


def test_duplicate_decision_key_and_missing_lineage_are_rejected() -> None:
    fixture = load_fixture()
    contract = DATASET_CONTRACTS_BY_NAME["replenishment_recommendations"]
    records = deepcopy(fixture["datasets"]["replenishment_recommendations"])
    records.append(deepcopy(records[0]))
    with pytest.raises(ContractValidationError) as duplicate_error:
        validate_dataset(contract, records, lineage_context=fixture["lineage"])
    _assert_code(duplicate_error.value, "DUPLICATE_KEY")

    record = deepcopy(records[0])
    del record["forecast_run_id"]
    with pytest.raises(ContractValidationError) as lineage_error:
        validate_record(contract, record, lineage_context=fixture["lineage"])
    _assert_code(lineage_error.value, "MISSING_LINEAGE")


def test_nullable_scenario_seed_is_accepted() -> None:
    fixture = load_fixture()
    record = deepcopy(fixture["datasets"]["inventory_scenario"][0])
    record["scenario_seed"] = None

    validate_record(
        DATASET_CONTRACTS_BY_NAME["inventory_scenario"],
        record,
        lineage_context=fixture["lineage"],
    )

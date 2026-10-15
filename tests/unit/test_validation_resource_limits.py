"""Regression checks for the bounded Python validation workload."""

from copy import deepcopy
from datetime import date, timedelta

import pytest

from tests.fixtures.loader import load_context, load_dataset
from warehouse_bigdata.contracts import DATASET_CONTRACTS_BY_NAME
from warehouse_bigdata.contracts.validation import ContractValidationError, validate_dataset


def _valid_sales_records(count: int):
    template = load_dataset("sales_daily")[0]
    start = date.fromisoformat(template["date"])
    for index in range(count):
        record = deepcopy(template)
        record["date"] = (start + timedelta(days=index)).isoformat()
        record["day_key"] = f"d_{index + 1:05d}"
        record["day_index"] = index + 1
        yield record


def test_small_dataset_is_accepted() -> None:
    validate_dataset(
        DATASET_CONTRACTS_BY_NAME["sales_daily"],
        load_dataset("sales_daily"),
        lineage_context=load_context()["lineage"],
    )


def test_dataset_at_the_configured_boundary_is_accepted() -> None:
    validate_dataset(
        DATASET_CONTRACTS_BY_NAME["sales_daily"],
        _valid_sales_records(10_000),
        lineage_context=load_context()["lineage"],
    )


def test_oversized_generator_stops_at_the_configured_boundary() -> None:
    consumed = 0

    def records():
        nonlocal consumed
        while True:
            consumed += 1
            yield {}

    with pytest.raises(ContractValidationError) as error:
        validate_dataset(DATASET_CONTRACTS_BY_NAME["sales_daily"], records())

    assert consumed == 10_001
    assert [issue.code for issue in error.value.issues] == ["DATASET_TOO_LARGE"]

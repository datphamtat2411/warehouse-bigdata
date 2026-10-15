"""Regression checks for selective, isolated fixture loading."""

import pytest

from tests.fixtures.loader import (
    load_context,
    load_dataset,
    load_expected,
    load_manifest,
    load_publication_fixture,
)


def test_known_fixture_resources_load_by_domain() -> None:
    assert len(load_dataset("sales_daily")) == 8
    assert len(load_dataset("forecast_daily")) == 14
    assert load_manifest("forecast_manifest")["horizon_days"] == 7
    publication = load_publication_fixture()
    assert publication["publication_context"]["publication_id"]
    assert publication["publication_copy"]["manifest_checksum"]


def test_unknown_fixture_resources_are_rejected() -> None:
    with pytest.raises(KeyError, match="Unknown dataset fixture"):
        load_dataset("unknown_dataset")
    with pytest.raises(KeyError, match="Unknown manifest fixture"):
        load_manifest("unknown_manifest")
    with pytest.raises(KeyError, match="Unknown expected fixture"):
        load_expected("unknown_expected")


def test_loader_returns_fresh_mutable_values() -> None:
    first_records = load_dataset("sales_daily")
    first_records[0]["unit_sales"] = 999
    assert load_dataset("sales_daily")[0]["unit_sales"] == 0

    first_context = load_context()
    first_context["lineage"]["source_version"] = "changed"
    assert load_context()["lineage"]["source_version"] == "synthetic-source-001"

    first_publication = load_publication_fixture()
    first_publication["publication_context"]["policy_ids"].append("changed")
    assert len(load_publication_fixture()["publication_context"]["policy_ids"]) == 1


def test_expected_values_are_deterministic_and_independent() -> None:
    first = load_expected("forecast_daily")
    second = load_expected("forecast_daily")

    assert first == second
    first["forecast_dates"].append("changed")
    assert "changed" not in load_expected("forecast_daily")["forecast_dates"]

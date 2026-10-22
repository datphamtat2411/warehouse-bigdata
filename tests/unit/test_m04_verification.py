"""Independent manifest reconciliation from persisted scalar metrics, without Spark."""

import json
from types import SimpleNamespace

import pytest

from warehouse_bigdata.common.config import HDFSConfig
from warehouse_bigdata.contracts.datasets import SALES_ENRICHED
from warehouse_bigdata.processing import m04_verification as verification


@pytest.mark.parametrize("layout", ["canonical", "partition-last", "missing", "wrong-type"])
def test_enriched_schema_allows_spark_year_partition_order_but_keeps_field_and_type_checks(
    layout: str,
) -> None:
    fields = SALES_ENRICHED.field_names
    types = tuple("int" if field == "year" else "string" for field in fields)
    columns = list(fields)
    if layout != "canonical":
        columns.remove("year")
        columns.append("year")
    if layout == "missing":
        columns.remove("price_missing")
    schema = {
        name: SimpleNamespace(name=name, dataType=kind)
        for name, kind in zip(fields, types, strict=True)
    }
    if layout == "wrong-type":
        schema["year"].dataType = "string"
    frame = SimpleNamespace(columns=columns, schema=schema)

    if layout in {"missing", "wrong-type"}:
        with pytest.raises(verification.M04VerificationError, match="fields differ|type differs"):
            verification._check_schema(frame, fields, types, "enriched", partition_fields=("year",))
    else:
        verification._check_schema(frame, fields, types, "enriched", partition_fields=("year",))
        if layout == "partition-last":
            with pytest.raises(verification.M04VerificationError, match="fields differ"):
                verification._check_schema(frame, fields, types, "raw CSV")


@pytest.mark.parametrize("nullable_prices", [0, 4, 10])
@pytest.mark.parametrize("wrong_matched_count", [False, True])
def test_enriched_manifest_reconciles_matched_prices_from_actual_reread_counts(
    nullable_prices: int, wrong_matched_count: bool
) -> None:
    config = HDFSConfig("hdfs://hdfs-namenode:8020", "/data/warehouse")
    path = "/data/warehouse/curated/sales_enriched/source_snapshot_id=snapshot"
    # _sales_metrics returns sales counts only; it does not supply a price count.
    metrics = {
        "row_count": 10,
        "series_count": 2,
        "day_count": 5,
        "unit_sales_total": 18,
        "zero_sales_count": 3,
        "null_count": 0,
    }
    manifest = {
        "schema_version": "enrichment-dataset-manifest-v1",
        "status": "VALIDATED",
        "dataset_name": SALES_ENRICHED.name,
        "dataset_version": SALES_ENRICHED.version,
        "provenance": SALES_ENRICHED.provenance,
        "source_version": "sha256:" + "a" * 64,
        "source_snapshot_id": "snapshot",
        "pipeline_run_id": "enrichment",
        "artifact_uri": config.uri + path,
        "schema": list(SALES_ENRICHED.field_names),
        "schema_fingerprint": verification._schema_fingerprint(SALES_ENRICHED.field_names),
        "physical_schema": {"sell_price": "double"},
        "logical_unique_key": list(SALES_ENRICHED.unique_key),
        "persisted_parquet_validated": True,
        "row_count": 10,
        "unit_sales_total": 18,
        "zero_sales_count": 3,
        "matched_price_count": 10 - nullable_prices + int(wrong_matched_count),
        "missing_price_count": nullable_prices,
        "price_match_rate": (10 - nullable_prices) / 10,
    }
    client = SimpleNamespace(read_remote=lambda _: json.dumps(manifest).encode())
    arguments = {
        "source_version": manifest["source_version"],
        "source_snapshot_id": "snapshot",
        "pipeline_run_id": "enrichment",
        "physical_schema": manifest["physical_schema"],
        "metrics": metrics,
        "nullable_prices": nullable_prices,
    }

    if wrong_matched_count:
        with pytest.raises(verification.M04VerificationError, match="independent Parquet reread"):
            verification._validate_enriched_manifest(client, config, path, **arguments)
    else:
        verification._validate_enriched_manifest(client, config, path, **arguments)

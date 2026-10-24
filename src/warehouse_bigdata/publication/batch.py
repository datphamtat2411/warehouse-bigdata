"""Validated M5 Small publication with an explicit offline replacement boundary.

The availability marker fences readers while the old approval remains as history.
No cross-shard atomicity or rollback is implied. A failed attempt must be reloaded
from empty tables, never resumed by appending.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import sqlite3
import struct
from tempfile import NamedTemporaryFile, TemporaryDirectory
from uuid import uuid4
from datetime import timedelta

from warehouse_bigdata.common.hdfs_paths import (
    render_curated_sales_daily_path,
    render_curated_store_daily_sales_path,
    render_decision_output_path,
    render_evaluation_output_path,
    render_forecast_output_path,
    render_publication_manifest_path,
    validate_pipeline_run_id,
)
from warehouse_bigdata.common.hdfs_publication import document_digest
from warehouse_bigdata.common.small_artifacts import validate_small_artifacts
from warehouse_bigdata.common.small_preflight import validate_small_publications
from warehouse_bigdata.contracts.datasets import DATASET_CONTRACTS_BY_NAME, SERVING_DATASET_MAPPINGS
from warehouse_bigdata.contracts.validation import validate_dataset, validate_publication, validate_record
from warehouse_bigdata.forecasting.feature_input import read_json, require_sealed

from .parquet import DEFAULT_BATCH_ROWS, MAX_BATCH_ROWS, ParquetFile, iter_parquet_batches

MAPPINGS = {m.logical_dataset: m for m in SERVING_DATASET_MAPPINGS}
HISTORICAL = ("forecast_metrics", "inventory_policy_evaluation")
IDENTITIES = (
    "source_snapshot_id", "model_run_id", "forecast_run_id", "feature_version", "scenario_id",
    "decision_run_id", "evaluation_run_id", "holdout_id", "issue_date", "policy_id",
    "policy_version", "run_type",
)


class PublicationError(ValueError):
    """An input, serving result, or approval gate failed closed."""


def _require(condition, message):
    if not condition:
        raise PublicationError(message)


def _bytes(document):
    return (json.dumps(document, sort_keys=True, separators=(",", ":"),
                       default=_json_value, allow_nan=False) + "\n").encode()


def _json_value(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"unsupported publication value: {type(value).__name__}")


@dataclass(frozen=True)
class PublicationDataset:
    name: str
    path: str
    files: tuple[ParquetFile, ...]
    expected_rows: int
    manifest: Mapping
    binding: Mapping


@dataclass(frozen=True)
class PublicationSelection:
    selector: Mapping
    scope: Mapping
    datasets: tuple[PublicationDataset, ...]
    context: Mapping

    @property
    def bindings(self):
        return {d.name: dict(d.binding) for d in self.datasets}


@dataclass(frozen=True)
class ReconciliationReport:
    validated: bool
    loaded_rows_per_shard: Mapping = field(default_factory=dict)
    evidence: Mapping = field(default_factory=dict)


@dataclass(frozen=True)
class PublicationResult:
    publication_id: str
    status: str
    reconciliation: ReconciliationReport
    manifest_path: str


def validate_offline_gate(offline_reload, confirm_offline_reload):
    _require(offline_reload is True and confirm_offline_reload is True,
             "both --offline-reload and --confirm-offline-reload are required")


def validate_dataset_bindings(bindings):
    """Historical bindings share source/scope, not forward-demo run identities."""
    primary = bindings.get("forecast_daily", {})
    _require(primary.get("run_type") == "forward_demo", "primary forecast must be forward_demo")
    for name, binding in bindings.items():
        for key in ("source_snapshot_id", "selection_digest", "dataset_scope", "model_run_id"):
            _require(binding.get(key) == primary.get(key) and binding.get(key),
                     f"incompatible {name} binding: {key}")
        _require(binding["dataset_scope"] == "demo_sample", "publication requires M5 Small")
        if name in HISTORICAL:
            _require(binding.get("run_type") in {"validation", "test"},
                     "historical evaluation cannot be forward_demo")
            _require(binding.get("evaluation_run_id"), "missing historical evaluation identity")
            if name == "forecast_metrics":
                _require(binding.get("holdout_id") and binding.get("origin_forecasts"),
                         "missing historical forecast bindings")
            else:
                for key in ("forecast_run_id", "issue_date", "scenario_id", "decision_run_ids",
                            "policy_ids", "policy_version"):
                    _require(binding.get(key), f"missing policy evaluation binding: {key}")
                _require(binding.get("label_provenance") == "observed_m5_unit_sales_demand_proxy",
                         "historical policy evaluation requires observed labels")


def _cast(value, mapping):
    kind = mapping.logical_type
    if value is None:
        _require(mapping.nullable, f"null in required field {mapping.logical_field}")
        return None
    if kind == "Date":
        result = date.fromisoformat(value) if isinstance(value, str) else value
        _require(type(result) is date and date(1970, 1, 1) <= result <= date(2149, 6, 6),
                 "date outside ClickHouse Date range")
        return result
    if kind == "TimestampUTC":
        result = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
        _require(isinstance(result, datetime) and result.tzinfo is not None,
                 "timestamp requires an explicit UTC offset")
        result = result.astimezone(timezone.utc)
        _require(1900 <= result.year <= 2299, "timestamp outside ClickHouse DateTime64 range")
        # DateTime64(3) has an explicit millisecond precision boundary. Original
        # microseconds remain in the sealed Parquet artifact and its manifest.
        return result.replace(microsecond=(result.microsecond // 1000) * 1000)
    if kind == "String":
        _require(isinstance(value, str), "string conversion would be lossy")
        return value
    if kind == "Bool":
        _require(type(value) is bool, "boolean conversion would be lossy")
        return int(value)
    if kind in {"Integer", "NonNegativeInt"}:
        _require(type(value) is int, "integer conversion would be lossy")
        physical = mapping.serving_type or ("UInt64" if kind == "NonNegativeInt" else "Int32")
        lower, upper = {"UInt8": (0, 2**8), "UInt32": (0, 2**32), "UInt64": (0, 2**64),
                        "Int32": (-2**31, 2**31)}[physical]
        _require(lower <= value < upper, f"overflow in {mapping.logical_field}")
        return value
    _require(type(value) in (int, float) and math.isfinite(value), "non-finite numeric value")
    _require(kind != "NonNegativeFloat" or value >= 0, "negative numeric value")
    result = float(value)
    _require(not isinstance(value, int) or int(result) == value, "lossy Float64 conversion")
    return result


def _replace_remote(client, payload, path):
    pending = path + ".pending-" + uuid4().hex
    client.upload_bytes(payload, pending)
    _require(client.read_remote(pending) == payload, "publication metadata readback differs")
    client.rename_replace(pending, path)


def write_publication_manifest(client, manifest, *, base_path="/data/warehouse", initial=False):
    """Retain immutable lifecycle evidence and atomically advance the candidate record."""
    path = render_publication_manifest_path(base_path, manifest["publication_id"])
    client.mkdir_parents(str(PurePosixPath(path).parent))
    payload = _bytes(manifest)
    evidence = path.removesuffix(".json") + "." + manifest["status"].lower() + ".json"
    client.upload_bytes(payload, evidence)
    _require(client.read_remote(evidence) == payload, "publication evidence readback differs")
    if initial:
        client.upload_bytes(payload, path)
    else:
        _replace_remote(client, payload, path)
    return path


def _atomic_local(payload, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(prefix=".j08-", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        try:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def approve_publication(
    selection, manifest, reconciliation, *, client, application_path,
    base_path="/data/warehouse",
):
    """Validate exact application metadata before making either approval copy visible."""
    _require(manifest["status"] == "VALIDATED" and reconciliation.validated,
             "approval requires a VALIDATED reconciled candidate")
    validate_dataset_bindings(selection.bindings)
    approved = dict(manifest, status="APPROVED", approved_status="APPROVED")
    manifest_path = render_publication_manifest_path(base_path, approved["publication_id"])
    coverage = dict(
        series_total=len(selection.scope["selected_series"]),
        series_eligible=manifest["expected_rows_map"]["forecast_daily"] // 7,
        series_scored=manifest["expected_rows_map"]["forecast_daily"] // 7,
        loaded_rows=sum(manifest["expected_rows_map"].values()),
        available_stores=sorted({r["store_id"] for r in selection.scope["selected_series"]}),
    )
    # The content digest binds source selections and measured serving evidence; it
    # deliberately excludes self-referential application/manifest checksum fields.
    release = dict(publication_id=approved["publication_id"], datasets=approved["datasets"],
                   dataset_bindings=selection.bindings, reconciliation=reconciliation.evidence,
                   context=dict(selection.context), coverage=coverage)
    approved["artifact_checksum"] = document_digest(release)
    approved["release_digest_payload"] = release
    context = dict(selection.context, contract_version="1.0", publication_id=approved["publication_id"],
                   manifest_checksum=approved["artifact_checksum"], coverage=coverage,
                   dataset_bindings=selection.bindings)
    application_copy = dict(context, status="APPROVED", manifest_uri=base_path +
                            "/manifests/publications/" + approved["publication_id"] + ".json")
    payload = _bytes(application_copy)
    approved["application_manifest_checksum"] = "sha256:" + hashlib.sha256(payload).hexdigest()
    approved["application_checksum_definition"] = "sha256-canonical-application-copy-v1"
    approved["publication_context"] = context
    validate_publication(approved, context=context, application_copy=application_copy)
    write_publication_manifest(client, approved, base_path=base_path)
    pointer = base_path + "/manifests/published/current.json"
    # Both writes occur only after all validation gates. The availability fence is
    # retained across activation failure, including failures of compensation.
    old_local = Path(application_path).read_bytes() if Path(application_path).exists() else None
    old_pointer = client.read_remote(pointer) if client.exists(pointer) else None
    fence = base_path + "/manifests/published/unavailable.json"
    _require(client.exists(fence), "offline availability fence is required before activation")
    try:
        _atomic_local(payload, application_path)
        _replace_remote(client, payload, pointer)
        _require(client.read_remote(pointer) == Path(application_path).read_bytes() == payload,
                 "authoritative pointer/application copy differ")
        client.remove(fence)
    except BaseException:
        if not client.exists(fence):
            _replace_remote(client, _bytes(dict(publication_id=manifest["publication_id"],
                                               status="UNAVAILABLE", reason="activation-failure")), fence)
        if old_local is None:
            Path(application_path).unlink(missing_ok=True)
        else:
            _atomic_local(old_local, application_path)
        if old_pointer is not None:
            _replace_remote(client, old_pointer, pointer)
        elif client.exists(pointer):
            client.remove(pointer)
        raise
    return approved


def publish_offline_reload(
    selection, *, client, serving, scratch_dir, application_path=Path("artifacts/approved_publication.json"),
    batch_rows=DEFAULT_BATCH_ROWS, offline_reload=False, confirm_offline_reload=False,
    base_path="/data/warehouse",
) -> PublicationResult:
    """Every attempt crosses a clean reload boundary; failures leave a durable fence."""
    validate_offline_gate(offline_reload, confirm_offline_reload)
    _require(type(batch_rows) is int and 1 <= batch_rows <= MAX_BATCH_ROWS, "invalid bounded batch size")
    publication_id = "j08-" + uuid4().hex
    parent = base_path + "/manifests/published"
    client.mkdir_parents(parent)
    lock = parent + "/offline-reload.lock"
    timestamp = datetime.now(timezone.utc).isoformat()
    manifest = dict(
        manifest_schema_version="1.0", contract_name="publication_manifest", contract_version="1.0",
        manifest_kind="publication", status="RUNNING", producer="warehouse-bigdata:publish-clickhouse",
        publication_id=publication_id, source_snapshot_id=selection.selector.get("source_snapshot_id"),
        artifact_uri=render_publication_manifest_path(base_path, publication_id), artifact_checksum=None,
        schema_fingerprint=document_digest({n: list(DATASET_CONTRACTS_BY_NAME[n].field_names) for n in MAPPINGS}),
        grain="One validated nine-table M5 Small serving release", logical_unique_key="publication_id",
        row_count=sum(d.expected_rows for d in selection.datasets), required_field_null_counts="{}",
        code_revision="sha256:" + hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        configuration_digest=document_digest(dict(batch_rows=batch_rows,
                        selector=dict(selection.selector), bindings=selection.bindings)),
        created_at=timestamp, completed_at=None, coverage_and_exclusion_counts="{}",
        selected_dataset_manifests=json.dumps({d.name: "1.0" for d in selection.datasets}, sort_keys=True),
        serving_schema_versions=json.dumps({d.name: "1.0" for d in selection.datasets}, sort_keys=True),
        expected_rows=json.dumps({d.name: d.expected_rows for d in selection.datasets}, sort_keys=True),
        expected_rows_map={d.name: d.expected_rows for d in selection.datasets},
        loaded_rows_per_shard="{}", representative_measure_reconciliation="{}", approved_status="RUNNING",
        publication_timestamp=timestamp, application_manifest_checksum="pending",
        materialization_reference=str(application_path), dataset_bindings=selection.bindings,
        datasets={d.name: dict(path=d.path, contract_version="1.0", expected_rows=d.expected_rows,
                             provenance=DATASET_CONTRACTS_BY_NAME[d.name].provenance,
                             manifest=dict(d.manifest),
                             files=[dict(path=f.path, size_bytes=f.size_bytes, checksum=f.checksum,
                                         sha256=f.sha256) for f in d.files]) for d in selection.datasets},
        **{k: v for k, v in selection.context.items()
           if k in IDENTITIES and k != "source_snapshot_id" and v is not None},
    )
    mutated = False
    client.mkdir_exclusive(lock)
    try:
        path = write_publication_manifest(client, manifest, base_path=base_path, initial=True)
        serving.prepare()
        expected = validate_source_data(selection, client=client, scratch_dir=scratch_dir, batch_rows=batch_rows)
        # The durable fence is published before the first physical table is cleared.
        _replace_remote(client, _bytes(dict(publication_id=publication_id, status="UNAVAILABLE",
                                           reason="offline-reload")), parent + "/unavailable.json")
        mutated = True
        serving.clear()
        for dataset in selection.datasets:
            loaded = _new_stats(dataset.name)
            for _, (columns, rows) in _iterate_dataset(dataset, client, scratch_dir, batch_rows):
                serving.insert(dataset, columns, rows)
                _add_stats(loaded, dataset.name, rows)
            _require(_same_stats(loaded, expected[dataset.name]), "source changed across offline reload")
        reconciliation = reconcile_publication(selection, expected, serving=serving)
        manifest.update(status="VALIDATED", approved_status="VALIDATED", completed_at=datetime.now(timezone.utc).isoformat(),
                        loaded_rows_per_shard=json.dumps(reconciliation.loaded_rows_per_shard, sort_keys=True),
                        representative_measure_reconciliation=json.dumps(reconciliation.evidence, sort_keys=True),
                        source_statistics=expected)
        write_publication_manifest(client, manifest, base_path=base_path)
        approve_publication(selection, manifest, reconciliation, client=client,
                            application_path=application_path, base_path=base_path)
        return PublicationResult(publication_id, "APPROVED", reconciliation, path)
    except BaseException as error:
        manifest.update(status="FAILED", approved_status="FAILED",
                        serving_unavailable=mutated or client.exists(parent + "/unavailable.json"),
                        completed_at=datetime.now(timezone.utc).isoformat(),
                        failure=dict(type=type(error).__name__, message=str(error)[:4000]))
        write_publication_manifest(client, manifest, base_path=base_path)
        raise
    finally:
        client.remove(lock)


def _columns(name):
    mapping = MAPPINGS[name]
    result = [(f.serving_field, f.serving_type, f.nullable)
              for f in mapping.field_mappings if f.serving_field]
    if name == "sales_daily":
        result.append(("source_snapshot_id", "String", False))
    result.extend((f, "String", False) for f in mapping.extra_serving_fields)
    return result


def _physical_bytes(value, kind):
    if value is None:
        return b"N"
    if kind == "String":
        encoded = value.encode()
        payload = struct.pack("<Q", len(encoded)) + encoded
    elif kind == "Date":
        payload = struct.pack("<H", (value - date(1970, 1, 1)).days)
    elif kind.startswith("DateTime64"):
        delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
        milliseconds = delta.days * 86400000 + delta.seconds * 1000 + delta.microseconds // 1000
        payload = struct.pack("<q", milliseconds)
    else:
        payload = struct.pack({"UInt8": "<B", "UInt32": "<I", "UInt64": "<Q",
                               "Int32": "<i", "Float64": "<d"}[kind], value)
    return b"V" + payload


def _new_stats(name):
    return dict(rows=0, measures={c: 0 for c, t, _ in _columns(name)
                                 if t in {"UInt8", "UInt32", "UInt64", "Int32", "Float64"}},
                nulls={c: 0 for c, _, nullable in _columns(name) if nullable}, fingerprint=[0, 0])


def _add_stats(stats, name, rows):
    columns = _columns(name)
    for row in rows:
        stats["rows"] += 1
        data = bytearray()
        for (column, kind, nullable), value in zip(columns, row, strict=True):
            data.extend(_physical_bytes(value, kind))
            if value is None:
                stats["nulls"][column] += 1
            elif column in stats["measures"]:
                stats["measures"][column] += value
        digest = hashlib.md5(data, usedforsecurity=False).digest()
        for index in (0, 1):
            stats["fingerprint"][index] = (stats["fingerprint"][index]
                + int.from_bytes(digest[index * 8:index * 8 + 8], "little")) % 2**64


def _validate_historical_rows(dataset, records):
    binding = dataset.binding
    if dataset.name == "forecast_metrics":
        for row in records:
            _require(row["evaluation_run_id"] == binding["evaluation_run_id"]
                     and row["holdout_id"] == binding["holdout_id"], "metric historical binding differs")
            _require(row["forecast_run_id"] is None
                     or row["forecast_run_id"] in binding["origin_forecasts"].values(),
                     "metric forecast is outside historical origin bindings")
    elif dataset.name == "inventory_policy_evaluation":
        for row in records:
            _require(row["policy_id"] in binding["decision_run_ids"]
                     and row["decision_run_id"] == binding["decision_run_ids"][row["policy_id"]],
                     "policy evaluation decision binding differs")


def _iterate_dataset(dataset, client, scratch_dir, batch_rows):
    batches = iter_parquet_batches(client, dataset.files, scratch_dir=scratch_dir, batch_rows=batch_rows)
    try:
        for batch in batches:
            _require(tuple(batch.schema.names) == DATASET_CONTRACTS_BY_NAME[dataset.name].field_names,
                     f"physical Parquet fields differ: {dataset.name}")
            records = batch.to_pylist()  # Only the current bounded Arrow batch.
            # Catalog validators operate on JSON logical records. Spark's UTC
            # TimestampType may be exposed as a naive Arrow datetime; its producer
            # contract fixes the session zone to UTC, not the host local timezone.
            for record in records:
                for spec in DATASET_CONTRACTS_BY_NAME[dataset.name].fields:
                    value = record[spec.name]
                    if spec.logical_type == "Date" and type(value) is date:
                        record[spec.name] = value.isoformat()
                    elif spec.logical_type == "TimestampUTC" and isinstance(value, datetime):
                        if value.tzinfo is None:
                            value = value.replace(tzinfo=timezone.utc)
                        record[spec.name] = value.isoformat()
            _validate_historical_rows(dataset, records)
            lineage = {k: v for k, v in dataset.binding.items()
                       if k in IDENTITIES and k not in {"policy_id", "decision_run_id"}}
            if dataset.name not in HISTORICAL:
                lineage = {k: v for k, v in dataset.binding.items() if k in IDENTITIES}
            yield records, map_batch_to_serving_rows(dataset.name, records, lineage=lineage)
    finally:
        batches.close()


def validate_source_data(selection, *, client, scratch_dir, batch_rows):
    """Stream validation before mutation, spilling business keys and sales groups to SQLite."""
    Path(scratch_dir).mkdir(parents=True, exist_ok=True)
    expected = {}
    population = {(r["item_id"], r["store_id"]) for r in selection.scope["selected_series"]}
    with TemporaryDirectory(prefix="j08-keys-", dir=scratch_dir) as directory:
        with sqlite3.connect(Path(directory) / "keys.sqlite") as db:
            db.execute("PRAGMA cache_size=-2048")
            db.execute("CREATE TABLE keys (dataset TEXT, key TEXT, PRIMARY KEY(dataset,key)) WITHOUT ROWID")
            db.execute("CREATE TABLE sales (store TEXT, day TEXT, units INTEGER, series INTEGER, PRIMARY KEY(store,day)) WITHOUT ROWID")
            db.execute("CREATE TABLE stores (store TEXT, day TEXT, units INTEGER, series INTEGER, PRIMARY KEY(store,day)) WITHOUT ROWID")
            for dataset in selection.datasets:
                _require(dataset.manifest.get("status") == "VALIDATED", "source artifact is not VALIDATED")
                stats = _new_stats(dataset.name)
                contract = DATASET_CONTRACTS_BY_NAME[dataset.name]
                small = []
                zero_sales = 0
                for records, (_, rows) in _iterate_dataset(dataset, client, scratch_dir, batch_rows):
                    _add_stats(stats, dataset.name, rows)
                    for record in records:
                        key = _bytes([record[k] for k in contract.unique_key]).decode()
                        try:
                            db.execute("INSERT INTO keys VALUES (?,?)", (dataset.name, key))
                        except sqlite3.IntegrityError as error:
                            raise PublicationError(f"duplicate source business key: {dataset.name}") from error
                        if "item_id" in record and "store_id" in record:
                            _require((record["item_id"], record["store_id"]) in population,
                                     "source row outside frozen M5 Small population")
                        if dataset.name == "sales_daily":
                            zero_sales += record["unit_sales"] == 0
                            index = record["day_index"]
                            day = record["date"]
                            if isinstance(day, str):
                                day = date.fromisoformat(day)
                            _require(1 <= index <= 1941 and day == date(2011, 1, 29) + timedelta(days=index - 1),
                                     "sales day/calendar differs from frozen M5 Small")
                            db.execute("INSERT INTO sales VALUES (?,?,?,1) ON CONFLICT(store,day) "
                                       "DO UPDATE SET units=units+excluded.units, series=series+1",
                                       (record["store_id"], str(day), record["unit_sales"]))
                        elif dataset.name == "store_daily_sales":
                            db.execute("INSERT INTO stores VALUES (?,?,?,?)",
                                       (record["store_id"], str(record["date"]), record["total_units"],
                                        record["observed_series_count"]))
                    if dataset.name not in {"sales_daily", "store_daily_sales"}:
                        _require(len(small) + len(records) <= 10000, "bounded analytical dataset exceeded")
                        small.extend(records)
                _require(stats["rows"] == dataset.expected_rows, f"source row count differs: {dataset.name}")
                if dataset.name == "sales_daily":
                    for key, measured in (("unit_sales_total", stats["measures"]["unit_sales"]),
                                          ("zero_sales_count", zero_sales)):
                        if key in dataset.manifest:
                            _require(dataset.manifest[key] == measured, "source manifest measure differs: " + key)
                if small:
                    validate_dataset(contract, small)
                expected[dataset.name] = stats
                db.commit()
            _require(expected["sales_daily"]["rows"] == 194100 and len(population) == 100,
                     "publication requires exactly 194100 frozen M5 Small observations")
            mismatch = db.execute(
                "SELECT count(*) FROM (SELECT * FROM sales EXCEPT SELECT * FROM stores)"
            ).fetchone()[0] + db.execute(
                "SELECT count(*) FROM (SELECT * FROM stores EXCEPT SELECT * FROM sales)"
            ).fetchone()[0]
            _require(mismatch == 0, "source sales/store daily aggregates differ")
    return expected


def _same_stats(actual, expected):
    if actual["rows"] != expected["rows"] or actual["nulls"] != expected["nulls"]:
        return False
    if list(actual["fingerprint"]) != list(expected["fingerprint"]):
        return False
    return set(actual["measures"]) == set(expected["measures"]) and all(
        math.isclose(actual["measures"][key], value, rel_tol=1e-12, abs_tol=1e-9)
        for key, value in expected["measures"].items()
    )


def reconcile_publication(selection, expected, *, serving) -> ReconciliationReport:
    """Compare exact source fingerprints, physical shard aggregates and Distributed reads."""
    evidence, counts = {}, {}
    for dataset in selection.datasets:
        name = dataset.name
        report = serving.inspect(dataset, expected[name])
        shards = report["shards"]
        _require(set(shards) == {1, 2}, f"missing physical shard: {name}")
        combined = _new_stats(name)
        for stats in shards.values():
            combined["rows"] += stats["rows"]
            for key, value in stats["measures"].items():
                combined["measures"][key] += value
            for key, value in stats["nulls"].items():
                combined["nulls"][key] += value
            combined["fingerprint"] = [(a + b) % 2**64 for a, b in
                                        zip(combined["fingerprint"], stats["fingerprint"], strict=True)]
        _require(_same_stats(combined, expected[name]), f"physical shard/source reconciliation failed: {name}")
        _require(_same_stats(report["distributed"], expected[name]), f"Distributed/source reconciliation failed: {name}")
        _require(report["routing_errors"] == 0, f"_shard_num routing differs: {name}")
        counts[name] = {shard: value["rows"] for shard, value in shards.items()}
        evidence[name] = report
    _require(all(counts["sales_daily"][shard] > 0 for shard in (1, 2)),
             "both shards must contain project-derived observed sales")
    evidence["relations"] = serving.validate_relations()
    return ReconciliationReport(True, counts, evidence)


def _hash_expression(name):
    pieces = []
    for column, kind, _ in _columns(name):
        value = f"assumeNotNull(`{column}`)"
        if kind == "String":
            payload = f"concat(reinterpretAsFixedString(toUInt64(length({value}))),{value})"
        elif kind == "Date":
            payload = f"reinterpretAsFixedString(toUInt16({value}))"
        elif kind.startswith("DateTime64"):
            payload = f"reinterpretAsFixedString(toUnixTimestamp64Milli({value}))"
        else:
            payload = f"reinterpretAsFixedString(CAST({value},'{kind}'))"
        pieces.append(f"if(isNull(`{column}`),'N',concat('V',{payload}))")
    return "MD5(concat(" + ",".join(pieces) + "))"


class ClickHouseServing:
    """Pinned two-shard topology; publisher reads/inserts, admin only truncates."""

    settings = {"skip_unavailable_shards": 0, "distributed_foreground_insert": 1}

    def __init__(self, publisher, admin, database):
        _require(database.replace("_", "a").isalnum(), "unsafe ClickHouse database")
        self.publisher, self.admin, self.database = publisher, admin, database

    def _query(self, sql):
        return self.publisher.query(sql, settings=self.settings).result_rows

    def prepare(self):
        version = self._query("SELECT version()")[0][0]
        _require(version.startswith("26.8."), "offline publisher requires the pinned ClickHouse 26.8 version")
        topology = self._query("SELECT shard_num,replica_num,shard_weight FROM system.clusters "
                               "WHERE cluster='warehouse_cluster' ORDER BY shard_num,replica_num")
        _require(topology == [(1, 1, 1), (2, 1, 1)], "publication requires exactly two equal-weight shards")
        settings = self._query("SELECT name,value FROM system.settings WHERE name IN "
                               "('skip_unavailable_shards','distributed_foreground_insert')")
        _require(dict(settings) == {"skip_unavailable_shards": "0", "distributed_foreground_insert": "1"},
                 "required synchronous delivery settings were not applied")
        pending = self._query("SELECT sum(data_files) FROM system.distribution_queue")[0][0]
        _require(pending == 0, "pending Distributed queue must be resolved before offline reload")
        for name in MAPPINGS:
            table = MAPPINGS[name].serving_dataset
            self._query(f"SELECT count() FROM cluster('warehouse_cluster',{self.database},{table}_local)")

    def clear(self):
        for mapping in MAPPINGS.values():
            self.admin.command(
                f"TRUNCATE TABLE {self.database}.{mapping.serving_dataset}_local "
                "ON CLUSTER warehouse_cluster SYNC"
            )
        for mapping in MAPPINGS.values():
            _require(self._query(f"SELECT count() FROM cluster('warehouse_cluster',"
                                 f"{self.database},{mapping.serving_dataset}_local)")[0][0] == 0,
                     "offline clear did not empty both physical shards")

    def insert(self, dataset, columns, rows):
        self.publisher.insert(f"{self.database}.{MAPPINGS[dataset.name].serving_dataset}", rows,
                              column_names=columns, settings=self.settings)

    def _aggregate(self, name, source, *, grouped=False):
        template = _new_stats(name)
        expressions = ["count()"]
        expressions.extend(f"sum(ifNull(`{c}`,0))" for c in template["measures"])
        expressions.extend(f"countIf(isNull(`{c}`))" for c in template["nulls"])
        hashed = _hash_expression(name)
        expressions.extend(f"sumWithOverflow(reinterpretAsUInt64(substring({hashed},{offset},8)))"
                           for offset in (1, 9))
        sql = "SELECT " + ("_shard_num," if grouped else "") + ",".join(expressions) + " FROM " + source
        if grouped:
            # Include empty physical shards: GROUP BY would otherwise omit them.
            sql += " GROUP BY _shard_num ORDER BY _shard_num"
        result = {}
        for row in self._query(sql):
            shard, values = (row[0], row[1:]) if grouped else (0, row)
            stats = _new_stats(name)
            stats["rows"] = values[0]
            index = 1
            for key in stats["measures"]:
                stats["measures"][key] = values[index]
                index += 1
            for key in stats["nulls"]:
                stats["nulls"][key] = values[index]
                index += 1
            stats["fingerprint"] = list(values[index:index + 2])
            result[shard] = stats
        return result if grouped else result[0]

    def inspect(self, dataset, expected):
        name = dataset.name
        table = MAPPINGS[name].serving_dataset
        physical = f"cluster('warehouse_cluster',{self.database},{table}_local)"
        distributed = f"{self.database}.{table}"
        shards = self._aggregate(name, physical, grouped=True)
        # Empty shards are legal for small metric/scenario tables; confirm reachability
        # separately in prepare(), and account for their zero aggregate here.
        for shard in (1, 2):
            shards.setdefault(shard, _new_stats(name))
        routing = {
            "store_daily_sales": "store_id,sale_date",
            "forecast_metrics": "evaluation_run_id,holdout_id,method_id",
            "central_supply_scenario": "item_id",
        }.get(name, "item_id,store_id")
        errors = self._query(f"SELECT countIf(_shard_num != 1+modulo(cityHash64({routing}),2)) "
                             f"FROM {physical}")[0][0]
        key_columns = {f.logical_field: f.serving_field for f in MAPPINGS[name].field_mappings}
        key = ",".join(f"`{key_columns[k]}`" for k in MAPPINGS[name].logical_key)
        duplicates = self._query(f"SELECT count()-uniqExact(tuple({key})) FROM {distributed}")[0][0]
        _require(duplicates == 0, f"duplicate serving business keys: {name}")
        return dict(distributed=self._aggregate(name, distributed), shards=shards, routing_errors=errors)

    def validate_relations(self):
        db = self.database
        checks = {
            "sales_store_daily": f"SELECT (SELECT sum(unit_sales) FROM {db}.sales_daily) != (SELECT sum(total_units) FROM {db}.store_daily_sales)",
            "forecast_seven_day": f"SELECT count() FROM (SELECT forecast_run_id,item_id,store_id,issue_date,count() n,uniqExact(horizon) h,min(horizon) lo,max(horizon) hi,countIf(forecast_date != addDays(issue_date,horizon) OR forecast_status != 'ok') bad FROM {db}.forecast_daily GROUP BY forecast_run_id,item_id,store_id,issue_date HAVING n!=7 OR h!=7 OR lo!=1 OR hi!=7 OR bad!=0)",
            "replenishment_equations": f"SELECT count() FROM {db}.replenishment WHERE planning_demand_units != ceil(forecast_7d_units) OR target_stock_units != planning_demand_units+safety_stock_units OR critical_need_units != greatest(toInt128(planning_demand_units)-opening_stock_units,0) OR request_units != greatest(toInt128(target_stock_units)-opening_stock_units,0) OR request_units != critical_need_units+safety_need_units OR forecast_shortage_flag != (planning_demand_units>opening_stock_units)",
            "allocation_limits": f"SELECT count() FROM {db}.allocation WHERE allocated_units>requested_units OR allocated_units>central_available_units OR unmet_request_units != requested_units-allocated_units OR (critical_allocated_units IS NULL)!=(safety_allocated_units IS NULL) OR (critical_allocated_units IS NOT NULL AND allocated_units != critical_allocated_units+safety_allocated_units)",
            "policy_observed_window": f"SELECT count() FROM {db}.policy_evaluation WHERE run_type NOT IN ('validation','test') OR observed_label_window_start != addDays(issue_date,1) OR observed_label_window_end != addDays(issue_date,7) OR simulated_fulfilled_units+simulated_unmet_units != actual_sales_proxy_7d_units OR simulated_stockout_days>7",
            "metrics_historical": f"SELECT count() FROM {db}.forecast_metrics WHERE run_type NOT IN ('validation','test') OR observation_count=0 OR NOT isFinite(sum_absolute_error) OR NOT isFinite(sum_actual_units) OR sum_absolute_error<0 OR sum_actual_units<0 OR (metric_value IS NOT NULL AND (NOT isFinite(metric_value) OR metric_value<0)) OR (metric_name='wape' AND ((sum_actual_units=0 AND metric_value IS NOT NULL) OR (sum_actual_units>0 AND metric_value IS NULL)))",
        }
        result = {name: self._query(sql)[0][0] for name, sql in checks.items()}
        supply_rows = self._query(f"SELECT scenario_id,issue_date,item_id,source_warehouse_id,central_available_units FROM {db}.central_supply_scenario")
        allocation_rows = self._query(f"SELECT scenario_id,issue_date,item_id,source_warehouse_id,policy_id,sum(allocated_units),min(central_available_units),max(central_available_units) FROM {db}.allocation GROUP BY scenario_id,issue_date,item_id,source_warehouse_id,policy_id")
        _require(len(supply_rows) <= 100 and len(allocation_rows) <= 200, "central reconciliation exceeds bounded M5 Small layout")
        supply = {tuple(row[:4]): row[4] for row in supply_rows}
        result["central_supply_conservation"] = sum(
            tuple(row[:4]) not in supply or row[5] > supply.get(tuple(row[:4]), -1)
            or row[6] != row[7] or row[7] != supply.get(tuple(row[:4])) for row in allocation_rows
        )
        _require(all(value == 0 for value in result.values()), "serving relation checks failed: " + str(result))
        return result


def map_batch_to_serving_rows(dataset_name, records, *, lineage):
    """Validate logical fields and lineage before strictly typed Contract v1 mapping."""
    mapping = MAPPINGS[dataset_name]
    contract = DATASET_CONTRACTS_BY_NAME[dataset_name]
    active = tuple(f for f in mapping.field_mappings if f.serving_field)
    columns = [f.serving_field for f in active]
    if dataset_name == "sales_daily":
        columns.append("source_snapshot_id")
    columns.extend(mapping.extra_serving_fields)
    rows = []
    _require(len(records) <= MAX_BATCH_ROWS, "mapping batch exceeds bounded row limit")
    for record in records:
        validate_record(contract, record, lineage_context=lineage)
        for key in IDENTITIES:
            if key in record and key in lineage and record[key] is not None:
                _require(str(record[key]) == str(lineage[key]), f"row lineage differs: {key}")
        values = [_cast(record[f.parquet_field], f) for f in active]
        if dataset_name == "sales_daily":
            _require(lineage.get("source_snapshot_id"), "missing sales source snapshot")
            values.append(lineage["source_snapshot_id"])
        if dataset_name == "forecast_daily":
            # Complete seven-day coverage is checked before any serving mutation.
            values.append("ok")
        rows.append(values)
    return tuple(columns), rows


def _selected_files(client, config, path, manifest, *, inventory=None, prefix=""):
    """Enumerate an exact selected dataset, rejecting partitions/links/unexpected files."""
    statuses = client.list_status(path)
    files = []
    expected = {i["path"]: i for i in inventory or []}
    for status in statuses:
        remote = status["path"]
        _require(str(PurePosixPath(remote).parent) == path, "non-child Parquet inventory path")
        name = PurePosixPath(remote).name
        if name.startswith("_"):
            _require(status["type"] == "FILE", "unexpected selected dataset directory")
            continue
        _require(status["type"] == "FILE" and name.startswith("part-") and name.endswith(".parquet"),
                 "unexpected selected dataset layout")
        require_sealed(client, remote, config.base_path)
        checksum = client.checksum(remote)
        relative = prefix + name
        if inventory is not None:
            _require(relative in expected and expected[relative]["size_bytes"] == status["length"]
                     and expected[relative]["checksum"] == checksum, "altered selected Parquet")
        files.append(ParquetFile(remote, status["length"], checksum=checksum))
    _require(0 < len(files) <= 1024, "selected dataset lacks bounded Parquet files")
    if inventory is not None:
        selected = {prefix + PurePosixPath(f.path).name for f in files}
        expected_parts = {p for p in expected if p.startswith(prefix)
                          and "/" not in p.removeprefix(prefix) and p.endswith(".parquet")}
        _require(selected == expected_parts, "selected Parquet file population differs")
    return tuple(sorted(files, key=lambda f: f.path))


def preflight_publication_inputs(
    selector, *, decision_run_id, decision_pipeline_run_id,
    policy_evaluation_run_id, policy_evaluation_pipeline_run_id,
    forecast_evaluation_run_id, client, config, spark,
) -> PublicationSelection:
    """Read-only explicit selection; J07's existing verifier checks its bounded bundle."""
    from warehouse_bigdata.common.scope_evidence import read_scope
    from warehouse_bigdata.optimization.inventory_artifacts import verify_inventory_bundle

    validate_small_artifacts(selector, through="test")
    for value in (decision_run_id, decision_pipeline_run_id, policy_evaluation_run_id,
                  policy_evaluation_pipeline_run_id, forecast_evaluation_run_id):
        validate_pipeline_run_id(value)
    _require(forecast_evaluation_run_id in (selector["validation_evaluation_run_id"],
                                          selector["test_evaluation_run_id"]),
             "forecast evaluation must be explicitly selected in the canonical selector")
    validate_small_publications(selector, through="test", client=client, config=config)
    snapshot = selector["source_snapshot_id"]
    sales_path = render_curated_sales_daily_path(config.base_path, snapshot)
    scope = read_scope(client, sales_path, source_version=selector["source_version"],
                       source_snapshot_id=snapshot, expected_digest=selector["selection_digest"])
    common = {k: selector[k] for k in ("source_snapshot_id", "selection_digest", "dataset_scope",
                                     "model_run_id", "feature_version", "source_version")}
    datasets = []

    def add(name, path, manifest, binding, rows, *, inventory=None, prefix=""):
        _require(manifest.get("status") == "VALIDATED", "selected artifact is not VALIDATED")
        _require(type(rows) is int and rows > 0, f"missing selected {name} row count")
        files = _selected_files(client, config, path, manifest, inventory=inventory, prefix=prefix)
        datasets.append(PublicationDataset(name, path, files, rows, manifest, {**common, **binding}))

    for name, path, sidecar in (
        ("sales_daily", sales_path, "_snapshot_manifest.json"),
        ("store_daily_sales", render_curated_store_daily_sales_path(config.base_path, snapshot),
         "_dataset_manifest.json"),
    ):
        manifest = read_json(client, path + "/" + sidecar)
        add(name, path, manifest, {}, manifest["row_count"], inventory=manifest.get("file_integrity"))

    forecast_path = render_forecast_output_path(config.base_path, selector["forecast_run_id"])
    forecast = read_json(client, forecast_path + "/scoring_manifest.json")
    quality = read_json(client, forecast_path + "/evidence/publication.json")
    forward = {k: forecast[k] for k in IDENTITIES if k in forecast}
    add("forecast_daily", forecast_path + "/forecast_daily", forecast, forward,
        forecast["row_count"], inventory=quality["file_integrity"], prefix="forecast_daily/")

    decision_path = render_decision_output_path(config.base_path, decision_run_id)
    policy_path = render_evaluation_output_path(config.base_path, policy_evaluation_run_id)
    bundle_bindings = {}
    for kind, path, run, identity in (
        ("decision", decision_path, decision_pipeline_run_id, decision_run_id),
        ("evaluation", policy_path, policy_evaluation_pipeline_run_id, policy_evaluation_run_id),
    ):
        manifest = verify_inventory_bundle(spark, client=client, config=config, path=path)
        binding = read_json(client, path + "/evidence/decision_binding.json")
        quality = read_json(client, path + "/evidence/publication.json")
        key = "decision_run_id" if kind == "decision" else "evaluation_run_id"
        _require(binding[key] == identity and binding["pipeline_run_id"] == run,
                 "J07 selected identity differs")
        _require(binding["scope"] == scope, "J07 frozen selection differs")
        upstream = binding["upstream_identities"]
        for name in ("source_version", "source_snapshot_id", "selection_digest", "model_run_id",
                     "feature_version", "j02_pipeline_run_id", "j03_pipeline_run_id",
                     "j04_pipeline_run_id", "j05_pipeline_run_id"):
            _require(upstream.get(name) == selector[name], "J07 incompatible upstream: " + name)
        local = {k: binding[k] for k in IDENTITIES if k in binding}
        local.update(pipeline_run_id=run)
        if kind == "decision":
            for key in ("source_snapshot_id", "model_run_id", "forecast_run_id", "issue_date"):
                _require(local[key] == forward[key], "primary decision/forecast lineage differs")
            names = ("inventory_scenario", "central_supply_scenario",
                     "replenishment_recommendations", "allocation_recommendations")
        else:
            audit = read_json(client, path + "/evidence/audit.json")
            for name in ("validation_evaluation_run_id", "validation_pipeline_run_id",
                         "test_evaluation_run_id", "test_pipeline_run_id"):
                _require(upstream.get(name) == selector[name], "policy evaluation upstream differs: " + name)
            metadata = audit["metadata"]
            local.update(decision_run_ids=metadata["decision_run_ids"],
                         policy_ids=list(metadata["decision_run_ids"]),
                         policy_version=audit["frozen"]["policy_version"],
                         label_provenance=metadata["label_provenance"])
            local.update({key: manifest[key] for key in ("holdout_id", "evaluation_start", "evaluation_end")
                          if key in manifest})
            local.pop("decision_run_id", None)
            names = ("inventory_policy_evaluation",)
        bundle_bindings[kind] = {**common, **local}
        for name in names:
            add(name, path + "/" + name, manifest, local,
                quality["datasets"][name]["row_count"],
                inventory=quality["file_integrity"], prefix=name + "/")

    evaluation_path = render_evaluation_output_path(config.base_path, forecast_evaluation_run_id)
    metrics_manifest = read_json(client, evaluation_path + "/evaluation_manifest.json")
    binding = read_json(client, evaluation_path + "/evidence/evaluation.json")
    quality = read_json(client, evaluation_path + "/evidence/publication.json")
    historical = {k: metrics_manifest[k] for k in IDENTITIES if k in metrics_manifest}
    historical.update(origin_forecasts=binding["origin_forecasts"],
                      pipeline_run_id=binding["pipeline_run_id"],
                      evaluation_start=metrics_manifest["evaluation_start"],
                      evaluation_end=metrics_manifest["evaluation_end"])
    add("forecast_metrics", evaluation_path + "/metrics", metrics_manifest, historical,
        metrics_manifest["row_count"], inventory=quality["file_integrity"], prefix="metrics/")
    context = {k: v for k, v in bundle_bindings["decision"].items() if k in IDENTITIES}
    context.update(policy_ids=[context.pop("policy_id")], evaluation_run_id=None, holdout_id=None)
    result = PublicationSelection(dict(selector), scope, tuple(datasets), context)
    validate_dataset_bindings(result.bindings)
    _require({d.name for d in datasets} == set(MAPPINGS), "publication must select all nine datasets")
    return result

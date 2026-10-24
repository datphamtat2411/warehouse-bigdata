"""Read-only M5 HDFS-extract adapter for the explicitly separate preview UI.

This is NOT an approved ClickHouse publication or an implementation of J07.
No official approval gates, repositories or publication identity are modified.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
import hashlib
import json
import math
from pathlib import Path
from types import SimpleNamespace


DATASETS = ("sales_daily", "store_daily_sales", "forecast_daily")
COLUMNS = {
    "sales_daily": ("item_id", "store_id", "date", "unit_sales"),
    "store_daily_sales": ("source_snapshot_id", "store_id", "date", "total_units"),
    "forecast_daily": (
        "forecast_run_id", "model_run_id", "source_snapshot_id", "issue_date",
        "forecast_date", "horizon", "item_id", "store_id", "predicted_units", "run_type",
    ),
}


@dataclass(frozen=True)
class PreviewResult:
    state: str
    rows: tuple = ()
    provenance: str = ""
    reason: str = ""


@dataclass(frozen=True)
class PreviewContext:
    publication_id: str
    manifest_checksum: str
    source_snapshot_id: str
    model_run_id: str
    forecast_run_id: str
    issue_date: date
    coverage: object
    dataset_bindings: dict
    policy_ids: tuple = ()
    scenario_id: str = "NOT-AVAILABLE-J07"
    decision_run_id: str = "NOT-AVAILABLE-J07"
    evaluation_run_id: str | None = None
    holdout_id: str | None = None

    def evaluation_binding(self, _dataset):
        raise ValueError("No approved evaluation binding in demo preview")


class PreviewRepository:
    """Small bounded lookup facade consumed by the project's unmodified UI pages."""

    def __init__(self, source_rows, store_rows, forecast_rows, manifest, selector, signature):
        if len(source_rows) != 194100 or len(store_rows) != 19410:
            raise ValueError("Unexpected M5 Small observed-sales row count")
        self.limits = SimpleNamespace(max_history_days=366, max_rows=1000)
        self.history = defaultdict(dict)
        self.trends = defaultdict(dict)
        self.predictions = defaultdict(list)
        stores, days = set(), set()
        for row in source_rows:
            item, store, day, units = (row[key] for key in COLUMNS["sales_daily"])
            if not isinstance(item, str) or not isinstance(store, str) or type(day) is not date:
                raise ValueError("Invalid observed-sales identity/date")
            if type(units) is not int or units < 0:
                raise ValueError("Invalid observed unit sales")
            if day in self.history[(item, store)]:
                raise ValueError("Duplicate observed item/store/date")
            self.history[(item, store)][day] = units
            stores.add(store)
            days.add(day)
        for row in store_rows:
            if row["source_snapshot_id"] != selector["source_snapshot_id"]:
                raise ValueError("Store trend source identity mismatch")
            store, day, value = (row["store_id"], row["date"], row["total_units"])
            if type(day) is not date or type(value) is not int or value < 0:
                raise ValueError("Invalid store/day value")
            if day in self.trends[store]:
                raise ValueError("Duplicate store/day")
            self.trends[store][day] = value
        if len(stores) != 10 or len(days) != 1941 or len(self.history) != 100:
            raise ValueError("Incomplete M5 Small population")
        if len(self.trends) != 10 or any(len(s) != 1941 for s in self.trends.values()):
            raise ValueError("Incomplete store/day trend")
        for (item, store), hist in self.history.items():
            if len(hist) != 1941 or set(hist) != days:
                raise ValueError("Incomplete item/store observed history")
        if any(set(trend) != days for trend in self.trends.values()):
            raise ValueError("Store trend has mismatched dates")
        totals = defaultdict(int)
        for (_, store), history in self.history.items():
            for day, units in history.items():
                totals[store, day] += units
        if any(totals[store, day] != units for store, hist in self.trends.items()
               for day, units in hist.items()):
            raise ValueError("J02 item-level units do not reconcile with J03 store totals")
        issue = date(2016, 5, 22)
        if not 0 < len(forecast_rows) <= 700 or len(forecast_rows) % 7:
            raise ValueError("Unexpected J06 forecast cardinality")
        for row in forecast_rows:
            if any(row[key] != selector[val] for key, val in (
                ("forecast_run_id", "forecast_run_id"),
                ("source_snapshot_id", "source_snapshot_id"),
                ("model_run_id", "model_run_id"),
            )) or row["issue_date"] != issue or row["run_type"] != "forward_demo":
                raise ValueError("J06 forecast lineage mismatch")
            item, store, horizon, pred = (row[k] for k in
                                          ("item_id", "store_id", "horizon", "predicted_units"))
            if (not isinstance(item, str) or not isinstance(store, str)
                or (item, store) not in self.history or type(horizon) is not int
                or horizon not in range(1, 8)
                or row["forecast_date"] != issue + timedelta(days=horizon)
                or type(pred) not in (float, int) or not math.isfinite(pred) or pred < 0):
                raise ValueError("Malformed J06 forecast")
            self.predictions[item, store].append(dict(row, forecast_status="ok"))
        if not self.predictions or any(
            len(rows) != 7 or {r["horizon"] for r in rows} != set(range(1, 8))
            for rows in self.predictions.values()
        ):
            raise ValueError("J06 forecast is not seven-horizon complete")
        self.pairs = tuple(sorted(self.predictions))
        bindings = {
            name: {"dataset_scope": "demo_sample", "selection_digest": selector["selection_digest"]}
            for name in DATASETS
        }
        self.context = PreviewContext(
            publication_id="PREVIEW-NOT-J08-APPROVED",
            manifest_checksum="preview-only:" + signature,
            source_snapshot_id=selector["source_snapshot_id"],
            model_run_id=selector["model_run_id"],
            forecast_run_id=selector["forecast_run_id"],
            issue_date=issue,
            coverage=SimpleNamespace(
                available_stores=tuple(sorted(stores)), series_total=len(self.history),
                series_eligible=len(self.predictions), series_scored=len(self.predictions),
                loaded_rows=sum(manifest["datasets"][k]["rows"] for k in DATASETS),
            ),
            dataset_bindings=bindings,
        )

    def get_eligible_items(self, store, search="", *, limit=50):
        if store not in self.trends or not isinstance(search, str) or limit > 50:
            return PreviewResult("error", provenance="predicted")
        items = [item for item, other in self.pairs if other == store and search.lower() in item.lower()]
        rows = tuple({"item_id": item, "store_id": store} for item in items[:limit])
        return PreviewResult("ok" if rows else "empty", rows, "predicted")

    def get_store_history_coverage(self, store):
        days = self.trends.get(store)
        if not days:
            return PreviewResult("empty", provenance="observed")
        return PreviewResult("ok", (dict(store_id=store, start_date=min(days),
            end_date=max(days), day_count=len(days), row_count=len(days)),), "observed")

    def _window(self, start, end):
        return (type(start) is date and type(end) is date and start <= end
                and (end - start).days < self.limits.max_history_days)

    def get_store_trend(self, store, start, end):
        if not self._window(start, end):
            return PreviewResult("error", provenance="observed")
        rows = tuple({"store_id": store, "sale_date": day, "total_units": count}
                     for day, count in sorted(self.trends.get(store, {}).items())
                     if start <= day <= end)
        return PreviewResult("ok" if rows else "empty", rows, "observed")

    def get_item_history(self, item, store, start, end):
        if not self._window(start, end):
            return PreviewResult("error", provenance="observed")
        rows = tuple({"item_id": item, "store_id": store, "sale_date": day, "unit_sales": count}
                     for day, count in sorted(self.history.get((item, store), {}).items())
                     if start <= day <= end)
        return PreviewResult("ok" if rows else "empty", rows, "observed")

    def get_forecast_series(self, item, store):
        rows = self.predictions.get((item, store), ())
        return PreviewResult("ok" if rows else "empty",
                             tuple(sorted(rows, key=lambda r: r["horizon"])), "predicted")

    def get_forecast_metrics(self):
        return PreviewResult("unavailable", provenance="evaluated",
                             reason="No approved J08 evaluation publication")


def checked_manifest(root: Path):
    """Validate every local part checksum and exact selector identities on each render."""
    src = root / "artifacts/small-artifacts.json"
    base = root / "artifacts/quick-demo"
    selector = json.loads(src.read_text())
    manifest_bytes = (base / "manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    if (manifest.get("demo_mode") != "HDFS read-only artifact extract; NOT J08 publication"
        or manifest.get("dataset_scope") != "demo_sample"
        or any(manifest.get(key) != selector.get(key) for key in
               ("source_snapshot_id", "model_run_id", "forecast_run_id", "selection_digest"))
        or set(manifest.get("datasets", {})) != set(DATASETS)):
        raise ValueError("Preview extraction does not match the frozen selector")
    dig = hashlib.sha256(manifest_bytes)
    for dataset in DATASETS:
        meta = manifest["datasets"][dataset]
        files = meta["files"]
        if not files or len(files) > 64:
            raise ValueError("Missing or oversized preview Parquet inventory")
        local_names = {p.name for p in (base / dataset).glob("part-*.parquet")}
        expected_names = {rec["hdfs_path"].rsplit("/", 1)[-1] for rec in files}
        if local_names != expected_names or len(expected_names) != len(files):
            raise ValueError("Preview local file inventory mismatch")
        for rec in files:
            name = rec["hdfs_path"].rsplit("/", 1)[-1]
            path = base / dataset / name
            if not name.startswith("part-") or not name.endswith(".parquet"):
                raise ValueError("Invalid preview Parquet filename")
            if path.stat().st_size != rec["bytes"]:
                raise ValueError("Preview extract file size mismatch")
            sha = hashlib.sha256()
            with path.open("rb") as file:
                for chunk in iter(lambda: file.read(1048576), b""):
                    sha.update(chunk)
            if sha.hexdigest() != rec["sha256"]:
                raise ValueError("Preview extract SHA-256 mismatch")
            dig.update(bytes.fromhex(rec["sha256"]))
    return base, manifest, selector, dig.hexdigest()


def make_repository(base: Path, manifest: dict, selector: dict, signature: str):
    import pyarrow.dataset as ds

    rows = {}
    for name in DATASETS:
        table = ds.dataset(str(base / name), format="parquet").to_table(columns=list(COLUMNS[name]))
        if table.num_rows != manifest["datasets"][name]["rows"]:
            raise ValueError("Preview Parquet row-count mismatch")
        rows[name] = table.to_pylist()
    return PreviewRepository(rows["sales_daily"], rows["store_daily_sales"],
                             rows["forecast_daily"], manifest, selector, signature)

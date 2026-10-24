"""Read-only extraction of frozen M5 Small HDFS Parquet artifacts for a local UI.

Run at repository root: bash scripts/load-env.sh uv run --locked python prepare_demo.py
Never writes to HDFS or alters immutable selection/artifact paths.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path.cwd()
SELECTOR = ROOT / "artifacts/small-artifacts.json"
DEST = ROOT / "artifacts/quick-demo"
COMPOSE = [
    "docker", "compose", "--env-file", str(ROOT / ".env"),
    "-f", str(ROOT / "infrastructure/compose.yaml"),
    "exec", "-T", "hdfs-namenode", "hdfs", "dfs",
]
MAX_FILE_BYTES = 128 * 1024 * 1024
MAX_DATASET_BYTES = 256 * 1024 * 1024
MAX_PARTS = 64


def hdfs_list(path: str) -> list[str]:
    response = subprocess.run(
        [*COMPOSE, "-ls", "-C", path], text=True, capture_output=True, timeout=60
    )
    if response.returncode != 0:
        raise RuntimeError(f"Cannot list HDFS path {path}: {response.stderr[-1000:]}")
    files = [s.strip() for s in response.stdout.splitlines() if s.strip()]
    parts = sorted(
        f for f in files if f.rsplit("/", 1)[-1].startswith("part-")
        and f.endswith(".parquet")
    )
    if not parts or len(parts) > MAX_PARTS:
        raise RuntimeError(f"Expected 1..{MAX_PARTS} Parquet parts under {path}; got {len(parts)}")
    if any(not p.startswith(path.rstrip("/") + "/") for p in parts):
        raise RuntimeError("Unexpected HDFS listing outside selected dataset")
    return parts


def download_one(remote: str, dest: Path) -> dict:
    if dest.exists():
        raise RuntimeError(f"Refusing to overwrite an existing file: {dest}")
    tmp = dest.with_name(dest.name + ".partial")
    tmp.unlink(missing_ok=True)  # Only a local quick-demo download scratch file.
    digest = hashlib.sha256()
    total = 0
    try:
        with tmp.open("xb") as sink:
            proc = subprocess.Popen([*COMPOSE, "-cat", remote], stdout=subprocess.PIPE,
                                    stderr=subprocess.DEVNULL)
            try:
                assert proc.stdout is not None
                while chunk := proc.stdout.read(1024 * 1024):
                    total += len(chunk)
                    if total > MAX_FILE_BYTES:
                        raise RuntimeError(f"Parquet file exceeds demo bound: {remote}")
                    sink.write(chunk)
                    digest.update(chunk)
                if proc.wait(timeout=45) != 0:
                    raise RuntimeError(f"HDFS cat failed: {remote}")
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
                if proc.stdout is not None:
                    proc.stdout.close()
        tmp.replace(dest)
        return {"hdfs_path": remote, "bytes": total, "sha256": digest.hexdigest()}
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def main() -> None:
    if not SELECTOR.is_file():
        raise SystemExit("Run this script from the repository root with artifacts/small-artifacts.json")
    import pyarrow.dataset as ds

    sel = json.loads(SELECTOR.read_text())
    snapshot = sel["source_snapshot_id"]
    forecast_id = sel["forecast_run_id"]
    # Exact canonical paths; no discovery of latest or fallback runs.
    roots = {
        "sales_daily": f"/data/warehouse/curated/sales_daily/source_snapshot_id={snapshot}",
        "store_daily_sales": f"/data/warehouse/curated/store_daily_sales/source_snapshot_id={snapshot}",
        "forecast_daily": f"/data/warehouse/outputs/forecasts/{forecast_id}/forecast_daily",
    }
    DEST.mkdir(parents=True, exist_ok=True)
    manifest = {
        "demo_mode": "HDFS read-only artifact extract; NOT J08 publication",
        "dataset_scope": sel["dataset_scope"],
        "selection_digest": sel["selection_digest"],
        "source_snapshot_id": snapshot,
        "forecast_run_id": forecast_id,
        "model_run_id": sel["model_run_id"],
        "datasets": {},
    }
    try:
        for dataset, path in roots.items():
            folder = DEST / dataset
            folder.mkdir(parents=True, exist_ok=True)
            files = hdfs_list(path)
            print(f"{dataset}: {len(files)} HDFS Parquet files", flush=True)
            evidence = []
            for remote in files:
                local = folder / remote.rsplit("/", 1)[-1]
                if local.exists():
                    # This is a local cache of remote immutable artifacts, not a publication.
                    digest = hashlib.sha256()
                    with local.open("rb") as fh:
                        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                            digest.update(chunk)
                    record = {"hdfs_path": remote, "bytes": local.stat().st_size,
                              "sha256": digest.hexdigest()}
                    if record["bytes"] > MAX_FILE_BYTES:
                        raise RuntimeError(f"Oversized local file: {local}")
                else:
                    record = download_one(remote, local)
                evidence.append(record)
                print(f"  {local.name}: {record['bytes']:,} bytes", flush=True)
            if sum(item["bytes"] for item in evidence) > MAX_DATASET_BYTES:
                raise RuntimeError(f"Dataset exceeds demo extraction bound: {dataset}")
            rows = ds.dataset(str(folder), format="parquet").count_rows()
            if rows <= 0:
                raise RuntimeError(f"Empty data: {dataset}")
            if dataset == "sales_daily" and rows != 194100:
                raise RuntimeError(f"Unexpected scoped sales count: {rows}")
            if dataset == "store_daily_sales" and rows != 19410:
                raise RuntimeError(f"Unexpected scoped store/day count: {rows}")
            if dataset == "forecast_daily" and (rows > 700 or rows % 7 != 0):
                raise RuntimeError(f"Unexpected forecast count: {rows}")
            manifest["datasets"][dataset] = {
                "hdfs_path": path, "rows": rows, "files": evidence,
            }
            print(f"  validated row count: {rows:,}", flush=True)
        checked_store = ds.dataset(str(DEST / "store_daily_sales"), format="parquet").to_table(
            columns=["source_snapshot_id", "store_id", "date"]
        ).to_pydict()
        if any(x != snapshot for x in checked_store["source_snapshot_id"]):
            raise RuntimeError("J03 store/day snapshot identity mismatch")
        if len(set(checked_store["store_id"])) != 10 or len(set(checked_store["date"])) != 1941:
            raise RuntimeError("Unexpected J03 store/day coverage")
        check_forecast = ds.dataset(str(DEST / "forecast_daily"), format="parquet").to_table(
            columns=["forecast_run_id", "source_snapshot_id", "model_run_id", "issue_date",
                     "run_type", "horizon", "predicted_units", "item_id", "store_id"]
        ).to_pydict()
        if any(x != forecast_id for x in check_forecast["forecast_run_id"]):
            raise RuntimeError("Forecast run identity mismatch")
        if any(x != snapshot for x in check_forecast["source_snapshot_id"]):
            raise RuntimeError("Forecast snapshot identity mismatch")
        if any(x != sel["model_run_id"] for x in check_forecast["model_run_id"]):
            raise RuntimeError("Forecast model identity mismatch")
        if any(str(x) != "2016-05-22" for x in check_forecast["issue_date"]):
            raise RuntimeError("Unexpected forward-demo forecast issue date")
        if any(x != "forward_demo" for x in check_forecast["run_type"]):
            raise RuntimeError("Unexpected forecast run type")
        groups: dict[tuple[str, str], set[int]] = {}
        for item, store, horizon in zip(
            check_forecast["item_id"], check_forecast["store_id"],
            check_forecast["horizon"], strict=True
        ):
            key = (item, store)
            if horizon in groups.setdefault(key, set()):
                raise RuntimeError(f"Duplicate forecast horizon: {key}")
            groups[key].add(horizon)
        if any(horizons != set(range(1, 8)) for horizons in groups.values()):
            raise RuntimeError("Forecast series incomplete: expected exactly seven horizons")
        if not all(isinstance(v, (int, float)) and 0 <= v < float("inf")
                   for v in check_forecast["predicted_units"]):
            raise RuntimeError("Invalid nonnegative forecast values")
        (DEST / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print("SUCCESS: read-only M5 Small extract ready at artifacts/quick-demo", flush=True)
    except Exception as exc:
        (DEST / "manifest.json").unlink(missing_ok=True)
        print(f"FAILED: {exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()

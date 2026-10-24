"""Opt-in serial, read-only acceptance of an existing approved M5 Small release."""

import json
import math
import os
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import pytest

pytestmark = pytest.mark.integration


def enabled(environment):
    return all(
        environment.get(key) == "1"
        for key in ("WAREHOUSE_VERIFY_APPLICATION", "WAREHOUSE_VERIFY_APPROVED_DATA")
    )


def classify(error):
    return "BLOCKED" if isinstance(error, (OSError, ConnectionError)) else "FAIL"


def reconcile(name, expected, actual, tolerance=1e-8):
    numeric = type(expected) in (int, float) and type(actual) in (int, float)
    equal = (
        (
            math.isfinite(expected)
            and math.isfinite(actual)
            and math.isclose(expected, actual, rel_tol=tolerance, abs_tol=tolerance)
        )
        if numeric
        else expected == actual
    )
    return dict(
        check=name,
        status="PASS" if equal else "FAIL",
        expected=expected,
        actual=actual,
        tolerance=tolerance if numeric else 0,
    )


def source_rows(manifest, dataset, filters, scratch):
    from warehouse_bigdata.common.hdfs_client import HdfsCliClient
    from warehouse_bigdata.publication.batch import MAPPINGS
    from warehouse_bigdata.publication.parquet import ParquetFile, iter_parquet_batches

    mapping = MAPPINGS[dataset]
    files = tuple(ParquetFile(**f) for f in manifest["datasets"][dataset]["files"])
    rows = []
    for batch in iter_parquet_batches(HdfsCliClient(), files, scratch_dir=scratch):
        for raw in batch.to_pylist():
            row = {
                f.serving_field: raw[f.parquet_field]
                for f in mapping.field_mappings
                if f.parquet_field in raw
            }
            if all(row.get(k) == v for k, v in filters.items()):
                rows.append(row)
    return rows


@pytest.mark.skipif(
    not enabled(os.environ), reason="requires both explicit read-only acceptance gates"
)
def test_application_acceptance():
    from streamlit.testing.v1 import AppTest

    from warehouse_bigdata.application import runtime
    from warehouse_bigdata.application.read_repository import approved_metadata, binding_filter
    from warehouse_bigdata.application.session_state import PREFIX, publication_identity
    from warehouse_bigdata.common.config import load_paths_config
    from warehouse_bigdata.common.small_artifacts import load_small_artifacts
    from warehouse_bigdata.jobs.verify_clickhouse_distributed import run_verification
    from warehouse_bigdata.publication.batch import MAPPINGS

    started = perf_counter()
    root = load_paths_config().artifacts_dir
    output = root / "verification" / ("application-" + uuid4().hex)
    output.mkdir(parents=True, exist_ok=False)
    report = dict(
        timestamp=datetime.now(UTC).isoformat(),
        checks=[],
        git_revision=subprocess.run(
            ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True
        ).stdout.strip(),
        limitations=[
            dict(check=name, status="NOT RUN")
            for name in (
                "deployed-shard outage",
                "browser-visible responsiveness",
                "warm operation <=10 seconds",
            )
        ],
    )
    checks = report["checks"]
    stage = "prerequisites"

    def check(name, expected, actual, tolerance=1e-8):
        result = reconcile(name, expected, actual, tolerance)
        checks.append(result)
        assert result["status"] == "PASS", name

    try:
        publication = root / "approved_publication.json"
        if not publication.is_file():
            raise FileNotFoundError("existing approved publication is missing")
        selector = load_small_artifacts()
        ctx = runtime.load_render_context()
        _, manifest = approved_metadata(publication)
        report["identity"] = publication_identity(ctx)
        report["scope"] = ctx.dataset_bindings["forecast_daily"]
        check("small scope", "demo_sample", selector["dataset_scope"], 0)
        for key in ("source_snapshot_id", "model_run_id", "selection_digest", "dataset_scope"):
            check("selector " + key, ctx.dataset_bindings["forecast_daily"][key], selector[key], 0)
        # Existing verifier checks running nodes, frozen population and all nine datasets.
        distributed = run_verification(publication=publication, evidence_dir=root / "verification")
        report["distributed_evidence"] = str(distributed / "evidence.json")
        stage = "dashboard reconciliation"
        with runtime.open_render_repository(ctx) as repo:
            store = sorted(ctx.coverage.available_stores)[0]
            catalog = repo.get_eligible_items(store, "", limit=50)
            check("eligible catalog", "ok", catalog.state, 0)
            item = sorted(r["item_id"] for r in catalog.rows)[0]
            coverage = repo.get_store_history_coverage(store)
            check("history coverage", "ok", coverage.state, 0)
            end = coverage.rows[0]["end_date"]
            start = max(coverage.rows[0]["start_date"], end - timedelta(days=6))
            policy = ctx.policy_ids[0]
            report["filters"] = dict(store=store, item=item, start=start, end=end, policy=policy)
            timings = []

            def sql(dataset, filters):
                where, params = binding_filter(ctx, dataset)
                for index, (key, value) in enumerate(filters.items()):
                    where += f" AND `{key}`={{filter{index}:String}}"
                    params["filter" + str(index)] = value
                begin = perf_counter()
                result = tuple(
                    repo.client.query(
                        f"SELECT * FROM {repo.database}.{MAPPINGS[dataset].serving_dataset} WHERE {where} LIMIT 1001",
                        parameters=params,
                        settings={
                            "readonly": 1,
                            "skip_unavailable_shards": 0,
                            "max_execution_time": 30,
                            "max_result_rows": 1001,
                            "result_overflow_mode": "throw",
                        },
                    ).named_results()
                )
                timings.append(perf_counter() - begin)
                assert len(result) <= 1000
                return result

            sources, distributed_rows = {}, {}
            filters_by_dataset = {
                "sales_daily": dict(item_id=item, store_id=store),
                "forecast_daily": dict(item_id=item, store_id=store),
                "replenishment_recommendations": dict(item_id=item, store_id=store),
                "central_supply_scenario": dict(item_id=item),
                "allocation_recommendations": dict(item_id=item, policy_id=policy),
                "forecast_metrics": {},
                "inventory_policy_evaluation": dict(item_id=item),
            }
            for dataset, filters in filters_by_dataset.items():
                # Sales source is the frozen 1,941-day series; keep selected window only.
                sources[dataset] = source_rows(manifest, dataset, filters, output / "scratch")
                if dataset == "sales_daily":
                    # Bound the independent Distributed history read to seven days.
                    where, params = binding_filter(ctx, dataset)
                    params.update(item=item, store=store, start=start, end=end)
                    distributed_rows[dataset] = tuple(
                        repo.client.query(
                            f"SELECT * FROM {repo.database}.{MAPPINGS[dataset].serving_dataset} WHERE {where} "
                            "AND item_id={item:String} AND store_id={store:String} "
                            "AND sale_date BETWEEN {start:Date} AND {end:Date} LIMIT 8",
                            parameters=params,
                            settings={
                                "readonly": 1,
                                "skip_unavailable_shards": 0,
                                "max_execution_time": 30,
                                "max_result_rows": 8,
                                "result_overflow_mode": "throw",
                            },
                        ).named_results()
                    )
                    sources[dataset] = [
                        r for r in sources[dataset] if start <= r["sale_date"] <= end
                    ]
                else:
                    distributed_rows[dataset] = sql(dataset, filters)
            reads = {
                "sales_daily": repo.get_item_history(item, store, start, end),
                "forecast_daily": repo.get_forecast_series(item, store),
                "replenishment_recommendations": repo.get_replenishment(item, store),
                "central_supply_scenario": repo.get_central_supply(item),
                "allocation_recommendations": repo.get_allocation(item, policy),
                "forecast_metrics": repo.get_forecast_metrics(),
                "inventory_policy_evaluation": repo.get_policy_evaluation(item),
            }
            fields = {
                "sales_daily": ("sale_date", "unit_sales"),
                "forecast_daily": ("forecast_date", "horizon", "predicted_units"),
                "replenishment_recommendations": (
                    "forecast_7d_units",
                    "planning_demand_units",
                    "opening_stock_units",
                    "safety_stock_units",
                    "target_stock_units",
                    "critical_need_units",
                    "safety_need_units",
                    "request_units",
                ),
                "central_supply_scenario": ("central_available_units",),
                "allocation_recommendations": (
                    "store_id",
                    "requested_units",
                    "allocated_units",
                    "unmet_request_units",
                ),
                "forecast_metrics": ("method_id", "metric_name", "horizon", "metric_value"),
                "inventory_policy_evaluation": (
                    "store_id",
                    "policy_id",
                    "simulated_fulfilled_units",
                    "simulated_unmet_units",
                ),
            }
            for dataset, result in reads.items():
                check(dataset + " repository state", "ok", result.state, 0)

                def ordered(rows, dataset=dataset):
                    return sorted(rows, key=lambda r: str(tuple(r.get(k) for k in fields[dataset])))

                expected = ordered(sources[dataset])
                for label, rows in (
                    ("SQL", distributed_rows[dataset]),
                    ("repository", result.rows),
                ):
                    actual = ordered(rows)
                    check(dataset + " " + label + " count", len(expected), len(actual), 0)
                    for index, (left, right) in enumerate(zip(expected, actual, strict=True)):
                        for field in fields[dataset]:
                            check(f"{dataset} {label} {index} {field}", left[field], right[field])
            forecast = sum(r["predicted_units"] for r in reads["forecast_daily"].rows)
            check(
                "forecast/replenishment agreement",
                forecast,
                reads["replenishment_recommendations"].rows[0]["forecast_7d_units"],
            )
            selected_allocation = next(
                r for r in reads["allocation_recommendations"].rows if r["store_id"] == store
            )
            check(
                "replenishment/allocation request agreement",
                reads["replenishment_recommendations"].rows[0]["request_units"],
                selected_allocation["requested_units"],
                0,
            )
            report["query_seconds"] = timings
        at = AppTest.from_file(
            Path(__file__).resolve().parents[2] / "app" / "main.py", default_timeout=120
        )
        at.run()
        assert not at.exception and not at.error
        at.session_state[PREFIX + "store_id"] = store
        at.session_state[PREFIX + "item_id"] = item
        at.session_state[PREFIX + "overview.dates"] = (start, end)
        at.session_state[PREFIX + "overview.applied_dates"] = (start, end)
        at.run()
        from app.forecast import format_units
        from app.inventory import _format
        from app.overview import format_count

        check(
            "Overview displayed total",
            format_count(sum(r["unit_sales"] for r in sources["sales_daily"])),
            at.metric[-1].value,
            0,
        )
        for page, expected in (
            ("Dự báo nhu cầu", [format_units(forecast)]),
            ("Tồn kho", [_format(forecast, 1) + " đơn vị"]),
            (
                "Phân bổ hàng",
                [
                    _format(sources["central_supply_scenario"][0]["central_available_units"])
                    + " đơn vị",
                    *[
                        _format(sum(r[k] for r in sources["allocation_recommendations"]))
                        + " đơn vị"
                        for k in ("requested_units", "allocated_units", "unmet_request_units")
                    ],
                ],
            ),
        ):
            at.sidebar.radio[0].set_value(page).run()
            assert not at.exception and not at.error
            for index, value in enumerate(expected):
                check(page + " displayed KPI " + str(index), value, at.metric[index].value, 0)
            assert at.dataframe and at.get("plotly_chart")
        check(
            "final approval identity",
            publication_identity(ctx),
            publication_identity(runtime.load_render_context()),
            0,
        )
    except Exception as error:
        # Never persist exception text, endpoints or credentials.
        status = classify(error)
        if stage == "prerequisites" and not isinstance(error, (AssertionError, ValueError)):
            status = "BLOCKED"
        checks.append(dict(check=stage, status=status, exception_category=type(error).__name__))
    finally:
        report["duration_seconds"] = perf_counter() - started
        report["status"] = (
            "FAIL"
            if any(c["status"] == "FAIL" for c in checks)
            else "BLOCKED"
            if any(c["status"] == "BLOCKED" for c in checks)
            else "PASS"
        )
        (output / "application-acceptance.json").write_text(
            json.dumps(report, indent=2, default=str, allow_nan=False) + "\n"
        )
    if report["status"] == "BLOCKED":
        pytest.skip("BLOCKED: existing prerequisites unavailable; evidence: " + str(output))
    assert report["status"] == "PASS", "acceptance evidence: " + str(output)

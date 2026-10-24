"""Self-contained smoke tests for the demo-only J07/J08 adapter."""

import unittest
from datetime import date, timedelta
from types import SimpleNamespace

from app.mock_j78 import (
    MOCK_POLICIES,
    MockServingRepository,
    equal_share,
    proportional,
)
from app.preview_repository import PreviewContext, PreviewResult


class TestMock(unittest.TestCase):
    def setUp(self):
        self.issue = date(2016, 5, 22)
        ctx = PreviewContext(
            publication_id="PREVIEW-NOT-J08-APPROVED",
            manifest_checksum="preview-only:abc",
            source_snapshot_id="snap",
            model_run_id="model",
            forecast_run_id="forecast",
            issue_date=self.issue,
            coverage=SimpleNamespace(available_stores=("S1", "S2", "S3")),
            dataset_bindings={"forecast_daily": {"dataset_scope": "demo_sample"}},
        )

        class Real:
            def __init__(self, outer):
                self.context = ctx
                self.limits = SimpleNamespace(max_rows=1000, max_history_days=366)
                self.pairs = tuple(("I", s) for s in ("S1", "S2", "S3"))

            def get_forecast_series(self, item, store):
                if (item, store) not in self.pairs:
                    return PreviewResult("empty", provenance="predicted")
                number = int(store[-1])
                rows = tuple(
                    dict(
                        item_id=item,
                        store_id=store,
                        horizon=h,
                        issue_date=ctx.issue_date,
                        forecast_date=ctx.issue_date + timedelta(days=h),
                        predicted_units=float(number + h),
                        forecast_run_id=ctx.forecast_run_id,
                    )
                    for h in range(1, 8)
                )
                return PreviewResult("ok", rows, "predicted")

            def get_forecast_metrics(self):
                return PreviewResult("unavailable", provenance="evaluated")

        self.real = Real(self)
        self.repo = MockServingRepository(self.real)

    def test_apportionment(self):
        demands = {"A": 10, "B": 5, "C": 1}
        for allocator in (equal_share, proportional):
            for supply in (0, 1, 4, 8, 16, 100):
                assigned = allocator(demands, supply)
                self.assertEqual(sum(assigned.values()), min(supply, sum(demands.values())))
                self.assertTrue(all(0 <= assigned[k] <= demands[k] for k in demands))
                self.assertEqual(allocator(demands, supply), assigned)

    def test_immutable_forecast_and_replenishment(self):
        self.assertEqual(
            self.repo.get_forecast_series("I", "S1"), self.real.get_forecast_series("I", "S1")
        )
        self.assertEqual(self.repo.context.publication_id, "MOCK-J08-NOT-APPROVED")
        self.assertEqual(self.repo.context.policy_ids, MOCK_POLICIES)
        self.assertNotEqual(self.real.context.publication_id, self.repo.context.publication_id)
        for store in self.repo.context.coverage.available_stores:
            response = self.repo.get_replenishment("I", store)
            self.assertEqual(response.state, "ok")
            row = response.rows[0]
            self.assertEqual(
                row["request_units"], row["critical_need_units"] + row["safety_need_units"]
            )
            self.assertEqual(
                row["target_stock_units"], row["planning_demand_units"] + row["safety_stock_units"]
            )
            self.assertEqual(
                row["forecast_7d_units"],
                sum(x["predicted_units"] for x in self.real.get_forecast_series("I", store).rows),
            )
            self.assertEqual(
                row["origin_label"], "calculated_simulated_replenishment_recommendation"
            )
            self.assertEqual(row["source_snapshot_id"], "snap")

    def test_supply_and_allocation_reconcile(self):
        supply = self.repo.get_central_supply("I")
        self.assertEqual(supply.state, "ok")
        self.assertEqual(supply.rows[0]["origin_label"], "simulated_central_supply")
        capacity = supply.rows[0]["central_available_units"]
        for policy in MOCK_POLICIES:
            rows = self.repo.get_allocation("I", policy).rows
            self.assertEqual({r["store_id"] for r in rows}, {"S1", "S2", "S3"})
            self.assertEqual(sum(r["allocated_units"] for r in rows), capacity)
            for row in rows:
                self.assertEqual(
                    row["allocated_units"],
                    row["critical_allocated_units"] + row["safety_allocated_units"],
                )
                self.assertEqual(
                    row["unmet_request_units"], row["requested_units"] - row["allocated_units"]
                )
                self.assertLessEqual(row["allocated_units"], row["requested_units"])
                self.assertEqual(row["scenario_id"], self.repo.context.scenario_id)
                self.assertEqual(row["decision_run_id"], self.repo.context.decision_run_id)
            self.assertEqual(rows, self.repo.get_allocation("I", policy).rows)
        self.assertEqual(self.repo.get_policy_evaluation("I").state, "unavailable")
        self.assertEqual(self.repo.get_allocation("I", "other").state, "error")
        self.assertEqual(self.repo.get_central_supply("MISSING").state, "empty")


if __name__ == "__main__":
    unittest.main()

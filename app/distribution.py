"""Read-only, product-wide presentation of approved allocation decisions."""

from datetime import date, timedelta

import plotly.graph_objects as go
import streamlit as st

from app.inventory import _catalog, _change_selection, _format
from app.views import read_page
from warehouse_bigdata.application.read_repository import ReadResult
from warehouse_bigdata.application.session_state import PREFIX, reconcile_item, set_selection

POLICIES = ("shortage_first_proportional", "equal_share")
LABELS = {
    "shortage_first_proportional": "Ưu tiên thiếu hàng",
    "equal_share": "Chia đều có giới hạn",
}
QUANTITIES = ("requested_units", "allocated_units", "unmet_request_units")


def _integer(value):
    return type(value) is int and 0 <= value <= 2**63 - 1


def _matches(row, binding):
    for key, expected in binding.items():
        if key not in row:
            continue
        actual = row[key]
        if key in ("issue_date", "observed_label_window_start", "observed_label_window_end"):
            if type(actual) is not date:
                return False
            expected = date.fromisoformat(expected) if isinstance(expected, str) else expected
        if actual != expected:
            return False
    return True


def allocation_totals(supply, rows):
    return (supply["central_available_units"], *(sum(r[k] for r in rows) for k in QUANTITIES))


def normalize_allocation(supply, allocation, context, item, policy):
    """Fail closed before aggregating any quantities; never compute a policy."""
    invalid = ReadResult("incomplete", provenance="calculated")
    if supply.state != "ok":
        return ReadResult(supply.state, provenance="simulated")
    if allocation.state != "ok":
        return ReadResult(allocation.state, provenance="calculated")
    if (
        policy not in POLICIES
        or policy not in context.policy_ids
        or supply.provenance != "simulated"
        or allocation.provenance != "calculated"
        or len(supply.rows) != 1
    ):
        return invalid
    central = supply.rows[0]
    common = dict(item_id=item, scenario_id=context.scenario_id, issue_date=context.issue_date)
    if (
        any(central.get(k) != v for k, v in common.items())
        or type(central.get("issue_date")) is not date
        or central.get("origin_label") != "simulated_central_supply"
        or not isinstance(central.get("source_warehouse_id"), str)
        or not central["source_warehouse_id"].strip()
        or not _integer(central.get("central_available_units"))
        or not _matches(
            central, (context.dataset_bindings or {}).get("central_supply_scenario", {})
        )
    ):
        return invalid
    expected = set(context.coverage.available_stores)
    rows = allocation.rows
    if not expected or len(rows) != len(expected) or {r.get("store_id") for r in rows} != expected:
        return invalid
    identity = dict(
        common,
        policy_id=policy,
        policy_version="v1",
        **{
            k: getattr(context, k)
            for k in ("source_snapshot_id", "forecast_run_id", "model_run_id", "decision_run_id")
        },
    )
    for row in rows:
        if (
            any(v is None or row.get(k) != v for k, v in identity.items())
            or type(row.get("issue_date")) is not date
            or not _matches(
                row, (context.dataset_bindings or {}).get("allocation_recommendations", {})
            )
            or any(not _integer(row.get(k)) for k in (*QUANTITIES, "central_available_units"))
            or any(
                row.get(k) != central[k] for k in ("source_warehouse_id", "central_available_units")
            )
            or row["allocated_units"] > row["requested_units"]
            or row["unmet_request_units"] != row["requested_units"] - row["allocated_units"]
        ):
            return invalid
        stages = [row.get(k) for k in ("critical_allocated_units", "safety_allocated_units")]
        if stages != [None, None] or policy == POLICIES[0]:
            if not all(_integer(v) for v in stages) or sum(stages) != row["allocated_units"]:
                return invalid
    capacity, requested, allocated, unmet = allocation_totals(central, rows)
    if allocated > capacity or (capacity >= requested and unmet):
        return invalid
    return ReadResult("ok", tuple(sorted(rows, key=lambda r: r["store_id"])), "calculated")


def allocation_figure(rows):
    figure = go.Figure()
    for field, label in (
        ("requested_units", "Yêu cầu bổ sung"),
        ("allocated_units", "Được phân bổ"),
    ):
        figure.add_bar(
            x=[r[field] for r in rows],
            y=[r["store_id"] for r in rows],
            orientation="h",
            name=label,
            hovertemplate="Cửa hàng: %{y}<br>Số đơn vị: %{x}<extra>%{fullData.name}</extra>",
        )
    figure.update_layout(
        barmode="group", xaxis_title="Số đơn vị sản phẩm", yaxis_title="Cửa hàng", separators=",."
    )
    return figure


def normalize_comparison(result, context, item):
    invalid = ReadResult("incomplete", provenance="evaluated")
    if result.state != "ok":
        return ReadResult(result.state, provenance="evaluated")
    try:
        binding = context.evaluation_binding("inventory_policy_evaluation")
    except ValueError:
        return ReadResult("unavailable", provenance="evaluated")
    if (
        result.provenance != "evaluated"
        or not binding
        or not set(POLICIES) <= set(context.policy_ids)
        or set(binding.get("policy_ids", ())) != set(POLICIES)
        or binding.get("label_provenance") != "observed_m5_unit_sales_demand_proxy"
    ):
        return invalid
    decisions = binding.get("decision_run_ids", {})
    required = (
        "evaluation_run_id",
        "source_snapshot_id",
        "forecast_run_id",
        "scenario_id",
        "issue_date",
        "run_type",
    )
    if any(not binding.get(k) for k in required) or any(not decisions.get(p) for p in POLICIES):
        return invalid
    keyed = {}
    quantities = (
        "actual_sales_proxy_7d_units",
        "simulated_fulfilled_units",
        "simulated_unmet_units",
        "ending_stock_units",
        "excess_over_safety_units",
        "simulated_stockout_days",
    )
    for row in result.rows:
        policy, store, origin = row.get("policy_id"), row.get("store_id"), row.get("issue_date")
        if (
            policy not in POLICIES
            or store not in context.coverage.available_stores
            or row.get("item_id") != item
            or row.get("policy_version") != "v1"
            or row.get("decision_run_id") != decisions.get(policy)
            or any(k not in row for k in required)
            or not _matches(row, {k: binding[k] for k in required})
            or type(origin) is not date
            or type(row.get("observed_label_window_start")) is not date
            or type(row.get("observed_label_window_end")) is not date
            or row["observed_label_window_start"] != origin + timedelta(days=1)
            or row["observed_label_window_end"] != origin + timedelta(days=7)
            or any(not _integer(row.get(k)) for k in quantities)
            or row["simulated_fulfilled_units"] + row["simulated_unmet_units"]
            != row["actual_sales_proxy_7d_units"]
            or row["excess_over_safety_units"] > row["ending_stock_units"]
            or row["simulated_stockout_days"] > 7
        ):
            return invalid
        key = (policy, store, origin)
        if key in keyed:
            return invalid
        keyed[key] = row
    origin = (
        date.fromisoformat(binding["issue_date"])
        if isinstance(binding["issue_date"], str)
        else binding["issue_date"]
    )
    expected = {(p, s, origin) for p in POLICIES for s in context.coverage.available_stores}
    if not expected or set(keyed) != expected:
        return invalid
    for store in context.coverage.available_stores:
        if (
            keyed[POLICIES[0], store, origin]["actual_sales_proxy_7d_units"]
            != keyed[POLICIES[1], store, origin]["actual_sales_proxy_7d_units"]
        ):
            return invalid
    return ReadResult("ok", result.rows, "evaluated")


def comparison_totals(rows):
    totals = []
    for policy in POLICIES:
        cohort = [r for r in rows if r["policy_id"] == policy]
        demand = sum(r["actual_sales_proxy_7d_units"] for r in cohort)
        totals.append(
            dict(
                policy=policy,
                count=len(cohort),
                denominator=demand,
                fulfillment=sum(r["simulated_fulfilled_units"] for r in cohort) / demand
                if demand
                else None,
                **{
                    k: sum(r[k] for r in cohort)
                    for k in (
                        "simulated_unmet_units",
                        "ending_stock_units",
                        "excess_over_safety_units",
                        "simulated_stockout_days",
                    )
                },
            )
        )
    return totals


def render_allocation(context, repository, selections):
    from app.views import render_read_state

    st.title("Phân bổ hàng")
    st.info(
        "Nguồn cung và tồn kho là dữ liệu mô phỏng; phân bổ là đề xuất điều chuyển, "
        "không phải giao hàng đã thực hiện hoặc tồn kho Walmart thực tế."
    )
    stores = sorted(set(context.coverage.available_stores))
    if not stores:
        render_read_state(ReadResult("unavailable"))
        return
    membership = {}
    for store in stores:
        catalog = _catalog(read_page(repository.get_eligible_items, store, "", limit=50), store)
        if catalog.state not in ("ok", "empty"):
            render_read_state(catalog)
            return
        for row in catalog.rows:
            membership.setdefault(row["item_id"], set()).add(store)
    unsupported_item = reconcile_item(st.session_state, membership)
    item = st.session_state.get(PREFIX + "item_id", selections.get("item_id"))
    policy = st.session_state.get(PREFIX + "policy_id", selections.get("policy_id"))
    if policy is None:
        policy = next((p for p in context.policy_ids if p in POLICIES), None)
        set_selection(st.session_state, "policy_id", policy)
    for field, label, options, value in (
        ("item_id", "Sản phẩm", sorted(membership), item),
        ("policy_id", "Chính sách", [p for p in context.policy_ids if p in POLICIES], policy),
    ):
        widget = PREFIX + "widget_" + field
        st.session_state[widget] = value if value in options else None
        st.selectbox(
            label,
            [None, *options],
            key=widget,
            format_func=lambda v: "Chọn giá trị" if v is None else LABELS.get(v, v),
            on_change=_change_selection,
            args=(field, widget),
        )
        if value is not None and value not in options:
            st.warning("Lựa chọn hiện tại không được hỗ trợ trong bản công bố này.")
            return
    if unsupported_item:
        st.warning("Lựa chọn hiện tại không được hỗ trợ trong bản công bố này.")
        return
    if item is None or policy is None:
        st.info("Vui lòng chọn sản phẩm và chính sách để xem phân bổ.")
        return
    if membership[item] != set(stores):
        st.warning(
            "Phạm vi cửa hàng của sản phẩm không tương thích với phạm vi phân bổ đã phê duyệt."
        )
        return
    supply = read_page(repository.get_central_supply, item)
    result = normalize_allocation(
        supply, read_page(repository.get_allocation, item, policy), context, item, policy
    )
    if supply.state == "empty":
        st.info("Chưa có nguồn cung trung tâm mô phỏng đã công bố.")
        return
    if not render_read_state(result):
        return
    st.caption(
        f"Kịch bản: {context.scenario_id} · Ngày phát hành: {context.issue_date:%d/%m/%Y} · "
        f"Chính sách: {LABELS[policy]} (v1) · Bản công bố: {context.publication_id}"
    )
    st.caption(
        f"Ảnh chụp nguồn: {context.source_snapshot_id} · Dự báo: {context.forecast_run_id} · "
        f"Quyết định: {context.decision_run_id} · Phạm vi: toàn bộ {len(stores)} cửa hàng đủ điều kiện."
    )
    totals = allocation_totals(supply.rows[0], result.rows)
    for column, label, value in zip(
        st.columns(4),
        (
            "Nguồn cung trung tâm mô phỏng",
            "Tổng yêu cầu",
            "Tổng được phân bổ",
            "Yêu cầu chưa được đáp ứng",
        ),
        totals,
        strict=True,
    ):
        column.metric(label, _format(value) + " đơn vị")
    st.caption(
        "Không phát sinh yêu cầu"
        if totals[1] == 0
        else f"Tỷ lệ đáp ứng yêu cầu bổ sung: {_format(100 * totals[2] / totals[1], 1)}%. "
        "Đây không phải tỷ lệ đáp ứng nhu cầu khách hàng lịch sử."
    )
    st.plotly_chart(allocation_figure(result.rows), width="stretch", config={"displaylogo": False})
    st.dataframe(
        [
            {
                "Cửa hàng": r["store_id"],
                "Yêu cầu (đơn vị)": r["requested_units"],
                "Được phân bổ (đơn vị)": r["allocated_units"],
                "Chưa đáp ứng (đơn vị)": r["unmet_request_units"],
            }
            for r in result.rows
        ],
        hide_index=True,
        width="stretch",
    )
    st.markdown(
        "Chính sách ưu tiên phần thiếu hàng trước, phần an toàn sau; làm tròn số nguyên "
        "theo phần dư lớn nhất và phá hòa theo mã cửa hàng tăng dần."
        if policy == POLICIES[0]
        else "Chính sách chia đều có giới hạn theo yêu cầu và phân phối lại phần dư."
    )
    st.subheader("So sánh chính sách trên dữ liệu lịch sử")
    comparison = normalize_comparison(
        read_page(repository.get_policy_evaluation, item), context, item
    )
    if comparison.state != "ok":
        st.info(
            "Chưa có kết quả so sánh chính sách trên các nhóm đánh giá đầy đủ, trùng khớp đã phê duyệt."
        )
        return
    first = comparison.rows[0]
    st.caption(
        f"Kỳ quan sát: {first['observed_label_window_start']:%d/%m/%Y} – "
        f"{first['observed_label_window_end']:%d/%m/%Y}. "
        "Doanh số M5 quan sát là đại diện nhu cầu; tồn kho và thiếu hụt là mô phỏng, "
        "khác với yêu cầu bổ sung chưa được đáp ứng. Chỉ mô tả phạm vi mẫu đã phê duyệt."
    )
    st.dataframe(
        [
            {
                "Chính sách": LABELS[t["policy"]],
                "Số cửa hàng–kỳ": t["count"],
                "Doanh số đại diện (đơn vị)": t["denominator"],
                "Tỷ lệ đáp ứng nhu cầu đại diện": "Không xác định"
                if t["fulfillment"] is None
                else _format(t["fulfillment"] * 100, 1) + "%",
                "Thiếu hụt nhu cầu mô phỏng": t["simulated_unmet_units"],
                "Tồn cuối kỳ mô phỏng": t["ending_stock_units"],
                "Vượt tồn an toàn": t["excess_over_safety_units"],
                "Ngày–cửa hàng hết hàng mô phỏng": t["simulated_stockout_days"],
            }
            for t in comparison_totals(comparison.rows)
        ],
        hide_index=True,
        width="stretch",
    )

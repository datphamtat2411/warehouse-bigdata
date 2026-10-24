"""Publication-pinned historical observed-sales presentation; no fact queries here."""

import math
from datetime import date, timedelta

import plotly.graph_objects as go
import streamlit as st

from app.views import UNSUPPORTED_ITEM, read_page
from warehouse_bigdata.application.read_repository import ReadResult
from warehouse_bigdata.application.session_state import PREFIX, reconcile_item, set_selection

KEY = PREFIX + "overview."


def format_count(value):
    """Vietnamese separators, without rounding the underlying measurements."""
    return format(value, ",").replace(",", "_").replace(".", ",").replace("_", ".")


def valid_window(value, start, end, limit):
    return (
        isinstance(value, (tuple, list))
        and len(value) == 2
        and all(type(day) is date for day in value)
        and start <= value[0] <= value[1] <= end
        and (value[1] - value[0]).days < limit
    )


def normalize_daily(result, store, item, start, end):
    """Validate the entire selected window before exposing any presentation rows."""
    if result.state != "ok":
        return result
    if result.provenance != "observed":
        return ReadResult("error", provenance="observed", reason="unexpected sales provenance")
    seen, rows = set(), []
    for row in result.rows:
        if row.get("store_id") != store or (item is not None and row.get("item_id") != item):
            return ReadResult("error", provenance="observed", reason="selected identity mismatch")
        day = row.get("sale_date")
        units = row.get("unit_sales" if item is not None else "total_units")
        if (
            type(day) is not date
            or not start <= day <= end
            or day in seen
            or type(units) not in (int, float)
            or not math.isfinite(units)
            or units < 0
        ):
            return ReadResult("incomplete", provenance="observed", reason="invalid daily sales")
        seen.add(day)
        rows.append(dict(date=day, units=units))
    if len(seen) != (end - start).days + 1:
        return ReadResult("incomplete", provenance="observed", reason="daily sales missing")
    if not math.isfinite(sum(row["units"] for row in rows)):
        return ReadResult("incomplete", provenance="observed", reason="invalid sales total")
    return ReadResult("ok", tuple(sorted(rows, key=lambda row: row["date"])), "observed")


def daily_figure(rows):
    figure = go.Figure(
        go.Scatter(
            x=[row["date"] for row in rows],
            y=[row["units"] for row in rows],
            mode="lines+markers" if len(rows) <= 31 else "lines",
            name="Thực tế — số đơn vị bán quan sát",
            line=dict(color="#16756d", width=2),
            marker=dict(size=5),
            fill="tozeroy",
            fillcolor="rgba(22, 117, 109, 0.06)",
            hovertemplate="Ngày: %{x|%d/%m/%Y}<br>Số đơn vị bán quan sát: %{y}<extra></extra>",
        )
    )
    figure.update_layout(
        xaxis_title="Ngày",
        yaxis_title="Số đơn vị sản phẩm",
        separators=",.",
        height=370,
        margin=dict(l=12, r=16, t=20, b=12),
        font=dict(family="Arial, sans-serif", color="#203547", size=12),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
        hoverlabel=dict(bgcolor="#ffffff"),
        showlegend=True,
        legend=dict(orientation="h", y=1.14, x=0),
    )
    figure.update_xaxes(
        tickformat="%d/%m/%Y",
        showgrid=False,
        zeroline=False,
        dtick=86400000 if len(rows) <= 7 else None,
        nticks=6,
    )
    figure.update_yaxes(rangemode="tozero", gridcolor="#e8edf1", zeroline=False)
    return figure


def _change_selection(field, widget):
    set_selection(st.session_state, field, st.session_state[widget])


def _coverage(result, store):
    if result.state != "ok":
        return result
    if result.provenance != "observed" or len(result.rows) != 1:
        return ReadResult("incomplete", reason="invalid coverage result")
    row = result.rows[0]
    start, end = row.get("start_date"), row.get("end_date")
    days, count = row.get("day_count"), row.get("row_count")
    if not (
        row.get("store_id") == store
        and type(start) is date
        and type(end) is date
        and start <= end
        and type(days) is int
        and type(count) is int
        and days == count == (end - start).days + 1
    ):
        return ReadResult("incomplete", reason="invalid coverage measurements")
    return result


def _catalog(result, store):
    if result.state not in {"ok", "empty"}:
        return result
    # The bounded API truncates at 50. At saturation completeness is not proven.
    items = [row.get("item_id") for row in result.rows]
    if (
        result.provenance != "predicted"
        or len(items) >= 50
        or (result.state == "ok" and not items)
        or (result.state == "empty" and items)
        or any(not isinstance(item, str) or not item.strip() for item in items)
        or len(set(items)) != len(items)
        or any(row.get("store_id") != store for row in result.rows)
    ):
        return ReadResult("incomplete", reason="complete supported catalog unavailable")
    return result


def _source(context, store, item, window=None):
    with st.expander("Nguồn và phạm vi dữ liệu"):
        st.markdown(f"**Bản công bố:** {context.publication_id}")
        st.markdown(f"**Ảnh chụp nguồn:** {context.source_snapshot_id}")
        # Sales scope belongs to the publication binding, not the chosen date window.
        for dataset in ("store_daily_sales", "sales_daily"):
            binding = (context.dataset_bindings or {}).get(dataset, {})
            if binding.get("dataset_scope"):
                st.markdown(f"**Phạm vi dữ liệu ({dataset}):** {binding['dataset_scope']}")
            if binding.get("selection_digest"):
                st.markdown(f"**Mã định danh phạm vi ({dataset}):** {binding['selection_digest']}")
        st.markdown(f"**Cửa hàng:** {store} · **Sản phẩm:** {item or 'Tất cả sản phẩm'}")
        if window is not None:
            st.markdown(f"**Khoảng ngày hiển thị:** {window[0]:%d/%m/%Y} – {window[1]:%d/%m/%Y}")
        st.caption(
            "Nguồn gốc: số đơn vị bán quan sát từ M5. M5 Small chỉ là mẫu đã được "
            "phê duyệt, không đại diện toàn bộ danh mục M5."
        )
        st.caption(
            "Số lượng bán là đơn vị sản phẩm, không phải đơn hàng, doanh thu hay tồn kho "
            "thực tế của Walmart. Danh mục lựa chọn gồm các sản phẩm đủ điều kiện dự báo."
        )


def render_overview(context, repository, selections):
    # Local import keeps the shared failure presenter independent of page modules.
    from app.views import render_read_state

    st.title("Tổng quan")
    st.caption("Dữ liệu bán hàng lịch sử từ bộ M5 Walmart.")
    st.markdown("Theo dõi số lượng bán, phạm vi dữ liệu và xu hướng theo cửa hàng, sản phẩm.")

    stores = sorted(set(context.coverage.available_stores))
    if not stores:
        render_read_state(ReadResult("unavailable"))
        return
    store = selections.get("store_id")
    if store is None:
        store = stores[0]
        set_selection(st.session_state, "store_id", store)
    unsupported_store = store not in stores
    store_key = KEY + "store"
    st.session_state[store_key] = None if unsupported_store else store
    with st.container(border=True, key="overview_filters"):
        st.markdown("**Phạm vi phân tích**")
        store_filter, item_filter, date_filter, apply_filter = st.columns(
            [1, 1.5, 1.8, 0.7], vertical_alignment="bottom"
        )
        store_filter.selectbox(
            "Cửa hàng",
            [None, *stores],
            key=store_key,
            placeholder="Chọn cửa hàng",
            format_func=lambda value: "Chọn cửa hàng" if value is None else value,
            on_change=_change_selection,
            args=("store_id", store_key),
        )
    if unsupported_store:
        st.warning("Cửa hàng đã chọn không được hỗ trợ trong bản công bố hiện tại.")
        return

    catalog = _catalog(read_page(repository.get_eligible_items, store, "", limit=50), store)
    if catalog.state not in {"ok", "empty"}:
        st.caption("Độ phủ sản phẩm được hỗ trợ hiện không khả dụng; không coi là bằng không.")
        render_read_state(catalog)
        return
    items = sorted(row["item_id"] for row in catalog.rows)
    unsupported_item = reconcile_item(st.session_state, items)
    item = st.session_state.get(PREFIX + "item_id", selections.get("item_id"))
    item_key = PREFIX + "widget_item_id"
    st.session_state[item_key] = item
    item_filter.selectbox(
        "Sản phẩm được hỗ trợ",
        [None, *items],
        key=item_key,
        placeholder="Tất cả sản phẩm",
        format_func=lambda value: "Tất cả sản phẩm" if value is None else value,
        on_change=_change_selection,
        args=("item_id", item_key),
    )
    if unsupported_item:
        st.warning(UNSUPPORTED_ITEM)
        return
    if not items:
        st.info("Không có sản phẩm đủ điều kiện được hỗ trợ. Vẫn có thể xem tổng bán của cửa hàng.")

    coverage = _coverage(read_page(repository.get_store_history_coverage, store), store)
    if not render_read_state(coverage):
        return
    measured = coverage.rows[0]
    start, end = measured["start_date"], measured["end_date"]
    st.subheader("Phạm vi dữ liệu", divider="gray")
    a, b, c = st.columns(3)
    a.metric("Cửa hàng trong phạm vi", format_count(len(stores)))
    b.metric(
        "Sản phẩm được hỗ trợ",
        format_count(len(items)),
        help="Số sản phẩm đủ điều kiện dự báo tại cửa hàng đã chọn.",
    )
    c.metric(
        "Ngày lịch sử",
        format_count(measured["day_count"]),
        help="Số ngày đã nạp tại cửa hàng đã chọn, độc lập với khoảng ngày hiển thị.",
    )
    st.caption(
        f"M5 Small · Cửa hàng {store} · {start:%d/%m/%Y} – {end:%d/%m/%Y} · "
        f"{format_count(measured['row_count'])} dòng tổng hợp theo ngày. "
        "Độ phủ độc lập với bộ lọc ngày và sản phẩm."
    )

    limit = min(365, repository.limits.max_history_days, repository.limits.max_rows)
    default = (max(start, end - timedelta(days=limit - 1)), end)
    date_key, applied_key = KEY + "dates", KEY + "applied_dates"
    if st.session_state.get(KEY + "date_store") != store:
        st.session_state[KEY + "date_store"] = store
        st.session_state[date_key] = default
        st.session_state[applied_key] = default
    draft = date_filter.date_input(
        "Khoảng ngày bán hàng",
        value=default,
        min_value=start,
        max_value=end,
        format="DD/MM/YYYY",
        key=date_key,
    )
    if draft != st.session_state.get(KEY + "effective_dates"):
        for key in list(st.session_state):
            if key.startswith(PREFIX + "results"):
                del st.session_state[key]
        st.session_state[KEY + "effective_dates"] = draft
    apply = apply_filter.button("Áp dụng", key=KEY + "apply", type="primary", width="stretch")
    if not valid_window(draft, start, end, limit):
        st.warning(
            f"Vui lòng chọn đủ hai ngày đúng thứ tự trong lịch sử đã nạp, tối đa {limit} ngày."
        )
        _source(context, store, item)
        return
    if apply:
        st.session_state[applied_key] = tuple(draft)
    if tuple(draft) != st.session_state.get(applied_key):
        st.info("Khoảng ngày đã thay đổi. Chọn Áp dụng để hiển thị kết quả theo lựa chọn mới.")
        _source(context, store, item)
        return
    window = tuple(draft)
    raw = (
        read_page(repository.get_store_trend, store, *window)
        if item is None
        else read_page(repository.get_item_history, item, store, *window)
    )
    result = normalize_daily(raw, store, item, *window)
    if not render_read_state(result):
        _source(context, store, item, window)
        return
    rows = result.rows
    total = sum(row["units"] for row in rows)
    st.subheader("Xu hướng bán hàng", divider="gray")
    st.caption(
        f"Cửa hàng {store} · {item or 'Tất cả sản phẩm'} · "
        f"{window[0]:%d/%m/%Y} – {window[1]:%d/%m/%Y}"
    )
    chart, summary = st.columns([3.4, 1], gap="large")
    with chart:
        st.plotly_chart(daily_figure(rows), width="stretch", config={"displaylogo": False})
    with summary:
        st.metric(
            "Tổng số lượng bán", format_count(total), help="Tổng đơn vị sản phẩm theo bộ lọc."
        )
        st.caption("Số liệu trong khoảng ngày đã chọn")
        st.markdown("**Bình quân mỗi ngày**")
        st.markdown(f"{format_count(round(total / len(rows), 1))} đơn vị sản phẩm")
        peak = max(rows, key=lambda row: row["units"])
        st.markdown("**Ngày bán cao nhất**")
        st.markdown(f"{peak['date']:%d/%m/%Y} · {format_count(peak['units'])} đơn vị")
        st.caption(f"{format_count(len(rows))} ngày quan sát · Đơn vị: sản phẩm")
    with st.expander("Chi tiết bán hàng quan sát"):
        st.dataframe(
            [
                {
                    "Ngày": row["date"].strftime("%d/%m/%Y"),
                    "Cửa hàng": store,
                    "Sản phẩm": item or "Tất cả sản phẩm",
                    "Số đơn vị bán quan sát": row["units"],
                }
                for row in rows
            ],
            hide_index=True,
            width="stretch",
        )
    _source(context, store, item, window)

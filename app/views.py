"""Minimal destination handoffs and safe shared read-state presentation."""

import streamlit as st

LOADING = "Đang tải dữ liệu đã công bố..."
UNSUPPORTED_ITEM = "Sản phẩm đã chọn không được hỗ trợ tại cửa hàng trong bản công bố hiện tại."


def read_page(method, *args, **kwargs):
    """Bounded uncached reads with consistent loading feedback."""
    with st.spinner(LOADING):
        return method(*args, **kwargs)


def render_read_state(result):
    """Return whether rows may be shown; never expose technical failure reasons."""
    if result.state == "ok":
        return True
    presenter, message = {
        "empty": (st.info, "Không có dữ liệu phù hợp với lựa chọn hiện tại."),
        "incomplete": (st.warning, "Dữ liệu chưa đầy đủ; không hiển thị kết quả một phần."),
        "unavailable": (st.warning, "Dữ liệu hiện không khả dụng. Vui lòng thử làm mới."),
        "error": (st.error, "Không thể hiển thị kết quả. Vui lòng kiểm tra lựa chọn hoặc thử lại."),
    }.get(result.state, (st.error, "Không thể xác định trạng thái dữ liệu."))
    presenter(message)
    return False


def _placeholder(title, notice):
    st.title(title)
    st.info(notice)
    st.markdown("Nội dung trang đang được chuẩn bị.")


def render_overview(context, repository, selections):
    from app.overview import render_overview as render

    render(context, repository, selections)


def render_forecast(context, repository, selections):
    from app.forecast import render_forecast as render

    render(context, repository, selections)


def render_inventory(context, repository, selections):
    from app.inventory import render_inventory as render

    render(context, repository, selections)


def render_allocation(context, repository, selections):
    from app.distribution import render_allocation as render

    render(context, repository, selections)

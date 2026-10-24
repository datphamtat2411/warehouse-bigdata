"""Four original M09 Streamlit pages, backed by honest demo-only mock serving.

app/main.py, app/shell.py, HDFS publication and ClickHouse are untouched.
"""

from __future__ import annotations

from pathlib import Path

import streamlit as st

from app import views
from app.mock_j78 import make_mock_repository
from app.presentation import apply_style, sidebar_identity
from app.preview_repository import checked_manifest
from warehouse_bigdata.application.session_state import PREFIX, selections, sync_publication_state


@st.cache_resource(show_spinner="Đang kiểm tra bản trích xuất M5 Small...")
def _load_cached(base_string, _manifest, _selector, signature):
    return make_mock_repository(Path(base_string), _manifest, _selector, signature)


PAGES = {
    "Tổng quan": views.render_overview,
    "Dự báo nhu cầu": views.render_forecast,
    "Tồn kho": views.render_inventory,
    "Phân bổ hàng": views.render_allocation,
}


def render_preview():
    st.set_page_config(page_title="Phân tích M5 | Hỗ trợ quyết định kho hàng", layout="wide")
    apply_style()
    sidebar_identity()
    try:
        base, manifest, selector, digest = checked_manifest(Path.cwd())
        repository = _load_cached(str(base), manifest, selector, digest)
    except Exception as error:
        st.error(
            "Không thể xác minh dữ liệu M5 Small J02–J06. "
            "Chạy `prepare_demo.py` tại thư mục gốc repository."
        )
        st.caption("Error category: " + type(error).__name__)
        st.stop()
    ctx = repository.context
    sync_publication_state(st.session_state, ctx)
    st.sidebar.button("Làm mới dữ liệu", on_click=_load_cached.clear, width="stretch")
    page = st.sidebar.radio("Điều hướng", tuple(PAGES), key=PREFIX + "navigation")
    st.sidebar.caption("M5 Small · Phân tích phục vụ nghiên cứu")
    with st.sidebar.expander("Thông tin dữ liệu"):
        st.markdown("**Bán hàng:** Quan sát từ M5 Small, trích xuất từ HDFS.")
        st.markdown("**Dự báo:** Kết quả mô hình Random Forest, chân trời 7 ngày.")
        st.markdown("**Tồn kho và phân bổ:** Kịch bản mô phỏng và đề xuất tính toán.")
        st.caption("Chế độ xem cục bộ: phục vụ trong bộ nhớ, chưa có bản công bố ClickHouse.")
        st.markdown(f"**Phiên dự báo:** `{ctx.forecast_run_id}`")
        st.markdown(f"**Phạm vi:** `demo_sample` · `{selector['selection_digest']}`")
    try:
        PAGES[page](ctx, repository, selections(st.session_state))
    except Exception as error:
        st.error("Không thể hiển thị nội dung demo. Hãy kiểm tra tệp dữ liệu và thử tải lại.")
        st.caption("Error category: " + type(error).__name__)

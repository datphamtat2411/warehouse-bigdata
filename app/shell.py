"""One approval gate and one captured publication per Streamlit render."""
import logging

import streamlit as st

from app import views
from warehouse_bigdata.application import runtime
from warehouse_bigdata.application.session_state import (
    PREFIX,
    publication_identity,
    selections,
    sync_publication_state,
)
from warehouse_bigdata.common.hdfs_client import HdfsCommandError

LOGGER = logging.getLogger(__name__)
PAGES = {
    'Tổng quan': views.render_overview,
    'Dự báo nhu cầu': views.render_forecast,
    'Tồn kho': views.render_inventory,
    'Phân bổ hàng': views.render_allocation,
}


def _log_failure(operation, page, error, identity=None):
    # Exception messages and tracebacks can contain endpoints or credentials.
    LOGGER.error('Application operation=%s page=%s exception_category=%s publication_identity=%s',
                 operation, page, type(error).__name__, identity)


def _approval_message(error):
    if isinstance(error, FileNotFoundError):
        return 'Chưa có bản công bố được phê duyệt tại ứng dụng. Vui lòng kiểm tra cấu hình dữ liệu.'
    if isinstance(error, ValueError) and str(error) == 'publication is fenced unavailable':
        return 'Bản công bố đã bị tạm ngừng phục vụ. Không thể hiển thị dữ liệu.'
    if isinstance(error, (HdfsCommandError, OSError)):
        return 'Không thể xác minh phê duyệt trên HDFS. Vui lòng kiểm tra kết nối và thử lại.'
    return 'Thông tin phê duyệt không hợp lệ hoặc chưa đầy đủ. Không thể hiển thị dữ liệu.'


def _summary(context):
    with st.sidebar.expander('Thông tin dữ liệu và phiên chạy'):
        for label, value in (
            ('Bản công bố', context.publication_id),
            ('Mã kiểm tra bản công bố', context.manifest_checksum),
            ('Ảnh chụp nguồn', context.source_snapshot_id),
            ('Phiên mô hình', context.model_run_id),
            ('Phiên dự báo', context.forecast_run_id),
            ('Kịch bản mô phỏng', context.scenario_id),
            ('Phiên quyết định', context.decision_run_id),
            ('Phiên đánh giá lịch sử', context.evaluation_run_id or 'Chưa có'),
            ('Tập đánh giá giữ lại', context.holdout_id or 'Chưa có'),
            ('Ngày phát hành', context.issue_date.strftime('%d/%m/%Y')),
            ('Chính sách được phê duyệt', ', '.join(context.policy_ids) or 'Chưa có'),
        ):
            st.markdown(f'**{label}:** {value}')
        binding = (context.dataset_bindings or {}).get('forecast_daily', {})
        st.markdown(f'**Phạm vi dữ liệu:** {binding.get("dataset_scope", "Chưa có")}')
        st.markdown(f'**Mã định danh phạm vi:** {binding.get("selection_digest", "Chưa có")}')
        coverage = context.coverage
        st.markdown(f'**Độ phủ phục vụ đã đo:** {coverage.series_total} chuỗi; '
                    f'{coverage.series_eligible} chuỗi đủ điều kiện; '
                    f'{coverage.series_scored} chuỗi được dự báo; '
                    f'{len(coverage.available_stores)} cửa hàng; '
                    f'{coverage.loaded_rows} dòng đã nạp trên các tập phục vụ.')
        if binding.get('dataset_scope') == 'demo_sample':
            st.markdown('**Quần thể mẫu chuẩn:** 10 mã hàng × 10 cửa hàng = 100 chuỗi; '
                        '1.941 ngày, 194.100 bản ghi doanh số quan sát. '
                        'Số dòng đã nạp phục vụ không phải số bản ghi doanh số quan sát. '
                        'Kết quả minh họa chỉ mô tả mẫu này, không đại diện toàn bộ danh mục.')
        st.markdown('**Nguồn gốc dữ liệu:** doanh số M5 quan sát được; dự đoán của mô hình; '
                    'tồn kho và nguồn cung mô phỏng; khuyến nghị được tính toán; '
                    'đánh giá lịch sử riêng biệt.')
        for dataset in ('forecast_metrics', 'inventory_policy_evaluation'):
            historical = (context.dataset_bindings or {}).get(dataset, {})
            if historical:
                label = ('Đánh giá dự báo lịch sử' if dataset == 'forecast_metrics'
                         else 'Đánh giá chính sách lịch sử')
                st.markdown(f'**{label}:** {historical.get("evaluation_run_id", "Chưa có")}')


def render_shell():
    st.set_page_config(page_title='Hỗ trợ quyết định kho hàng', layout='wide')
    # A button interaction already causes a full script rerun, including this gate.
    st.sidebar.button('Làm mới dữ liệu đã công bố')
    page = st.session_state.get(PREFIX + 'navigation', 'Tổng quan')
    previous = st.session_state.get(PREFIX + 'identity')
    try:
        with st.spinner('Đang tải thông tin dữ liệu...'):
            context = runtime.load_render_context()
    except Exception as error:
        sync_publication_state(st.session_state, None)
        _log_failure('approval_validation', page, error, previous)
        st.error(_approval_message(error))
        st.stop()

    sync_publication_state(st.session_state, context)
    page = st.sidebar.radio('Điều hướng', tuple(PAGES), key=PREFIX + 'navigation')
    _summary(context)
    operation = 'client_initialization'
    try:
        with runtime.open_render_repository(context) as repository:
            operation = 'page_render'
            PAGES[page](context, repository, selections(st.session_state))
    except Exception as error:
        _log_failure(operation, page, error, publication_identity(context))
        if operation == 'client_initialization':
            st.error('Không thể kết nối kho dữ liệu phục vụ. Vui lòng kiểm tra kết nối và thử lại.')
        else:
            st.error('Không thể hiển thị trang. Vui lòng thử làm mới dữ liệu đã công bố.')

"""Minimal destination handoffs and safe shared read-state presentation."""
import streamlit as st


def render_read_state(result):
    """Return whether rows may be shown; never expose technical failure reasons."""
    if result.state == 'ok':
        return True
    presenter, message = {
        'empty': (st.info, 'Không có dữ liệu phù hợp với lựa chọn hiện tại.'),
        'incomplete': (st.warning, 'Dữ liệu chưa đầy đủ; không hiển thị kết quả một phần.'),
        'unavailable': (st.warning, 'Dữ liệu hiện không khả dụng. Vui lòng thử làm mới.'),
        'error': (st.error, 'Không thể hiển thị kết quả. Vui lòng kiểm tra lựa chọn hoặc thử lại.'),
    }.get(result.state, (st.error, 'Không thể xác định trạng thái dữ liệu.'))
    presenter(message)
    return False


def _placeholder(title, notice):
    st.title(title)
    st.info(notice)
    st.markdown('Nội dung trang đang được chuẩn bị.')


def render_overview(context, repository, selections):
    from app.overview import render_overview as render
    render(context, repository, selections)


def render_forecast(context, repository, selections):
    from app.forecast import render_forecast as render
    render(context, repository, selections)


def render_inventory(context, repository, selections):
    _placeholder('Tồn kho', 'Tồn kho và nguồn cung là dữ liệu mô phỏng; đề xuất bổ sung hàng '
                 'là kết quả tính toán, không phản ánh tồn kho Walmart thực tế.')


def render_allocation(context, repository, selections):
    _placeholder('Phân bổ hàng', 'Phân bổ hàng là khuyến nghị được tính toán từ nguồn cung '
                 'mô phỏng và chính sách đã được phê duyệt.')

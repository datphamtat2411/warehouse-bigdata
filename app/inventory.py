"""Read-only presentation of publication-bound J07 replenishment decisions."""
from datetime import date, timedelta
import math

import plotly.graph_objects as go
import streamlit as st

from warehouse_bigdata.application.read_repository import ReadResult
from warehouse_bigdata.application.session_state import PREFIX, set_selection, reconcile_item
from app.views import read_page, UNSUPPORTED_ITEM


KEY = PREFIX + 'inventory.'
ORIGIN = 'calculated_simulated_replenishment_recommendation'
INTEGER_LABELS = {
    'planning_demand_units': 'Nhu cầu lập kế hoạch (đơn vị)',
    'opening_stock_units': 'Tồn kho đầu kỳ mô phỏng (đơn vị)',
    'safety_stock_units': 'Tồn kho an toàn giả định (đơn vị)',
    'target_stock_units': 'Tồn kho mục tiêu (đơn vị)',
    'critical_need_units': 'Nhu cầu bổ sung cấp thiết (đơn vị)',
    'safety_need_units': 'Nhu cầu bổ sung an toàn (đơn vị)',
    'request_units': 'Yêu cầu bổ sung (đơn vị)',
}


def _format(value, decimals=0):
    return format(value, f',.{decimals}f').replace(',', '_').replace('.', ',').replace('_', '.')


def normalize_replenishment(result, context, item, store):
    """Validate stored fields without reconstructing any policy calculation."""
    if result.state != 'ok':
        return result
    invalid = ReadResult('incomplete', provenance='calculated')
    if result.provenance != 'calculated' or len(result.rows) != 1:
        return invalid
    row = result.rows[0]
    identities = dict(item_id=item, store_id=store, issue_date=context.issue_date,
                      **{key: getattr(context, key) for key in
                         ('source_snapshot_id', 'forecast_run_id', 'model_run_id',
                          'scenario_id', 'decision_run_id')})
    if (any(value is None or row.get(key) != value for key, value in identities.items())
            or type(row.get('issue_date')) is not date
            or row.get('origin_label') != ORIGIN
            or row.get('policy_version') != 'replenishment_v1'
            or not isinstance(row.get('feature_version'), str)
            or not row['feature_version'].strip()):
        return invalid
    # Additional bound lineage, when present, must also agree with the pinned publication.
    binding = (context.dataset_bindings or {}).get('replenishment_recommendations', {})
    for key in ('feature_version', 'source_snapshot_id', 'model_run_id', 'forecast_run_id',
                'scenario_id', 'decision_run_id', 'policy_version', 'origin_label'):
        if key in binding and row.get(key) != binding[key]:
            return invalid
    forecast = row.get('forecast_7d_units')
    try:
        valid_forecast = type(forecast) in (int, float) and math.isfinite(forecast) and forecast >= 0
    except OverflowError:
        valid_forecast = False
    if not valid_forecast:
        return invalid
    if any(type(row.get(key)) is not int or not 0 <= row[key] <= 2**63 - 1
           for key in INTEGER_LABELS):
        return invalid
    flag = row.get('forecast_shortage_flag')
    if type(flag) not in (bool, int) or flag not in (0, 1):
        return invalid
    return ReadResult('ok', result.rows, 'calculated')


def inventory_figure(row):
    """Compare distinct stored quantities, preserving their original precision."""
    figure = go.Figure(go.Bar(
        x=['Tổng dự báo 7 ngày', 'Tồn kho đầu kỳ (mô phỏng)', 'Tồn kho mục tiêu'],
        y=[row['forecast_7d_units'], row['opening_stock_units'], row['target_stock_units']],
        name='Số đơn vị sản phẩm',
        hovertemplate='%{x}<br>Số đơn vị sản phẩm: %{y}<extra></extra>'))
    figure.update_layout(title='So sánh dự báo, tồn kho mô phỏng và mục tiêu',
                         xaxis_title='Đại lượng đã công bố', yaxis_title='Số đơn vị sản phẩm',
                         barmode='group', separators=',.')
    return figure


def _change_selection(field, widget):
    set_selection(st.session_state, field, st.session_state[widget])


def _catalog(result, store):
    if result.state not in {'ok', 'empty'}:
        return result
    items = [row.get('item_id') for row in result.rows]
    if (result.provenance != 'predicted' or len(items) >= 50
            or (result.state == 'ok') != bool(items)
            or any(not isinstance(item, str) or not item.strip() for item in items)
            or len(set(items)) != len(items)
            or any(row.get('store_id') != store for row in result.rows)):
        return ReadResult('incomplete')
    return result


def render_inventory(context, repository, selections):
    from app.views import render_read_state

    st.title('Tồn kho')
    st.info('Tồn kho là dữ liệu mô phỏng, tồn kho an toàn là giả định; yêu cầu bổ sung là '
            'kết quả tính toán đã công bố, không phản ánh tồn kho Walmart thực tế.')
    stores = sorted(set(context.coverage.available_stores))
    if not stores:
        render_read_state(ReadResult('unavailable'))
        return
    store = selections.get('store_id')
    if store is None:
        store = stores[0]
        set_selection(st.session_state, 'store_id', store)
    unsupported_store = store not in stores
    store_key = KEY + 'store'
    st.session_state[store_key] = None if unsupported_store else store
    st.selectbox('Cửa hàng', [None, *stores], key=store_key,
                 format_func=lambda value: 'Chọn cửa hàng' if value is None else value,
                 on_change=_change_selection, args=('store_id', store_key))
    if unsupported_store:
        st.warning('Cửa hàng đã chọn không được hỗ trợ trong bản công bố hiện tại.')
        return
    catalog = _catalog(read_page(repository.get_eligible_items, store, '', limit=50), store)
    if not render_read_state(catalog):
        return
    items = sorted(row['item_id'] for row in catalog.rows)
    unsupported_item = reconcile_item(st.session_state, items)
    item = st.session_state.get(PREFIX + 'item_id', selections.get('item_id'))
    item_key = PREFIX + 'widget_item_id'
    st.session_state[item_key] = None if unsupported_item else item
    st.selectbox('Sản phẩm được hỗ trợ', [None, *items], key=item_key,
                 format_func=lambda value: 'Chọn sản phẩm' if value is None else value,
                 on_change=_change_selection, args=('item_id', item_key))
    if unsupported_item:
        st.warning(UNSUPPORTED_ITEM)
        return
    if item is None:
        st.info('Vui lòng chọn một sản phẩm để xem yêu cầu bổ sung đã công bố.')
        return
    result = normalize_replenishment(read_page(repository.get_replenishment, item, store), context, item, store)
    if result.state == 'empty':
        st.info('Chưa có khuyến nghị bổ sung đã công bố cho sản phẩm–cửa hàng này.')
        return
    if not render_read_state(result):
        return
    row = result.rows[0]
    st.caption(f'Kịch bản: {row["scenario_id"]} · Ngày phát hành: {row["issue_date"]:%d/%m/%Y} · '
               f'Khoảng lập kế hoạch 7 ngày: {context.issue_date + timedelta(days=1):%d/%m/%Y} – '
               f'{context.issue_date + timedelta(days=7):%d/%m/%Y}')
    st.caption(f'Bản công bố: {context.publication_id} · Lần quyết định: {row["decision_run_id"]} · '
               f'Lần dự báo: {row["forecast_run_id"]} · Lần mô hình: {row["model_run_id"]} · '
               f'Phiên bản chính sách: {row["policy_version"]}')
    st.caption(f'Nguồn gốc: kết quả tính toán từ tồn kho mô phỏng ({row["origin_label"]}) · '
               f'Ảnh chụp nguồn: {row["source_snapshot_id"]} · Phiên bản đặc trưng: {row["feature_version"]}')
    shortage = 'Có nguy cơ' if row['forecast_shortage_flag'] else 'Không có nguy cơ'
    cards = [
        ('Tổng dự báo 7 ngày', _format(row['forecast_7d_units'], 1) + ' đơn vị'),
        ('Tồn kho đầu kỳ (mô phỏng)', _format(row['opening_stock_units']) + ' đơn vị'),
        ('Tồn kho an toàn (giả định)', _format(row['safety_stock_units']) + ' đơn vị'),
        ('Tồn kho mục tiêu', _format(row['target_stock_units']) + ' đơn vị'),
        ('Yêu cầu bổ sung (trước phân bổ)', _format(row['request_units']) + ' đơn vị'),
        ('Nguy cơ thiếu hàng theo dự báo', shortage),
    ]
    for offset in (0, 3):
        for column, (label, value) in zip(st.columns(3), cards[offset:offset + 3]):
            column.metric(label, value)
    st.caption('Đơn vị: sản phẩm. Dự báo hiển thị một chữ số thập phân; các quyết định là số nguyên. '
               'Biểu đồ và bảng chi tiết giữ nguyên giá trị đã lưu.')
    st.plotly_chart(inventory_figure(row), width='stretch', config={'displaylogo': False})
    st.caption('Tồn kho mục tiêu là ngưỡng chính sách. Tồn kho an toàn là phần đệm giả định, '
               'không phải một thành phần nhu cầu dự báo để cộng chồng lên biểu đồ.')
    st.subheader('Chi tiết yêu cầu bổ sung đã công bố')
    details = {'Sản phẩm': item, 'Cửa hàng': store,
               'Tổng dự báo 7 ngày (đơn vị)': row['forecast_7d_units'],
               **{label: row[key] for key, label in INTEGER_LABELS.items()},
               'Nguy cơ thiếu hàng theo dự báo': shortage}
    st.dataframe([details], hide_index=True, width='stretch')
    st.subheader('Giải thích kết quả đã công bố')
    st.markdown(f'Tổng số đơn vị bán dự đoán trong 7 ngày là **{_format(row["forecast_7d_units"], 1)}**; '
                f'tồn kho đầu kỳ mô phỏng là **{_format(row["opening_stock_units"])}**; '
                f'tồn kho an toàn giả định là **{_format(row["safety_stock_units"])}**; '
                f'tồn kho mục tiêu là **{_format(row["target_stock_units"])}**. '
                f'Yêu cầu bổ sung đã lưu là **{_format(row["request_units"])} đơn vị**.')
    if row['request_units'] == 0:
        st.info('Không cần yêu cầu bổ sung theo kết quả đã công bố.')
    elif not row['forecast_shortage_flag']:
        st.info('Kết quả đã công bố có yêu cầu bổ sung nhưng không gắn cờ nguy cơ thiếu hàng theo '
                'dự báo. Mục tiêu an toàn là ngưỡng chính sách riêng, không đồng nghĩa với thiếu hàng.')
    st.markdown('Yêu cầu bổ sung có trước phân bổ nguồn cung trung tâm; đây không phải đơn đặt mua '
                'đã phát hành và không bảo đảm được phân bổ hoặc giao hàng. '
                f'Cờ đã lưu: **{shortage}**. Đây là chỉ báo thay thế dựa trên dự báo, '
                'không phải thiếu hàng Walmart đã quan sát hay xác suất thiếu hàng.')

"""Approved seven-day predicted-sales presentation and historical comparison."""
from datetime import date, timedelta
import math

import plotly.graph_objects as go
import streamlit as st

from warehouse_bigdata.application.read_repository import ReadResult
from warehouse_bigdata.application.session_state import PREFIX, set_selection, reconcile_item
from app.views import read_page, UNSUPPORTED_ITEM


KEY = PREFIX + 'forecast.'
UNAVAILABLE = 'Không khả dụng'
MISSING_EVALUATION = 'Chưa có kết quả đánh giá lịch sử phù hợp với bản công bố hiện tại.'


def format_units(value):
    return format(value, ',.1f').replace(',', '_').replace('.', ',').replace('_', '.')


def _numeric(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def normalize_forecast(result, context, item, store):
    """Reject the whole series before any successful presentation output."""
    if result.state != 'ok':
        return result
    if result.provenance != 'predicted' or len(result.rows) != 7:
        return ReadResult('incomplete', provenance='predicted')
    seen = set()
    for row in result.rows:
        horizon = row.get('horizon')
        if (type(horizon) is not int or horizon not in range(1, 8) or horizon in seen
                or row.get('item_id') != item or row.get('store_id') != store
                or type(row.get('forecast_date')) is not date
                or row['forecast_date'] != context.issue_date + timedelta(days=horizon)
                or row.get('forecast_status') != 'ok'
                or not _numeric(row.get('predicted_units'))
                or row.get('forecast_run_id') != context.forecast_run_id
                or row.get('model_run_id') != context.model_run_id
                or row.get('source_snapshot_id') != context.source_snapshot_id
                or row.get('issue_date') != context.issue_date):
            return ReadResult('incomplete', provenance='predicted')
        seen.add(horizon)
    rows = tuple(sorted(result.rows, key=lambda row: row['horizon']))
    if not math.isfinite(sum(row['predicted_units'] for row in rows)):
        return ReadResult('incomplete', provenance='predicted')
    return ReadResult('ok', rows, 'predicted')


def forecast_figure(rows):
    figure = go.Figure(go.Scatter(
        x=[row['forecast_date'] for row in rows],
        y=[row['predicted_units'] for row in rows], mode='lines+markers',
        name='Số đơn vị bán dự đoán',
        hovertemplate='Ngày: %{x|%d/%m/%Y}<br>Số đơn vị bán dự đoán: %{y}<extra></extra>'))
    figure.update_layout(title='Dự báo số đơn vị bán trong bảy ngày', xaxis_title='Ngày',
                         yaxis_title='Số đơn vị bán dự đoán', separators=',.')
    figure.update_xaxes(tickformat='%d/%m/%Y')
    return figure


def compare_metrics(result, context):
    """Keep unmatched or invalid cells unavailable; never synthesize metrics."""
    if result.state != 'ok':
        return result
    try:
        binding = context.evaluation_binding('forecast_metrics')
    except ValueError:
        return ReadResult('unavailable')
    primary = (context.dataset_bindings or {}).get('forecast_daily', {})
    if (result.provenance != 'evaluated' or not binding.get('holdout_id')
            or not binding.get('origin_forecasts')
            or any(not binding.get(key) or binding[key] != primary.get(key) for key in
                   ('source_snapshot_id', 'model_run_id', 'selection_digest', 'dataset_scope'))):
        return ReadResult('unavailable')
    indexed = {}
    for row in result.rows:
        method, metric, horizon = row.get('method_id'), row.get('metric_name'), row.get('horizon')
        start, end = row.get('evaluation_start'), row.get('evaluation_end')
        key = method, metric, horizon
        if (method not in ('random_forest', 'seasonal_naive') or metric not in ('MAE', 'RMSE', 'WAPE')
                or type(horizon) is not int or horizon not in range(8) or key in indexed
                or any(row.get(field) != binding.get(field) for field in
                       ('evaluation_run_id', 'holdout_id', 'run_type', 'model_run_id'))
                or type(start) is not date or type(end) is not date or start > end
                or end > context.issue_date):
            return ReadResult('incomplete')
        indexed[key] = row
    if not indexed:
        return ReadResult('unavailable')
    horizon = min(key[2] for key in indexed)
    output = []
    cohort_fields = ('evaluation_run_id', 'holdout_id', 'run_type', 'evaluation_start',
                     'evaluation_end', 'horizon', 'observation_count', 'sum_actual_units')
    for metric in ('MAE', 'RMSE', 'WAPE'):
        left, right = (indexed.get((method, metric, horizon)) for method in
                       ('random_forest', 'seasonal_naive'))
        comparable = (left is not None and right is not None
                      and all(left.get(key) == right.get(key) for key in cohort_fields)
                      and type(left.get('observation_count')) is int
                      and left['observation_count'] > 0 and _numeric(left.get('sum_actual_units')))
        cells = []
        for row in (left, right):
            value = row.get('metric_value') if row else None
            if not comparable:
                cells.append(UNAVAILABLE)
            elif metric == 'WAPE' and value is None:
                cells.append('Không xác định')
            elif not _numeric(value) or (metric == 'WAPE' and row['sum_actual_units'] == 0):
                cells.append('Không xác định' if metric == 'WAPE' and row['sum_actual_units'] == 0
                             else UNAVAILABLE)
            else:
                cells.append(format_units(value) + ('%' if metric == 'WAPE' else ''))
        output.append(dict(metric=metric, horizon=horizon, **{'Random Forest': cells[0],
                      'Seasonal-naïve': cells[1]}, cohort=left if comparable else None))
    return ReadResult('ok', tuple(output), 'evaluated')


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


def _historical(context, repository):
    from app.views import render_read_state

    st.subheader('Đánh giá mô hình trên dữ liệu lịch sử')
    st.caption('Các chỉ số đo trên nhóm chuỗi lịch sử đã phê duyệt, không phải độ chính xác '
               'riêng của sản phẩm–cửa hàng đang chọn; không đại diện toàn bộ danh mục M5.')
    result = compare_metrics(read_page(repository.get_forecast_metrics), context)
    if result.state != 'ok':
        st.info(MISSING_EVALUATION)
        render_read_state(result)
        return
    binding = context.evaluation_binding('forecast_metrics')
    horizon = result.rows[0]['horizon']
    st.caption('Chân trời đánh giá: ' + ('gộp bảy ngày (0)' if horizon == 0 else f'ngày {horizon}')
               + f' · Phạm vi: {binding["dataset_scope"]} · Mã phạm vi: {binding["selection_digest"]}'
               + f' · Lần đánh giá: {binding["evaluation_run_id"]} · Tập giữ lại: {binding["holdout_id"]}')
    for row in result.rows:
        cohort = row['cohort']
        if cohort:
            st.caption(f'{row["metric"]}: {cohort["evaluation_start"]:%d/%m/%Y} – '
                       f'{cohort["evaluation_end"]:%d/%m/%Y} · '
                       f'Số quan sát: {cohort["observation_count"]} · '
                       f'Tổng số đơn vị bán thực tế: {format_units(cohort["sum_actual_units"])}')
    st.dataframe([{'Chỉ số': row['metric'], 'Random Forest': row['Random Forest'],
                   'Seasonal-naïve': row['Seasonal-naïve']} for row in result.rows],
                 hide_index=True, width='stretch')
    st.caption('MAE: sai số tuyệt đối trung bình (đơn vị sản phẩm). RMSE: căn sai số bình phương '
               'trung bình (đơn vị sản phẩm). WAPE: tỷ lệ tổng sai số tuyệt đối trên tổng số đơn vị '
               'bán thực tế (%); không xác định khi mẫu số bằng không. Ô thiếu hoặc không so sánh '
               'được hiển thị là Không khả dụng; không suy ra mức cải thiện.')


def render_forecast(context, repository, selections):
    from app.views import render_read_state

    st.title('Dự báo nhu cầu')
    st.caption(f'Ngày phát hành: {context.issue_date:%d/%m/%Y} · '
               f'Bản công bố: {context.publication_id} · Lần dự báo: {context.forecast_run_id} · '
               f'Lần mô hình: {context.model_run_id}')
    binding = (context.dataset_bindings or {}).get('forecast_daily', {})
    st.caption(f'M5 Small · Phạm vi: {binding.get("dataset_scope", "Không khả dụng")} · '
               f'Mã phạm vi: {binding.get("selection_digest", "Không khả dụng")}. '
               'Mẫu đã phê duyệt không đại diện toàn bộ danh mục M5.')
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
        st.info('Vui lòng chọn một sản phẩm để xem dự báo bảy ngày.')
        return
    result = normalize_forecast(read_page(repository.get_forecast_series, item, store), context, item, store)
    if result.state != 'ok':
        st.warning('Chưa có đủ bảy ngày dự báo hợp lệ liên tiếp; không hiển thị kết quả một phần.')
        render_read_state(result)
        return
    rows = result.rows
    total = sum(row['predicted_units'] for row in rows)
    a, b, c = st.columns(3)
    a.metric('Tổng số đơn vị bán dự đoán', format_units(total))
    b.metric('Trung bình số đơn vị bán dự đoán mỗi ngày', format_units(total / 7))
    c.metric('Số ngày dự báo hợp lệ', '7/7')
    st.caption('Số ngày dự báo hợp lệ: 7/7 · Hiển thị một chữ số thập phân; tính toán và biểu đồ '
               'giữ nguyên độ chính xác của dữ liệu.')
    st.info('Dự báo là số đơn vị bán dự đoán của mô hình, không phải doanh số quan sát, '
            'đơn hàng hay tồn kho thực tế; không bảo đảm kết quả tương lai.')
    st.plotly_chart(forecast_figure(rows), width='stretch', config={'displaylogo': False})
    st.subheader('Chi tiết dự báo bảy ngày')
    st.dataframe([{'Ngày': row['forecast_date'].strftime('%d/%m/%Y'), 'Chân trời (ngày)': row['horizon'],
                   'Sản phẩm': item, 'Cửa hàng': store,
                   'Số đơn vị bán dự đoán': row['predicted_units']} for row in rows],
                 column_config={'Số đơn vị bán dự đoán': st.column_config.NumberColumn(format='%.1f',
                                help='Số đơn vị bán dự đoán; giá trị gốc giữ nguyên độ chính xác.')},
                 hide_index=True, width='stretch')
    st.caption('Giá trị theo ngày (một chữ số thập phân): '
               + ' · '.join(f'{row["forecast_date"]:%d/%m}: {format_units(row["predicted_units"])}'
                            for row in rows))
    _historical(context, repository)

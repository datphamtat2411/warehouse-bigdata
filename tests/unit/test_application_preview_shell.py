"""Synthetic preview presentation checks without M5 files or external services."""
from pathlib import Path
from unittest.mock import Mock

from streamlit.testing.v1 import AppTest

from app import preview_shell
from tests.unit.test_application_overview import repository
from tests.unit.test_application_read_repository import context


ENTRYPOINT = Path(__file__).resolve().parents[2] / 'app' / 'preview_main.py'


def test_preview_has_compact_source_context_and_refresh(monkeypatch):
    repo = repository()
    repo.context = context()
    loader = Mock(return_value=repo)
    monkeypatch.setattr(preview_shell, 'checked_manifest', lambda _: (
        Path('/unused'), {}, {'selection_digest': 'synthetic-scope'}, 'digest'))
    monkeypatch.setattr(preview_shell, '_load_cached', loader)
    at = AppTest.from_file(ENTRYPOINT).run()

    assert not at.exception and not at.error
    assert at.title[0].value == 'Tổng quan'
    assert at.sidebar.radio[0].options == [
        'Tổng quan', 'Dự báo nhu cầu', 'Tồn kho', 'Phân bổ hàng']
    source = next(el for el in at.sidebar.expander if el.label == 'Thông tin dữ liệu')
    assert not source.proto.expanded
    text = '\n'.join(el.value for el in list(at.sidebar.markdown) + list(at.sidebar.caption))
    assert 'M5 Small' in text and 'mô phỏng' in text
    assert 'trong bộ nhớ' in text and 'synthetic-scope' in text
    assert 'DEMO MOCK' not in text and '**J07:**' not in text and '**J08:**' not in text
    repo.get_store_trend.assert_called_once()

    at.sidebar.button[0].click().run()
    assert not at.exception
    loader.clear.assert_called_once()

"""Restrained shared styling for the native Streamlit analytical interface."""

import streamlit as st


def apply_style():
    """Style native components without replacing their controls or focus behavior."""
    st.markdown(
        """
<style>
    [data-testid="stAppViewContainer"] { background: #edf2f6; }
    .stMainBlockContainer {
        max-width: 1480px;
        padding-top: 2.4rem;
        padding-bottom: 3rem;
    }
    [data-testid="stSidebar"] {
        background: #e1e9ef;
        border-right: 1px solid #bcccd8;
    }
    .st-key-overview_filters {
        background: #ffffff;
        border-color: #bdcdd8 !important;
        box-shadow: 0 2px 6px rgba(32, 53, 71, 0.05);
    }
    [data-testid="stPlotlyChart"] {
        background: #ffffff;
        border: 1px solid #c4d1dc;
        border-radius: 8px;
        padding: 0.5rem;
        box-shadow: 0 2px 6px rgba(32, 53, 71, 0.05);
    }
    h1 { font-size: 2rem !important; letter-spacing: -0.04em; }
    h2 { font-size: 1.25rem !important; letter-spacing: -0.02em; }
    h3 { font-size: 1.05rem !important; }
    [data-testid="stCaptionContainer"], [data-testid="stCaptionContainer"] p {
        color: #526778;
    }
    [data-testid="stMetric"] {
        background: #e0eeeb;
        border: 1px solid #a9c9c2;
        border-left: 4px solid #16756d;
        border-radius: 8px;
        padding: 1rem 1.2rem;
    }
    [data-testid="stMetricValue"] {
        font-variant-numeric: tabular-nums;
        font-size: 1.85rem;
        font-weight: 600;
    }
    [data-testid="stMetricLabel"] { color: #34554f; }
    [data-testid="stExpander"] details {
        background: #ffffff;
        border-color: #bdcdd8;
        border-radius: 8px;
    }
    [data-testid="stExpander"] summary { background: #e5ecf2; }
    [data-testid="stSidebar"] [role="radiogroup"] { gap: 0.35rem; }
    [data-testid="stSidebar"] [role="radiogroup"] label {
        padding: 0.55rem 0.65rem;
        border-radius: 6px;
    }
    [data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {
        background: #c7dfd9;
        color: #104f49;
        box-shadow: inset 3px 0 #16756d;
    }
    .dashboard-identity { border-bottom: 1px solid #b8c9d5; padding: 0 0 1.3rem; }
    .dashboard-identity small {
        color: #16756d; font-size: 0.7rem; font-weight: 700; letter-spacing: 0.12em;
    }
    .dashboard-identity strong {
        display: block; margin: 0.5rem 0; font-size: 1.2rem; line-height: 1.4;
    }
    .dashboard-identity p { color: #526778; font-size: 0.8rem; margin: 0; }
    @media (max-width: 640px) {
        .stMainBlockContainer { padding-top: 1.5rem; padding-inline: 1rem; }
        h1 { font-size: 1.7rem !important; }
        [data-testid="stMetric"] { padding: 0.75rem 1rem; }
    }
</style>
""",
        unsafe_allow_html=True,
    )


def sidebar_identity():
    st.sidebar.markdown(
        """
<div class="dashboard-identity">
    <small>PHÂN TÍCH DỮ LIỆU M5</small>
    <strong>Hỗ trợ quyết định<br>quản lý tồn kho</strong>
    <p>Bán hàng · Dự báo · Phân bổ</p>
</div>
""",
        unsafe_allow_html=True,
    )

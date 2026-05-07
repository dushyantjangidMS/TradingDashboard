"""
Trading Master Dashboard — Entry Point
========================================
Multi-page Streamlit application combining:
  1. Bhavcopy Downloader
  2. PNL Calculator (with auto-fetch)
  3. Excel Merger
  4. VAR Cost Calculator
  5. Hedge Summary Calculator

Run:
    cd TradingDashboard
    streamlit run main_app.py
"""

import streamlit as st

# ──────────────────────────────────────────────────────────────────────
# Page configuration
# ──────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Trading Master Dashboard",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────────────────────────────
# Global CSS — sidebar styling + branding cleanup
# ──────────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* ── Force sidebar always visible ── */
    section[data-testid="stSidebar"] {
        min-width: 280px !important;
        max-width: 320px !important;
        width: 300px !important;
        transform: none !important;
        position: relative !important;
        transition: none !important;
    }

    /* ── Hide collapse / expand buttons ── */
    section[data-testid="stSidebar"] button[data-testid="stBaseButton-headerNoPadding"] {
        display: none !important;
    }
    button[data-testid="stExpandSidebarButton"],
    button[data-testid="stSidebarCollapseButton"],
    button[data-testid="stSidebarCollapsedControl"],
    [data-testid="collapsedControl"] {
        display: none !important;
    }

    /* ── Sidebar content always visible ── */
    section[data-testid="stSidebar"] > div:first-child {
        width: 100% !important;
    }

    /* ── Sidebar gradient ── */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f0c29 0%, #1a1a2e 50%, #16213e 100%) !important;
        border-right: 1px solid rgba(255,255,255,0.06) !important;
    }

    /* ── Hide default Streamlit branding & Deploy button ── */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    button[data-testid="stBaseButton-header"] {display: none !important;}

    /* ── Dividers ── */
    hr {
        border: none;
        border-top: 1px solid rgba(255,255,255,0.06);
        margin: 1.5rem 0;
    }
    </style>

    <script>
    // Continuously enforce: remove collapse/expand buttons & keep sidebar open
    const fixSidebar = () => {
        const sidebar = document.querySelector('section[data-testid="stSidebar"]');
        if (sidebar) {
            sidebar.querySelectorAll('button[data-testid="stBaseButton-headerNoPadding"]').forEach(btn => {
                btn.remove();
            });
            sidebar.style.transform = 'none';
            sidebar.style.width = '300px';
            sidebar.style.minWidth = '280px';
            sidebar.style.position = 'relative';
        }
        document.querySelectorAll('button[data-testid="stExpandSidebarButton"]').forEach(btn => {
            btn.remove();
        });
    };
    fixSidebar();
    const observer = new MutationObserver(fixSidebar);
    observer.observe(document.body, { childList: true, subtree: true });
    </script>
    """,
    unsafe_allow_html=True,
)

# ──────────────────────────────────────────────────────────────────────
# Multi-page navigation
# ──────────────────────────────────────────────────────────────────────
bhavcopy_page = st.Page(
    "apps/bhavcopy.py", title="Bhavcopy Downloader", icon="📈", default=True
)
pnl_page = st.Page(
    "apps/pnl_calculator.py", title="PNL Calculator", icon="💰"
)
excel_merger_page = st.Page(
    "apps/excel_merger.py", title="Excel Merger", icon="📊"
)
var_cost_page = st.Page(
    "apps/var_cost.py", title="VAR Cost Calculator", icon="📉"
)
hedge_summary_page = st.Page(
    "apps/hedge_summary.py", title="Hedge Summary", icon="🛡️"
)
daily_data_page = st.Page(
    "apps/daily_data.py", title="Daily Data Manager", icon="📅"
)

pg = st.navigation([bhavcopy_page, pnl_page, excel_merger_page, var_cost_page, hedge_summary_page, daily_data_page])
pg.run()

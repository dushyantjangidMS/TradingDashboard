"""
PNL Calculator — Streamlit Page
================================
Unified PNL dashboard that automatically downloads bhavcopies
(or accepts manual uploads) and computes Realized + Settlement PNL.

Settlement scope:
  • NFO → NIFTY only
  • BFO → SENSEX only

Tabs:
  1. Full PNL Calculation (with auto-fetch integration)
  2. Portfolio Exit Analysis
"""

import base64
import logging
from datetime import datetime

import pandas as pd
import streamlit as st

from utils.bhavcopy_provider import (
    auto_fetch_bfo_settlement,
    auto_fetch_nfo_settlement,
    process_uploaded_bfo_bhavcopy,
    process_uploaded_nfo_bhavcopy,
)
from utils.date_parser import extract_date_from_filename, format_date_display
from utils.expiry_logic import (
    DEFAULT_NSE_HOLIDAYS_2026,
    compute_bse_expiry,
    compute_nse_expiry,
)
from utils.pnl_engine import (
    build_user_summary,
    enrich_positions_with_pnl,
    parse_summary_file,
    process_portfolio_data,
    styled_excel_bytes,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────
# CSS
# ──────────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    /* ───── Hero banner ───── */
    .pnl-hero {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f172a 100%);
        padding: 2rem 2.5rem;
        border-radius: 16px;
        margin-bottom: 1.6rem;
        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
        border: 1px solid rgba(99,102,241,0.15);
    }
    .pnl-hero h1 {
        color: #ffffff;
        font-size: 1.9rem;
        font-weight: 700;
        margin: 0;
        letter-spacing: -0.5px;
    }
    .pnl-hero p {
        color: #94a3b8;
        font-size: 1rem;
        margin: 0.4rem 0 0 0;
    }

    /* ───── Metric cards ───── */
    .pnl-metric {
        background: linear-gradient(135deg, #1e1e2f 0%, #2d2b55 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 14px;
        padding: 1.3rem 1.5rem;
        margin-bottom: 0.8rem;
        box-shadow: 0 4px 20px rgba(0,0,0,0.15);
        transition: transform 0.2s ease;
        text-align: center;
    }
    .pnl-metric:hover {
        transform: translateY(-2px);
    }
    .pnl-metric .label {
        color: #9e9cc2;
        font-size: 0.82rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-bottom: 0.3rem;
    }
    .pnl-metric .value {
        font-size: 1.5rem;
        font-weight: 700;
    }

    /* ───── Mode selector card ───── */
    .mode-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 1px solid rgba(99,102,241,0.2);
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        margin-bottom: 1rem;
    }
    .mode-card .mode-title {
        color: #e2e8f0;
        font-size: 0.95rem;
        font-weight: 600;
        margin-bottom: 0.5rem;
    }
    .mode-card .mode-desc {
        color: #94a3b8;
        font-size: 0.82rem;
    }

    /* ───── Log panel ───── */
    .pnl-log-panel {
        background: #0d1117;
        border: 1px solid #21262d;
        border-radius: 12px;
        padding: 1rem 1.2rem;
        font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace;
        font-size: 0.8rem;
        line-height: 1.65;
        color: #c9d1d9;
        max-height: 300px;
        overflow-y: auto;
        white-space: pre-wrap;
        word-break: break-word;
    }
    .pnl-log-panel .log-ok   { color: #3fb950; }
    .pnl-log-panel .log-err  { color: #f85149; }
    .pnl-log-panel .log-nfo  { color: #58a6ff; }
    .pnl-log-panel .log-warn { color: #d29922; }

    /* ───── Section titles ───── */
    .pnl-section {
        font-size: 1.1rem;
        font-weight: 700;
        color: #e2e0f0;
        border-left: 4px solid #6366f1;
        padding-left: 0.8rem;
        margin: 1.4rem 0 0.8rem 0;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────
def _download_link(file_bytes: bytes, filename: str, link_text: str, mime: str) -> str:
    b64 = base64.b64encode(file_bytes).decode()
    return f'<a href="data:{mime};base64,{b64}" download="{filename}">{link_text}</a>'


def _render_pnl_logs(log_lines: list[str]):
    """Render fetch/processing logs in a styled panel."""
    if not log_lines:
        return
    rendered = []
    for line in log_lines:
        if "✅" in line:
            rendered.append(f'<span class="log-ok">{line}</span>')
        elif "❌" in line:
            rendered.append(f'<span class="log-err">{line}</span>')
        elif "⚠" in line:
            rendered.append(f'<span class="log-warn">{line}</span>')
        else:
            rendered.append(f'<span class="log-nfo">{line}</span>')
    html = '<div class="pnl-log-panel">' + "<br>".join(rendered) + "</div>"
    st.markdown(html, unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────
st.markdown(
    '<div class="pnl-hero">'
    "  <h1>💰 PNL Calculator</h1>"
    "  <p>Auto-fetch bhavcopies &amp; compute Realized + Settlement PNL "
    "across all users in one click</p>"
    "</div>",
    unsafe_allow_html=True,
)

tab1, tab2 = st.tabs(["📊 Full PNL Calculation", "📂 Portfolio Exit Analysis"])


# ══════════════════════════════════════════════════════════════════════
# TAB 1: PNL Calculation
# ══════════════════════════════════════════════════════════════════════
with tab1:
    # ── Upload files ─────────────────────────────────────────────────
    st.markdown('<div class="pnl-section">📁 Step 1 — Upload Files</div>', unsafe_allow_html=True)
    col_upload1, col_upload2 = st.columns(2)
    with col_upload1:
        positions_file = st.file_uploader(
            "Positions CSV (all users)",
            type="csv",
            help="Upload the consolidated positions CSV containing all users",
            key="pnl_positions",
        )
    with col_upload2:
        summary_upload = st.file_uploader(
            "Summary File (optional)",
            type=["xlsx", "xls"],
            help="Upload the summary Excel file to include Allocation & MTM per user",
            key="pnl_summary_upload",
        )

    # ── Auto-detect trade date from uploaded filename ─────────────────
    holidays = DEFAULT_NSE_HOLIDAYS_2026

    if positions_file is not None:
        detected_dt, detect_msg = extract_date_from_filename(positions_file.name)
        if detected_dt:
            detected_date = detected_dt.date()
            # Only update when a NEW file is uploaded (prevent infinite rerun)
            prev_filename = st.session_state.get("_pnl_last_filename")
            if prev_filename != positions_file.name:
                st.session_state["_pnl_last_filename"] = positions_file.name
                # Update trade date (auto mode)
                st.session_state["pnl_trade_date"] = detected_date
                # Sync expiry dates for auto mode
                _td = datetime.combine(detected_date, datetime.min.time())
                nfo_exp = compute_nse_expiry(_td, holidays).date()
                bfo_exp = compute_bse_expiry(_td, holidays).date()
                st.session_state["pnl_nfo_exp"] = nfo_exp
                st.session_state["pnl_bfo_exp"] = bfo_exp
                # Sync expiry dates for manual mode too
                st.session_state["pnl_nfo_exp_manual"] = nfo_exp
                st.session_state["pnl_bfo_exp_manual"] = bfo_exp
                # Auto-untick if trade date IS the expiry day
                st.session_state["pnl_inc_nfo"] = (detected_date != nfo_exp)
                st.session_state["pnl_inc_bfo"] = (detected_date != bfo_exp)
                st.rerun()
            st.success(f"📅 Detected trade date from filename: **{format_date_display(detected_dt)}**")
        else:
            st.caption(f"⚠️ Could not detect date from filename — select manually below.")
    else:
        # Reset tracking when file is removed
        st.session_state.pop("_pnl_last_filename", None)

    # ── Bhavcopy mode ────────────────────────────────────────────────
    st.markdown('<div class="pnl-section">⚙️ Step 2 — Bhavcopy Source</div>', unsafe_allow_html=True)

    bhavcopy_mode = st.radio(
        "How should bhavcopies be loaded?",
        ["🚀 Auto-fetch from NSE / BSE", "📁 Manual upload"],
        horizontal=True,
        label_visibility="collapsed",
        key="pnl_bhav_mode",
    )

    auto_mode = bhavcopy_mode == "🚀 Auto-fetch from NSE / BSE"

    if auto_mode:
        st.markdown(
            '<div class="mode-card">'
            '  <div class="mode-title">🚀 Auto-Fetch Mode</div>'
            '  <div class="mode-desc">Bhavcopies will be downloaded automatically from '
            "NSE &amp; BSE websites. Trade date is auto-detected from the filename. "
            "Expiry dates update automatically. Settlement is auto-disabled on expiry days.</div>"
            "</div>",
            unsafe_allow_html=True,
        )

    # ── Callback: sync expiry dates + auto-untick on expiry day ──────
    def _sync_expiry_on_trade_date_change():
        """Called when the trade date widget value changes."""
        td = st.session_state.pnl_trade_date
        _td = datetime.combine(td, datetime.min.time())
        nfo_exp = compute_nse_expiry(_td, holidays).date()
        bfo_exp = compute_bse_expiry(_td, holidays).date()
        st.session_state["pnl_nfo_exp"] = nfo_exp
        st.session_state["pnl_bfo_exp"] = bfo_exp
        # Auto-untick if trade date IS the expiry day (no bhavcopy needed)
        st.session_state["pnl_inc_nfo"] = (td != nfo_exp)
        st.session_state["pnl_inc_bfo"] = (td != bfo_exp)

    # ── Date / Expiry controls ───────────────────────────────────────
    if auto_mode:
        col_d1, col_d2, col_d3 = st.columns(3)
        with col_d1:
            trade_date_input = st.date_input(
                "Trade Date",
                value=datetime.now().date(),
                key="pnl_trade_date",
                on_change=_sync_expiry_on_trade_date_change,
            )

        # Ensure expiry keys + auto-untick exist for first render
        if "pnl_nfo_exp" not in st.session_state:
            _td = datetime.combine(trade_date_input, datetime.min.time())
            nfo_exp = compute_nse_expiry(_td, holidays).date()
            bfo_exp = compute_bse_expiry(_td, holidays).date()
            st.session_state["pnl_nfo_exp"] = nfo_exp
            st.session_state["pnl_bfo_exp"] = bfo_exp
            st.session_state["pnl_inc_nfo"] = (trade_date_input != nfo_exp)
            st.session_state["pnl_inc_bfo"] = (trade_date_input != bfo_exp)

        with col_d2:
            include_nfo = st.checkbox(
                "Include NFO Settlement (NIFTY)", value=True, key="pnl_inc_nfo"
            )
            expiry_nfo = st.date_input(
                "NFO Expiry",
                key="pnl_nfo_exp",
                disabled=not include_nfo,
            )
        with col_d3:
            include_bfo = st.checkbox(
                "Include BFO Settlement (SENSEX)", value=True, key="pnl_inc_bfo"
            )
            expiry_bfo = st.date_input(
                "BFO Expiry",
                key="pnl_bfo_exp",
                disabled=not include_bfo,
            )

        # Show info when auto-unticked
        if trade_date_input == st.session_state.get("pnl_nfo_exp"):
            st.info("ℹ️ NFO settlement auto-disabled — today is NSE expiry day.")
        if trade_date_input == st.session_state.get("pnl_bfo_exp"):
            st.info("ℹ️ BFO settlement auto-disabled — today is BSE expiry day.")

        # No file uploaders needed
        nfo_bhav_file = None
        bfo_bhav_file = None

    else:
        # Manual mode — checkboxes above file uploaders + expiry dates
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            include_nfo = st.checkbox(
                "Include NFO Settlement (NIFTY)", value=True, key="pnl_inc_nfo"
            )
            nfo_bhav_file = (
                st.file_uploader("NFO Bhavcopy CSV", type="csv", key="pnl_nfo_file")
                if include_nfo
                else None
            )
            if not include_nfo:
                st.info("NFO settlement disabled — no file needed.")
        with col_f2:
            include_bfo = st.checkbox(
                "Include BFO Settlement (SENSEX)", value=True, key="pnl_inc_bfo"
            )
            bfo_bhav_file = (
                st.file_uploader("BFO Bhavcopy CSV", type="csv", key="pnl_bfo_file")
                if include_bfo
                else None
            )
            if not include_bfo:
                st.info("BFO settlement disabled — no file needed.")

        # Ensure manual expiry keys exist for first render
        if "pnl_nfo_exp_manual" not in st.session_state:
            st.session_state["pnl_nfo_exp_manual"] = datetime.now().date()
        if "pnl_bfo_exp_manual" not in st.session_state:
            st.session_state["pnl_bfo_exp_manual"] = datetime.now().date()

        col_e1, col_e2 = st.columns(2)
        with col_e1:
            expiry_nfo = st.date_input(
                "NFO Expiry Date",
                key="pnl_nfo_exp_manual",
                disabled=not include_nfo,
            )
        with col_e2:
            expiry_bfo = st.date_input(
                "BFO Expiry Date",
                key="pnl_bfo_exp_manual",
                disabled=not include_bfo,
            )

    # ── Process button ───────────────────────────────────────────────
    st.markdown('<div class="pnl-section">🚀 Step 3 — Process</div>', unsafe_allow_html=True)
    process_btn = st.button(
        "⚡ Process All Users PNL",
        type="primary",
        use_container_width=True,
        key="pnl_process",
    )

    if process_btn:
        # ── Validation ───────────────────────────────────────────────
        if not positions_file:
            st.error("❌ Upload the positions file first.")
        elif not auto_mode and include_nfo and not nfo_bhav_file:
            st.error("❌ NFO settlement enabled but no NFO bhavcopy file uploaded.")
        elif not auto_mode and include_bfo and not bfo_bhav_file:
            st.error("❌ BFO settlement enabled but no BFO bhavcopy file uploaded.")
        else:
            all_logs: list[str] = []

            try:
                positions_file.seek(0)
                positions_df = pd.read_csv(positions_file)

                with st.spinner("Processing…"):
                    # ── Get NFO settlement data ──────────────────────
                    nfo_bhav_df = None
                    if include_nfo:
                        if auto_mode:
                            trade_dt = datetime.combine(trade_date_input, datetime.min.time())
                            nfo_bhav_df, nfo_logs = auto_fetch_nfo_settlement(
                                trade_dt, expiry_nfo
                            )
                            all_logs.extend(nfo_logs)
                        else:
                            nfo_bhav_df, nfo_logs = process_uploaded_nfo_bhavcopy(
                                nfo_bhav_file, expiry_nfo
                            )
                            all_logs.extend(nfo_logs)

                    # ── Get BFO settlement data ──────────────────────
                    bfo_bhav_df = None
                    if include_bfo:
                        if auto_mode:
                            trade_dt = datetime.combine(trade_date_input, datetime.min.time())
                            bfo_bhav_df, bfo_logs = auto_fetch_bfo_settlement(
                                trade_dt, expiry_bfo
                            )
                            all_logs.extend(bfo_logs)
                        else:
                            bfo_bhav_df, bfo_logs = process_uploaded_bfo_bhavcopy(
                                bfo_bhav_file, expiry_bfo
                            )
                            all_logs.extend(bfo_logs)

                    # ── Calculate PNL ────────────────────────────────
                    updated_positions_df = enrich_positions_with_pnl(
                        positions_df,
                        nfo_bhav_df=nfo_bhav_df,
                        bfo_bhav_df=bfo_bhav_df,
                        include_settlement_nfo=include_nfo,
                        include_settlement_bfo=include_bfo,
                    )
                    # ── Parse optional summary file ──────────────
                    summary_data = None
                    if summary_upload is not None:
                        summary_data, summary_logs = parse_summary_file(summary_upload)
                        all_logs.extend(summary_logs)

                    summary_df = build_user_summary(updated_positions_df, summary_data=summary_data)

                # ── Fetch / Processing Logs ──────────────────────────
                if all_logs:
                    with st.expander("📋 Bhavcopy Fetch Logs", expanded=False):
                        _render_pnl_logs(all_logs)

                st.success("✅ All users PNL processed successfully!")

                # ── Metric cards ─────────────────────────────────────
                total_realized = float(summary_df["Total Realized"].sum()) if not summary_df.empty else 0.0
                total_settlement = float(summary_df["Total Settlement"].sum()) if not summary_df.empty else 0.0
                grand_total = float(summary_df["Grand Total"].sum()) if not summary_df.empty else 0.0
                total_users = int(summary_df["UserID"].nunique()) if not summary_df.empty else 0

                m1, m2, m3, m4 = st.columns(4)
                with m1:
                    st.markdown(
                        f'<div class="pnl-metric">'
                        f'  <div class="label">Users</div>'
                        f'  <div class="value" style="color:#60a5fa">{total_users}</div>'
                        f"</div>",
                        unsafe_allow_html=True,
                    )
                with m2:
                    color = "#4ade80" if total_realized >= 0 else "#f87171"
                    st.markdown(
                        f'<div class="pnl-metric">'
                        f'  <div class="label">Total Realized</div>'
                        f'  <div class="value" style="color:{color}">₹{total_realized:,.2f}</div>'
                        f"</div>",
                        unsafe_allow_html=True,
                    )
                with m3:
                    color = "#4ade80" if total_settlement >= 0 else "#f87171"
                    st.markdown(
                        f'<div class="pnl-metric">'
                        f'  <div class="label">Total Settlement</div>'
                        f'  <div class="value" style="color:{color}">₹{total_settlement:,.2f}</div>'
                        f"</div>",
                        unsafe_allow_html=True,
                    )
                with m4:
                    color = "#4ade80" if grand_total >= 0 else "#f87171"
                    st.markdown(
                        f'<div class="pnl-metric">'
                        f'  <div class="label">Grand Total</div>'
                        f'  <div class="value" style="color:{color}">₹{grand_total:,.2f}</div>'
                        f"</div>",
                        unsafe_allow_html=True,
                    )

                # ── Tables ───────────────────────────────────────────
                st.markdown("### 📈 User-wise PNL Summary")
                st.dataframe(summary_df, use_container_width=True)

                st.markdown("### 📋 Updated Positions (All Users)")
                st.dataframe(updated_positions_df, use_container_width=True)

                # ── Downloads ────────────────────────────────────────
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

                summary_excel = styled_excel_bytes(summary_df, "Summary")
                summary_csv = summary_df.to_csv(index=False).encode()
                positions_excel = styled_excel_bytes(updated_positions_df, "Updated Positions")
                positions_csv = updated_positions_df.to_csv(index=False).encode()

                c1, c2 = st.columns(2)
                with c1:
                    st.markdown(
                        _download_link(
                            summary_excel,
                            f"all_users_pnl_summary_{timestamp}.xlsx",
                            "📥 Download PNL Summary (Excel)",
                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        ),
                        unsafe_allow_html=True,
                    )
                    st.markdown(
                        _download_link(
                            summary_csv,
                            f"all_users_pnl_summary_{timestamp}.csv",
                            "📥 Download PNL Summary (CSV)",
                            "text/csv",
                        ),
                        unsafe_allow_html=True,
                    )
                with c2:
                    st.markdown(
                        _download_link(
                            positions_excel,
                            f"updated_positions_all_users_{timestamp}.xlsx",
                            "📥 Download Updated Positions (Excel)",
                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        ),
                        unsafe_allow_html=True,
                    )
                    st.markdown(
                        _download_link(
                            positions_csv,
                            f"updated_positions_all_users_{timestamp}.csv",
                            "📥 Download Updated Positions (CSV)",
                            "text/csv",
                        ),
                        unsafe_allow_html=True,
                    )

            except Exception as e:
                logger.exception("Error during PNL processing")
                st.error(f"❌ Error during processing: {e}")


# ══════════════════════════════════════════════════════════════════════
# TAB 2: Portfolio Exit Analysis
# ══════════════════════════════════════════════════════════════════════
with tab2:
    st.markdown(
        '<div class="pnl-hero">'
        "  <h1>📂 Portfolio Exit Analysis</h1>"
        "  <p>Analyze portfolio exit reasons and timestamps from "
        "GridLog + Summary files</p>"
        "</div>",
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div class="pnl-section">📁 Upload Files</div>',
        unsafe_allow_html=True,
    )
    st.info("Upload the GridLog and Summary files, then click Process.")

    col_grid, col_summary = st.columns(2)
    with col_grid:
        gridlog_file = st.file_uploader(
            "GridLog File", type=["csv", "xlsx"], key="pnl_gridlog"
        )
    with col_summary:
        summary_file = st.file_uploader(
            "Summary Excel File", type="xlsx", key="pnl_summary_file"
        )

    if st.button(
        "⚡ Process Portfolio Data",
        type="primary",
        use_container_width=True,
        key="pnl_portfolio_btn",
    ):
        if gridlog_file and summary_file:
            try:
                with st.spinner("Processing portfolio data…"):
                    final_df, output_filename = process_portfolio_data(
                        gridlog_file, summary_file
                    )
                st.success("✅ Done!")
                st.dataframe(final_df, use_container_width=True)
                st.markdown(
                    _download_link(
                        final_df.to_csv(index=False).encode(),
                        output_filename,
                        f"📥 Download {output_filename}",
                        "text/csv",
                    ),
                    unsafe_allow_html=True,
                )
            except Exception as e:
                st.error(f"❌ Error: {e}")
        else:
            st.error("Please upload both files.")

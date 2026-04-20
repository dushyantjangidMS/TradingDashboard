"""
Bhavcopy Analyzer — Streamlit Page
====================================
Download, analyze, and display NSE & BSE derivative Bhavcopy data
with automatic expiry-date calculation.
"""

import io
import zipfile
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

from utils.date_parser import extract_date_from_filename, format_date_display
from utils.bhavcopy_fetcher import fetch_bse_bhavcopy, fetch_nse_bhavcopy
from utils.expiry_logic import (
    DEFAULT_NSE_HOLIDAYS_2026,
    compute_bse_expiry,
    compute_nse_expiry,
    validate_trading_day,
)


# ──────────────────────────────────────────────────────────────────────
# Custom CSS
# ──────────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    /* ───── Header banner ───── */
    .hero-banner {
        background: linear-gradient(135deg, #0f0c29 0%, #302b63 50%, #24243e 100%);
        padding: 2.2rem 2.5rem;
        border-radius: 16px;
        margin-bottom: 1.8rem;
        box-shadow: 0 8px 32px rgba(0,0,0,0.25);
    }
    .hero-banner h1 {
        color: #ffffff;
        font-size: 2rem;
        font-weight: 700;
        margin: 0;
        letter-spacing: -0.5px;
    }
    .hero-banner p {
        color: #b8b5d4;
        font-size: 1.05rem;
        margin: 0.4rem 0 0 0;
    }

    /* ───── Metric cards ───── */
    .bv-metric-card {
        background: linear-gradient(135deg, #1e1e2f 0%, #2d2b55 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 14px;
        padding: 1.4rem 1.6rem;
        margin-bottom: 1rem;
        box-shadow: 0 4px 20px rgba(0,0,0,0.15);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .bv-metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 30px rgba(0,0,0,0.25);
    }
    .bv-metric-card .label {
        color: #9e9cc2;
        font-size: 0.82rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-bottom: 0.35rem;
    }
    .bv-metric-card .value {
        color: #ffffff;
        font-size: 1.35rem;
        font-weight: 700;
    }
    .bv-metric-card .value.success { color: #4ade80; }
    .bv-metric-card .value.info    { color: #60a5fa; }
    .bv-metric-card .value.warn    { color: #fbbf24; }

    /* ───── Log panel ───── */
    .log-panel {
        background: #0d1117;
        border: 1px solid #21262d;
        border-radius: 12px;
        padding: 1.2rem 1.4rem;
        font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace;
        font-size: 0.82rem;
        line-height: 1.7;
        color: #c9d1d9;
        max-height: 360px;
        overflow-y: auto;
        white-space: pre-wrap;
        word-break: break-word;
    }
    .log-panel .log-success { color: #3fb950; }
    .log-panel .log-error   { color: #f85149; }
    .log-panel .log-info    { color: #58a6ff; }
    .log-panel .log-warn    { color: #d29922; }

    /* ───── Section titles ───── */
    .bv-section-title {
        font-size: 1.15rem;
        font-weight: 700;
        color: #e2e0f0;
        border-left: 4px solid #7c3aed;
        padding-left: 0.8rem;
        margin: 1.6rem 0 0.8rem 0;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ──────────────────────────────────────────────────────────────────────
# Session-state helpers
# ──────────────────────────────────────────────────────────────────────
def _init_state():
    defaults = {
        "bv_logs": [],
        "bv_nse_df": None,
        "bv_bse_df": None,
        "bv_nse_expiry": None,
        "bv_bse_expiry": None,
        "bv_trade_date": None,
        "bv_global_skip": None,
        "bv_nse_skip": None,
        "bv_bse_skip": None,
        "bv_holidays": DEFAULT_NSE_HOLIDAYS_2026.copy(),
        "bv_active_mode": None,
        "bv_bulk_zip": None,
        "bv_bulk_zip_name": None,
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


def _clear_results():
    st.session_state.bv_logs = []
    st.session_state.bv_nse_df = None
    st.session_state.bv_bse_df = None
    st.session_state.bv_nse_expiry = None
    st.session_state.bv_bse_expiry = None
    st.session_state.bv_trade_date = None
    st.session_state.bv_global_skip = None
    st.session_state.bv_nse_skip = None
    st.session_state.bv_bse_skip = None
    st.session_state.bv_bulk_zip = None
    st.session_state.bv_bulk_zip_name = None


def add_log(msg: str, level: str = "info"):
    ts = datetime.now().strftime("%H:%M:%S")
    st.session_state.bv_logs.append((ts, level, msg))


def render_logs():
    if not st.session_state.bv_logs:
        return
    css_map = {"success": "log-success", "error": "log-error", "info": "log-info", "warn": "log-warn"}
    lines = []
    for ts, level, msg in st.session_state.bv_logs:
        css = css_map.get(level, "log-info")
        icon = {"success": "✅", "error": "❌", "info": "ℹ️", "warn": "⚠️"}.get(level, "•")
        lines.append(f'<span class="{css}">[{ts}] {icon} {msg}</span>')
    html = '<div class="log-panel">' + "<br>".join(lines) + "</div>"
    st.markdown(html, unsafe_allow_html=True)


def metric_card(label: str, value: str, style: str = "info") -> str:
    return (
        f'<div class="bv-metric-card">'
        f'  <div class="label">{label}</div>'
        f'  <div class="value {style}">{value}</div>'
        f"</div>"
    )


# ──────────────────────────────────────────────────────────────────────
# Processing logic
# ──────────────────────────────────────────────────────────────────────
def process_date(trade_date: datetime, holidays: list[str]):
    st.session_state.bv_trade_date = trade_date
    st.session_state.bv_nse_df = None
    st.session_state.bv_bse_df = None
    st.session_state.bv_global_skip = None
    st.session_state.bv_nse_skip = None
    st.session_state.bv_bse_skip = None

    add_log(f"Processing trade date: {format_date_display(trade_date)}", "info")

    nse_exp = compute_nse_expiry(trade_date, holidays)
    bse_exp = compute_bse_expiry(trade_date, holidays)
    st.session_state.bv_nse_expiry = nse_exp
    st.session_state.bv_bse_expiry = bse_exp
    add_log(f"NSE Expiry (Tue, adjusted): {format_date_display(nse_exp)}", "success")
    add_log(f"BSE Expiry (Thu, adjusted): {format_date_display(bse_exp)}", "success")

    v = validate_trading_day(trade_date, holidays)

    if not v["is_trading_day"]:
        st.session_state.bv_global_skip = v["global_reason"]
        add_log(f"⛔ SKIPPED (both): {v['global_reason']}", "warn")
        return

    if v["nse_ok"]:
        add_log("NSE: ✅ Valid — fetching bhavcopy…", "success")
        with st.spinner("⏳ Fetching NSE Bhavcopy…"):
            nse_df, nse_log = fetch_nse_bhavcopy(trade_date)
        for line in nse_log.strip().split("\n"):
            lvl = "success" if "✅" in line else ("error" if "❌" in line else "info")
            add_log(line, lvl)
        st.session_state.bv_nse_df = nse_df
    else:
        st.session_state.bv_nse_skip = v["nse_reason"]
        add_log(f"NSE: ⛔ {v['nse_reason']}", "warn")

    if v["bse_ok"]:
        add_log("BSE: ✅ Valid — fetching bhavcopy…", "success")
        with st.spinner("⏳ Fetching BSE Bhavcopy…"):
            bse_df, bse_log = fetch_bse_bhavcopy(trade_date)
        for line in bse_log.strip().split("\n"):
            lvl = "success" if "✅" in line else ("error" if "❌" in line else "info")
            add_log(line, lvl)
        st.session_state.bv_bse_df = bse_df
    else:
        st.session_state.bv_bse_skip = v["bse_reason"]
        add_log(f"BSE: ⛔ {v['bse_reason']}", "warn")


# ──────────────────────────────────────────────────────────────────────
# Render helpers
# ──────────────────────────────────────────────────────────────────────
def render_results():
    if st.session_state.bv_trade_date is None:
        return

    st.markdown('<div class="bv-section-title">📋 Results</div>', unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(
            metric_card("Trade Date", format_date_display(st.session_state.bv_trade_date), "info"),
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            metric_card(
                "NSE Expiry (Tue)",
                format_date_display(st.session_state.bv_nse_expiry) if st.session_state.bv_nse_expiry else "—",
                "success",
            ),
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            metric_card(
                "BSE Expiry (Thu)",
                format_date_display(st.session_state.bv_bse_expiry) if st.session_state.bv_bse_expiry else "—",
                "warn",
            ),
            unsafe_allow_html=True,
        )

    st.markdown("---")

    if st.session_state.bv_global_skip:
        st.warning(f"🚫 {st.session_state.bv_global_skip}")
        return

    dl1, dl2 = st.columns(2)
    with dl1:
        if st.session_state.bv_nse_skip:
            st.warning(f"🚫 NSE: {st.session_state.bv_nse_skip}")
        elif st.session_state.bv_nse_df is not None:
            df = st.session_state.bv_nse_df
            csv_buf = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                f"⬇️ Download NSE Bhavcopy ({len(df)} rows)",
                csv_buf,
                file_name=f"NSE_Bhavcopy_{format_date_display(st.session_state.bv_trade_date)}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        else:
            st.info("No NSE Bhavcopy data available for this date.")

    with dl2:
        if st.session_state.bv_bse_skip:
            st.warning(f"🚫 BSE: {st.session_state.bv_bse_skip}")
        elif st.session_state.bv_bse_df is not None:
            df = st.session_state.bv_bse_df
            csv_buf = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                f"⬇️ Download BSE Bhavcopy ({len(df)} rows)",
                csv_buf,
                file_name=f"BSE_Bhavcopy_{format_date_display(st.session_state.bv_trade_date)}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        else:
            st.info("No BSE Bhavcopy data available for this date.")


# ──────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────
_init_state()

st.markdown(
    '<div class="hero-banner">'
    "  <h1>📊 Bhavcopy Analyzer</h1>"
    "  <p>Download &amp; analyze NSE / BSE derivative Bhavcopy data "
    "with automatic expiry-date calculation</p>"
    "</div>",
    unsafe_allow_html=True,
)

st.markdown('<div class="bv-section-title">🔧 Input Mode</div>', unsafe_allow_html=True)
mode = st.radio(
    "Select input mode",
    ["📁 Upload File(s)", "📅 Select Single Date", "📆 Select Date Range (Bulk)"],
    horizontal=True,
    label_visibility="collapsed",
)

if st.session_state.bv_active_mode != mode:
    _clear_results()
    st.session_state.bv_active_mode = mode

holidays = st.session_state.bv_holidays

# ── MODE 1: File upload ──────────────────────────────────────────
if mode == "📁 Upload File(s)":
    uploaded_files = st.file_uploader(
        "Upload CSV file(s)", type=["csv"], accept_multiple_files=True, key="bv_data_upload",
    )
    if uploaded_files:
        if st.button("🚀 Process Uploaded Files", type="primary", use_container_width=True):
            st.session_state.bv_logs = []
            progress = st.progress(0, text="Starting…")
            total = len(uploaded_files)
            for idx, uf in enumerate(uploaded_files, 1):
                progress.progress(idx / total, text=f"Processing {uf.name} ({idx}/{total})…")
                add_log(f"── File: {uf.name} ──", "info")
                dt, msg = extract_date_from_filename(uf.name)
                if dt is None:
                    add_log(msg, "error")
                    continue
                add_log(msg, "success")
                process_date(dt, holidays)
            progress.progress(1.0, text="✅ Done!")
            add_log("All files processed.", "success")

# ── MODE 2: Single date ─────────────────────────────────────────
elif mode == "📅 Select Single Date":
    selected = st.date_input("Pick a date", value=datetime.today(), key="bv_single_date")
    if st.button("🚀 Fetch Bhavcopy", type="primary", use_container_width=True):
        st.session_state.bv_logs = []
        trade_dt = datetime.combine(selected, datetime.min.time())
        process_date(trade_dt, holidays)

# ── MODE 3: Bulk date range ─────────────────────────────────────
elif mode == "📆 Select Date Range (Bulk)":
    col_a, col_b = st.columns(2)
    with col_a:
        start_date = st.date_input("Start date", value=datetime.today() - timedelta(days=7), key="bv_start")
    with col_b:
        end_date = st.date_input("End date", value=datetime.today(), key="bv_end")

    if start_date > end_date:
        st.error("Start date must be before end date.")
    else:
        all_days = []
        cursor = datetime.combine(start_date, datetime.min.time())
        end_dt = datetime.combine(end_date, datetime.min.time())
        while cursor <= end_dt:
            all_days.append(cursor)
            cursor += timedelta(days=1)

        processable_days = []
        fully_skipped = []
        for day in all_days:
            v = validate_trading_day(day, holidays)
            if not v["is_trading_day"]:
                fully_skipped.append((day, v["global_reason"]))
            else:
                processable_days.append((day, v))

        nse_count = sum(1 for _, v in processable_days if v["nse_ok"])
        bse_count = sum(1 for _, v in processable_days if v["bse_ok"])

        st.info(
            f"📆 {len(all_days)} total day(s) · "
            f"**{len(processable_days)}** trading day(s) · "
            f"NSE: {nse_count} files · BSE: {bse_count} files · "
            f"{len(fully_skipped)} fully skipped"
        )

        if st.button("🚀 Fetch All Bhavcopies", type="primary", use_container_width=True):
            st.session_state.bv_logs = []
            st.session_state.bv_bulk_zip = None
            st.session_state.bv_bulk_zip_name = None

            for day, reason in fully_skipped:
                add_log(f"⛔ {format_date_display(day)}: {reason} — skipped", "warn")

            if not processable_days:
                add_log("No trading days in the selected range.", "error")
            else:
                progress = st.progress(0, text="Starting bulk download…")
                zip_buffer = io.BytesIO()
                file_count = 0

                with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                    for idx, (day, v) in enumerate(processable_days, 1):
                        progress.progress(
                            idx / len(processable_days),
                            text=f"Processing {format_date_display(day)} ({idx}/{len(processable_days)})…",
                        )
                        add_log(f"── Date: {format_date_display(day)} ──", "info")
                        day_label = day.strftime("%d_%b_%Y").lower()

                        nse_exp = compute_nse_expiry(day, holidays)
                        bse_exp = compute_bse_expiry(day, holidays)
                        add_log(f"NSE Expiry: {format_date_display(nse_exp)}", "success")
                        add_log(f"BSE Expiry: {format_date_display(bse_exp)}", "success")

                        if v["nse_ok"]:
                            with st.spinner(f"⏳ NSE {format_date_display(day)}…"):
                                nse_df, nse_log = fetch_nse_bhavcopy(day)
                            for line in nse_log.strip().split("\n"):
                                lvl = "success" if "✅" in line else ("error" if "❌" in line else "info")
                                add_log(line, lvl)
                            if nse_df is not None:
                                zf.writestr(f"NSE_bhavcopy_{day_label}.csv", nse_df.to_csv(index=False))
                                file_count += 1
                        else:
                            add_log(f"NSE: ⛔ {v['nse_reason']}", "warn")

                        if v["bse_ok"]:
                            with st.spinner(f"⏳ BSE {format_date_display(day)}…"):
                                bse_df, bse_log = fetch_bse_bhavcopy(day)
                            for line in bse_log.strip().split("\n"):
                                lvl = "success" if "✅" in line else ("error" if "❌" in line else "info")
                                add_log(line, lvl)
                            if bse_df is not None:
                                zf.writestr(f"BSE_bhavcopy_{day_label}.csv", bse_df.to_csv(index=False))
                                file_count += 1
                        else:
                            add_log(f"BSE: ⛔ {v['bse_reason']}", "warn")

                progress.progress(1.0, text="✅ Bulk download complete!")
                add_log(f"Bulk processing finished — {file_count} file(s) in ZIP.", "success")

                if file_count > 0:
                    zip_buffer.seek(0)
                    range_label = f"{start_date.strftime('%d%b%Y')}_to_{end_date.strftime('%d%b%Y')}".lower()
                    st.session_state.bv_bulk_zip = zip_buffer.getvalue()
                    st.session_state.bv_bulk_zip_name = f"Bhavcopy_{range_label}.zip"

                    last_day = processable_days[-1][0]
                    st.session_state.bv_trade_date = last_day
                    st.session_state.bv_nse_expiry = compute_nse_expiry(last_day, holidays)
                    st.session_state.bv_bse_expiry = compute_bse_expiry(last_day, holidays)
                    st.session_state.bv_global_skip = None

st.markdown("---")

if mode != "📆 Select Date Range (Bulk)":
    render_results()
else:
    if st.session_state.bv_trade_date is not None:
        st.markdown('<div class="bv-section-title">📋 Results</div>', unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown(metric_card("Trade Date", format_date_display(st.session_state.bv_trade_date), "info"), unsafe_allow_html=True)
        with c2:
            st.markdown(
                metric_card("NSE Expiry (Tue)", format_date_display(st.session_state.bv_nse_expiry) if st.session_state.bv_nse_expiry else "—", "success"),
                unsafe_allow_html=True,
            )
        with c3:
            st.markdown(
                metric_card("BSE Expiry (Thu)", format_date_display(st.session_state.bv_bse_expiry) if st.session_state.bv_bse_expiry else "—", "warn"),
                unsafe_allow_html=True,
            )
        st.markdown("---")

    if st.session_state.bv_bulk_zip is not None:
        st.download_button(
            "📦 Download All Bhavcopies (ZIP)",
            st.session_state.bv_bulk_zip,
            file_name=st.session_state.bv_bulk_zip_name,
            mime="application/zip",
            use_container_width=True,
        )

# ── Log panel ────────────────────────────────────────────────────
st.markdown("---")
st.markdown('<div class="bv-section-title">🪵 Logs & Debug</div>', unsafe_allow_html=True)
if st.session_state.bv_logs:
    render_logs()
    if st.button("🗑️ Clear Logs", key="bv_clear_logs"):
        st.session_state.bv_logs = []
        st.rerun()
else:
    st.caption("No logs yet — run a fetch to see activity here.")

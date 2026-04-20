"""
VAR Cost Calculator — Dashboard Page
======================================
Calculates daily VAR (Value at Risk) cost per user based on orderbook
and allocation files.

Pipeline:
  1. Upload Orderbook + Allocation files
  2. Filters: Time (≥3 PM) → Tag (v_) → Status (COMPLETE/FILLED/TRADED)
  3. Parse symbols → Group by User × Strike
  4. Enter next-day open prices
  5. Calculate PnL, VAR Cost, VAR %
"""

import io
import logging
import re
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

from utils.var_cost.file_loader import load_file, validate_allocation_file
from utils.var_cost.data_processor import process_orderbook, extract_fields
from utils.var_cost.symbol_parser import enrich_with_parsed_symbols, group_by_user_strike
from utils.var_cost.calculator import calculate_pnl, calculate_var_cost, merge_allocation

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# Page-scoped CSS (complements main_app global CSS)
# ──────────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    /* ── VAR page header ── */
    .var-hero {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 60%, #0f3460 100%);
        border: 1px solid rgba(108, 99, 255, 0.15);
        border-radius: 18px;
        padding: 2rem 2.5rem;
        margin-bottom: 2rem;
        text-align: center;
    }
    .var-hero h1 {
        background: linear-gradient(135deg, #6C63FF 0%, #3B82F6 50%, #06B6D4 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 2rem;
        font-weight: 700;
        margin-bottom: 0.3rem;
    }
    .var-hero p {
        color: #94a3b8;
        font-size: 0.95rem;
    }

    /* ── Upload cards ── */
    .var-upload-card {
        background: linear-gradient(135deg, #1E293B 0%, #0F172A 100%);
        border: 1px solid #334155;
        border-radius: 16px;
        padding: 1.5rem 1.5rem 0.5rem;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    .var-upload-card .icon { font-size: 2.5rem; margin-bottom: 0.5rem; }
    .var-upload-card .title {
        font-size: 1.1rem; font-weight: 600; color: #E2E8F0; margin-bottom: 0.3rem;
    }
    .var-upload-card .desc {
        font-size: 0.8rem; color: #64748B; line-height: 1.4; margin-bottom: 0.5rem;
    }

    /* ── Metric cards ── */
    .var-metric {
        background: linear-gradient(135deg, #1E293B 0%, #0F172A 100%);
        border: 1px solid #334155;
        border-radius: 16px;
        padding: 1.2rem;
        text-align: center;
        transition: transform 0.2s, box-shadow 0.2s;
    }
    .var-metric:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(108, 99, 255, 0.15);
    }
    .var-metric .value {
        font-size: 1.6rem; font-weight: 700;
        background: linear-gradient(135deg, #6C63FF, #06B6D4);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .var-metric .label {
        font-size: 0.8rem; color: #94A3B8; margin-top: 0.3rem;
    }

    /* ── Section heading ── */
    .var-section {
        font-size: 1.2rem; font-weight: 700; color: #E2E8F0;
        border-left: 3px solid #6C63FF; padding-left: 10px;
        margin: 1.5rem 0 0.8rem;
    }
    .var-subtext {
        font-size: 0.85rem; color: #64748B; margin-bottom: 1rem;
    }

    /* ── Strike table header row ── */
    .strike-row-head {
        display: flex; padding: 0.6rem 1rem;
        background: rgba(108, 99, 255, 0.08);
        border-radius: 8px; margin-bottom: 0.8rem;
        font-size: 0.85rem; font-weight: 600; color: #94A3B8;
    }
    .strike-row-head span:first-child { flex: 1; }
    .strike-row-head span:last-child  { flex: 1; text-align: right; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ──────────────────────────────────────────────────────────────────────
# Hero header
# ──────────────────────────────────────────────────────────────────────
st.markdown(
    '<div class="var-hero">'
    "  <h1>📊 Daily VAR Cost Calculator</h1>"
    "  <p>Upload orderbook & allocation files → compute daily VAR cost per user</p>"
    "</div>",
    unsafe_allow_html=True,
)

# ──────────────────────────────────────────────────────────────────────
# Session-state (namespaced with "var_" prefix)
# ──────────────────────────────────────────────────────────────────────
_DEFAULTS = {
    "var_orderbook_raw": None,
    "var_allocation_raw": None,
    "var_processed_df": None,
    "var_grouped_df": None,
    "var_report_df": None,
    "var_unique_strikes": [],
    "var_processing_done": False,
    "var_pipeline_summary": None,
}
for key, default in _DEFAULTS.items():
    if key not in st.session_state:
        st.session_state[key] = default


# ══════════════════════════════════════════════════════════════════════
# UPLOAD SECTION
# ══════════════════════════════════════════════════════════════════════
if not st.session_state.var_processing_done:
    col1, col2 = st.columns(2, gap="large")

    with col1:
        st.markdown(
            '<div class="var-upload-card">'
            '<div class="icon">📄</div>'
            '<div class="title">Orderbook File</div>'
            '<div class="desc">CSV or Excel with Order Time, Symbol, Qty, Price, Status, Tag</div>'
            "</div>",
            unsafe_allow_html=True,
        )
        orderbook_file = st.file_uploader(
            "Upload Orderbook",
            type=["csv", "xlsx", "xls"],
            key="var_orderbook_uploader",
            label_visibility="collapsed",
        )

    with col2:
        st.markdown(
            '<div class="var-upload-card">'
            '<div class="icon">📋</div>'
            '<div class="title">Allocation File</div>'
            '<div class="desc">CSV or Excel with User ID and Allocation columns</div>'
            "</div>",
            unsafe_allow_html=True,
        )
        allocation_file = st.file_uploader(
            "Upload Allocation",
            type=["csv", "xlsx", "xls"],
            key="var_allocation_uploader",
            label_visibility="collapsed",
        )

    # ── Load allocation quietly ──
    if allocation_file is not None and st.session_state.var_allocation_raw is None:
        try:
            raw = load_file(allocation_file)
            validated = validate_allocation_file(raw)
            st.session_state.var_allocation_raw = validated
            logger.info("Allocation file loaded: %d users", len(validated))
        except Exception as exc:
            st.error(f"Allocation file error: {exc}")

    # ── Process button ──
    st.markdown("")
    _, col_c, _ = st.columns([1, 1, 1])
    with col_c:
        process_btn = st.button(
            "🚀  Process Orderbook",
            use_container_width=True,
            disabled=(orderbook_file is None),
            key="var_process_btn",
        )

    if process_btn and orderbook_file is not None:
        # Also load allocation if not done
        if allocation_file is not None and st.session_state.var_allocation_raw is None:
            try:
                raw = load_file(allocation_file)
                validated = validate_allocation_file(raw)
                st.session_state.var_allocation_raw = validated
            except Exception as exc:
                st.error(f"Allocation file error: {exc}")

        # ── Run pipeline ──
        try:
            with st.spinner("Loading orderbook..."):
                raw_df = load_file(orderbook_file)
                st.session_state.var_orderbook_raw = raw_df

            with st.spinner("Applying filters..."):
                row_before = len(raw_df)
                filtered_df = process_orderbook(raw_df)
                if filtered_df.empty:
                    st.warning(
                        "No rows passed all filters. Check that your orderbook has:\n"
                        "- Orders after 3:00 PM\n"
                        "- Tag columns containing 'v_'\n"
                        "- Status = COMPLETE / FILLED / TRADED"
                    )
                    st.stop()

            with st.spinner("Extracting fields..."):
                extracted_df = extract_fields(filtered_df)

            with st.spinner("Parsing symbols..."):
                enriched_df = enrich_with_parsed_symbols(extracted_df)

            with st.spinner("Grouping by User × Strike..."):
                grouped_df = group_by_user_strike(enriched_df)

            st.session_state.var_processed_df = extracted_df
            st.session_state.var_grouped_df = grouped_df
            st.session_state.var_unique_strikes = sorted(grouped_df["strike"].unique().tolist())
            st.session_state.var_processing_done = True
            st.session_state.var_report_df = None
            st.session_state.var_pipeline_summary = {
                "total_rows": row_before,
                "filtered_rows": len(filtered_df),
                "users": grouped_df["user_id"].nunique(),
                "strikes": st.session_state.var_unique_strikes,
                "groups": len(grouped_df),
            }

            st.success(
                f"Orderbook processed! {row_before} rows → {len(filtered_df)} after filters → "
                f"{len(grouped_df)} groups across {grouped_df['user_id'].nunique()} users "
                f"and {len(st.session_state.var_unique_strikes)} strikes."
            )
            st.rerun()

        except ValueError as ve:
            st.error(str(ve))
        except Exception as exc:
            st.error(f"Unexpected error: {exc}")
            logger.exception("Error during VAR orderbook processing")


# ══════════════════════════════════════════════════════════════════════
# STRIKE PRICE INPUTS & CALCULATION
# ══════════════════════════════════════════════════════════════════════
if st.session_state.var_processing_done and st.session_state.var_grouped_df is not None:
    grouped_df = st.session_state.var_grouped_df
    strikes = st.session_state.var_unique_strikes
    summary = st.session_state.var_pipeline_summary

    # ── Reset button ──
    if st.button("🔄 Reset & Upload New Files", key="var_reset_btn"):
        for key in _DEFAULTS:
            st.session_state[key] = _DEFAULTS[key]
        st.rerun()

    # ── Pipeline summary metrics ──
    if summary:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Rows", summary["total_rows"])
        c2.metric("After Filters", summary["filtered_rows"])
        c3.metric("Users", summary["users"])
        c4.metric("Unique Strikes", len(summary["strikes"]))
        st.markdown("---")

    # ── Grouped data preview ──
    with st.expander("View Grouped Data Preview", expanded=False):
        preview = grouped_df.copy()
        preview.columns = ["User ID", "Instrument", "Strike", "Total Qty", "Avg Price"]
        st.dataframe(preview, use_container_width=True, hide_index=True)

    st.markdown("---")

    # ── Strike price inputs ──
    st.markdown(
        '<div class="var-section">Enter Next Day Open Prices</div>'
        '<div class="var-subtext">Provide the next-day opening price for each strike</div>',
        unsafe_allow_html=True,
    )

    open_prices: Dict[str, Optional[float]] = {}
    for i in range(0, len(strikes), 2):
        cols = st.columns(2)
        for j, col in enumerate(cols):
            idx = i + j
            if idx >= len(strikes):
                break
            strike = strikes[idx]
            with col:
                val = st.number_input(
                    f"{strike}",
                    min_value=0.0,
                    step=0.05,
                    value=st.session_state.get(f"var_open_{strike}", 0.0),
                    key=f"var_open_price_{strike}",
                    format="%.2f",
                )
                open_prices[strike] = val if val > 0 else None
                st.session_state[f"var_open_{strike}"] = val

    st.markdown("---")

    # ── Calculate button ──
    _, col_c, _ = st.columns([1, 1, 1])
    with col_c:
        calc_btn = st.button(
            "⚡  Calculate VAR Cost",
            use_container_width=True,
            key="var_calc_btn",
        )

    if calc_btn:
        missing = [s for s, p in open_prices.items() if p is None or p <= 0]
        if missing:
            st.error(f"Please enter valid open prices for: {', '.join(missing)}")
        elif st.session_state.var_allocation_raw is None:
            st.error("Please upload the Allocation file before calculating.")
        else:
            try:
                with st.spinner("Calculating PnL..."):
                    clean_prices = {s: float(p) for s, p in open_prices.items() if p is not None}
                    pnl_df = calculate_pnl(grouped_df, clean_prices)

                with st.spinner("Computing VAR cost..."):
                    var_df = calculate_var_cost(pnl_df)

                with st.spinner("Merging allocation..."):
                    report_df = merge_allocation(pnl_df, var_df, st.session_state.var_allocation_raw)

                st.session_state.var_report_df = report_df
                st.success("VAR cost calculation complete!")
                st.rerun()

            except ValueError as ve:
                st.error(str(ve))
            except Exception as exc:
                st.error(f"Unexpected error: {exc}")
                logger.exception("Error during VAR calculation")


# ══════════════════════════════════════════════════════════════════════
# REPORT
# ══════════════════════════════════════════════════════════════════════
if st.session_state.var_report_df is not None:
    report_df = st.session_state.var_report_df
    st.markdown("---")

    # ── Summary metrics ──
    total_users = report_df["user_id"].nunique()
    total_var = report_df.groupby("user_id")["var_cost"].first().sum()
    total_debit = report_df.groupby("user_id")["total_debit"].first().sum()
    total_strikes = report_df["strike"].nunique()
    avg_var_pct = report_df.groupby("user_id")["var_pct"].first().mean()

    cols = st.columns(5)
    metrics = [
        ("Total Users", str(total_users)),
        ("Total Debit", f"₹{total_debit:,.2f}"),
        ("Total VAR Cost", f"₹{total_var:,.2f}"),
        ("Unique Strikes", str(total_strikes)),
        ("Avg VAR %", f"{avg_var_pct:.2f}%"),
    ]
    for col, (label, value) in zip(cols, metrics):
        col.markdown(
            f'<div class="var-metric">'
            f'  <div class="value">{value}</div>'
            f'  <div class="label">{label}</div>'
            f"</div>",
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # ── Report table ──
    st.markdown('<div class="var-section">VAR Cost Report</div>', unsafe_allow_html=True)

    display_df = report_df.copy()
    column_rename = {
        "user_id": "User ID",
        "instrument": "Instrument",
        "strike": "Strike",
        "total_quantity": "Quantity",
        "avg_price": "Avg Price",
        "debit": "Debit",
        "open_price": "Open Price",
        "pnl": "PnL",
        "var_cost": "VAR Cost",
        "total_debit": "Total Debit",
        "allocation": "Allocation",
        "var_pct": "VAR %",
    }
    display_df = display_df.rename(columns=column_rename)

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Avg Price": st.column_config.NumberColumn(format="₹%.2f"),
            "Debit": st.column_config.NumberColumn(format="₹%.2f"),
            "Open Price": st.column_config.NumberColumn(format="₹%.2f"),
            "PnL": st.column_config.NumberColumn(format="₹%.2f"),
            "VAR Cost": st.column_config.NumberColumn(format="₹%.2f"),
            "Total Debit": st.column_config.NumberColumn(format="₹%.2f"),
            "Allocation": st.column_config.NumberColumn(format="₹%.2f"),
            "VAR %": st.column_config.NumberColumn(format="%.2f%%"),
        },
    )

    # ── Export buttons ──
    st.markdown("---")
    st.markdown('<div class="var-section">Export Report</div>', unsafe_allow_html=True)

    col1, col2, _ = st.columns([1, 1, 2])
    with col1:
        csv_data = report_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Download CSV",
            data=csv_data,
            file_name="var_cost_report.csv",
            mime="text/csv",
            use_container_width=True,
            key="var_dl_csv",
        )
    with col2:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            report_df.to_excel(writer, index=False, sheet_name="VAR Report")
        buffer.seek(0)
        st.download_button(
            label="📥 Download Excel",
            data=buffer,
            file_name="var_cost_report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="var_dl_xlsx",
        )

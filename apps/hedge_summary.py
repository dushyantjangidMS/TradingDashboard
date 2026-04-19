"""
Hedge Summary Calculator — Dashboard Page
=========================================
Parses raw position data (Spaced/Compact Formats), cleans, and aggregates quantities 
into Buy and Sell sides, calculating an overall or structured Hedge Ratio (PE / CE).
"""

import io
import pandas as pd
import streamlit as st
import logging

from utils.hedge_engine import calculate_hedge_summary

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# Page-scoped CSS (complements main_app global CSS)
# ──────────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    /* ── Hero header ── */
    .hedge-hero {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 60%, #0f3460 100%);
        border: 1px solid rgba(16, 185, 129, 0.15); /* Emerald accent */
        border-radius: 18px;
        padding: 2rem 2.5rem;
        margin-bottom: 2rem;
        text-align: center;
    }
    .hedge-hero h1 {
        background: linear-gradient(135deg, #10B981 0%, #3B82F6 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 2rem;
        font-weight: 700;
        margin-bottom: 0.3rem;
    }
    .hedge-hero p {
        color: #94a3b8;
        font-size: 0.95rem;
    }

    /* ── Upload & Param Cards ── */
    .hedge-card {
        background: linear-gradient(135deg, #1E293B 0%, #0F172A 100%);
        border: 1px solid #334155;
        border-radius: 16px;
        padding: 1.5rem;
        margin-bottom: 1rem;
    }
    .hedge-card .icon { font-size: 2.2rem; margin-bottom: 0.5rem; }
    .hedge-card .title { font-size: 1.1rem; font-weight: 600; color: #E2E8F0; margin-bottom: 0.3rem; }
    .hedge-card .desc { font-size: 0.8rem; color: #64748B; margin-bottom: 1rem; }

    /* ── Section heading ── */
    .hedge-section {
        font-size: 1.2rem; font-weight: 700; color: #E2E8F0;
        border-left: 3px solid #10B981; padding-left: 10px;
        margin: 1.5rem 0 0.8rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ──────────────────────────────────────────────────────────────────────
# Hero header
# ──────────────────────────────────────────────────────────────────────
st.markdown(
    '<div class="hedge-hero">'
    "  <h1>🛡️ Hedge Summary Calculator</h1>"
    "  <p>Transform raw positions into structured hedge mapping and calculate precise PE/CE Ratio.</p>"
    "</div>",
    unsafe_allow_html=True,
)

# ──────────────────────────────────────────────────────────────────────
# UI Panel
# ──────────────────────────────────────────────────────────────────────
col1, col2, col3 = st.columns([1, 2, 1])

with col2:
    st.markdown(
        '<div class="hedge-card">'
        '<div class="icon">📁</div>'
        '<div class="title">Upload Positions Data</div>'
        '<div class="desc">Supports both Spaced (NIFTY 21APR2026 PE 24200) and Compact (NIFTY2642125300CE) symbols. Format expects Net Qty, Buy Qty, Sell Qty.</div>'
        "</div>",
        unsafe_allow_html=True,
    )
    positions_file = st.file_uploader(
        "Upload Positions",
        type=["csv", "xlsx", "xls"],
        label_visibility="collapsed",
    )


st.markdown("---")
_, btn_col, _ = st.columns([1, 1, 1])

with btn_col:
    process_btn = st.button("🚀 Process Hedge Summary", use_container_width=True, disabled=(positions_file is None))

# ──────────────────────────────────────────────────────────────────────
# Execution & Results
# ──────────────────────────────────────────────────────────────────────
if process_btn and positions_file:
    try:
        # Load data
        file_name = positions_file.name.lower()
        if file_name.endswith('.csv'):
            df_raw = pd.read_csv(positions_file)
        else:
            df_raw = pd.read_excel(positions_file)

        with st.spinner("Processing symbols and calculating Hedge Ratio..."):
            final_df = calculate_hedge_summary(df=df_raw)

        if final_df.empty:
            st.warning("No valid options data found in the uploaded file.")
        else:
            st.success(f"Processing Complete! Successfully mapped {len(final_df)} rows based on designated metrics.")

            # Display table
            st.markdown('<div class="hedge-section">Hedge Report Output</div>', unsafe_allow_html=True)
            st.dataframe(final_df, use_container_width=True, hide_index=True)

            st.markdown("---")
            st.markdown('<div class="hedge-section">Export Options</div>', unsafe_allow_html=True)

            d1, d2, _ = st.columns([1, 1, 2])
            
            with d1:
                csv_data = final_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="📥 Download CSV",
                    data=csv_data,
                    file_name="hedge_summary_report.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
            with d2:
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                    final_df.to_excel(writer, index=False, sheet_name="Hedge Summary")
                buffer.seek(0)
                st.download_button(
                    label="📥 Download Excel",
                    data=buffer,
                    file_name="hedge_summary_report.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )

    except Exception as e:
        logger.exception("Error processing Hedge Summary")
        st.error(f"Failed to process file. Error: {e}")

"""
Excel Merger — Streamlit Page
==============================
Combine multiple Excel and CSV files into one with automatic date extraction.
"""

import re
from io import BytesIO

import pandas as pd
import streamlit as st


def _extract_date(filename: str) -> str:
    """Try common date formats in filename and normalise to DD-MM-YYYY."""
    patterns = [
        r"(?P<d>\d{2})-(?P<m>\d{2})-(?P<y>\d{4})",
        r"(?P<d>\d{2})_(?P<m>\d{2})_(?P<y>\d{4})",
        r"(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})",
        r"(?P<y>\d{4})_(?P<m>\d{2})_(?P<d>\d{2})",
        r"(?P<y>\d{4})(?P<m>\d{2})(?P<d>\d{2})",
    ]
    for pat in patterns:
        match = re.search(pat, filename)
        if match:
            return f"{match.group('d')}-{match.group('m')}-{match.group('y')}"
    return "Unknown"


# ──────────────────────────────────────────────────────────────────────
# Page UI
# ──────────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    .merger-hero {
        background: linear-gradient(135deg, #064e3b 0%, #065f46 50%, #047857 100%);
        padding: 2rem 2.5rem;
        border-radius: 16px;
        margin-bottom: 1.6rem;
        box-shadow: 0 8px 32px rgba(0,0,0,0.25);
    }
    .merger-hero h1 { color: #ffffff; font-size: 1.9rem; font-weight: 700; margin: 0; }
    .merger-hero p  { color: #a7f3d0; font-size: 1rem; margin: 0.4rem 0 0 0; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="merger-hero">'
    "  <h1>📊 Excel Merger</h1>"
    "  <p>Combine multiple Excel &amp; CSV files into one with automatic date extraction</p>"
    "</div>",
    unsafe_allow_html=True,
)

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Upload Files")
    uploaded_files = st.file_uploader(
        "Select one or more Excel/CSV files",
        type=["xlsx", "xls", "csv"],
        accept_multiple_files=True,
        key="merger_upload",
    )

with col2:
    st.subheader("Instructions")
    st.info(
        """
    1. Select multiple files
    2. Dates will be extracted from filenames
    3. Click 'Merge Files' to combine
    4. Download the merged file
    """
    )

if uploaded_files:
    st.markdown("---")
    st.subheader(f"📁 Files Selected: {len(uploaded_files)}")

    for i, file in enumerate(uploaded_files, 1):
        cn, cd = st.columns([3, 1])
        with cn:
            st.write(f"{i}. {file.name}")
        with cd:
            st.write(f"📅 {_extract_date(file.name)}")

    st.markdown("---")

    if st.button("🔄 Merge Files", key="merger_btn", use_container_width=True):
        try:
            with st.spinner("Processing files…"):
                all_data = []
                for file in uploaded_files:
                    if file.name.endswith(".csv"):
                        df = pd.read_csv(file)
                    else:
                        df = pd.read_excel(file, engine="openpyxl")
                    df.insert(0, "Date", _extract_date(file.name))
                    all_data.append(df)

                combined_df = pd.concat(all_data, ignore_index=True)

                if "UserID" in combined_df.columns:
                    group_cols = ["Date", "UserID"]
                    num_cols = [
                        c
                        for c in combined_df.select_dtypes(include="number").columns
                        if c not in group_cols
                    ]
                    if num_cols:
                        combined_df = combined_df.groupby(group_cols, as_index=False)[
                            num_cols
                        ].sum()

            st.success(f"✅ Successfully merged {len(uploaded_files)} files!")

            st.subheader("📋 Preview of Merged Data")
            st.dataframe(combined_df.head(10), use_container_width=True)
            st.write(
                f"Total rows: {len(combined_df)} | Total columns: {len(combined_df.columns)}"
            )

            output_buffer = BytesIO()
            combined_df.to_excel(output_buffer, index=False, engine="openpyxl")
            output_buffer.seek(0)

            st.download_button(
                label="⬇️ Download Merged File (Excel)",
                data=output_buffer,
                file_name="combined_pnl.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

            csv_buffer = combined_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="⬇️ Download Merged File (CSV)",
                data=csv_buffer,
                file_name="combined_pnl.csv",
                mime="text/csv",
                use_container_width=True,
            )

        except Exception as e:
            st.error(f"❌ Error occurred: {str(e)}")
else:
    st.info("👆 Upload files to get started!")

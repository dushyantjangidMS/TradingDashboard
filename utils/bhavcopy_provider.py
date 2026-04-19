"""
bhavcopy_provider.py
--------------------
Bridge module that auto-fetches NSE / BSE bhavcopies and converts them
into the DataFrames expected by the PNL engine.

Settlement scope:
  • NFO → NIFTY options only  (OPTIDXNIFTY)
  • BFO → SENSEX options only (Asset Code = BSX)
"""

from datetime import datetime
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st

from utils.bhavcopy_fetcher import fetch_bse_bhavcopy, fetch_nse_bhavcopy


# ──────────────────────────────────────────────────────────────────────
# NFO (NSE) settlement provider
# ──────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def auto_fetch_nfo_settlement(
    trade_date: datetime,
    expiry_date,
) -> Tuple[Optional[pd.DataFrame], List[str]]:
    """
    Auto-fetch NSE bhavcopy for `trade_date`, extract settlement prices
    for NIFTY options with `expiry_date`.

    Returns
    -------
    (processed_df | None, list_of_log_lines)
        processed_df has columns: [Strike_Type, SETTLEMENT]
    """
    logs: List[str] = []

    # Step 1 — download raw bhavcopy
    nse_df, fetch_log = fetch_nse_bhavcopy(trade_date)
    logs.extend(fetch_log.strip().split("\n"))

    if nse_df is None:
        logs.append("❌ NFO bhavcopy download failed — cannot compute settlement.")
        return None, logs

    # Step 2 — process into settlement format
    nse_df.columns = nse_df.columns.str.strip()

    if "CONTRACT_D" not in nse_df.columns:
        logs.append("❌ 'CONTRACT_D' column missing in NFO Bhavcopy.")
        return None, logs
    if "SETTLEMENT" not in nse_df.columns:
        logs.append("❌ 'SETTLEMENT' column missing in NFO Bhavcopy.")
        return None, logs

    contract = nse_df["CONTRACT_D"].astype(str)
    nse_df["Date"] = contract.str.extract(r"(\d{2}-[A-Z]{3}-\d{4})")
    nse_df["Symbol"] = contract.str.extract(r"^(.*?)(\d{2}-[A-Z]{3}-\d{4})")[0]
    nse_df["Strike_Type"] = contract.str.extract(r"(PE\d+|CE\d+)$")

    nse_df["Date"] = pd.to_datetime(nse_df["Date"], format="%d-%b-%Y", errors="coerce")
    nse_df["Strike_Type"] = nse_df["Strike_Type"].str.replace(
        r"^(PE|CE)(\d+)$", r"\2\1", regex=True
    )
    nse_df["SETTLEMENT"] = pd.to_numeric(nse_df["SETTLEMENT"], errors="coerce")

    # Filter: NIFTY only + matching expiry
    expiry_dt = pd.to_datetime(expiry_date)
    nse_df = nse_df[
        (nse_df["Date"] == expiry_dt) & (nse_df["Symbol"] == "OPTIDXNIFTY")
    ].copy()

    result = nse_df[["Strike_Type", "SETTLEMENT"]].drop_duplicates("Strike_Type")
    logs.append(f"✅ NFO settlement ready — {len(result)} NIFTY strike(s) matched for expiry {expiry_dt.strftime('%d-%b-%Y')}")
    return result, logs


# ──────────────────────────────────────────────────────────────────────
# BFO (BSE) settlement provider
# ──────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def auto_fetch_bfo_settlement(
    trade_date: datetime,
    expiry_date,
) -> Tuple[Optional[pd.DataFrame], List[str]]:
    """
    Auto-fetch BSE bhavcopy for `trade_date`, extract close prices
    for SENSEX options (BSX) with `expiry_date`.

    Returns
    -------
    (processed_df | None, list_of_log_lines)
        processed_df has columns: [Symbols, Close Price]
    """
    logs: List[str] = []

    # Step 1 — download raw bhavcopy
    bse_df, fetch_log = fetch_bse_bhavcopy(trade_date)
    logs.extend(fetch_log.strip().split("\n"))

    if bse_df is None:
        logs.append("❌ BFO bhavcopy download failed — cannot compute settlement.")
        return None, logs

    # Step 2 — process into settlement format
    bse_df.columns = bse_df.columns.str.strip()

    required = ["Asset Code", "Expiry Date", "Series Code", "Close Price"]
    missing = [c for c in required if c not in bse_df.columns]
    if missing:
        logs.append(f"❌ Missing columns in BFO Bhavcopy: {missing}")
        return None, logs

    # Filter: SENSEX (BSX) only
    bse_df = bse_df[bse_df["Asset Code"] == "BSX"].copy()
    bse_df["Expiry Date"] = pd.to_datetime(
        bse_df["Expiry Date"], format="mixed", dayfirst=True
    )
    bse_df = bse_df[bse_df["Expiry Date"] == pd.to_datetime(expiry_date)].copy()
    bse_df["Symbols"] = bse_df["Series Code"].astype(str).str[-7:]
    bse_df["Close Price"] = pd.to_numeric(bse_df["Close Price"], errors="coerce")

    result = bse_df[["Symbols", "Close Price"]].drop_duplicates("Symbols")
    logs.append(
        f"✅ BFO settlement ready — {len(result)} SENSEX symbol(s) matched "
        f"for expiry {pd.to_datetime(expiry_date).strftime('%d-%b-%Y')}"
    )
    return result, logs


# ──────────────────────────────────────────────────────────────────────
# Manual upload processors (same logic, but from uploaded file objects)
# ──────────────────────────────────────────────────────────────────────
def process_uploaded_nfo_bhavcopy(
    file_obj, expiry_nfo
) -> Tuple[Optional[pd.DataFrame], List[str]]:
    """
    Process a user-uploaded NFO bhavcopy CSV into settlement format.
    Same output as auto_fetch_nfo_settlement.
    """
    logs: List[str] = []
    try:
        file_obj.seek(0)
    except Exception:
        pass

    try:
        raw_df = pd.read_csv(file_obj)
    except Exception as e:
        logs.append(f"❌ Failed to read NFO bhavcopy file: {e}")
        return None, logs

    raw_df.columns = raw_df.columns.str.strip()

    if "CONTRACT_D" not in raw_df.columns:
        logs.append("❌ 'CONTRACT_D' column missing in uploaded NFO Bhavcopy.")
        return None, logs
    if "SETTLEMENT" not in raw_df.columns:
        logs.append("❌ 'SETTLEMENT' column missing in uploaded NFO Bhavcopy.")
        return None, logs

    contract = raw_df["CONTRACT_D"].astype(str)
    raw_df["Date"] = contract.str.extract(r"(\d{2}-[A-Z]{3}-\d{4})")
    raw_df["Symbol"] = contract.str.extract(r"^(.*?)(\d{2}-[A-Z]{3}-\d{4})")[0]
    raw_df["Strike_Type"] = contract.str.extract(r"(PE\d+|CE\d+)$")

    raw_df["Date"] = pd.to_datetime(raw_df["Date"], format="%d-%b-%Y", errors="coerce")
    raw_df["Strike_Type"] = raw_df["Strike_Type"].str.replace(
        r"^(PE|CE)(\d+)$", r"\2\1", regex=True
    )
    raw_df["SETTLEMENT"] = pd.to_numeric(raw_df["SETTLEMENT"], errors="coerce")

    expiry_dt = pd.to_datetime(expiry_nfo)
    raw_df = raw_df[
        (raw_df["Date"] == expiry_dt) & (raw_df["Symbol"] == "OPTIDXNIFTY")
    ].copy()

    result = raw_df[["Strike_Type", "SETTLEMENT"]].drop_duplicates("Strike_Type")
    logs.append(f"✅ NFO file processed — {len(result)} NIFTY strike(s)")
    return result, logs


def process_uploaded_bfo_bhavcopy(
    file_obj, expiry_bfo
) -> Tuple[Optional[pd.DataFrame], List[str]]:
    """
    Process a user-uploaded BFO bhavcopy CSV into settlement format.
    Same output as auto_fetch_bfo_settlement.
    """
    logs: List[str] = []
    try:
        file_obj.seek(0)
    except Exception:
        pass

    try:
        raw_df = pd.read_csv(file_obj)
    except Exception as e:
        logs.append(f"❌ Failed to read BFO bhavcopy file: {e}")
        return None, logs

    raw_df.columns = raw_df.columns.str.strip()

    required = ["Asset Code", "Expiry Date", "Series Code", "Close Price"]
    missing = [c for c in required if c not in raw_df.columns]
    if missing:
        logs.append(f"❌ Missing columns in uploaded BFO Bhavcopy: {missing}")
        return None, logs

    raw_df = raw_df[raw_df["Asset Code"] == "BSX"].copy()
    raw_df["Expiry Date"] = pd.to_datetime(
        raw_df["Expiry Date"], format="mixed", dayfirst=True
    )
    raw_df = raw_df[raw_df["Expiry Date"] == pd.to_datetime(expiry_bfo)].copy()
    raw_df["Symbols"] = raw_df["Series Code"].astype(str).str[-7:]
    raw_df["Close Price"] = pd.to_numeric(raw_df["Close Price"], errors="coerce")

    result = raw_df[["Symbols", "Close Price"]].drop_duplicates("Symbols")
    logs.append(f"✅ BFO file processed — {len(result)} SENSEX symbol(s)")
    return result, logs

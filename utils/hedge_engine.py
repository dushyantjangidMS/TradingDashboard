import logging
import re
from typing import Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────
# Regex Patterns
# ─────────────────────────────────────────────────────────────────────────
# Format A (Spaced): "NIFTY 21APR2026 PE 24200"
SPACED_PATTERN = re.compile(
    r"^(?P<index>[a-zA-Z]+)\s+(?P<expiry>[A-Za-z0-9]+)\s+(?P<type>CE|PE)\s+(?P<strike>\d+(\.\d+)?)"
)

# Format B (Compact): "NIFTY2642125300CE"
COMPACT_PATTERN = re.compile(
    r"^(?P<index>[A-Za-z]+?)(?P<expiry>[A-Z0-9]+)?(?P<strike>\d{4,6})(?P<type>CE|PE)$"
)


def parse_symbol(symbol: str) -> Tuple[str, str, str, float]:
    """
    Robustly parses standard spaced and compact derivatives strings.
    """
    symbol = str(symbol).strip().upper()

    match = SPACED_PATTERN.search(symbol)
    if match:
        return (
            match.group("index"),
            match.group("expiry"),
            match.group("type"),
            float(match.group("strike")),
        )

    match = COMPACT_PATTERN.search(symbol)
    if match:
        return (
            match.group("index"),
            match.group("expiry") if match.group("expiry") else "",
            match.group("type"),
            float(match.group("strike")),
        )

    logger.warning(f"Failed to parse symbol: '{symbol}'")
    return ("UNKNOWN", "", "UNKNOWN", 0.0)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Handles missing values, drops bad rows, cleans columns."""
    cleaned = df.copy()

    user_col = next((c for c in cleaned.columns if "user" in c.lower()), "UserID")
    if user_col in cleaned.columns:
        cleaned = cleaned.dropna(subset=[user_col])
        cleaned["UserID"] = cleaned[user_col].astype(str).str.strip()
    else:
        cleaned["UserID"] = "UNKNOWN"

    for col in ["Symbol", "Exchange"]:
        if col not in cleaned.columns:
            cleaned[col] = "UNKNOWN"
        else:
            cleaned[col] = cleaned[col].astype(str).str.strip().str.upper()

    for q in ["Buy Qty", "Sell Qty"]:
        if q in cleaned.columns:
            cleaned[q] = pd.to_numeric(cleaned[q], errors="coerce").fillna(0.0)
        else:
            cleaned[q] = 0.0

    return cleaned


def calculate_hedge_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates per UserID, Exchange, and OptionType (CE/PE).
    Calculates Buy Qty, Sell Qty, and Ratio = Buy Qty / Sell Qty.
    Outputs exactly in visually grouped format.
    """
    df = clean_data(df)
    if df.empty:
        return pd.DataFrame()

    # Parse symbols
    parsed_tuples = df["Symbol"].apply(parse_symbol).tolist()
    if parsed_tuples:
        df["Index"], df["Expiry"], df["OptionType"], df["Strike"] = zip(*parsed_tuples)
    else:
        df["Index"], df["Expiry"], df["OptionType"], df["Strike"] = "UNKNOWN", "", "UNKNOWN", 0.0

    # Only CE and PE options
    df = df[df["OptionType"].isin(["CE", "PE"])].copy()
    if df.empty:
        return pd.DataFrame()

    # Grouping
    grouped = df.groupby(["UserID", "Exchange", "OptionType"], as_index=False)[["Buy Qty", "Sell Qty"]].sum()

    records = []
    for _, row in grouped.iterrows():
        u = row["UserID"]
        e = row["Exchange"]
        opt = row["OptionType"]
        buy_qty = row["Buy Qty"]
        sell_qty = row["Sell Qty"]
        
        ratio = ""
        if sell_qty > 0:
            raw_ratio = buy_qty / sell_qty
            # Formatting ratio cleanly - if it ends in .0, make it int
            if raw_ratio.is_integer():
                ratio = int(raw_ratio)
            else:
                ratio = round(raw_ratio, 5)
        elif buy_qty > 0:
            ratio = "Infinity"
        elif buy_qty == 0 and sell_qty == 0:
            continue # skip empty pairs completely

        # Create Buy Row
        records.append({
            "UserID": u,
            "Exchange": e,
            "strike": opt,      # Image requests column name 'strike' for CE/PE
            "type": "Buy",
            "Sum of Net Qty": int(buy_qty) if buy_qty.is_integer() else buy_qty,
            "Ratio": ratio
        })

        # Create Sell Row
        sell_val = -1 * (int(sell_qty) if sell_qty.is_integer() else sell_qty)
        records.append({
            "UserID": u,
            "Exchange": e,
            "strike": opt,
            "type": "Sell",
            "Sum of Net Qty": sell_val,
            "Ratio": ""
        })

    if not records:
        return pd.DataFrame()

    out_df = pd.DataFrame(records)

    # Sort exactly like pivot table: UserID ASC, Exchange ASC, OptionType ASC
    # Buy before Sell relies on original append order (stable sort)
    # So we don't strictly modify the order unless needed.
    # Out_df is already ordered by UserID -> Exchange -> OptionType appropriately if groupby was sorted.
    # We will do a visual blanking out of duplicated Exchange and strike.
    
    prev_u = None
    prev_e = None
    prev_o = None
    
    for idx, row in out_df.iterrows():
        # Blanking logic per user to mimic Pivot Table
        if row["UserID"] == prev_u and row["Exchange"] == prev_e:
            out_df.at[idx, "Exchange"] = ""
            if row["strike"] == prev_o:
                out_df.at[idx, "strike"] = ""
            else:
                prev_o = row["strike"]
        else:
            prev_u = row["UserID"]
            prev_e = row["Exchange"]
            prev_o = row["strike"]

    # Final Column Order
    final_cols = ["UserID", "Exchange", "strike", "type", "Sum of Net Qty", "Ratio"]
    return out_df[final_cols]

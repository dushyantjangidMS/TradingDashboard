"""
data_processor.py
─────────────────
Filters and transforms raw orderbook data before calculation.
"""

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Statuses that represent a successfully executed order
VALID_STATUSES = {"complete", "filled", "traded"}
REJECTED_STATUSES = {"rejected", "cancelled", "canceled"}

# Time threshold - only keep orders placed AFTER this time
CUTOFF_HOUR = 15  # 3:00 PM


def process_orderbook(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full pipeline:
        1. Filter by time (after 3 PM)
        2. Filter by Tag columns (must contain 'v_')
        3. Filter by status (keep COMPLETE / FILLED / TRADED)
        4. Extract required fields

    Returns a cleaned DataFrame.
    """
    logger.info("Starting orderbook processing - %d rows", len(df))

    df = _filter_by_time(df)
    df = _filter_by_tag(df)
    df = _filter_by_status(df)

    if df.empty:
        logger.warning("No rows remaining after filters")

    logger.info("Orderbook processing complete - %d rows remain", len(df))
    return df


# ── time filter ─────────────────────────────────────────────────────────────

def _detect_time_column(df: pd.DataFrame) -> Optional[str]:
    """Auto-detect the order-time column."""
    candidates = ["order time", "ordertime", "order_time", "time", "trade time", "trade_time"]
    for c in candidates:
        if c in df.columns:
            return c
    # fallback – any column with "time" in name
    for c in df.columns:
        if "time" in c:
            return c
    return None


def _filter_by_time(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only orders placed strictly after 3:00 PM."""
    time_col = _detect_time_column(df)
    if time_col is None:
        logger.warning("No time column found - skipping time filter")
        return df

    original_len = len(df)

    # attempt datetime parse
    parsed = pd.to_datetime(df[time_col], errors="coerce", dayfirst=True)
    valid_mask = parsed.notna()
    invalid_count = (~valid_mask).sum()
    if invalid_count:
        logger.warning(
            "%d rows have unparseable time values in '%s' - they will be dropped",
            invalid_count,
            time_col,
        )

    df = df.loc[valid_mask].copy()
    df["_parsed_time"] = parsed[valid_mask]

    # filter after 3 PM
    after_cutoff = df["_parsed_time"].dt.hour >= CUTOFF_HOUR
    df = df.loc[after_cutoff].copy()
    df.drop(columns=["_parsed_time"], inplace=True)

    logger.info("Time filter: %d -> %d rows (kept after %d:00)", original_len, len(df), CUTOFF_HOUR)
    return df


# ── tag filter ──────────────────────────────────────────────────────────────

def _filter_by_tag(df: pd.DataFrame) -> pd.DataFrame:
    """Keep rows where the LAST column contains 'v_' (case-insensitive).

    The tag data always lives in the last column of the orderbook,
    regardless of its header name.
    """
    tag_col = df.columns[-1]
    logger.info("Using last column '%s' as tag column", tag_col)

    original_len = len(df)
    mask = df[tag_col].astype(str).str.contains(r"v_", case=False, na=False)
    df = df.loc[mask].copy()
    logger.info("Tag filter (last col '%s'): %d -> %d rows", tag_col, original_len, len(df))
    return df


# ── status filter ───────────────────────────────────────────────────────────

def _detect_status_column(df: pd.DataFrame) -> Optional[str]:
    candidates = ["status", "order status", "order_status", "orderstatus"]
    for c in candidates:
        if c in df.columns:
            return c
    for c in df.columns:
        if "status" in c:
            return c
    return None


def _filter_by_status(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only orders with valid execution statuses."""
    status_col = _detect_status_column(df)
    if status_col is None:
        logger.warning("No status column found - skipping status filter")
        return df

    original_len = len(df)
    normed_status = df[status_col].astype(str).str.strip().str.lower()
    mask = normed_status.isin(VALID_STATUSES)
    df = df.loc[mask].copy()
    logger.info("Status filter: %d -> %d rows", original_len, len(df))
    return df


# ── field extraction ────────────────────────────────────────────────────────

def _auto_detect_column(df: pd.DataFrame, candidates: list[str], label: str) -> Optional[str]:
    """Find a column by candidate names."""
    for c in candidates:
        if c in df.columns:
            return c
    for c in df.columns:
        for cand in candidates:
            if cand.replace(" ", "") in c.replace(" ", ""):
                return c
    return None


def extract_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract and canonicalise:
        user_id, symbol, quantity, avg_price
    """
    mapping = {
        "user_id": ["user id", "userid", "user_id", "client id", "client_id", "clientid"],
        "symbol": ["symbol", "trading symbol", "tradingsymbol", "trading_symbol", "instrument"],
        "quantity": ["quantity", "qty", "filled qty", "filled_qty", "filledqty", "traded qty", "traded_qty"],
        "avg_price": [
            "avg price", "avg_price", "avgprice", "average price",
            "average_price", "price", "trade price", "trade_price",
        ],
    }

    rename_map: dict[str, str] = {}
    missing: list[str] = []

    for canonical, candidates in mapping.items():
        found = _auto_detect_column(df, candidates, canonical)
        if found is None:
            missing.append(canonical)
        else:
            rename_map[found] = canonical

    if missing:
        raise ValueError(
            f"Could not auto-detect columns for: {missing}. "
            f"Available columns: {list(df.columns)}"
        )

    df = df.rename(columns=rename_map)

    # ensure numeric types
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    df["avg_price"] = pd.to_numeric(df["avg_price"], errors="coerce").fillna(0.0)

    return df[["user_id", "symbol", "quantity", "avg_price"]].copy()

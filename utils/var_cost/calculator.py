"""
calculator.py
─────────────
PnL and VAR cost calculations.
"""

import logging
from typing import Dict

import pandas as pd

logger = logging.getLogger(__name__)


def calculate_pnl(
    grouped_df: pd.DataFrame,
    open_prices: Dict[str, float],
) -> pd.DataFrame:
    """
    Calculate PnL for each user × strike row.

    PnL = (Open Price − Avg Price) × Quantity

    Parameters
    ----------
    grouped_df : DataFrame with columns [user_id, instrument, strike, total_quantity, avg_price]
    open_prices : mapping of strike → next-day open price

    Returns
    -------
    DataFrame with added columns: open_price, pnl
    """
    df = grouped_df.copy()
    df["open_price"] = df["strike"].map(open_prices).astype(float)

    missing = df["open_price"].isna()
    if missing.any():
        missing_strikes = df.loc[missing, "strike"].unique().tolist()
        raise ValueError(
            f"Open price missing for strikes: {missing_strikes}. "
            "Please enter all open prices before calculating."
        )

    df["debit"] = (df["avg_price"] * df["total_quantity"]).round(2)
    df["pnl"] = ((df["open_price"] - df["avg_price"]) * df["total_quantity"]).round(2)
    logger.info("Calculated PnL for %d rows", len(df))
    return df


def calculate_var_cost(pnl_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate total PnL per user → VAR Cost.

    Returns a DataFrame with one row per user: [user_id, var_cost]
    """
    var_df = pnl_df.groupby("user_id", as_index=False).agg(
        var_cost=("pnl", "sum"),
        total_debit=("debit", "sum"),
    )
    var_df["var_cost"] = var_df["var_cost"].round(2)
    var_df["total_debit"] = var_df["total_debit"].round(2)
    logger.info("VAR cost computed for %d users", len(var_df))
    return var_df


def merge_allocation(
    pnl_df: pd.DataFrame,
    var_df: pd.DataFrame,
    alloc_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge PnL detail with per-user VAR cost and allocation.

    Adds columns:
        • var_cost   (total per user)
        • allocation
        • var_pct    = (var_cost / allocation) × 100
    """
    # Normalise user_id columns for merging
    for frame in [pnl_df, var_df, alloc_df]:
        if "user_id" in frame.columns:
            frame["user_id"] = frame["user_id"].astype(str).str.strip().str.upper()
        elif "user id" in frame.columns:
            frame.rename(columns={"user id": "user_id"}, inplace=True)
            frame["user_id"] = frame["user_id"].astype(str).str.strip().str.upper()

    # Merge VAR cost onto PnL detail
    result = pnl_df.merge(var_df, on="user_id", how="left", suffixes=("", "_total"))

    # Merge allocation
    alloc_cols = [c for c in alloc_df.columns if c in ("user_id", "allocation")]
    alloc_clean = alloc_df[alloc_cols].copy()
    alloc_clean["user_id"] = alloc_clean["user_id"].astype(str).str.strip().str.upper()

    result = result.merge(alloc_clean, on="user_id", how="left")

    # warn on mismatches
    unmatched = result["allocation"].isna()
    if unmatched.any():
        unmatched_users = result.loc[unmatched, "user_id"].unique().tolist()
        logger.warning("No allocation found for users: %s", unmatched_users)

    # calculate VAR %
    result["var_pct"] = (
        (result["var_cost"] / result["allocation"]) * 100
    ).round(2)

    # handle division by zero / NaN
    result["var_pct"] = result["var_pct"].fillna(0.0)

    # reorder columns for clean output
    final_cols = [
        "user_id", "instrument", "strike", "total_quantity",
        "avg_price", "debit", "open_price", "pnl", "var_cost",
        "total_debit", "allocation", "var_pct",
    ]
    final_cols = [c for c in final_cols if c in result.columns]

    logger.info("Final report: %d rows, %d users", len(result), result["user_id"].nunique())
    return result[final_cols]

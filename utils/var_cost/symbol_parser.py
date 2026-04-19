"""
symbol_parser.py
────────────────
Parses trading symbols to extract instrument name and strike details,
then groups data by User ID + Strike.
"""

import logging
import re
from typing import Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# Common F&O instrument prefixes
KNOWN_INSTRUMENTS = [
    "BANKNIFTY", "NIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX",
]

# Regex: captures instrument, optional expiry, strike number + CE/PE
# e.g. NIFTY2540323500CE → NIFTY, 23500CE
#      BANKNIFTY25APR52000PE → BANKNIFTY, 52000PE
SYMBOL_PATTERN = re.compile(
    r"^(?P<instrument>[A-Z]+?)"       # instrument (non-greedy)
    r"(?:\d{2}[A-Z0-9]+?)"           # expiry portion (YY + month/date)
    r"(?P<strike>\d+(?:CE|PE))$",     # strike + option type
    re.IGNORECASE,
)

# Simpler fallback: just grab trailing strike
FALLBACK_PATTERN = re.compile(
    r"(?P<strike>\d{3,6}(?:CE|PE))$",
    re.IGNORECASE,
)


def parse_symbol(symbol: str) -> Tuple[str, str]:
    """
    Extract (instrument, strike) from a trading symbol string.

    Strike is always the last 7 characters (5 digits + CE/PE).

    Examples
    --------
    >>> parse_symbol("NIFTY2540323500CE")
    ('NIFTY', '23500CE')

    >>> parse_symbol("BANKNIFTY2540352000PE")
    ('BANKNIFTY', '52000PE')

    Returns ('UNKNOWN', symbol) if parsing fails.
    """
    symbol = str(symbol).strip().upper()

    # Strike = last 7 characters (e.g. 23500CE, 52000PE)
    if len(symbol) >= 7 and symbol[-2:] in ("CE", "PE"):
        strike = symbol[-7:]
        prefix = symbol[:-7]

        # Identify instrument from prefix
        for inst in KNOWN_INSTRUMENTS:
            if prefix.startswith(inst):
                return inst, strike

        # Fallback: strip digits from prefix to get instrument name
        raw_inst = re.sub(r"\d", "", prefix).strip()
        return (raw_inst or "UNKNOWN"), strike

    logger.warning("Could not parse symbol: %s", symbol)
    return "UNKNOWN", symbol


def enrich_with_parsed_symbols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add 'instrument' and 'strike' columns derived from the 'symbol' column.
    """
    parsed = df["symbol"].apply(parse_symbol)
    df = df.copy()
    df["instrument"] = parsed.apply(lambda x: x[0])
    df["strike"] = parsed.apply(lambda x: x[1])
    logger.info("Parsed %d symbols -> %d unique strikes", len(df), df["strike"].nunique())
    return df


def group_by_user_strike(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group by (user_id, strike) and aggregate:
        • total_quantity  (sum)
        • weighted_avg_price (weighted mean by quantity)
    """
    df = df.copy()
    df["_value"] = df["avg_price"] * df["quantity"]

    grouped = df.groupby(["user_id", "instrument", "strike"], as_index=False).agg(
        total_quantity=("quantity", "sum"),
        _total_value=("_value", "sum"),
    )

    grouped["avg_price"] = (grouped["_total_value"] / grouped["total_quantity"]).round(2)
    grouped.drop(columns=["_total_value"], inplace=True)

    logger.info(
        "Grouped into %d (user × strike) combinations across %d users",
        len(grouped),
        grouped["user_id"].nunique(),
    )
    return grouped

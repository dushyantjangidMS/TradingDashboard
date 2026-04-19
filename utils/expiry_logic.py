"""
expiry_logic.py
---------------
Calculates NSE and BSE derivative expiry dates with holiday adjustment,
and provides trading-day validation utilities.

Rules:
  - NSE weekly expiry = Tuesday.
  - BSE weekly expiry = Thursday.
  - If the raw expiry falls on a holiday, shift to the PREVIOUS
    working day (skip weekends + holidays).
  - A date is a valid bhavcopy-download day only if it is:
      • Not a weekend (Sat/Sun)
      • Not a holiday
      • Not an expiry day (NSE or BSE, including shifted ones)
"""

from datetime import datetime, timedelta, date
from typing import List, Set


# Default NSE holidays for 2026 (ISO format strings)
DEFAULT_NSE_HOLIDAYS_2026 = [
    "2026-01-26",
    "2026-03-03",
    "2026-03-26",
    "2026-03-31",
    "2026-04-03",
    "2026-04-14",
    "2026-05-01",
    "2026-05-28",
    "2026-06-26",
    "2026-09-14",
    "2026-10-02",
    "2026-10-20",
    "2026-11-10",
    "2026-11-24",
    "2026-12-25",
]


# ──────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────
def parse_holidays(holiday_strings: List[str]) -> Set[date]:
    """Convert a list of date strings to a set of date objects.

    Accepted formats: YYYY-MM-DD, DD-MM-YYYY.
    """
    holidays: Set[date] = set()
    for h in holiday_strings:
        h = h.strip()
        if not h:
            continue
        for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
            try:
                holidays.add(datetime.strptime(h, fmt).date())
                break
            except ValueError:
                continue
    return holidays


def _to_date(dt) -> date:
    """Normalise datetime or date to a date object."""
    if isinstance(dt, datetime):
        return dt.date()
    return dt


def _current_or_next_weekday(start: date, target_weekday: int) -> date:
    """
    Return the current-or-next occurrence of `target_weekday`.

    If `start` is already that weekday, return `start` itself.
    target_weekday: 0=Monday … 6=Sunday
    """
    days_ahead = (target_weekday - start.weekday()) % 7
    return start + timedelta(days=days_ahead)


def _adjust_for_holidays(expiry_date: date, holidays: Set[date]) -> date:
    """
    If `expiry_date` falls on a holiday or weekend, shift to the
    previous working day (recursively).
    """
    while expiry_date.weekday() >= 5 or expiry_date in holidays:
        expiry_date -= timedelta(days=1)
    return expiry_date


# ──────────────────────────────────────────────────────────────────────
# Public: expiry computation
# ──────────────────────────────────────────────────────────────────────
def compute_nse_expiry(
    trade_date, holiday_strings: List[str] | None = None
) -> datetime:
    """
    Compute NSE weekly expiry for the week containing `trade_date`.

    The raw expiry is the current-or-next Tuesday.  If that Tuesday is
    a holiday, shift to the previous working day.
    """
    if holiday_strings is None:
        holiday_strings = DEFAULT_NSE_HOLIDAYS_2026
    holidays = parse_holidays(holiday_strings)

    td = _to_date(trade_date)
    # Tuesday = weekday 1
    raw_expiry = _current_or_next_weekday(td, 1)
    adjusted = _adjust_for_holidays(raw_expiry, holidays)
    return datetime.combine(adjusted, datetime.min.time())


def compute_bse_expiry(
    trade_date, holiday_strings: List[str] | None = None
) -> datetime:
    """
    Compute BSE weekly expiry for the week containing `trade_date`.

    The raw expiry is the current-or-next Thursday.  If that Thursday
    is a holiday, shift to the previous working day.
    """
    if holiday_strings is None:
        holiday_strings = DEFAULT_NSE_HOLIDAYS_2026
    holidays = parse_holidays(holiday_strings)

    td = _to_date(trade_date)
    # Thursday = weekday 3
    raw_expiry = _current_or_next_weekday(td, 3)
    adjusted = _adjust_for_holidays(raw_expiry, holidays)
    return datetime.combine(adjusted, datetime.min.time())


# ──────────────────────────────────────────────────────────────────────
# Public: trading-day validation (per-exchange)
# ──────────────────────────────────────────────────────────────────────
def is_weekend(dt) -> bool:
    """Return True if `dt` is Saturday or Sunday."""
    return _to_date(dt).weekday() >= 5


def is_holiday(dt, holiday_strings: List[str] | None = None) -> bool:
    """Return True if `dt` is in the holiday calendar."""
    if holiday_strings is None:
        holiday_strings = DEFAULT_NSE_HOLIDAYS_2026
    return _to_date(dt) in parse_holidays(holiday_strings)


def is_nse_expiry(dt, holiday_strings: List[str] | None = None) -> bool:
    """Return True if `dt` is an NSE expiry day (including shifted)."""
    return _to_date(dt) == _to_date(compute_nse_expiry(dt, holiday_strings))


def is_bse_expiry(dt, holiday_strings: List[str] | None = None) -> bool:
    """Return True if `dt` is a BSE expiry day (including shifted)."""
    return _to_date(dt) == _to_date(compute_bse_expiry(dt, holiday_strings))


def validate_trading_day(dt, holiday_strings: List[str] | None = None):
    """
    Check whether `dt` is a valid trading day and which exchanges
    allow bhavcopy download.

    Returns
    -------
    dict with keys:
        "is_trading_day": bool   — False if weekend or holiday
        "global_reason":  str    — reason if not a trading day at all
        "nse_ok":         bool   — True if NSE bhavcopy can be downloaded
        "nse_reason":     str    — reason if NSE is skipped
        "bse_ok":         bool   — True if BSE bhavcopy can be downloaded
        "bse_reason":     str    — reason if BSE is skipped
    """
    if holiday_strings is None:
        holiday_strings = DEFAULT_NSE_HOLIDAYS_2026

    td = _to_date(dt)
    result = {
        "is_trading_day": True,
        "global_reason": "",
        "nse_ok": True,
        "nse_reason": "",
        "bse_ok": True,
        "bse_reason": "",
    }

    # ── Global filters (block both exchanges) ────────────────────────
    if td.weekday() >= 5:
        day_name = "Saturday" if td.weekday() == 5 else "Sunday"
        result["is_trading_day"] = False
        result["global_reason"] = f"Weekend ({day_name}) – No Trading"
        result["nse_ok"] = False
        result["nse_reason"] = result["global_reason"]
        result["bse_ok"] = False
        result["bse_reason"] = result["global_reason"]
        return result

    if is_holiday(dt, holiday_strings):
        result["is_trading_day"] = False
        result["global_reason"] = "Market Holiday – No Data Available"
        result["nse_ok"] = False
        result["nse_reason"] = result["global_reason"]
        result["bse_ok"] = False
        result["bse_reason"] = result["global_reason"]
        return result

    # ── Per-exchange expiry check (independent) ──────────────────────
    if is_nse_expiry(dt, holiday_strings):
        result["nse_ok"] = False
        result["nse_reason"] = "NSE Expiry Day – Skipped"

    if is_bse_expiry(dt, holiday_strings):
        result["bse_ok"] = False
        result["bse_reason"] = "BSE Expiry Day – Skipped"

    return result

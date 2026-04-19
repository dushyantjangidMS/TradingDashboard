"""
date_parser.py
--------------
Extracts dates from filenames and provides date conversion utilities.

Expected filename format examples:
  - "VS13 13 MAR 2026 POSITIOS.csv"
  - "VS13 03 APR 2026 POSITIONS.csv"

The date portion (DD MMM YYYY) is extracted using regex and converted
to a datetime object.
"""

import re
from datetime import datetime
from typing import Optional, Tuple


# Regex: captures DD MMM YYYY anywhere in the string
_DATE_PATTERN = re.compile(
    r"(\d{1,2})\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d{4})",
    re.IGNORECASE,
)


def extract_date_from_filename(filename: str) -> Tuple[Optional[datetime], str]:
    """
    Extract a date from a filename string.

    Parameters
    ----------
    filename : str
        The name of the uploaded file (e.g. "VS13 13 MAR 2026 POSITIOS.csv").

    Returns
    -------
    (datetime | None, message)
        On success: (datetime object, success message)
        On failure: (None, error description)
    """
    match = _DATE_PATTERN.search(filename)
    if not match:
        return None, (
            f"Could not extract date from filename '{filename}'. "
            "Expected format: '... DD MMM YYYY ...'"
        )

    day_str, month_str, year_str = match.groups()
    date_str = f"{day_str} {month_str.upper()} {year_str}"

    try:
        parsed = datetime.strptime(date_str, "%d %b %Y")
        return parsed, f"Extracted date: {format_date_display(parsed)}"
    except ValueError as exc:
        return None, f"Invalid date '{date_str}' in filename: {exc}"


def format_date_display(dt: datetime) -> str:
    """Format datetime as DD-MMM-YYYY for display (e.g. 13-Mar-2026)."""
    return dt.strftime("%d-%b-%Y")


def format_date_nse(dt: datetime) -> str:
    """Format datetime for NSE API query string (DD-MMM-YYYY)."""
    return dt.strftime("%d-%b-%Y")


def format_date_bse(dt: datetime) -> str:
    """Format datetime for BSE URL (YYYYMMDD)."""
    return dt.strftime("%Y%m%d")

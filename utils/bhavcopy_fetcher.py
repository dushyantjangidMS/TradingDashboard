"""
bhavcopy_fetcher.py
-------------------
Downloads Bhavcopy data from NSE and BSE for a given date.

NSE: ZIP archive via API → extract CSV inside.
BSE: Direct CSV download.

Both functions return a pandas DataFrame on success or raise descriptive
exceptions on failure.
"""

import io
import time
import zipfile
from datetime import datetime
from typing import Tuple
from urllib.parse import quote

import pandas as pd
import requests

from utils.date_parser import format_date_bse, format_date_nse

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_NSE_API_BASE = "https://www.nseindia.com/api/reports"
_NSE_COOKIE_URL = "https://www.nseindia.com"

_BSE_URL_TEMPLATE = (
    "https://www.bseindia.com/download/Bhavcopy/Derivative/"
    "MS_{date}-01.csv"
)

_COMMON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _get_nse_session() -> requests.Session:
    """
    Create a requests Session with proper cookies from NSE's main page.
    NSE requires valid cookies to allow API access.
    """
    session = requests.Session()
    session.headers.update(_COMMON_HEADERS)

    # Hit the main page to pick up cookies
    try:
        resp = session.get(_NSE_COOKIE_URL, timeout=15)
        resp.raise_for_status()
    except requests.RequestException:
        pass  # proceed anyway; some cookies may still be set

    return session


def _build_nse_url(date_str: str) -> str:
    """Build the NSE bhavcopy download URL for the given DD-MMM-YYYY date."""
    archives_param = (
        '[{"name":"F&O - Bhavcopy (fo.zip)",'
        '"type":"archives",'
        '"category":"derivatives",'
        '"section":"equity"}]'
    )
    return (
        f"{_NSE_API_BASE}"
        f"?archives={quote(archives_param, safe='')}"
        f"&date={date_str}"
        f"&type=equity"
        f"&mode=single"
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def fetch_nse_bhavcopy(
    trade_date: datetime,
) -> Tuple[pd.DataFrame | None, str]:
    """
    Download and parse the NSE F&O Bhavcopy for `trade_date`.

    Returns
    -------
    (DataFrame | None, log_message)
    """
    date_str = format_date_nse(trade_date)
    url = _build_nse_url(date_str)
    logs = []

    session = _get_nse_session()
    session.headers.update({"Referer": "https://www.nseindia.com/all-reports-derivatives"})

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logs.append(f"[NSE] Attempt {attempt}: GET {url[:80]}…")
            resp = session.get(url, timeout=30)

            if resp.status_code == 200:
                # The response is a ZIP file
                try:
                    zf = zipfile.ZipFile(io.BytesIO(resp.content))
                    # Select only the Options file (op*.csv), ignore Futures (fo*.csv)
                    op_files = [
                        n for n in zf.namelist()
                        if n.lower().startswith("op") and n.lower().endswith(".csv")
                    ]
                    if not op_files:
                        logs.append(
                            "[NSE] ❌ Options file (op) not found in NSE ZIP. "
                            f"Files in archive: {zf.namelist()}"
                        )
                        return None, "\n".join(logs)

                    with zf.open(op_files[0]) as f:
                        df = pd.read_csv(f)

                    logs.append(
                        f"[NSE] ✅ Success — loaded '{op_files[0]}' "
                        f"({len(df)} rows, {len(df.columns)} cols)"
                    )
                    return df, "\n".join(logs)

                except zipfile.BadZipFile:
                    # Maybe it returned a CSV directly (not zipped)
                    try:
                        df = pd.read_csv(io.BytesIO(resp.content))
                        logs.append(
                            f"[NSE] ✅ Success — loaded CSV directly "
                            f"({len(df)} rows, {len(df.columns)} cols)"
                        )
                        return df, "\n".join(logs)
                    except Exception as e:
                        logs.append(f"[NSE] Response is neither ZIP nor CSV: {e}")
                        return None, "\n".join(logs)

            elif resp.status_code == 403:
                logs.append(f"[NSE] 403 Forbidden — retrying after {RETRY_DELAY}s…")
                time.sleep(RETRY_DELAY * attempt)
                # Refresh session cookies
                session = _get_nse_session()
                session.headers.update(
                    {"Referer": "https://www.nseindia.com/all-reports-derivatives"}
                )
            elif resp.status_code == 404:
                logs.append(
                    f"[NSE] 404 Not Found — No bhavcopy available for {date_str}. "
                    "This may be a holiday or weekend."
                )
                return None, "\n".join(logs)
            else:
                logs.append(f"[NSE] HTTP {resp.status_code} — retrying…")
                time.sleep(RETRY_DELAY)

        except requests.RequestException as exc:
            logs.append(f"[NSE] Request error: {exc}")
            time.sleep(RETRY_DELAY)

    logs.append("[NSE] ❌ All retries exhausted.")
    return None, "\n".join(logs)


def fetch_bse_bhavcopy(
    trade_date: datetime,
) -> Tuple[pd.DataFrame | None, str]:
    """
    Download and parse the BSE derivative Bhavcopy for `trade_date`.

    Returns
    -------
    (DataFrame | None, log_message)
    """
    date_str = format_date_bse(trade_date)
    url = _BSE_URL_TEMPLATE.format(date=date_str)
    logs = []

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logs.append(f"[BSE] Attempt {attempt}: GET {url}")
            resp = requests.get(url, headers=_COMMON_HEADERS, timeout=30)

            if resp.status_code == 200:
                try:
                    df = pd.read_csv(io.StringIO(resp.text))
                    logs.append(
                        f"[BSE] ✅ Success — loaded CSV "
                        f"({len(df)} rows, {len(df.columns)} cols)"
                    )
                    return df, "\n".join(logs)
                except Exception as e:
                    logs.append(f"[BSE] Failed to parse CSV: {e}")
                    return None, "\n".join(logs)

            elif resp.status_code == 404:
                logs.append(
                    f"[BSE] 404 Not Found — No bhavcopy for {date_str}. "
                    "This may be a holiday or weekend."
                )
                return None, "\n".join(logs)
            else:
                logs.append(f"[BSE] HTTP {resp.status_code} — retrying…")
                time.sleep(RETRY_DELAY)

        except requests.RequestException as exc:
            logs.append(f"[BSE] Request error: {exc}")
            time.sleep(RETRY_DELAY)

    logs.append("[BSE] ❌ All retries exhausted.")
    return None, "\n".join(logs)

"""
file_loader.py
──────────────
Handles loading and initial normalisation of CSV / Excel files.
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)


# ── public helpers ──────────────────────────────────────────────────────────

def load_file(uploaded_file) -> pd.DataFrame:
    """
    Read an uploaded Streamlit file object into a DataFrame.

    Supports:
        • .csv
        • .xlsx / .xls

    After loading the raw frame, unnamed columns are renamed to Tag / Tag_N
    and all column names are normalised (lowercase, stripped).

    Raises
    ------
    ValueError  – if the extension is unsupported or the file is empty.
    """
    filename: str = uploaded_file.name
    ext = Path(filename).suffix.lower()
    logger.info("Loading file: %s (extension: %s)", filename, ext)

    try:
        if ext == ".csv":
            # Auto-detect delimiter (comma vs tab)
            import csv
            sample = uploaded_file.read(4096)
            uploaded_file.seek(0)
            try:
                if isinstance(sample, bytes):
                    sample = sample.decode("utf-8", errors="ignore")
                dialect = csv.Sniffer().sniff(sample, delimiters=",\t|;")
                sep = dialect.delimiter
            except csv.Error:
                sep = ","
            logger.info("CSV delimiter detected: %r", sep)
            df = pd.read_csv(uploaded_file, sep=sep, index_col=False)
        elif ext in (".xlsx", ".xls"):
            df = pd.read_excel(uploaded_file)
        else:
            raise ValueError(f"Unsupported file type: {ext}. Please upload CSV or Excel.")
    except ValueError:
        raise
    except Exception as exc:
        logger.exception("Failed to read file %s", filename)
        raise ValueError(f"Could not read '{filename}': {exc}") from exc

    if df.empty:
        raise ValueError(f"The file '{filename}' is empty.")

    df = _fix_missing_headers(df)
    df = _rename_unnamed_columns(df)
    df = _normalise_column_names(df)
    logger.info("Loaded %d rows x %d columns from %s", len(df), len(df.columns), filename)
    return df


# ── internal helpers ────────────────────────────────────────────────────────

def _fix_missing_headers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle columns with completely missing/empty/blank headers.

    Common case: orderbook has data in column 20 (index 19) but the
    header cell is blank.  Pandas reads it as NaN or empty string.
    This function renames such columns to 'Tag', 'Tag_1', etc.
    """
    new_cols: list[str] = list(df.columns)
    tag_counter = 0
    for i, col in enumerate(new_cols):
        col_str = str(col).strip()
        # Check for NaN, empty, or purely whitespace headers
        if col_str in ("", "nan", "None") or pd.isna(df.columns[i] if not isinstance(df.columns[i], str) else False):
            if tag_counter == 0:
                new_cols[i] = "Tag"
            else:
                new_cols[i] = f"Tag_{tag_counter}"
            tag_counter += 1
            logger.info("Column %d had no header -> renamed to '%s'", i + 1, new_cols[i])
    df.columns = new_cols
    return df


def _rename_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns starting with 'Unnamed' to Tag, Tag_1, Tag_2 ..."""
    # Count how many Tag columns already exist (from _fix_missing_headers)
    existing_tags = sum(1 for c in df.columns if str(c).startswith("Tag"))
    new_cols: list[str] = []
    tag_counter = existing_tags  # continue numbering
    for col in df.columns:
        col_str = str(col)
        if col_str.startswith("Unnamed"):
            if tag_counter == 0:
                new_cols.append("Tag")
            else:
                new_cols.append(f"Tag_{tag_counter}")
            tag_counter += 1
        else:
            new_cols.append(col_str)
    df.columns = new_cols
    renamed_count = tag_counter - existing_tags
    if renamed_count:
        logger.info("Renamed %d 'Unnamed' column(s) to Tag / Tag_N", renamed_count)
    return df


def _normalise_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase + strip whitespace from every column name."""
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


# ── validation helpers ──────────────────────────────────────────────────────

def validate_allocation_file(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure the allocation file contains the required columns.

    Required (after normalisation):
        • user id  (or userid / user_id)
        • allocation

    Returns the dataframe with canonical column names.
    """
    col_map = _find_column(df, ["user id", "userid", "user_id"], "User ID")
    alloc_map = _find_column(df, ["allocation"], "Allocation")

    df = df.rename(columns={**col_map, **alloc_map})
    df["allocation"] = pd.to_numeric(df["allocation"], errors="coerce")

    missing = df["allocation"].isna().sum()
    if missing:
        logger.warning("%d rows have non-numeric Allocation values", missing)

    return df


def _find_column(
    df: pd.DataFrame,
    candidates: list[str],
    label: str,
) -> dict[str, str]:
    """Return a rename map {found_col: canonical} or raise."""
    for c in candidates:
        if c in df.columns:
            return {c: candidates[0].replace(" ", "_") if " " in candidates[0] else candidates[0]}
    # try fuzzy – column might have extra whitespace already stripped
    normed = {c.replace(" ", "").replace("_", ""): c for c in df.columns}
    for c in candidates:
        key = c.replace(" ", "").replace("_", "")
        if key in normed:
            return {normed[key]: candidates[0].replace(" ", "_") if " " in candidates[0] else candidates[0]}
    raise ValueError(
        f"Missing required column '{label}'. "
        f"Expected one of: {candidates}. Found: {list(df.columns)}"
    )

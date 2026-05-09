import pandas as pd
import re
import numpy as np

def clean_user_id(user_id_series):
    """
    Safely converts a UserID series to string, handling NaNs and numeric floats.
    Strips leading and trailing whitespace.
    """
    def process_id(val):
        if pd.isna(val):
            return np.nan
        # If float ending in .0, remove .0
        if isinstance(val, float) and val.is_integer():
            val = int(val)
        return str(val).strip()

    return user_id_series.apply(process_id)

def extract_mtm(remark_series):
    """
    Extracts MTM numeric values from REMARK string.
    Supports MTM=123, MTM = -123, MTM=+123.45 etc.
    """
    # Regular expression pattern to capture numbers after MTM and optional spaces/=
    # Group 1 captures the number itself: optional sign [-+], followed by digits and optional decimals
    pattern = r'MTM\s*=\s*([-+]?\d*\.?\d+)'
    
    def process_remark(val):
        if pd.isna(val):
            return np.nan
        
        val_str = str(val)
        match = re.search(pattern, val_str, re.IGNORECASE)
        if match:
            return float(match.group(1))
        return np.nan

    return remark_series.apply(process_remark)

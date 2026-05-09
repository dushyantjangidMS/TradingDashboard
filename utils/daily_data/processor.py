import pandas as pd
import numpy as np
from .extractor import clean_user_id, extract_mtm

def merge_data(vs22_pnl, vs25_pnl, vs22_summary, vs25_summary):
    """
    Main processing logic:
    1. Merge PnL files together.
    2. Merge Summary files together.
    3. Match UserID and join Summary data (ALLOCATION, REMARK) to PnL.
    4. Extract MTM from REMARK.
    5. Calculate Allocation*100.
    """
    
    # Check if empty dataframes
    if vs22_pnl is None and vs25_pnl is None:
        raise ValueError("Both PnL files cannot be empty or missing.")
    if vs22_summary is None and vs25_summary is None:
        raise ValueError("Both Summary files cannot be empty or missing.")

    # 1. Merge PnL Files
    pnl_dfs = []
    if vs22_pnl is not None:
        pnl_dfs.append(vs22_pnl)
    if vs25_pnl is not None:
        pnl_dfs.append(vs25_pnl)
    
    merged_pnl = pd.concat(pnl_dfs, ignore_index=True)

    # 2. Merge Summary Files
    summary_dfs = []
    if vs22_summary is not None:
        summary_dfs.append(vs22_summary)
    if vs25_summary is not None:
        summary_dfs.append(vs25_summary)
        
    merged_summary = pd.concat(summary_dfs, ignore_index=True)

    # 3. Clean UserIDs
    # Verify UserID column exists
    pnl_user_col = None
    for col in merged_pnl.columns:
        if str(col).lower() == 'userid':
            pnl_user_col = col
            break
            
    summary_user_col = None
    for col in merged_summary.columns:
        if str(col).lower() == 'userid':
            summary_user_col = col
            break

    if not pnl_user_col:
        raise ValueError("UserID column not found in PnL files.")
    if not summary_user_col:
        raise ValueError("UserID column not found in Summary files.")

    merged_pnl['clean_userid'] = clean_user_id(merged_pnl[pnl_user_col])
    merged_summary['clean_userid'] = clean_user_id(merged_summary[summary_user_col])
    
    # Handle missing essential columns in summary
    allocation_col = None
    for col in merged_summary.columns:
        if str(col).lower() == 'allocation':
            allocation_col = col
            break
            
    remark_col = None
    for col in merged_summary.columns:
        if str(col).lower() == 'remark':
            remark_col = col
            break
            
    if allocation_col is None:
        merged_summary['ALLOCATION_TEMP'] = np.nan
        allocation_col = 'ALLOCATION_TEMP'
        
    if remark_col is None:
        merged_summary['REMARK_TEMP'] = ''
        remark_col = 'REMARK_TEMP'

    # Filter summary to required columns to avoid dropping PnL columns if name overlaps
    # Deduplicate summary based on UserID if necessary
    summary_subset = merged_summary[['clean_userid', allocation_col, remark_col]].drop_duplicates(subset=['clean_userid'], keep='first')

    # Merge! Left join on PnL
    final_df = pd.merge(merged_pnl, summary_subset, on='clean_userid', how='left')
    
    # Rename for output clarity
    if allocation_col in final_df.columns:
        final_df.rename(columns={allocation_col: 'Allocation'}, inplace=True)
    
    # 4. Extract MTM stoxxo from remark
    final_df['MTM stoxxo'] = extract_mtm(final_df[remark_col])
    
    # 5. Calculate Allocation*100
    # ensure Allocation is numeric
    final_df['Allocation'] = pd.to_numeric(final_df['Allocation'], errors='coerce')
    final_df['Allocation*100'] = final_df['Allocation'] * 100
    
    # Clean up intermediate columns
    columns_to_drop = ['clean_userid']
    if remark_col in final_df.columns:
        columns_to_drop.append(remark_col)
    
    final_df.drop(columns=columns_to_drop, inplace=True, errors='ignore')
    
    return final_df

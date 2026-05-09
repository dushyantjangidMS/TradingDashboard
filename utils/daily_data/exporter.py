import pandas as pd
import io

def generate_excel(df, sheet_name="Final Data"):
    """
    Generates an Excel file from a DataFrame and returns the bytes.
    Uses xlsxwriter to adjust column widths automatically.
    """
    output = io.BytesIO()
    
    # Use xlsxwriter engine
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        
        # Get the xlsxwriter workbook and worksheet objects
        workbook  = writer.book
        worksheet = writer.sheets[sheet_name]
        
        # Auto-fit column widths
        for i, col in enumerate(df.columns):
            # find length of column header
            column_len = len(str(col))
            # find max length in the column data
            max_len = df[col].astype(str).map(len).max()
            
            # Use whichever is larger + some padding
            width = max(column_len, max_len) + 2
            
            # set column width
            worksheet.set_column(i, i, width)
            
    # return the byte value
    return output.getvalue()

import streamlit as st
import traceback

from utils.daily_data.file_handler import load_uploaded_file
from utils.daily_data.processor import merge_data
from utils.daily_data.exporter import generate_excel

def render():
    st.title("📊 Daily Data Manager")
    st.markdown("Process and merge VS22 and VS25 PnL and Summary files easily.")
    
    # Layout with columns for neatness
    st.subheader("1. Upload Files")
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### VS22 Files")
        vs22_pnl_file = st.file_uploader("Upload VS22 PnL File", type=['xlsx', 'xls'])
        vs22_summary_file = st.file_uploader("Upload VS22 Summary File", type=['xlsx', 'xls'])
        
    with col2:
        st.markdown("### VS25 Files")
        vs25_pnl_file = st.file_uploader("Upload VS25 PnL File", type=['xlsx', 'xls'])
        vs25_summary_file = st.file_uploader("Upload VS25 Summary File", type=['xlsx', 'xls'])
        
    st.markdown("---")
    
    if st.button("Generate Result", type="primary"):
        # Validate that at least PnL and Summary are present
        if not (vs22_pnl_file or vs25_pnl_file):
            st.error("Please upload at least one PnL file.")
            return
            
        if not (vs22_summary_file or vs25_summary_file):
            st.error("Please upload at least one Summary file.")
            return
            
        with st.spinner("Processing files..."):
            try:
                # Load files
                vs22_pnl = load_uploaded_file(vs22_pnl_file) if vs22_pnl_file else None
                vs22_summary = load_uploaded_file(vs22_summary_file) if vs22_summary_file else None
                vs25_pnl = load_uploaded_file(vs25_pnl_file) if vs25_pnl_file else None
                vs25_summary = load_uploaded_file(vs25_summary_file) if vs25_summary_file else None
                
                # Process data
                final_df = merge_data(vs22_pnl, vs25_pnl, vs22_summary, vs25_summary)
                
                st.success("✅ Files processed and merged successfully!")
                
                # Show Preview
                st.subheader("Data Preview")
                st.dataframe(final_df.head(50), use_container_width=True)
                
                # Generate Excel
                excel_bytes = generate_excel(final_df)
                
                # Download button
                st.download_button(
                    label="📥 Download Final Excel",
                    data=excel_bytes,
                    file_name="Final_Merged_Data.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                
            except Exception as e:
                st.error(f"An error occurred during processing: {str(e)}")
                with st.expander("View Error Details"):
                    st.code(traceback.format_exc())

# When loaded via st.Page, it typically expects a function to be executed if it's imported,
# or simply executes the module. TradingDashboard seems to execute the module directly.
# Let's run render()
render()

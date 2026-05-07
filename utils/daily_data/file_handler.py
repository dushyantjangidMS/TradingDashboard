import pandas as pd
import streamlit as st

def load_uploaded_file(uploaded_file):
    """
    Reads an uploaded Streamlit file into a Pandas DataFrame safely.
    Handles invalid formats and missing values gracefully.
    """
    if uploaded_file is None:
        return None

    try:
        # Load the first sheet
        df = pd.read_excel(uploaded_file)
        
        # Remove empty rows entirely
        df = df.dropna(how='all')
        
        return df
    except Exception as e:
        st.error(f"Error loading {uploaded_file.name}: {str(e)}")
        return None

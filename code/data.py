# code/data.py

import streamlit as st
import pandas as pd
import cml.data_v1 as cmldata

CONNECTION_NAME = "default-impala-aws"

@st.cache_data(ttl=600)  # Cache data for 10 minutes
def load_data(schema, table):
    """
    Connects to the data source, executes a query, and returns the data
    as a pandas DataFrame.
    """
    conn = None
    try:
        conn = cmldata.get_connection(CONNECTION_NAME)
        sql_query = f"SELECT * FROM {schema}.{table}"
        df = conn.get_pandas_dataframe(sql_query)
        return df
    except Exception as e:
        st.error(f"Failed to connect or query the data source: {e}")
        return pd.DataFrame()  # Return empty dataframe on error
    finally:
        if conn:
            conn.close()

def process_data(df):
    """Performs initial data processing and feature engineering."""
    if not df.empty and 'db_name' in df.columns and 'table_name' in df.columns:
        df['full_table_name'] = df['db_name'] + '.' + df['table_name']
        return df
    return pd.DataFrame() # Return empty if required columns are missing
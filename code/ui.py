# code/ui.py

import streamlit as st
import pandas as pd
import utils

def load_css(file_path):
    """Loads a CSS file and injects it into the Streamlit app."""
    with open(file_path) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
#
def display_custom_title():
    """Displays the custom HTML title with an image."""
    img_path = "images/iceberg.png"
    img_base64 = utils.img_to_bytes(img_path)
    title_html = f"""
    <div class="title-container">
        <img src="data:image/png;base64,{img_base64}" width="65">
        <h1 style="margin: 0; padding: 0;">Iceberg Estate Storage Analyzer</h1>
    </div>
    """
    st.markdown(title_html, unsafe_allow_html=True)
    st.markdown("An interactive dashboard to explore storage metrics of your Iceberg tables.")


def calculate_average_file_size(df):
    """Calculates the average data file size for each row and adds it as a new column."""
    if df.empty:
        return df
    df['avg_file_size_mb'] = df.apply(
        lambda row: (row['data_folder_size'] / row['data_folder_count'] / (1024**2)) if row['data_folder_count'] > 0 else 0,
        axis=1
    )
    df['data_folder_size_readable'] = df.apply(
        lambda row: (utils.format_bytes(row['data_folder_size'])) if row['data_folder_size'] > 0 else 0,
        axis=1
    )
    df['metadata_folder_size_readable'] = df.apply(
        lambda row: (utils.format_bytes(row['metadata_folder_size'])) if row['metadata_folder_size'] > 0 else 0,
        axis=1
    )
    return df

def display_sidebar_inputs(clear_callback):
    """Displays the sidebar widgets for data source input."""
    st.sidebar.header("Data Source")
    schema_name = st.sidebar.text_input("Schema Name", utils.get_table_database(), on_change=clear_callback)
    table_name = st.sidebar.text_input("Table Name", utils.get_table_name(), on_change=clear_callback)
    st.sidebar.markdown("---")
    return schema_name, table_name

def display_sidebar_filters(df, clear_callback):
    """Displays the sidebar widgets for filtering the loaded DataFrame."""
    st.sidebar.header("Filters")
    
    if df.empty:
        st.warning("Data could not be loaded or is empty. Please check the data source settings.")
        st.stop()
    
    db_options = ['All'] + sorted(df['db_name'].unique().tolist())
    
    selected_db = st.sidebar.multiselect(
        "Select Database",
        options=db_options,
        default='All',
        on_change=clear_callback,
        key="database_multiselect" 
    )
    
    if not selected_db:
        st.sidebar.warning("Please select at least one database.")
        st.stop()
        
    if 'All' in selected_db:
        filtered_df = df.copy()
    else:
        filtered_df = df[df['db_name'].isin(selected_db)].copy()
        
    # ✨ MODIFIED: The "Filter by Data Folder Size" slider and its logic have been removed.
    
    return filtered_df

def display_kpis(df):
    """Calculates and displays the main summary metrics."""
    total_tables = df.shape[0]
    total_data_size = df['data_folder_size'].sum()
    total_data_files = df['data_folder_count'].sum()
    total_metadata_size = df['metadata_folder_size'].sum()
    total_metadata_files = df['metadata_folder_count'].sum()
    total_json_size = df['metadata_file_size'].sum()
    total_json_files = df['metadata_file_count'].sum()
    total_snapshot_size = df['snapshot_file_size'].sum()
    total_snapshot_files = df['snapshot_file_count'].sum()
    total_manifest_size = df['manifest_file_size'].sum()
    total_manifest_files = df['manifest_file_count'].sum()
    total_storage = total_data_size + total_metadata_size
    total_files = total_data_files + total_metadata_files
    
    col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
    with col1:
        st.metric("Total Tables", f"{total_tables:,}")
    with col2:
        st.metric("All Files Count", f"{int(total_files):,}")
        st.metric("All Files Size", utils.format_bytes(total_storage))
    with col3:
        st.metric("Data Files Count", f"{int(total_data_files):,}")
        st.metric("Data Files Size", utils.format_bytes(total_data_size))
    with col4:
        st.metric("Metadata Files Count", f"{int(total_metadata_files):,}")
        st.metric("Metadata Files Size", utils.format_bytes(total_metadata_size))
    with col5:
        st.metric("Metadata .json Count", f"{int(total_json_files):,}")
        st.metric("Metadata .json Size", utils.format_bytes(total_json_size))
    with col6:
        st.metric("Snapshot .avro Count", f"{int(total_snapshot_files):,}")
        st.metric("Snapshot .avro Size", utils.format_bytes(total_snapshot_size))  
    with col7:
        st.metric("Manifest .avro Count", f"{int(total_manifest_files):,}")
        st.metric("Manifest .avro Size", utils.format_bytes(total_manifest_size))

def display_small_file_kpis(df, small_file_threshold):
    """Calculates and displays KPIs specifically for small files analysis."""
    small_files_df = df[df['avg_file_size_mb'] < small_file_threshold]
    small_files_table_count = small_files_df.shape[0]
    total_small_files_count = small_files_df['data_folder_count'].sum()

    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            f"Tables with Avg. File Size < {small_file_threshold}MB",
            f"{small_files_table_count:,}"
        )
    with col2:
        st.metric(
            "Total Files in these Tables",
            f"{int(total_small_files_count):,}"
        )

def display_raw_data(df):
    """Displays the raw, filtered data."""
    st.dataframe(
        df,
        column_config={
            "data_folder_size": st.column_config.NumberColumn(format="%d B"),
            "metadata_folder_size": st.column_config.NumberColumn(format="%d B"),
            "manifest_file_size": st.column_config.NumberColumn(format="%d B"),
            "metadata_file_size": st.column_config.NumberColumn(format="%d B"),
            "snapshot_file_size": st.column_config.NumberColumn(format="%d B"),
        },
        use_container_width=True
    )

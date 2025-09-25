# code/app.py

import streamlit as st
import pandas as pd
import data
import ui
import plots
import analysis

# --- Page Configuration & Setup ---
st.set_page_config(
    page_title="Iceberg Table Storage Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Apply Custom CSS ---
ui.load_css("style/main.css")

def clear_ai_analysis():
    if 'ai_analysis' in st.session_state:
        st.session_state.ai_analysis = None

schema_name, table_name = ui.display_sidebar_inputs(clear_callback=clear_ai_analysis)
raw_df = data.load_data(schema_name, table_name)
processed_df = data.process_data(raw_df)

processed_df_with_avg = ui.calculate_average_file_size(processed_df)

# ✨ MODIFIED: The sidebar no longer returns the threshold
filtered_df = ui.display_sidebar_filters(processed_df_with_avg, clear_callback=clear_ai_analysis)

# --- Main Page Rendering ---
ui.display_custom_title()

if filtered_df.empty:
    st.warning("No data matches the current filter settings.")
    st.stop()

# --- Summary Metrics (KPIs) ---
# ✨ MODIFIED: The main KPI display no longer needs the threshold
ui.display_kpis(filtered_df)
st.markdown("---")

# --- Visualizations ---
tab1, tab2, tab3, tab4, tab5 = st.tabs(["Storage Analysis", "Small Files Analysis", "Relationship Explorer", "Raw Data View", "AI Analysis"])

with tab1:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 20 Tables by Storage Size")
        ranking_option = st.radio(
            "Select ranking criteria:",
            ("Data + Metadata", "Data Only", "Metadata Only"),
            horizontal=True, key="ranking_radio", label_visibility="collapsed"
        )
        fig_bar = plots.create_top_tables_bar_chart(filtered_df, ranking_option)
        st.plotly_chart(fig_bar, use_container_width=True)
    with col2:
        st.subheader("All Databases")
        st.text("Data + Metadata Size Composition")
        st.markdown('<div style="margin-top: 15px;"></div>', unsafe_allow_html=True)
        fig_treemap = plots.create_db_composition_treemap(filtered_df)
        st.plotly_chart(fig_treemap, use_container_width=True)

with tab2:
    # ✨ NEW: The slider is now inside the tab
    slider_col, _, _ = st.columns(3)
    with slider_col:
      small_file_threshold = st.slider(
          "Set the Small File Threshold (MB) for Analysis:",
          min_value=1,
          max_value=256,
          value=128,
          key="small_file_slider_tab" # Unique key for the slider
      )

    # ✨ NEW: Display the KPIs specific to this tab's analysis
    ui.display_small_file_kpis(filtered_df, small_file_threshold)
    st.markdown("---") # Visual separator

    col1, col2 = st.columns(2)
    with col1:
        st.info("Tables in the top-left quadrant are the highest priority for compaction.")
        fig_small_files_scatter = plots.create_small_files_scatter_plot(filtered_df, small_file_threshold)
        st.plotly_chart(fig_small_files_scatter, use_container_width=True)
    with col2:
        st.info("Large, red rectangles represent tables with many small files.")
        fig_hotspot_treemap = plots.create_hotspot_treemap(filtered_df)
        st.plotly_chart(fig_hotspot_treemap, use_container_width=True)


with tab3:
    # --- Dynamic Scatter Plot ---
    st.subheader("Dynamic Relationship Explorer")
    axis_col1, axis_col2 = st.columns(2)
    with axis_col1:
        plot_options = [
            'metadata_folder_count', 'metadata_folder_size', 'data_folder_count',
            'data_folder_size', 'metadata_file_count', 'metadata_file_size',
            'snapshot_file_count', 'snapshot_file_size', 'manifest_file_count',
            'manifest_file_size'
        ]
        x_axis_selection = st.selectbox("Choose the X-axis:", options=plot_options, index=6)
    with axis_col2:
        y_axis_selection = st.selectbox("Choose the Y-axis:", options=plot_options, index=1)
    fig_scatter = plots.create_snapshot_scatter_plot(filtered_df, x_axis_selection, y_axis_selection)
    st.plotly_chart(fig_scatter, use_container_width=True)

with tab4:
    st.subheader("Raw Data")
    # This line is now correctly aligned
    st.dataframe(
        filtered_df,
        column_config={
            "data_folder_size": st.column_config.NumberColumn(format="%d B"),
            "metadata_folder_size": st.column_config.NumberColumn(format="%d B"),
            "manifest_file_size": st.column_config.NumberColumn(format="%d B"),
            "metadata_file_size": st.column_config.NumberColumn(format="%d B"),
            "snapshot_file_size": st.column_config.NumberColumn(format="%d B"),
        },
        use_container_width=True
    )
    
with tab5:
    st.subheader("🤖 Get Programmatic Analysis")
    # Initialize session state to hold the analysis text
    if 'ai_analysis' not in st.session_state:
        st.session_state.ai_analysis = None

    if st.button("Analyze Current Metrics"):
        metrics = {
            "Total Iceberg Tables": filtered_df.shape[0],
            "All Files Count": filtered_df['data_folder_count'].sum() + filtered_df['metadata_folder_count'].sum(),
            "All Files Size (TB)": (filtered_df['data_folder_size'].sum() + filtered_df['metadata_folder_size'].sum()) / (1024**4),
            "Data Files Count": filtered_df['data_folder_count'].sum(),
            "Data Files Size (TB)": filtered_df['data_folder_size'].sum() / (1024**4),
            "Metadata Files Count": filtered_df['metadata_folder_count'].sum(),
            "Metadata Files Size (TB)": filtered_df['metadata_folder_size'].sum() / (1024**4),
            "Metadata Files (.json) Count": filtered_df['metadata_file_count'].sum(),
            "Metadata Files (.json) Size (TB)": filtered_df['metadata_file_size'].sum() / (1024**4),
            "Snapshot Files (.avro) Count": filtered_df['snapshot_file_count'].sum(),
            "Snapshot Files (.avro) Size (GB)": filtered_df['snapshot_file_size'].sum() / (1024**3),
            "Manifest Files (.avro) Count": filtered_df['manifest_file_count'].sum(),
            "Manifest Files (.avro) Size (GB)": filtered_df['manifest_file_size'].sum() / (1024**3),
        }
        with st.spinner("Analyzing your Iceberg estate... This may take a moment."):
            # Store the analysis in the session state
            st.session_state.ai_analysis = analysis.get_iceberg_analysis(metrics)

# If an analysis exists in the session state, display it in an expander
if st.session_state.ai_analysis:
    with st.expander("View AI Analysis", expanded=True):
        # We no longer need the broken container here; we just render the markdown directly.
        st.markdown(st.session_state.ai_analysis)

# --- Programmatic Analysis & Raw Data View ---
# ... (rest of the code is unchanged)



## code/app.py
#
#import streamlit as st
#import pandas as pd
#import data
#import ui
#import plots
#import analysis
#
## --- Page Configuration ---
#st.set_page_config(
#    page_title="Iceberg Table Storage Analyzer",
#    page_icon="📊",
#    layout="wide",
#    initial_sidebar_state="expanded" 
#)
#
## --- Apply Custom CSS ---
#ui.load_css("style/main.css")
#
## Define the callback function to clear the analysis from the session state.
#def clear_ai_analysis():
#    """Sets the ai_analysis in the session state back to None."""
#    if 'ai_analysis' in st.session_state:
#        st.session_state.ai_analysis = None
#
## --- Sidebar and Data Loading Logic ---
## 1. Display the input widgets and get the user-provided schema and table names.
#schema_name, table_name = ui.display_sidebar_inputs(clear_callback=clear_ai_analysis)
#
## 2. Load and process the data based on those inputs.
#raw_df = data.load_data(schema_name, table_name)
#processed_df = data.process_data(raw_df)
#
## 3. Display the filter widgets using the loaded data and get the final filtered DataFrame.
#filtered_df = ui.display_sidebar_filters(processed_df, clear_callback=clear_ai_analysis)
#
## ✨ MODIFIED: Calculate avg file size once, after loading data
#processed_df_with_avg = ui.calculate_average_file_size(processed_df)
#
#filtered_df, small_file_threshold = ui.display_sidebar_filters(processed_df_with_avg, clear_callback=clear_ai_analysis)
#
#
#
## --- Main Page Rendering ---
#ui.display_custom_title()
#
## This check is now more robust. It verifies if anything remains after filtering.
#if filtered_df.empty:
#    st.warning("No data matches the current filter settings. Please adjust the filters in the sidebar.")
#    st.stop()
#
## --- Summary Metrics (KPIs) ---
#ui.display_kpis(filtered_df, small_file_threshold)
##st.markdown("---")
#
## --- Add the new section for programmatic analysis ---
##st.markdown("---")
#st.subheader("🤖 Get Programmatic Analysis")
#
#
## Initialize session state to hold the analysis text
#if 'ai_analysis' not in st.session_state:
#    st.session_state.ai_analysis = None
#
#if st.button("Analyze Current Metrics"):
#    metrics = {
#        "Total Iceberg Tables": filtered_df.shape[0],
#        "All Files Count": filtered_df['data_folder_count'].sum() + filtered_df['metadata_folder_count'].sum(),
#        "All Files Size (TB)": (filtered_df['data_folder_size'].sum() + filtered_df['metadata_folder_size'].sum()) / (1024**4),
#        "Data Files Count": filtered_df['data_folder_count'].sum(),
#        "Data Files Size (TB)": filtered_df['data_folder_size'].sum() / (1024**4),
#        "Metadata Files Count": filtered_df['metadata_folder_count'].sum(),
#        "Metadata Files Size (TB)": filtered_df['metadata_folder_size'].sum() / (1024**4),
#        "Metadata Files (.json) Count": filtered_df['metadata_file_count'].sum(),
#        "Metadata Files (.json) Size (TB)": filtered_df['metadata_file_size'].sum() / (1024**4),
#        "Snapshot Files (.avro) Count": filtered_df['snapshot_file_count'].sum(),
#        "Snapshot Files (.avro) Size (GB)": filtered_df['snapshot_file_size'].sum() / (1024**3),
#        "Manifest Files (.avro) Count": filtered_df['manifest_file_count'].sum(),
#        "Manifest Files (.avro) Size (GB)": filtered_df['manifest_file_size'].sum() / (1024**3),
#    }
#    with st.spinner("Analyzing your Iceberg estate... This may take a moment."):
#        # Store the analysis in the session state
#        st.session_state.ai_analysis = analysis.get_iceberg_analysis(metrics)
#
## If an analysis exists in the session state, display it in an expander
#if st.session_state.ai_analysis:
#    with st.expander("View AI Analysis", expanded=True):
#        # We no longer need the broken container here; we just render the markdown directly.
#        st.markdown(st.session_state.ai_analysis)
#
#        
#st.markdown("---")    
#
#tab1, tab2, tab3 = st.tabs(["Storage Analysis", "Small Files Analysis", "Relationship Explorer"])
#
#with tab1:
#    col1, col2 = st.columns(2)
#    with col1:
#        st.subheader("Top 20 Tables by Storage Size")
#        ranking_option = st.radio(
#            "Select ranking criteria:",
#            ("Data + Metadata", "Data Only", "Metadata Only"),
#            horizontal=True, key="ranking_radio"
#        )
#        fig_bar = plots.create_top_tables_bar_chart(filtered_df, ranking_option)
#        st.plotly_chart(fig_bar, use_container_width=True)
#    with col2:
#        st.subheader("Database Composition")
#        fig_treemap = plots.create_db_composition_treemap(filtered_df)
#        st.plotly_chart(fig_treemap, use_container_width=True)
#
#with tab2:
#    # ✨ NEW: Small Files Analysis Chart
#    st.subheader("Top 20 Tables with Smallest Average File Size")
#    fig_small_files = plots.create_small_files_bar_chart(filtered_df, small_file_threshold)
#    st.plotly_chart(fig_small_files, use_container_width=True)
#
#with tab3:
#    # --- Dynamic Scatter Plot ---
#    st.subheader("Dynamic Relationship Explorer")
#    ## ✨ NEW: Added dropdowns for user to select axes.
#    axis_col1, axis_col2, empty3, empty4, empty5, empty6 = st.columns(6)
#
#    with axis_col1:
#        plot_options = [
#            'metadata_folder_count', 'metadata_folder_size', 'data_folder_count',
#            'data_folder_size', 'metadata_file_count', 'metadata_file_size',
#            'snapshot_file_count', 'snapshot_file_size', 'manifest_file_count',
#            'manifest_file_size'
#        ]
#        x_axis_selection = st.selectbox(
#            "Choose the X-axis:",
#            options=plot_options,
#            index=6 # Default to 'snapshot_file_count'
#        )
#
#    with axis_col2:
#        y_axis_selection = st.selectbox(
#            "Choose the Y-axis:",
#            options=plot_options,
#            index=1 # Default to 'metadata_folder_size'
#        )
#
##col1, col2, col3, col4 = st.columns(4)
##with col1:
##    ranking_option = st.radio(
##        "Select ranking criteria:",
##        ("Data + Metadata", "Data Only", "Metadata Only"),
##        horizontal=True, key="ranking_radio", label_visibility="hidden"
##    )
##
### --- Visualizations ---
##col1, col2 = st.columns(2)
##
##with col1:
##    st.subheader("Top 20 Tables by Storage Size")
###    ranking_option = st.radio(
###        "Select ranking criteria:",
###        ("Data + Metadata", "Data Only", "Metadata Only"),
###        horizontal=True, key="ranking_radio", label_visibility="hidden"
###    )
##    fig_bar = plots.create_top_tables_bar_chart(filtered_df, ranking_option)
##    st.plotly_chart(fig_bar, use_container_width=True)
##
##with col2:
##    st.subheader("All Databases: Data + Metadata Size Composition")
##    fig_treemap = plots.create_db_composition_treemap(filtered_df)
##    st.plotly_chart(fig_treemap, use_container_width=True)
##
##st.markdown("---")
##
#### Scatter Plot
###fig_scatter = plots.create_snapshot_scatter_plot(filtered_df)
###st.plotly_chart(fig_scatter, use_container_width=True)
##
### --- Dynamic Scatter Plot ---
##st.subheader("Dynamic Relationship Explorer")
##
### ✨ NEW: Added dropdowns for user to select axes.
##axis_col1, axis_col2, empty3, empty4, empty5, empty6 = st.columns(6)
##
##with axis_col1:
##    plot_options = [
##        'metadata_folder_count', 'metadata_folder_size', 'data_folder_count',
##        'data_folder_size', 'metadata_file_count', 'metadata_file_size',
##        'snapshot_file_count', 'snapshot_file_size', 'manifest_file_count',
##        'manifest_file_size'
##    ]
##    x_axis_selection = st.selectbox(
##        "Choose the X-axis:",
##        options=plot_options,
##        index=6 # Default to 'snapshot_file_count'
##    )
##
##with axis_col2:
##    y_axis_selection = st.selectbox(
##        "Choose the Y-axis:",
##        options=plot_options,
##        index=1 # Default to 'metadata_folder_size'
##    )
##
### ✨ MODIFIED: Pass the user's axis selections to the plot function.
##fig_scatter = plots.create_snapshot_scatter_plot(filtered_df, x_axis_selection, y_axis_selection)
##st.plotly_chart(fig_scatter, use_container_width=True)
#
## Raw Data View
#st.markdown("---")
#ui.display_raw_data_expander(filtered_df)
#    
#    
#

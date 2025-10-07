import streamlit as st
import pandas as pd
import data
import ui
import utils
import plots
import analysis
from pathlib import Path
import os
import json 

def load_local_css(file_path):
    """Loads a local CSS file and injects it into the Streamlit app."""
    try:
        with open(file_path) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        st.error(f"CSS file not found at {file_path}. Please check the file path.")

# --- Page Configuration & Setup ---
st.set_page_config(
    page_title="Iceberg Estate Storage Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded")  
  
  # The options you want to display
options = ["Data + Metadata", "Data Only", "Metadata Only"]

# 1. Initialize the state on the first run.
#    This sets the first option to True (checked) and all others to False.
if 'checkbox_states_initialized' not in st.session_state:
    for i, option in enumerate(options):
        st.session_state[option] = (i == 0)
    st.session_state.checkbox_states_initialized = True

# 2. Define the callback function to enforce mutual exclusivity.
def handle_checkbox_change(changed_option):
    """
    This function is called whenever any checkbox is changed.
    It ensures only one checkbox is checked at a time.
    """
    # If the user checks a box, its state becomes True.
    if st.session_state[changed_option]:
        # Uncheck all other boxes by setting their state to False.
        for option in options:
            if option != changed_option:
                st.session_state[option] = False
    # If the user tries to uncheck the currently selected box...
    else:
        # ...force it to remain checked. This mimics radio button behavior
        # where one option must always be selected.
        st.session_state[changed_option] = True


# --- Apply Custom CSS using the new robust function ---
CSS_PATH = "style/main.css"
load_local_css(CSS_PATH)

# --- Apply Custom CSS ---
# Ensure your style/main.css contains the new rules for st.radio
ui.load_css("style/main.css")

def clear_ai_analysis():
    if 'ai_analysis' in st.session_state:
        st.session_state.ai_analysis = None

# --- Initialize Session State for LLM Config ---
if 'llm_prompt' not in st.session_state:
    st.session_state.llm_prompt = analysis.DEFAULT_PROMPT
if 'llm_endpoint' not in st.session_state:
    st.session_state.llm_endpoint = "https://api.openai.com/v1/chat/completions"
if 'llm_api_key' not in st.session_state:
    st.session_state.llm_api_key = ""
if 'llm_model_name' not in st.session_state:
    st.session_state.llm_model_name = "gpt-4o-mini"

# --- Data Loading and Processing ---
schema_name, table_name = ui.display_sidebar_inputs(clear_callback=clear_ai_analysis)
raw_df = data.load_data(schema_name, table_name)
processed_df = data.process_data(raw_df)

processed_df_with_avg = ui.calculate_average_file_size(processed_df)
filtered_df = ui.display_sidebar_filters(processed_df_with_avg, clear_callback=clear_ai_analysis)

# --- Main Page Rendering ---
ui.display_custom_title()

if filtered_df.empty:
    st.warning("No data matches the current filter settings.")
    st.stop()

# --- Summary Metrics (KPIs) ---
ui.display_kpis(filtered_df)
st.markdown("---")

# --- Tab Management with a Styled st.radio Widget ---
TABS = ["Storage Analysis", "Small Files Analysis", "Relationship Explorer", "Raw Data View", "AI Analysis", "⚙️ LLM Configuration"]

st.radio(
    "Select a tab:",
    options=TABS,
    key='active_tab',  # The key to store the selected tab in session state
    horizontal=True, 
    label_visibility='collapsed' # Hides the "Select a tab:" label
)

# --- Page Content (Controlled by session state) ---

if st.session_state.active_tab == "Storage Analysis":
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top 20 Tables by Storage Size")

        # --- THE FIX: Move this block BEFORE the checkboxes are created ---
        # 1. Defensively check and restore the state if it was lost.
        selected_options = [option for option in options if st.session_state.get(option, False)]
        if not selected_options:
            st.session_state[options[0]] = True
            st.rerun() # Rerun to apply the fix before drawing the widgets below.

        # 2. Now that the state is valid, create the checkboxes.
        column_spec = [1.2, 1, 1.2, 4] 
        cols = st.columns(column_spec, gap="small")

        for i, option in enumerate(options):
            with cols[i]:
                st.checkbox(
                    option,
                    key=option,
                    on_change=handle_checkbox_change,
                    args=(option,)
                )

        # 3. Get the selected option to use for the chart. This is now safe.
        ranking_option = [option for option in options if st.session_state[option]][0]

        # 4. Create and display the chart.
        fig_bar = plots.create_top_tables_bar_chart(filtered_df, ranking_option)
        st.plotly_chart(fig_bar, use_container_width=True) 
      
    with col2:
        st.subheader("All Databases")
        st.text("Data + Metadata Size Composition")
        fig_treemap = plots.create_db_composition_treemap(filtered_df)
        st.plotly_chart(fig_treemap, use_container_width=True)

elif st.session_state.active_tab == "Small Files Analysis":
#    slider_col, _, _ = st.columns(3)
#    with slider_col:
#        small_file_threshold = st.slider(
#            "Set the Small File Threshold (MB) for Analysis:",
#            min_value=1,
#            max_value=256,
#            value=128,
#            key="small_file_slider_tab"
#        )
#
#    ui.display_small_file_kpis(filtered_df, small_file_threshold)
#    st.markdown("---")

    col1, col2, _ = st.columns(3)
    with col2:
        small_file_threshold = st.slider(
            "Set the Small File Threshold (MB) for Analysis:",
            min_value=1,
            max_value=256,
            value=128,
            key="small_file_slider_tab"
        )  
    with col1:
        ui.display_small_file_kpis(filtered_df, small_file_threshold)

    col1,col2 = st.columns([2, 1])
    with col1:
        fig_small_files_scatter = plots.create_small_files_scatter_plot(filtered_df, small_file_threshold)
        st.plotly_chart(fig_small_files_scatter, use_container_width=True, config = {'height': 800})
    with col2:
        fig_hotspot_treemap = plots.create_hotspot_treemap(filtered_df)
        st.plotly_chart(fig_hotspot_treemap, use_container_width=True)

elif st.session_state.active_tab == "Relationship Explorer":
#    st.subheader("Dynamic Relationship Explorer")
    axis_col1, axis_col2, _, _, _, _ = st.columns(6)
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

elif st.session_state.active_tab == "Raw Data View":
#    st.subheader("Raw Data")
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


elif st.session_state.active_tab == "AI Analysis":
 
    if 'ai_analysis' not in st.session_state:
        st.session_state.ai_analysis = None

    column_spec = [1, 1.5, 2, 4]  # Adjusted column spec for radio button
    cols = st.columns(column_spec, vertical_alignment="bottom")


    with cols[0]:
        model_choice = st.radio(
            "Select Model Source", # This label is hidden but good for context
            options=["Default", "Use CAAI Model"],
            horizontal=True,
            label_visibility="collapsed" # Hides the label for a cleaner UI
        )
    
    # This boolean flag is now derived from the radio button's selection
    use_caai = (model_choice == "Use CAAI Model")

    # 3. Place the dropdown (or handle the 'else' case) in the third column
    with cols[1]:
        if use_caai:
            # --- CAAI MODEL SELECTION ---
            caai_models = utils.get_caai_models()
            if caai_models:
                display_names = list(caai_models.keys())
                selected_display_name = st.selectbox(
                    "Select a CAAI Model Endpoint:",
                    options=display_names,
                    label_visibility="collapsed" # Hide label for a cleaner look
                )
                # Get the technical details for the selected model
                model_to_use = caai_models[selected_display_name]['model_name']
                endpoint_to_use = caai_models[selected_display_name]['url']
            else:
                st.warning("Could not load CAAI models.")
                model_to_use = ""
                endpoint_to_use = ""
        else:
            # --- ORIGINAL OPENAI MODEL SELECTION ---
            # When "Default" is selected, we use the values from the Project Enviornments Variables tab.
            model_to_use = utils.get_default_llm()
            endpoint_to_use = utils.get_default_llm_endpoint()
            

    # --- Analyze Button and Logic ---
    col1, col2, _ = st.columns([1,1,6], gap="small")
    with col1:
        analyze_button = st.button("Analyze Current Metrics")

    with col2:
        # Display the model that will be used
        st.markdown(f"<div style='padding-top: 5px;'>Using model: <strong>{model_to_use}</strong></div>", unsafe_allow_html=True)

    if analyze_button:
        # Use the correct API key based on the selection
        api_key = os.environ["DEFAULT_AI_API_KEY"] if not use_caai else json.load(open("/tmp/jwt"))["access_token"]

        
        if not use_caai and not api_key:
            st.error("Default API Key is not configured. Please add it (DEFAULT_AI_API_KEY) in the Project Environment Variables.")
        elif not use_caai and not api_key:
            st.error("Default Model is not configured. Please add it (DEFAULT_AI_MODEL) in the Project Environment Variables.")
        elif not use_caai and not api_key:
          st.error("Default Model Endpoint is not configured. Please add it (DEFAULT_AI_ENDPOINT_URL) in the Project Environment Variables.")
        else:
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
            with st.spinner("Analyzing your Iceberg estate..."):
                st.session_state.ai_analysis = analysis.get_iceberg_analysis(
                    metrics,
                    prompt_template=st.session_state.llm_prompt,
                    api_endpoint=endpoint_to_use,
                    api_key=api_key, # Pass the relevant key
                    model_name=model_to_use
                )

    if st.session_state.ai_analysis:
        with st.expander("View AI Analysis", expanded=True):
            st.markdown(st.session_state.ai_analysis)

elif st.session_state.active_tab == "⚙️ LLM Configuration":
    st.subheader("Default LLM Configuration")
    st.info("This is the default LLM configuration from the Project Enviornment Variables.")

    st.session_state.llm_model_name = st.text_input(
        "Model Name",
        value = utils.get_default_llm()
    )
    
    st.session_state.llm_endpoint = st.text_input(
        "Model API Endpoint",
        value = utils.get_default_llm_endpoint()
    )


    st.session_state.llm_prompt = st.text_area(
        "LLM Prompt",
        height=400,
        value=st.session_state.get('llm_prompt', '')
    )
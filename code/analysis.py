import streamlit as st
import requests
import json

# The endpoint for the OpenAI API
API_ENDPOINT = "https://api.openai.com/v1/chat/completions"
MODEL_NAME = "gpt-4o-mini" # A powerful and cost-effective model for this task

def get_iceberg_analysis(metrics_dict):
    """
    Sends metrics to the OpenAI API and returns a detailed analysis.
    """
    # 1. Get the API Key from Streamlit secrets
    api_key = st.secrets.get("OPENAI_API_KEY")
    if not api_key:
        st.error("OpenAI API Key not found. Please add it to your secrets.toml file.")
        st.code("OPENAI_API_KEY = \"YOUR_API_KEY_HERE\"", language="toml")
        return None

    # This correctly converts numpy numbers to native Python types
    cleaned_metrics_dict = {k: v.item() if hasattr(v, 'item') else v for k, v in metrics_dict.items()}

    # 2. Craft the Detailed Prompt
    prompt = f"""
    You are an expert data architect specializing in big data systems and Apache Iceberg.
    Your task is to provide a detailed, visually appealing analysis of the following metrics from an entire Iceberg estate.

    ---
    **✨ NEW: Key Principles of a Healthy Iceberg Estate (Your Guiding Rules)**
    1.  **Metadata-to-Data Ratio:** The total size of metadata should be a very small fraction of the total data size (ideally less than 5%). A ratio approaching 1:1 (which is 100%) or higher is a critical red flag indicating severe metadata bloat, which slows down query planning.
    2.  **Optimal File Size:** The average data file size should be between 128MB and 512MB. An average size in the KB range indicates a severe "small file problem," which kills query performance due to I/O overhead.
    ---

    **Metrics Data to Analyze:**
    ```json
    {json.dumps(cleaned_metrics_dict, indent=2)}
    ```

    **Your Analysis Structure**
    Please structure your response in Markdown with the following sections, using emojis in the headers as shown:
    1.  **📊 Executive Summary:** A brief, high-level overview of the estate's health, focusing on the most critical issues.
    2.  **🔬 Detailed Analysis:** In-depth observations. **You must apply the guiding principles above.**
        * Show the provided metrics.
        * Calculate the metadata-to-data ratio and explicitly state whether it is healthy or unhealthy. 
        * Calculate the average data file size and and state clearly whether it is healthy or indicates a 'small file problem'.
    3.  **🤔 Probable Root Causes:** Explain why these issues are likely occurring.
    4.  **✅ Actionable Recommendations:** Provide a prioritized list of specific actions. Include a Markdown table summarizing the projected state before and after the recommended actions. 
    5.  **🎯 Outcomes: Explain the positive impact by following the Actionable Recommendations that can be made by reducing the number and total size of Iceberg data files and metadata files in terms of Cloud Storage costs, Cloud Storage API costs and improved performamnce of Analytic Engines like Hive, Impala and Trino.
    
    **Important Formatting Instruction:** Do not use LaTeX. Present all calculations and results as plain text only (e.g., "3.67 TB divided by 2.51 TB").
    """

    # 3. Format the Request Headers and Payload for OpenAI
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    payload = {
        "model": MODEL_NAME,
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0.2, # Lower temperature for more deterministic, analytical responses
    }

    # 4. Make the API Call
    try:
        response = requests.post(API_ENDPOINT, headers=headers, json=payload)
        response.raise_for_status()

        # 5. Extract the Analysis Text
        result = response.json()
        analysis_text = result['choices'][0]['message']['content']
        return analysis_text

    except requests.exceptions.RequestException as e:
        st.error(f"An error occurred while contacting the OpenAI API: {e}")
        st.error(f"Response Body: {response.text}")
        return None
    except (KeyError, IndexError):
        st.error("Could not parse the API response. The response format may have changed or an error occurred.")
        st.error(f"Full API Response: {response.json()}")
        return None
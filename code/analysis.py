import streamlit as st
import requests
import json

# A powerful and cost-effective model for this task
MODEL_NAME = "gpt-4o-mini"

# The default prompt is now a constant to be used for initialization
DEFAULT_PROMPT = f"""
You are an expert data architect specializing in big data systems and Apache Iceberg.
Your task is to provide a detailed, visually appealing analysis of the following metrics from an entire Iceberg estate.

---
**✨ NEW: Key Principles of a Healthy Iceberg Estate (Your Guiding Rules)**
1.  **Metadata-to-Data Ratio:** The total size of metadata should be a very small fraction of the total data size (ideally less than 5%). A ratio approaching 1:1 (which is 100%) or higher is a critical red flag indicating severe metadata bloat, which slows down query planning.
2.  **Optimal File Size:** The average data file size should be between 128MB and 512MB. An average size in the KB range indicates a severe "small file problem," which kills query performance due to I/O overhead.
---

**Metrics Data to Analyze:**
```json
{{metrics_json}}
```

Your Analysis Structure
Please structure your response in Markdown with the following sections, using emojis in the headers as shown:

📊 Executive Summary: A brief, high-level overview of the estate's health, focusing on the most critical issues.

🔬 Detailed Analysis: In-depth observations. You must apply the guiding principles above.

Show the provided metrics.

Calculate the metadata-to-data ratio and explicitly state whether it is healthy or unhealthy.

Calculate the average data file size and and state clearly whether it is healthy or indicates a 'small file problem'.

🤔 Probable Root Causes: Explain why these issues are likely occurring.

✅ Actionable Recommendations: Provide a prioritized list of specific actions. Include a Markdown table summarizing the projected state before and after the recommended actions.

**🎯 Outcomes: Explain the positive impact by following the Actionable Recommendations that can be made by reducing the number and total size of Iceberg data files and metadata files in terms of Cloud Storage costs, Cloud Storage API costs and improved performamnce of Analytic Engines like Hive, Impala and Trino.

Important Formatting Instruction: Do not use LaTeX. Present all calculations and results as plain text only (e.g., "3.67 TB divided by 2.51 TB").
"""

def get_iceberg_analysis(metrics_dict, prompt_template, api_endpoint, api_key, model_name):
    """
    Sends metrics to the OpenAI API and returns a detailed analysis.

    Args:
        metrics_dict (dict): A dictionary containing the iceberg metrics.
        prompt_template (str): The template for the LLM prompt.
        api_endpoint (str): The API endpoint for the LLM.
        api_key (str): The API key for authentication.
        model_name (str): The name of the OpenAI model to use.
    """
    if not api_key:
        st.error("OpenAI API Key not found. Please configure it in the 'Configuration' tab or in your Streamlit secrets.")
        return None

    # This correctly converts numpy numbers to native Python types
    cleaned_metrics_dict = {k: v.item() if hasattr(v, 'item') else v for k, v in metrics_dict.items()}
    metrics_json = json.dumps(cleaned_metrics_dict, indent=2)

    prompt = prompt_template.replace("{metrics_json}", metrics_json)

    # Format the Request Headers and Payload for OpenAI
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    payload = {
        "model": model_name, # <-- MODIFIED: Use the model_name parameter
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0.2, # Lower temperature for more deterministic, analytical responses
    }

    # Initialize response to None before the try block
    response = None
    
    # Make the API Call
    try:
        response = requests.post(api_endpoint, headers=headers, json=payload, timeout=90)
        response.raise_for_status()

        # Extract the Analysis Text
        result = response.json()
        analysis_text = result['choices'][0]['message']['content']
        return analysis_text

    except requests.exceptions.RequestException as e:
        st.error(f"An error occurred while contacting the OpenAI API: {e}")
        if response is not None:
            st.error(f"Response Body: {response.text}")
        return None
    except (KeyError, IndexError):
        st.error("Could not parse the API response. The response format may have changed or an error occurred.")
        if response is not None:
            st.error(f"Full API Response: {response.json()}")
        return None


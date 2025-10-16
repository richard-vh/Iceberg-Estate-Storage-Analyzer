# code/utils.py

import base64
from pathlib import Path
import numpy as np
import streamlit as st
import requests
import json
import os

def img_to_bytes(img_path):
    """Encodes a local image file to a base64 string."""
    img_bytes = Path(img_path).read_bytes()
    encoded = base64.b64encode(img_bytes).decode()
    return encoded

def format_bytes(size_bytes):
    """Converts bytes to a human-readable format (KB, MB, GB, etc.)."""
    if size_bytes == 0:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(np.floor(np.log(size_bytes) / np.log(1024)))
    p = np.power(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"
  
@st.cache_data(ttl=600) # Cache the results for 10 minutes
def get_caai_models():
    """Fetches the list of CAAI model endpoints and returns a dictionary."""
    try:
        domain = get_caii_domain()
        url = f'https://{domain}/api/v1alpha1/listEndpoints'
        
        # Safely read the JWT token
        with open("/tmp/jwt") as f:
            access_token = json.load(f)["access_token"]
        
        headers = {'authorization': f'Bearer {access_token}'}
        payload = {'namespace': 'serving-default'}
        
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        
        data = response.json()
        endpoints_list = data.get('endpoints', [])
        
        # Format the data for the dropdown: {display_name: {model_name: ..., url: ...}}
        models_dict = {
            endpoint.get('name'): {
                'model_name': endpoint.get('model_name'),
                'url': endpoint.get('url')
            }
            for endpoint in endpoints_list
        }
        return models_dict
        
    except FileNotFoundError:
        st.error("JWT token file not found at /tmp/jwt. Cannot authenticate with CAAI.")
        return {}
    except KeyError:
        st.error("CAII_DOMAIN environment variable is not set.")
        return {}
    except requests.exceptions.HTTPError as http_err:
        st.error(f"HTTP error occurred while fetching models: {http_err}")
        return {}
    except Exception as err:
        st.error(f"An error occurred: {err}")
        return {}
      
def get_default_llm():
    try:
        model_to_use = os.environ["DEFAULT_AI_MODEL"] 
        return model_to_use
    except KeyError as e:
#        st.error(f"Required environment variable {e} is not set.")
        return {}
        
def get_default_llm_endpoint():
    try:
        endpoint_to_use = os.environ["DEFAULT_AI_ENDPOINT_URL"] 
        return endpoint_to_use
    except KeyError as e:
#        st.error(f"Required environment variable {e} is not set.")
        return {}
      
def get_default_ai_api_key():
    try:
        endpoint_to_use = os.environ["DEFAULT_AI_API_KEY"] 
        return endpoint_to_use
    except KeyError as e:
#        st.error(f"Required environment variable {e} is not set.")
        return {}
      
def get_caii_domain():
    try:
        endpoint_to_use = os.environ["CAII_DOMAIN"] 
        return endpoint_to_use
    except KeyError as e:
#        st.error(f"Required environment variable {e} is not set.")
        return {}
      
def get_impala_conn():
    try:
        endpoint_to_use = os.environ["IMPALA_CONNECTION_NAME"] 
        return endpoint_to_use
    except KeyError as e:
        st.error(f"Required environment variable {e} is not set.")
        return {}
      
def get_table_database():
    try:
        endpoint_to_use = os.environ["SOURCE_TABLE_DATABASE"] 
        return endpoint_to_use
    except KeyError as e:
        st.error(f"Required environment variable {e} is not set.")
        return {}
      
def get_table_name():
    try:
        endpoint_to_use = os.environ["SOURCE_TABLE_NAME"] 
        return endpoint_to_use
    except KeyError as e:
        st.error(f"Required environment variable {e} is not set.")
        return {}
      
# ![](/images/iceberg_small.png) Iceberg Estate Storage Analyzer

An interactive Streamlit dashboard designed to explore and analyze the storage metrics of Apache Iceberg tables. This tool provides visualizations to help you understand storage composition, identify large tables, and analyze metadata overhead and small files issues.

![alt text](/images/readme_imageapp.png)

![alt text](/images/readme_imageapp2.png)

![alt text](/images/readme_imageapp3.png)

---

## Key Features

-   **Summary KPIs**: View high-level Iceberg metrics at a glance, including total tables, total storage size, and file counts.
-   **Database filter**: Focus in on individual or specific groups of databases.
-   **Interactive Visualizations**:
    -   **Storage Analysis**:Top 20 tables by storage size and databases composition (Data vs. Metadata) visualizations.
    -   **Small Files Analysis**: Iceberg table small files visualization
    -   **Metric Relationship Explorer**: Visualize Iceberg table metrics against each other.
    -   **Raw Data View**: Table with raw view ofIceberg metrics - view, filter, sort.
    -   **AI Analysis**: Use your choice of LLM to analyze Iceberg metric context and make suggestions.
-   **Custom Styling**: A clean, modern UI with a custom stylesheet.

---

## Project Structure

The project is organized into a modular structure to separate concerns, making it easy to maintain and extend.

```
iceberg-analyzer-app/
├── 🗂️ code/
│   ├── app.py              # Main application script
│   ├── data.py             # Data loading and processing
│   ├── plots.py            # Visualization functions
│   ├── ui.py               # UI components (sidebar, KPIs, etc.)
│   └── utils.py            # Helper functions
├── 🗂️ iceberg_analytics_extract/
│   └── iceberg_extract_analytics.py   # Extract Datalake Iceberg metrics
├── 🗂️ images/
│   └── iceberg.png      # Image assets
├── 🗂️ style/
│   └── main.css         # Custom stylesheet
├── README.md            # This file
└── requirements.txt     # Project dependencies
```

---

## Setup and Installation

Follow these steps to get the application running on your local machine.

### Prerequisites

-   Python 3.8+
-   Access to the Impala data source where the table metrics are stored.
-   Run the iceberg_extract_analytics.py process first. The target table that the data is written to by this process is used as the source table by this Streamlit application below. 
    The database and table name used in this process is configured in the CAI project settings as the SOURCE_TABLE_DATABASE and SOURCE_TABLE_NAME respectively (see below).

### Installation Steps

1.  Create a new project in Cloudera AI. Give your Project a name and and choose Git for Initial Setup. Supply this Git Repo URL and choose a PBJ Workbench of Python 3.8 or greater:

    ![alt text](/images/readme_image1.png)

2.  Start a Python session and install the Python packages in the requirements.txt file:

    ![alt text](/images/readme_image2.png)


3. Set the required project enviornment variables in your CAI project sesstings:

 - **Required**
    - IMPALA_CONNECTION_NAME: e.g. `default-impala-aws`
    - SOURCE_TABLE_DATABASE: e.g. `iceberg_metrics_db`
    - SOURCE_TABLE_NAME: e.g. `iceberg_table_metrics`
 - **Optional: If you want to use Cloudera AI Inference deployed models**
    - CAII_DOMAIN : e.g. `ml-64288c66-5dd.my-env.ylcu-atmi.cloudera.site`
 - **Optional: If you want to use a default model like OpenAI ChatGPT**
    - DEFAULT_AI_API_KEY : e.g.`sk-proj-2OurRGz_PbkxkP4y1w2wI2rUUD0I-sgnZBixivL4fET3Blbk`
    - DEFAULT_AI_ENDPOINT_URL : e.g. `https://api.openai.com/v1/chat/completions`
    - DEFAULT_AI_MODEL : e.g. `gpt-4o-mini`
   
   
  ![alt text](/images/readme_image3.png)



---

## How to Run the Application

In your CAI project select Applications in the menu and Create New Application. Give your application a name, subdomain and select the provided app_deploy.py file. Choose your your PBJ Workbench Python version, select resources required and create the application:

![alt text](/images/readme_image4.png)


Once the application is running click on it to open it in your browser.

![alt text](/images/readme_image5.png)


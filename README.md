# 📊 Iceberg Table Storage Analyzer

An interactive Streamlit dashboard designed to explore and analyze the storage metrics of Apache Iceberg tables. This tool provides visualizations to help you understand storage composition, identify large tables, and analyze metadata overhead.

![Screenshot of the Iceberg Analyzer Dashboard]

*(Suggestion: Add a screenshot of your running application here for a more engaging README.)*

---

## ## Key Features

-   **Dynamic Data Connection**: Connects to an Impala data source to fetch the latest storage metrics.
-   **Summary KPIs**: View high-level metrics at a glance, including total tables, total storage size, and file counts.
-   **Interactive Visualizations**:
    -   **Top Tables Chart**: A bar chart showing the top 20 tables by storage size (Data vs. Metadata).
    -   **Database Treemap**: A treemap visualizing the storage composition of each database.
    -   **Snapshot Analysis**: A scatter plot correlating metadata size with the number of snapshot files.
-   **Powerful Filtering**: Interactively filter the data by database and table size.
-   **Custom Styling**: A clean, modern UI with a custom stylesheet.

---

## ## Project Structure

The project is organized into a modular structure to separate concerns, making it easy to maintain and extend.

```
iceberg_analyzer/
├── 📂 code/
│   ├── app.py              # Main application script
│   ├── data.py             # Data loading and processing
│   ├── plots.py            # Visualization functions
│   ├── ui.py               # UI components (sidebar, KPIs, etc.)
│   └── utils.py            # Helper functions
├── 📂 images/
│   └── iceberg.png         # Image assets
├── 📂 style/
│   └── main.css            # Custom stylesheet
├── 📄 README.md              # This file
└── 📄 requirements.txt      # Project dependencies
```

---

## ## Setup and Installation

Follow these steps to get the application running on your local machine.

### ### Prerequisites

-   Python 3.8+
-   Access to the Impala data source where the table metrics are stored.

### ### Installation Steps

1.  **Clone the repository:**
    ```bash
    git clone <your-repository-url>
    cd iceberg_analyzer
    ```

2.  **Create and activate a virtual environment (recommended):**
    ```bash
    # For Mac/Linux
    python3 -m venv venv
    source venv/bin/activate

    # For Windows
    python -m venv venv
    venv\Scripts\activate
    ```

3.  **Install the required packages:**
    ```bash
    pip install -r requirements.txt
    ```

---

## ## How to Run the Application

Execute the following command from the **root directory** of the project (`iceberg_analyzer/`):

```bash
streamlit run code/app.py
```

Your web browser should open a new tab with the running application.

---

## ## Configuration

To adapt the application to your environment, you may need to modify the following:

-   **Data Connection**: In `code/data.py`, change the `CONNECTION_NAME` constant to match your CML Data Connection name.
    ```python
    # code/data.py
    CONNECTION_NAME = "your-connection-name"
    ```
-   **Default Schema/Table**: In `code/ui.py`, you can change the default values for the schema and table name in the sidebar.
    ```python
    # code/ui.py
    schema_name = st.sidebar.text_input("Schema Name", "your_default_schema")
    table_name = st.sidebar.text_input("Table Name", "your_default_table")
    ```
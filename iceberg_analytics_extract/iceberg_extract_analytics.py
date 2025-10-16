# iceberg_analytics_erxtract/iceberg_extract_analytics.py

import os
import time
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.utils import AnalysisException
import cml.data_v1 as cmldata

# --- Constants for SQL Queries and Configuration ---

# Hive is required to get a list of Iceberg tables
HIVE_CONNECTION_NAME = "<specify_your_hive_connection_name>" 
# Spark is used for processing the metrics for the Iceberg tables
SPARK_CONNECTION_NAME = "<specify_your_spark_connection_name>"
# This will be the table where the analysis data is stored. Use this table as the source table for the Streamlit app (configured in CAI Project setting - see README.md)
RESULTS_TABLE = "<specify_your_db.tablename>"

# Query to find all Iceberg tables. This will be run via the Hive cursor.
ICEBERG_TABLES_QUERY = """
    SELECT
        tb.tbl_id AS tbl_id,
        db.name AS db_name,
        tb.tbl_name AS tbl_name,
        split(tp.param_value, '/metadata/')[0] AS metadata_path
    FROM sys.table_params AS tp
    JOIN sys.tbls AS tb ON tb.tbl_id = tp.tbl_id
    JOIN sys.dbs AS db ON db.db_id = tb.db_id
    WHERE tp.param_key = 'metadata_location'
      AND EXISTS (
          SELECT 1 FROM sys.table_params
          WHERE tbl_id = tp.tbl_id
            AND param_key = 'table_type'
            AND param_value = 'ICEBERG'
      )
"""

RESULTS_SCHEMA = StructType([
    StructField("tbl_id", LongType(), True),
    StructField("db_name", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("metadata_folder_count", LongType(), True),
    StructField("metadata_folder_size", LongType(), True),
    StructField("data_folder_count", LongType(), True),
    StructField("data_folder_size", LongType(), True),
    StructField("metadata_file_count", LongType(), True),
    StructField("metadata_file_size", LongType(), True),
    StructField("snapshot_file_count", LongType(), True),
    StructField("snapshot_file_size", LongType(), True),
    StructField("manifest_file_count", LongType(), True),
    StructField("manifest_file_size", LongType(), True)
])

# --- Core Logic ---

import time
import os
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException


def analyze_and_insert(spark: SparkSession, conn):
    """
    Finds, analyzes, and inserts stats using a robust, hybrid approach.
    It pre-validates paths on the driver before launching a single Spark job.
    """
    print("🚀 Starting Iceberg table analysis with robust hybrid method...")
    sc = spark.sparkContext

    # 1. SETUP and FETCH DATA
    tables_to_analyze_pandas_df = conn.get_pandas_dataframe(ICEBERG_TABLES_QUERY)
    table_count = len(tables_to_analyze_pandas_df)
    print(f"✅ Found {table_count} tables to analyze.")

    analysis_start_time = time.time()

    print(f"📊 Analyzing {table_count} tables sequentially on the driver (fast metadata lookup)...")
    all_results = []
    # Get the Hadoop FileSystem object on the driver
    conf = sc._jsc.hadoopConfiguration()
    URI = sc._jvm.java.net.URI
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem

    for i, table_row in enumerate(tables_to_analyze_pandas_df.itertuples()):
        base_path = table_row.metadata_path
        print(f"  [{i+1}/{table_count}] Processing: {table_row.db_name}.{table_row.tbl_name}")

        path_stats = {}

        # Initialize dictionary for detailed metadata stats
        detailed_meta_stats = {
            "json_count": 0, "json_size": 0,
            "snapshot_count": 0, "snapshot_size": 0,
            "manifest_count": 0, "manifest_size": 0
        }

        for subfolder in ["metadata", "data"]:
            count, size = 0, 0
            full_path = os.path.join(base_path, subfolder)

            try:
                # Get the filesystem for the specific path
                encoded_path = full_path.replace(" ", "%20")
                fs = FileSystem.get(URI(encoded_path), conf)
                hadoop_path = Path(encoded_path)

                if fs.exists(hadoop_path):
                    # getContentSummary is the API equivalent of 'hdfs dfs -du -s'
                    summary = fs.getContentSummary(hadoop_path)
                    count = summary.getFileCount()
                    size = summary.getLength()

                    # If it's the metadata folder, perform detailed analysis by listing files
                    if subfolder == "metadata":
                        print(f"    - Performing detailed analysis on metadata folder...")
                        file_iterator = fs.listFiles(hadoop_path, False)

                        while file_iterator.hasNext():
                            file_status = file_iterator.next()
                            file_name = file_status.getPath().getName()
                            file_len = file_status.getLen()

                            if file_name.endswith(".json"):
                                detailed_meta_stats["json_count"] += 1
                                detailed_meta_stats["json_size"] += file_len
                            elif file_name.startswith("snap-"):
                                detailed_meta_stats["snapshot_count"] += 1
                                detailed_meta_stats["snapshot_size"] += file_len
                            elif file_name.endswith(".avro") and not file_name.startswith("snap-"):
                                detailed_meta_stats["manifest_count"] += 1
                                detailed_meta_stats["manifest_size"] += file_len

            except Exception as e:
                 print(f"    - Could not get summary for {full_path}: {e}")

            path_stats[subfolder] = (count, size)

        # ✨ MODIFIED: Append the final stats for this table to our results list with explicit fields
        all_results.append({
            "tbl_id": table_row.tbl_id,
            "db_name": table_row.db_name,
            "table_name": table_row.tbl_name,
            "metadata_folder_count": path_stats["metadata"][0],
            "metadata_folder_size": path_stats["metadata"][1],
            "data_folder_count": path_stats["data"][0],
            "data_folder_size": path_stats["data"][1],
            "metadata_file_count": detailed_meta_stats["json_count"],
            "metadata_file_size": detailed_meta_stats["json_size"],
            "snapshot_file_count": detailed_meta_stats["snapshot_count"],
            "snapshot_file_size": detailed_meta_stats["snapshot_size"],
            "manifest_file_count": detailed_meta_stats["manifest_count"],
            "manifest_file_size": detailed_meta_stats["manifest_size"]
        })

    # 3. CONVERT results list back to a Spark DataFrame for writing.
    if not all_results:
        print("⚠️ Analysis finished with 0 results. Nothing to write.")
        return

    print("\n🔄 Converting results back to a Spark DataFrame for writing...")
    results_df = spark.createDataFrame(all_results, schema=RESULTS_SCHEMA)

    print("Results DataFrame Schema:")
    results_df.printSchema()

    analysis_end_time = time.time()
    analysis_duration = analysis_end_time - analysis_start_time
    print(f"--- Analysis Phase Time: {analysis_duration:.2f} seconds ---")

    # 6. INSERT results
    insert_start_time = time.time()
    print(f"💾 Writing {results_df.count()} rows to Iceberg table '{RESULTS_TABLE}' using Spark...")

    results_df.writeTo(RESULTS_TABLE).using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()

    insert_end_time = time.time()
    insert_duration = insert_end_time - insert_start_time
    print(f"--- Insert Data Phase Time: {insert_duration:.2f} seconds ---")


if __name__ == "__main__":
    script_start_time = time.time()
    print("https://spark-"+os.environ["CDSW_ENGINE_ID"]+"."+os.environ["CDSW_DOMAIN"])
    SparkContext.setSystemProperty('spark.dynamicAllocation.enabled', 'true')
    SparkContext.setSystemProperty('spark.driver.maxResultSize', '2g')
    SparkContext.setSystemProperty('spark.executor.cores', '8')
    SparkContext.setSystemProperty('spark.executor.memory', '16g')
    SparkContext.setSystemProperty('spark.dynamicAllocation.minExecutors', '1')
    SparkContext.setSystemProperty('spark.dynamicAllocation.maxExecutors', '5')

    conn = cmldata.get_connection(SPARK_CONNECTION_NAME)
    spark = conn.get_spark_session()
    conn = None

    try:
        conn = cmldata.get_connection(HIVE_CONNECTION_NAME)
        analyze_and_insert(spark, conn)
    except Exception as e:
        print(f"An unexpected error occurred during the main execution: {e}")
    finally:
        script_end_time = time.time()
        total_duration = script_end_time - script_start_time
        print(f"\n--- Total Script Execution Time: {total_duration:.2f} seconds ---")

        print("\n--- 📋 Full execution finished ---")
        if conn:
            conn.close()
        spark.stop()
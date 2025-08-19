from pyspark.sql import SparkSession, DataFrame
from pyspark import StorageLevel
from pyspark.sql.functions import (
    col)
import requests
import json
from typing import Optional
from google.cloud import storage
import os

def druidDFOption(query: str, host: str, result_format: str = "object", limit: int = 10000, spark: SparkSession = None) -> Optional[DataFrame]:
    """
    PySpark version of druidDFOption function
    """
    if spark is None:
        spark = SparkSession.getActiveSession()
    
    # Try using Spark Druid connector if available
    try:
        # Check if Druid connector is available
        df = spark.read \
            .format("druid") \
            .option("url", f"http://{host}:8888/druid/v2/sql") \
            .option("query", query) \
            .option("resultFormat", result_format) \
            .option("limit", limit) \
            .load()
        
        if df.count() == 0:
            print("ERROR: Druid connector returned empty dataframe")
            return None
            
        return df.persist(StorageLevel.MEMORY_ONLY)
        
    except Exception as e:
        print(f"Druid connector not available or failed: {e}")
        print("Falling back to HTTP API approach")
        
        # Fallback to HTTP API approach
        result = druidSQLAPI(query, host, result_format, limit).strip()
        
        # return None if result is an empty string
        if result == "":
            print("ERROR: druidSQLAPI returned empty string")
            return None
            
        df = dataframe_from_json_string(result, spark).persist(StorageLevel.MEMORY_ONLY)
        
        if df.count() == 0:
            print("ERROR: druidSQLAPI json parse result is empty")
            return None
            
        # return None if there is an `error` field in the json
        if has_column(df, "error"):
            print(f"ERROR: druidSQLAPI returned error response, response={result}")
            return None
            
        # now that error handling is done, proceed with business as usual
        return df


def druidSQLAPI(query: str, host: str, result_format: str = "object", limit: int = 10000) -> str:
    """
    PySpark version of druidSQLAPI function
    """
    # TODO: tech-debt, use proper spark druid connector when available
    url = f"http://{host}:8888/druid/v2/sql"
    request_body = {
        "resultFormat": result_format,
        "header": False,
        "context": {"sqlOuterLimit": limit},
        "query": query
    }
    
    return api("POST", url, json.dumps(request_body))


def api(method: str, url: str, body: str) -> str:
    """
    Simple HTTP API call function
    """
    try:
        if method.upper() == "POST":
            response = requests.post(
                url, 
                data=body, 
                headers={'Content-Type': 'application/json'}
            )
        elif method.upper() == "GET":
            response = requests.get(url)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
            
        response.raise_for_status()
        return response.text
        
    except requests.exceptions.RequestException as e:
        print(f"ERROR: API call failed: {e}")
        return ""

def sync_reports(local_path, remote_path, config):
    """
    Upload all files from `local_path` to GCS path: gs://<container>/<remote_path> using GCP service account.

    Parameters:
        local_path (str): Local directory to upload.
        remote_path (str): GCS path under the bucket (like 'reports/standalone-reports/merged').
        conf (object): Configuration object with attributes:
            - conf.container (str): GCS bucket name
            - conf.store (str): Expected to be 'gcs'
            - conf.gcp_service_account_key (str): Path to GCP credentials JSON
    """
    print(f"REPORT: Syncing reports from {local_path} to gs://{config.gcpBucket}/{remote_path} ...")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.googleServiceAccountFilePath
    client = storage.Client()
    bucket = client.bucket(config.gcpBucket)

    for root, _, files in os.walk(local_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_path)
            gcs_blob_path = os.path.join(remote_path, relative_path).replace("\\", "/")

            blob = bucket.blob(gcs_blob_path)
            blob.upload_from_filename(local_file_path)
            print(f"✅ Synced: {local_file_path} → gs://{config.gcpBucket}/{gcs_blob_path}")

    print(f"REPORT: Finished syncing reports from {local_path} to gs://{config.gcpBucket}/{remote_path}")

def dataframe_from_json_string(json_str: str, spark: SparkSession) -> DataFrame:
    """
    Convert JSON string to DataFrame
    """
    try:
        # Parse JSON string to get list of records
        data = json.loads(json_str)
        
        # Create DataFrame from JSON data
        if isinstance(data, list) and len(data) > 0:
            df = spark.read.json(spark.sparkContext.parallelize([json.dumps(record) for record in data]))
            return df
        else:
            # Return empty DataFrame with no schema
            return spark.createDataFrame([], schema=None)
            
    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to parse JSON: {e}")
        return spark.createDataFrame([], schema=None)
    except Exception as e:
        print(f"ERROR: Failed to create DataFrame from JSON: {e}")
        return spark.createDataFrame([], schema=None)


def has_column(df: DataFrame, column_name: str) -> bool:
    """
    Check if DataFrame has a specific column
    """
    return column_name in df.columns

def read_elasticsearch_data(spark: SparkSession,host: str, port:str,index: str, query: str, fields: list, array_fields: list) -> "DataFrame":
        """Read data from Elasticsearch"""
        dfr = spark.read.format("org.elasticsearch.spark.sql") \
                .option("es.read.metadata", "false") \
                .option("es.nodes", host) \
                .option("es.port",port) \
                .option("es.index.auto.create", "false") \
                .option("es.nodes.wan.only", "true") \
                .option("es.nodes.discovery", "false")
            
        # Add array field handling if specified
        if array_fields:
            dfr = dfr.option("es.read.field.as.array.include", ",".join(array_fields))
        
        # Add query and load data
        df = dfr.option("query", query).load(index)
        
        # Select only the specified fields
        if fields:
            # Create column expressions for field selection
            field_cols = [col(f) for f in fields]
            df = df.select(*field_cols)
        
        # Persist with MEMORY_ONLY storage level for performance
        df = df.persist(StorageLevel.MEMORY_ONLY)
        
        # Force evaluation to ensure data is loaded
        count = df.count()
        print(f"Successfully loaded {count} rows from Elasticsearch index: {index}")
        
        return df
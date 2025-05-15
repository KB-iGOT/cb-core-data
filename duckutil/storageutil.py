import os
import duckdb
import pandas as pd
import glob
from typing import List, Optional, Union, Any
from enum import Enum
import shutil

class SaveMode(Enum):
    Overwrite = "overwrite"
    Append = "append"
    ErrorIfExists = "error_if_exists"
    Ignore = "ignore"

class FrameworkContext:
    # Placeholder for compatibility with original code
    pass

class DashboardConfig:
    def __init__(self, local_report_dir: str):
        self.localReportDir = local_report_dir

class StorageUtil:
    @staticmethod
    def removeFile(path: str) -> None:
        """Remove a file if it exists."""
        if os.path.exists(path):
            os.remove(path)
    
    @staticmethod
    def renameCSVWithoutPartitions(directory: str, new_name: str) -> None:
        """Rename the CSV file in the directory to the provided name."""
        csv_files = glob.glob(os.path.join(directory, "*.csv"))
        if csv_files:
            # Get the first CSV file (should be only one without partitions)
            csv_file = csv_files[0]
            new_path = os.path.join(directory, f"{new_name}.csv")
            os.rename(csv_file, new_path)
    
    @staticmethod
    def renameCSV(ids: List[str], base_directory: str, new_name: str, partition_key: str) -> None:
        """Rename CSV files in partitioned directories."""
        for id_value in ids:
            partition_dir = os.path.join(base_directory, f"{partition_key}={id_value}")
            if os.path.exists(partition_dir):
                csv_files = glob.glob(os.path.join(partition_dir, "*.csv"))
                for csv_file in csv_files:
                    new_path = os.path.join(partition_dir, f"{new_name}.csv")
                    os.rename(csv_file, new_path)

def generate_report(
    data: Any,
    report_path: str,
    partition_key: Optional[str] = None,
    file_name: Optional[str] = None,
    file_save_mode: SaveMode = SaveMode.Overwrite,
    config: Optional[DashboardConfig] = None,
    framework_context: Optional[FrameworkContext] = None
) -> None:
    """
    Generate a report using DuckDB for fast CSV export.
    
    Args:
        data: PySpark DataFrame, Pandas DataFrame, or SQL query string to be used for the report
        report_path: Path where the report should be saved (relative to config.localReportDir)
        partition_key: Column name to partition the data by (optional)
        file_name: Name for the output file without extension (optional)
        file_save_mode: Save mode for the file (Overwrite, Append, ErrorIfExists, Ignore)
        config: Dashboard configuration object
        framework_context: Framework context object
    """
    # Initialize DuckDB connection
    conn = duckdb.connect(database=':memory:')
    
    # Determine the full path for the report
    local_report_dir = config.localReportDir if config else "reports"
    report_full_path = os.path.join(local_report_dir, report_path)
    
    print(f"REPORT: Writing report to {report_full_path} ...")
    
    # Ensure the directory exists
    os.makedirs(report_full_path, exist_ok=True)
    
    # Check if data is a PySpark DataFrame
    is_pyspark_df = False
    try:
        from pyspark.sql import DataFrame as PySparkDataFrame
        if isinstance(data, PySparkDataFrame):
            is_pyspark_df = True
    except ImportError:
        pass  # PySpark not available, continue with other data types
    
    # Load data into DuckDB
    if is_pyspark_df:
        # Handle PySpark DataFrame - write to temp CSV and load into DuckDB
        temp_csv_path = os.path.join(report_full_path, "temp_spark_data.csv")
        
        # Use PySpark's CSV writer to create the temp file
        data.coalesce(1).write.option("header", "true").mode("overwrite").csv(temp_csv_path)
        
        # Find the actual CSV file (PySpark creates a directory)
        actual_csv_files = glob.glob(os.path.join(temp_csv_path, "*.csv"))
        if actual_csv_files:
            actual_csv_file = actual_csv_files[0]
            # Load into DuckDB
            conn.execute(f"CREATE TABLE input_data AS SELECT * FROM read_csv_auto('{actual_csv_file}')")
            table_name = 'input_data'
            
            # Clean up temporary files
            shutil.rmtree(temp_csv_path)
        else:
            raise ValueError("Failed to create temporary CSV file from PySpark DataFrame")
    
    elif isinstance(data, pd.DataFrame):
        # Register the DataFrame as a view in DuckDB
        conn.register('input_data', data)
        table_name = 'input_data'
    
    elif isinstance(data, str):
        # Assume it's a SQL query to execute
        conn.execute(f"CREATE TABLE input_data AS {data}")
        table_name = 'input_data'
    
    else:
        raise ValueError("Data must be a PySpark DataFrame, pandas DataFrame, or a SQL query string")
    
    # Determine save mode
    save_mode_str = file_save_mode.value
    if save_mode_str == SaveMode.Overwrite.value and os.path.exists(report_full_path):
        if partition_key is None:
            # Remove existing files for overwrite mode
            for file in glob.glob(os.path.join(report_full_path, "*.csv")):
                os.remove(file)
    
    if partition_key is None:
        # Non-partitioned export
        output_file = os.path.join(report_full_path, "output.csv")
        conn.execute(f"COPY (SELECT * FROM {table_name}) TO '{output_file}' (HEADER, DELIMITER ',')")
        
        # Rename the file if a name is provided
        if file_name is not None:
            StorageUtil.renameCSVWithoutPartitions(report_full_path, file_name)
    else:
        # Get unique values in the partition column
        distinct_values = conn.execute(f"SELECT DISTINCT {partition_key} FROM {table_name} WHERE {partition_key} IS NOT NULL AND {partition_key} != ''").fetchall()
        ids = [str(row[0]) for row in distinct_values]
        
        # Generate partitioned report
        for id_value in ids:
            # Create directory for this partition
            partition_dir = os.path.join(report_full_path, f"{partition_key}={id_value}")
            os.makedirs(partition_dir, exist_ok=True)
            
            # Export data for this partition
            output_file = os.path.join(partition_dir, "part-0.csv")
            conn.execute(f"""
                COPY (SELECT * FROM {table_name} WHERE {partition_key} = '{id_value}')
                TO '{output_file}' (HEADER, DELIMITER ',')
            """)
        
        # Rename files if a name is provided
        if file_name is not None:
            StorageUtil.renameCSV(ids, report_full_path, file_name, partition_key)
    
    # Remove success file if it exists
    StorageUtil.removeFile(os.path.join(report_full_path, "_SUCCESS"))
    
    print(f"REPORT: Finished Writing report to {report_full_path}")
    
    # Close the connection
    conn.close()

# Alternative function names that follow the original naming convention
def generateReport(
    data: Any,
    reportPath: str,
    partitionKey: Optional[str] = None,
    fileName: Optional[str] = None,
    fileSaveMode: SaveMode = SaveMode.Overwrite,
    config: Optional[DashboardConfig] = None,
    framework_context: Optional[FrameworkContext] = None
) -> None:
    """Alias for generate_report with camelCase naming to match original."""
    return generate_report(data, reportPath, partitionKey, fileName, fileSaveMode, config, framework_context)

def csv_write_partition(
    data: Any,
    path: str,
    partition_key: str,
    header: bool = True,
    save_mode: SaveMode = SaveMode.Overwrite
) -> None:
    """
    Write data to CSV files partitioned by the specified key.
    
    This is a simplified version that just calls generate_report with appropriate parameters.
    """
    config = DashboardConfig(os.path.dirname(path))
    report_path = os.path.basename(path)
    generate_report(data, report_path, partition_key, None, save_mode, config)

def csvWritePartition(
    data: Any,
    path: str,
    partitionKey: str,
    header: bool = True,
    saveMode: SaveMode = SaveMode.Overwrite
) -> None:
    """Alias for csv_write_partition with camelCase naming to match original."""
    return csv_write_partition(data, path, partitionKey, header, saveMode)
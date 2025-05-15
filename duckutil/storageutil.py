import os
import duckdb
import pandas as pd
import glob
from typing import List, Optional, Union, Any, Dict
from enum import Enum
import shutil

class SaveMode(Enum):
    Overwrite = "overwrite"
    Append = "append"
    ErrorIfExists = "error_if_exists"
    Ignore = "ignore"

class OutputFormat(Enum):
    CSV = "csv"
    PARQUET = "parquet"
    BOTH = "both"

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
    def removeDirectory(path: str) -> None:
        """Remove a directory if it exists."""
        if os.path.exists(path):
            shutil.rmtree(path)
    
    @staticmethod
    def renameFileWithoutPartitions(directory: str, new_name: str, extension: str = "csv") -> None:
        """Rename the file in the directory to the provided name."""
        files = glob.glob(os.path.join(directory, f"*.{extension}"))
        if files:
            old_file = files[0]
            new_path = os.path.join(directory, f"{new_name}.{extension}")
            os.rename(old_file, new_path)
    
    @staticmethod
    def renameFiles(ids: List[str], base_directory: str, new_name: str, partition_key: str, extension: str = "csv") -> None:
        """Rename files in partitioned directories."""
        for id_value in ids:
            partition_dir = os.path.join(base_directory, f"{partition_key}={id_value}")
            if os.path.exists(partition_dir):
                files = glob.glob(os.path.join(partition_dir, f"*.{extension}"))
                for file in files:
                    new_path = os.path.join(partition_dir, f"{new_name}.{extension}")
                    os.rename(file, new_path)

def generate_report(
    data: Any,
    report_path: str,
    partition_key: Optional[str] = None,
    file_name: Optional[str] = None,
    file_save_mode: SaveMode = SaveMode.Overwrite,
    output_format: OutputFormat = OutputFormat.CSV,
    config: Optional[DashboardConfig] = None,
    framework_context: Optional[FrameworkContext] = None,
    parquet_compression: str = "snappy",
    csv_options: Optional[Dict[str, Any]] = None,
    parquet_options: Optional[Dict[str, Any]] = None,
    parquet_output_path: Optional[str] = None
) -> None:
    """
    Generate a report using DuckDB for fast CSV/Parquet export.
    
    Args:
        data: Input data (PySpark DF, Pandas DF, or SQL query string)
        report_path: Base path for output files
        partition_key: Column to partition by (optional)
        file_name: Base name for output files
        file_save_mode: How to handle existing files
        output_format: CSV, PARQUET, or BOTH
        config: Dashboard configuration
        framework_context: Framework context
        parquet_compression: Compression for Parquet files
        csv_options: Additional CSV options
        parquet_options: Additional Parquet options
        parquet_output_path: Custom path for Parquet files
    """
    conn = duckdb.connect(database=':memory:')
    
    local_report_dir = config.localReportDir if config else ""
    report_full_path = os.path.join(local_report_dir, report_path)
    
    print(f"REPORT: Writing report to {report_full_path}...")
    os.makedirs(report_full_path, exist_ok=True)
    
    # Handle different input data types
    if hasattr(data, '_jdf'):  # PySpark DataFrame
        temp_csv_path = os.path.join(report_full_path, "temp_spark_data.csv")
        data.coalesce(1).write.option("header", "true").mode("overwrite").csv(temp_csv_path)
        actual_csv_files = glob.glob(os.path.join(temp_csv_path, "*.csv"))
        if actual_csv_files:
            conn.execute(f"CREATE TABLE input_data AS SELECT * FROM read_csv_auto('{actual_csv_files[0]}')")
            shutil.rmtree(temp_csv_path)
        else:
            raise ValueError("Failed to create temporary CSV from PySpark DataFrame")
    elif isinstance(data, pd.DataFrame):
        conn.register('input_data', data)
    elif isinstance(data, str):
        conn.execute(f"CREATE TABLE input_data AS {data}")
    else:
        raise ValueError("Unsupported data type")
    
    table_name = 'input_data'
    
    # Handle save mode
    if file_save_mode == SaveMode.Overwrite and os.path.exists(report_full_path):
        if partition_key is None:
            for file in glob.glob(os.path.join(report_full_path, "*.*")):
                if not file.endswith(".tmp"):
                    os.remove(file)
    
    # Set default options
    default_csv_options = {"header": True, "delimiter": ","}
    if csv_options:
        default_csv_options.update(csv_options)
    
    default_parquet_options = {"compression": parquet_compression}
    if parquet_options:
        default_parquet_options.update(parquet_options)
    
    def export_data(query: str, output_path: str, fmt: str) -> None:
        """Helper to export data in specified format."""
        if fmt == "csv":
            options = ",".join([f"{k} {repr(v)}" if isinstance(v, str) else f"{k} {v}" 
                              for k, v in default_csv_options.items()])
            conn.execute(f"COPY ({query}) TO '{output_path}' ({options})")
        elif fmt == "parquet":
            options = ",".join([f"{k} {repr(v)}" if isinstance(v, str) else f"{k} {v}" 
                              for k, v in default_parquet_options.items()])
            conn.execute(f"COPY ({query}) TO '{output_path}' (FORMAT PARQUET, {options})")
    
    def get_output_path(base_path: str, fmt: str) -> str:
        """Determine output path considering custom Parquet location."""
        if fmt == "parquet" and parquet_output_path:
            if partition_key:
                rel_path = os.path.relpath(base_path, report_full_path)
                return os.path.join(parquet_output_path, rel_path)
            return parquet_output_path
        return base_path
    
    if partition_key is None:
        # Non-partitioned export
        formats = []
        if output_format in [OutputFormat.CSV, OutputFormat.BOTH]:
            formats.append(("csv", "output.csv"))
        if output_format in [OutputFormat.PARQUET, OutputFormat.BOTH]:
            formats.append(("parquet", "output.parquet"))
        
        for fmt, output_file in formats:
            output_dir = get_output_path(report_full_path, fmt)
            os.makedirs(output_dir, exist_ok=True)
            full_path = os.path.join(output_dir, output_file)
            export_data(f"SELECT * FROM {table_name}", full_path, fmt)
            
            if file_name:
                if fmt == "csv":
                    StorageUtil.renameFileWithoutPartitions(report_full_path, file_name, "csv")
                elif fmt == "parquet":
                    parquet_dir = get_output_path(report_full_path, "parquet")
                    StorageUtil.renameFileWithoutPartitions(parquet_dir, file_name, "parquet")
    else:
        # Partitioned export
        distinct_values = conn.execute(
            f"SELECT DISTINCT {partition_key} FROM {table_name} "
            f"WHERE {partition_key} IS NOT NULL AND {partition_key} != ''"
        ).fetchall()
        ids = [str(row[0]) for row in distinct_values]
        
        for id_value in ids:
            formats = []
            if output_format in [OutputFormat.CSV, OutputFormat.BOTH]:
                formats.append(("csv", "part-0.csv"))
            if output_format in [OutputFormat.PARQUET, OutputFormat.BOTH]:
                formats.append(("parquet", "part-0.parquet"))
            
            for fmt, output_file in formats:
                base_dir = os.path.join(report_full_path, f"{partition_key}={id_value}")
                output_dir = get_output_path(base_dir, fmt)
                os.makedirs(output_dir, exist_ok=True)
                full_path = os.path.join(output_dir, output_file)
                export_data(
                    f"SELECT * FROM {table_name} WHERE {partition_key} = '{id_value}'",
                    full_path,
                    fmt
                )
        
        if file_name:
            if output_format in [OutputFormat.CSV, OutputFormat.BOTH]:
                StorageUtil.renameFiles(ids, report_full_path, file_name, partition_key, "csv")
            if output_format in [OutputFormat.PARQUET, OutputFormat.BOTH]:
                parquet_base = parquet_output_path if parquet_output_path else report_full_path
                StorageUtil.renameFiles(ids, parquet_base, file_name, partition_key, "parquet")
    
    StorageUtil.removeFile(os.path.join(report_full_path, "_SUCCESS"))
    print(f"REPORT: Finished writing to {report_full_path}")
    conn.close()

# Alternative naming conventions
def generateReport(*args, **kwargs):
    return generate_report(*args, **kwargs)

def csv_write_partition(
    data: Any,
    path: str,
    partition_key: str,
    header: bool = True,
    save_mode: SaveMode = SaveMode.Overwrite
) -> None:
    config = DashboardConfig(os.path.dirname(path))
    report_path = os.path.basename(path)
    generate_report(
        data, report_path, partition_key, None, save_mode, OutputFormat.CSV, config
    )

def csvWritePartition(*args, **kwargs):
    return csv_write_partition(*args, **kwargs)

def parquet_write_partition(
    data: Any,
    path: str,
    partition_key: str,
    save_mode: SaveMode = SaveMode.Overwrite,
    compression: str = "snappy",
    options: Optional[Dict[str, Any]] = None,
    output_path: Optional[str] = None
) -> None:
    config = DashboardConfig(os.path.dirname(path))
    report_path = os.path.basename(path)
    generate_report(
        data, report_path, partition_key, None, save_mode, OutputFormat.PARQUET,
        config, None, compression, None, options, output_path
    )

def parquetWritePartition(*args, **kwargs):
    return parquet_write_partition(*args, **kwargs)
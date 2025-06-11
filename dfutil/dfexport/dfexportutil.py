import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, when, sum, bround, broadcast, coalesce, lit,
    current_timestamp, date_format, from_unixtime, concat_ws
)
import os
import duckdb

# Add parent directory to sys.path for importing project-specific modules
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Import reusable utilities from project
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.enrolment.acbp import acbpDFUtil
from dfutil.enrolment import enrolmentDFUtil
from dfutil.content import contentDFUtil

# Initialize the Spark Session with tuning configurations
spark = SparkSession.builder \
    .appName("UserReportGenerator") \
    .config("spark.executor.memory", "12g") \
    .config("spark.driver.memory", "10g") \
    .config("spark.sql.shuffle.partitions", "64") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

def write_csv_per_mdo_id(df, output_dir, groupByAttr,isIndividualWrite=False,threshold=100000):
    """
    Hybrid write strategy: partition small/medium groups via partitionBy, large groups via fallback.
    
    Args:
        df (DataFrame): Source DataFrame
        output_dir (str): Output directory path
        threshold (int): Max row count per mdo_id to consider for fast write
    """
    # df.cache().count()  # Cache and count to trigger any lazy evaluation
    if isIndividualWrite == False:
        # Step 1: Get group counts
        group_counts = df.groupBy(groupByAttr).count()
        group_counts.printSchema()
        # Step 2: Collect IDs for fast + fallback paths
        small_ids = [row[groupByAttr] for row in group_counts.filter(col("count") <= threshold).collect()]
        large_ids = [row[groupByAttr] for row in group_counts.filter(col("count") > threshold).collect()]
        
        print(f"Small groups (fast write): {len(small_ids)}")
        # Step 3: Fast write for small/medium mdo_ids
        if small_ids:
            df.filter(col(groupByAttr).isin(small_ids)) \
            .repartition(groupByAttr) \
            .write \
            .mode("overwrite") \
            .partitionBy(groupByAttr) \
            .option("header", True) \
            .csv(output_dir)

            print(f"Large groups (manual write): {len(large_ids)}")
            # Step 4: Manual write for large mdo_ids (one folder per group)
            # for mdo in large_ids:
            #     print(f"Writing large group mdo_id={mdo} separately...")
            #     df.filter(col(groupByAttr) == mdo) \
            #     .coalesce(1) \
            #     .write \
            #     .mode("overwrite") \
            #     .option("header", True) \
            #     .csv(f"{output_dir}/mdo_id_large={mdo}")\
            if len(large_ids) > 0:
                write_csv_per_mdo_id_duckdb(df,output_dir, groupByAttr,output_dir+f'tmp',large_ids)
    else:
        df.repartition(groupByAttr) \
            .write \
            .mode("overwrite") \
            .partitionBy(groupByAttr) \
            .option("header", True) \
            .option("compression", "snappy") \
            .parquet(output_dir)

def write_csv_per_mdo_id_duckdb(df, output_dir: str, group_by_attr: str, parquet_tmp_path: str,large_ids):
    """
    Writes CSVs per group_by_attr using Spark for partitioned Parquet write, then DuckDB for fast CSV export.

    Args:
        df (DataFrame): Spark DataFrame
        output_dir (str): Output directory to write CSVs
        group_by_attr (str): Column to group by (e.g., "mdo_id")
        parquet_tmp_path (str): Temporary Parquet output path
    """
    print(f"📦 Step 1: Writing partitioned Parquet by '{group_by_attr}'...")
    
    df.repartition(8) \
        .write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(parquet_tmp_path)

    print("🦆 Step 2: Loading into DuckDB...")
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='6GB';")       # Sets memory cap
    con.execute("PRAGMA threads=8;")              # Multithreading
    con.execute("PRAGMA temp_directory='/tmp/duckdb_spill';")
    con.execute("INSTALL parquet; LOAD parquet;")
    con.execute(f"CREATE TABLE result_df AS SELECT * FROM parquet_scan('{parquet_tmp_path}/**/*.parquet');")

    print("🔍 Step 3: Fetching distinct group values...")
    if large_ids is None or not large_ids:
        group_ids = con.execute(f"SELECT DISTINCT {group_by_attr} FROM result_df").fetchall()
    else:
        group_ids = [(val,) for val in large_ids] 

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    print("📤 Step 4: Writing individual CSV files...")
    
    for (group_val,) in group_ids:
        safe_val = str(group_val).replace("/", "_") if group_val is not None else "null"
        output_path = Path(output_dir) / f"{group_by_attr}={safe_val}.csv"

        con.execute(f"""
            COPY (
                SELECT * FROM result_df
                WHERE {group_by_attr} = ?
            ) TO '{output_path}' (FORMAT CSV, HEADER, DELIMITER ',');
        """, [group_val])

        print(f"✅ Wrote: {output_path}")

    con.close()
    print("🎉 Done writing all CSV files.")
  
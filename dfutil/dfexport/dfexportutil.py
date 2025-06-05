import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, when, sum, bround, broadcast, coalesce, lit,
    current_timestamp, date_format, from_unixtime, concat_ws
)
import os

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
        for mdo in large_ids:
            print(f"Writing large group mdo_id={mdo} separately...")
            df.filter(col(groupByAttr) == mdo) \
            .coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(f"{output_dir}/mdo_id_large={mdo}")
    else:
        df.repartition(groupByAttr) \
            .write \
            .mode("overwrite") \
            .partitionBy(groupByAttr) \
            .option("header", True) \
            .option("compression", "snappy") \
            .parquet(output_dir)
  
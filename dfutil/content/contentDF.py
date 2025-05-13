import sys
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode_outer


# ==============================
# 1. Configuration and Constants
# ==============================

# Ensure the parent directory is in sys.path for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckutil import duckutil, datautil  # Assuming duckutil is in the parent directory
from constants.ParquetFileConstants import ParquetFileConstants
from constants.QueryConstants import QueryConstants


def content_es_dataframe(spark_session, primary_categories: list, prefix: str = "course") -> DataFrame:

    # Fetch the ElasticSearch data directly as DuckDB Parquet Path
    content_parquet_path = ParquetFileConstants.ESCONTENT_PARQUET_FILE  # Replace with the actual path
    contentForPrimaryCategoryDF = spark_session.read.parquet(content_parquet_path) \
        .filter(col("primaryCategory").isin(primary_categories))

    # Process DataFrame
    processed_df = (
        contentForPrimaryCategoryDF
        .withColumn(f"{prefix}OrgID", explode_outer(col("createdFor")))
        .select(
            col("identifier").alias(f"{prefix}ID"),
            col("primaryCategory").alias(f"{prefix}Category"),
            col("name").alias(f"{prefix}Name"),
            col("status").alias(f"{prefix}Status"),
            col("reviewStatus").alias(f"{prefix}ReviewStatus"),
            col("channel").alias(f"{prefix}Channel"),
            col("lastPublishedOn").alias(f"{prefix}LastPublishedOn"),
            col("duration").cast("float").alias(f"{prefix}Duration"),
            col("leafNodesCount").alias(f"{prefix}ResourceCount"),
            col("lastStatusChangedOn").alias(f"{prefix}LastStatusChangedOn"),
            col("programDirectorName").alias(f"{prefix}ProgramDirectorName"),
            col(f"{prefix}OrgID")
        )
        .dropDuplicates([f"{prefix}ID", f"{prefix}Category"])
        .na.fill({f"{prefix}Duration": 0.0, f"{prefix}ResourceCount": 0})
    )

    return processed_df

def all_course_program_es_data_frame(spark_session, primary_categories: list) -> DataFrame:
    # Directly load the Parquet file using Spark (replace with the actual path)
    content_parquet_path = ParquetFileConstants.ESCONTENT_PARQUET_FILE  # Replace with the actual path
    content_df = spark_session.read.parquet(content_parquet_path) \
        .filter(col("primaryCategory").isin(primary_categories))

    # Process DataFrame without Pandas
    processed_df = (
        content_df
        .withColumn("courseOrgID", explode_outer(col("createdFor")))
        .withColumn("contentLanguage", explode_outer(col("language")))
        .select(
            col("identifier").alias("courseID"),
            col("primaryCategory").alias("category"),
            col("name").alias("courseName"),
            col("status").alias("courseStatus"),
            col("reviewStatus").alias("courseReviewStatus"),
            col("channel").alias("courseChannel"),
            col("lastPublishedOn").alias("courseLastPublishedOn"),
            col("duration").cast("float").alias("courseDuration"),
            col("leafNodesCount").alias("courseResourceCount"),
            col("lastStatusChangedOn").alias("lastStatusChangedOn"),
            col("courseOrgID"),
            col("competencies_v6.competencyAreaRefId"),
            col("competencies_v6.competencyThemeRefId"),
            col("competencies_v6.competencySubThemeRefId"),
            col("contentLanguage"),
            col("courseCategory")
        )
        .dropDuplicates(["courseID", "category"])
        .na.fill({"courseDuration": 0.0, "courseResourceCount": 0})
    )

    return processed_df

def assessment_es_dataframe(spark_session: SparkSession, primary_categories: list) -> DataFrame:
    """
    Creates an assessment DataFrame from Elasticsearch content with specified primary categories.

    Args:
        spark_session (SparkSession): The active Spark session.
        primary_categories (list): List of primary categories for filtering.

    Returns:
        DataFrame: Processed DataFrame with assessment data.
    """
    # Load the ElasticSearch Content DataFrame
    content_parquet_path = ParquetFileConstants.ESCONTENT_PARQUET_FILE  # Replace with the actual path
    content_df = spark_session.read.parquet(content_parquet_path) \
        .filter(col("primaryCategory").isin(primary_categories))
    
    # Process the DataFrame for assessments
    processed_df = (
        content_df
        .withColumn("assessOrgID", explode_outer(col("createdFor")))
        .select(
            col("identifier").alias("assessID"),
            col("primaryCategory").alias("assessCategory"),
            col("name").alias("assessName"),
            col("status").alias("assessStatus"),
            col("reviewStatus").alias("assessReviewStatus"),
            col("channel").alias("assessChannel"),
            col("duration").cast("float").alias("assessDuration"),
            col("leafNodesCount").alias("assessChildCount"),
            col("lastPublishedOn").alias("assessLastPublishedOn"),
            col("assessOrgID")
        )
        .dropDuplicates(["assessID", "assessCategory"])
        .na.fill({"assessDuration": 0.0, "assessChildCount": 0})
    )

    return processed_df

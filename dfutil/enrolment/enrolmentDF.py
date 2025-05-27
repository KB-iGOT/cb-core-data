from ParquetFileConstants import ParquetFileConstants
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, expr, size, when, lit
from typing import List
from pyspark.sql.functions import (
    col, expr, explode_outer, when, lit, concat_ws, size, to_json,
    count as spark_count
)
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F
import logging

logger = logging.getLogger(__name__)

def _apply_enrollment_transformations(df: DataFrame) -> DataFrame:
    """
    Apply complex transformations to enrollment data
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: Transformed DataFrame
    """
    
    return (df
        # Convert array/complex columns to strings for CSV compatibility
        .withColumn("competencyAreaRefIdStr", 
            when(col("competencyAreaRefId").isNull(), lit(""))
            .otherwise(concat_ws(",", col("competencyAreaRefId"))))
        .withColumn("competencyThemeRefIdStr", 
            when(col("competencyThemeRefId").isNull(), lit(""))
            .otherwise(concat_ws(",", col("competencyThemeRefId"))))
        .withColumn("competencySubThemeRefIdStr", 
            when(col("competencySubThemeRefId").isNull(), lit(""))
            .otherwise(concat_ws(",", col("competencySubThemeRefId"))))
        
        
        # Certificate-related columns
        .withColumn("issuedCertificateCount", size(col("issued_certificates")))
        .withColumn("issuedCertificateCountPerContent", 
            when(size(col("issued_certificates")) > 0, lit(1)).otherwise(lit(0)))
        .withColumn("certificateGeneratedOn", 
            when(col("issued_certificates").isNull(), lit(""))
            .otherwise(col("issued_certificates")
                    .getItem(size(col("issued_certificates")) - 1)
                    .getItem("lastIssuedOn")))
        .withColumn("firstCompletedOn", 
            when(col("issued_certificates").isNull(), lit(""))
            .otherwise(when(size(col("issued_certificates")) > 0, 
                        col("issued_certificates").getItem(0).getItem("lastIssuedOn"))
                    .otherwise(lit(""))))
        .withColumn("certificateID", 
            when(col("issued_certificates").isNull(), lit(""))
            .otherwise(col("issued_certificates")
                    .getItem(size(col("issued_certificates")) - 1)
                    .getItem("identifier")))
             
        # Fill null values
        .na.fill({
            "courseDuration": 0.0,
            "courseResourceCount": 0,
            "courseProgress": 0,
            "issuedCertificateCount": 0,
            "certificateGeneratedOn": "",
            "courseBatchAttrs": "{}"
        })
    )


def process_enrollment_master(spark: SparkSession, 
                                    primary_categories: list = None,
                                    cache_intermediate: bool = False) -> DataFrame:
    """
    Optimized version of enrollment master processing with caching options
    
    Args:
        spark: SparkSession instance
        primary_categories: List of primary categories to filter by
        cache_intermediate: Whether to cache intermediate results
        
    Returns:
        DataFrame: Processed enrollment master DataFrame
    """
    
    if primary_categories is None:
        primary_categories = ["Course", "Program", "Blended Program", "CuratedCollections", "Curated Program"]
    
    # Read with optimized settings
    master_enrollment = (spark.read
        .option("mergeSchema", "false")
        .parquet(ParquetFileConstants.ENROLMENT_MASTER)
        .where(col("active") == True)  # More efficient than expr
        .filter(col("primaryCategory").isin(primary_categories))
    )
    
    if cache_intermediate:
        master_enrollment.cache()
        logger.info("Cached intermediate DataFrame")
    
    # Continue with transformations
    result = (master_enrollment
        .withColumn("courseOrgID", explode_outer(col("createdFor")))
        .withColumn("competencyAreaRefId", col("competencies_v6.competencyAreaRefId"))
        .withColumn("competencyThemeRefId", col("competencies_v6.competencyThemeRefId"))
        .withColumn("competencySubThemeRefId", col("competencies_v6.competencySubThemeRefId"))
        .withColumn("contentLanguage", explode_outer(col("language")))
    )
    
    # Apply selections and transformations
    result = _select_enrollment_course_batch_columns(result)
    result = _apply_enrollment_transformations(result)
    
    # Final cleanup
    final_df = (result
        .dropDuplicates(["userID", "courseID", "batchID"])
        .drop("completedon", "lastcontentaccesstime", "issued_certificates", "contentstatus",
            "competencyAreaRefId", "competencyThemeRefId", "competencySubThemeRefId")
    )
    
    return final_df


def _select_enrollment_course_batch_columns(df: DataFrame) -> DataFrame:
    """
    Select required columns from enrollment DataFrame
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: DataFrame with selected columns
    """
    
    return df.select(
        # Enrollment columns
        col("userid").alias("userID"),
        col("courseid").alias("enrollmentCourseID"),
        col("batchid").alias("batchID"),
        col("enrolled_date").alias("enrolledDate"),
        col("completionpercentage").alias("completionPercentage"),
        col("progress").alias("courseProgress"),
        col("status").alias("dbCompletionStatus"),
        col("completedon").alias("courseCompletedTimestamp"),
        col("enrolled_date").alias("courseEnrolledTimestamp"),
        col("lastcontentaccesstime").alias("lastContentAccessTimestamp"),
        col("contentstatus").alias("courseContentStatus"),

        # Course details columns
        col("identifier").alias("courseID"),
        col("primaryCategory").alias("category"),
        col("name").alias("courseName"),
        col("status_1").alias("courseStatus"),
        col("reviewStatus").alias("courseReviewStatus"),
        col("channel").alias("courseChannel"),
        col("lastPublishedOn").alias("courseLastPublishedOn"),
        col("duration").cast(FloatType()).alias("courseDuration"),
        col("leafNodesCount").alias("courseResourceCount"),
        col("lastStatusChangedOn").alias("lastStatusChangedOn"),
        col("courseOrgID"),
        col("competencyAreaRefId"),
        col("competencyThemeRefId"),
        col("competencySubThemeRefId"),
        col("contentLanguage"),
        col("courseCategory"),
        col("programDirectorName"),

        # Batch columns
        col("name").alias("courseBatchName"),
        col("createdby").alias("courseBatchCreatedBy"),
        col("start_date").alias("courseBatchStartDate"),
        col("end_date").alias("courseBatchEndDate"),
        col("batch_attributes").alias("courseBatchAttrs"),

        #Org ownership
        col("orgname").alias("courseOrgName"),
        col("status_1_1").alias("courseOrgStatus"),
        
        # Additional columns for transformations
        col("issued_certificates"),
    )
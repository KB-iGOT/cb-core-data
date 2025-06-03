import sys
from pathlib import Path
from typing import List

from dfutil.user.userDFUtil import exportDFToParquet
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, lit, explode, element_at, size, when, coalesce,
    expr, date_format, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

sys.path.append(str(Path(__file__).resolve().parents[2]))
from constants.ParquetFileConstants import ParquetFileConstants

def preComputeEnrolment(
    spark: SparkSession,
    extra_cols: List[str] = None,
    dates_as_long: bool = False
) -> DataFrame:
    if extra_cols is None:
        extra_cols = []
    
    # Define base columns - using list for better performance
    base_cols = [
        "userID", "courseID", "batchID", "courseProgress", 
        "dbCompletionStatus", "courseCompletedTimestamp",
        "courseEnrolledTimestamp", "lastContentAccessTimestamp", 
        "issuedCertificateCount", "issuedCertificateCountPerContent", 
        "firstCompletedOn", "certificateGeneratedOn", "certificateID"
    ]
    
    select_cols = base_cols + extra_cols
        
    enrolmentRawDF=spark.read.parquet(ParquetFileConstants.ENROLMENT_PARQUET_FILE)
    enrolmentDF=enrolmentRawDF.withColumn("courseCompletedTimestamp", col("completedon")) \
        .withColumn("courseEnrolledTimestamp", col("enrolled_date")) \
        .withColumn("lastContentAccessTimestamp", col("lastcontentaccesstime")) \
        .withColumn("cert_array_size", coalesce(size(col("issued_certificates")), lit(0))) \
        .withColumn("issuedCertificateCount", col("cert_array_size")) \
        .withColumn(
            "issuedCertificateCountPerContent",
            when(col("cert_array_size") > 0, lit(1)).otherwise(lit(0)),
        ) \
        .withColumn(
            "certificateGeneratedOn",
            when(
                col("cert_array_size") > 0,
                element_at(col("issued_certificates"), -1)["lastIssuedOn"],
            ).otherwise(lit("")),
        ) \
        .withColumn(
            "firstCompletedOn",
            when(
                col("cert_array_size") > 0,
                element_at(col("issued_certificates"), 1)["lastIssuedOn"],
            ).otherwise(lit("")),
        ) \
        .withColumn(
            "certificateID",
            when(
                col("cert_array_size") > 0,
                element_at(col("issued_certificates"), -1)["identifier"],
            ).otherwise(lit("")),
        ) \
        .withColumnRenamed("userid", "userID") \
        .withColumnRenamed("courseid", "courseID") \
        .withColumnRenamed("batchid", "batchID") \
        .withColumnRenamed("progress", "courseProgress") \
        .withColumnRenamed("status", "dbCompletionStatus") \
        .withColumnRenamed("contentstatus", "courseContentStatus") \
        .fillna(
            {
                "courseProgress": 0,
                "issuedCertificateCount": 0,
                "certificateGeneratedOn": "",
                "firstCompletedOn": "",
                "certificateID": "",
            }
        ) \
        .drop("cert_array_size") \
        .select(*select_cols)
    
    if dates_as_long:
        date_columns = [
            "courseCompletedTimestamp", 
            "courseEnrolledTimestamp", 
            "lastContentAccessTimestamp"
        ]
        
        for date_col in date_columns:
            enrolmentDF = enrolmentDF.withColumn(date_col, col(date_col).cast("long"))

    exportDFToParquet(enrolmentDF,ParquetFileConstants.ENROLMENT_SELECT_PARQUET_FILE)

    batchDF= spark.read.parquet(ParquetFileConstants.BATCH_PARQUET_FILE) \
            .select(
                col("courseid").alias("courseID"),
                col("batchid").alias("batchID"),
                col("name").alias("courseBatchName"),
                col("createdby").alias("courseBatchCreatedBy"),
                col("start_date").alias("courseBatchStartDate"),
                col("end_date").alias("courseBatchEndDate"),
                col("batch_attributes").alias("courseBatchAttrs")
            ) \
            .fillna("{}", subset=["courseBatchAttrs"])
    
    enrolmentDF = enrolmentDF.join(
    batchDF,
    on=["courseID", "batchID"],
    how="left"
)
    exportDFToParquet(enrolmentDF,ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE)

def preComputeExternalEnrolment(spark: SparkSession,) -> DataFrame:
    currentDateTime = date_format(current_timestamp(), ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT)


    externalEnrolmentDF= spark.read.parquet(ParquetFileConstants.EXTERNAL_COURSE_ENROLMENTS_PARQUET_FILE) \
        .withColumnRenamed("userid", "userID") \
        .withColumnRenamed("courseid", "content_id") \
        .withColumnRenamed("content_id", "courseID") \
        .withColumnRenamed("progress", "courseProgress") \
        .withColumnRenamed("status", "dbCompletionStatus") \
        .withColumn(
            "courseCompletedTimestamp",
            date_format(col("completedon"), ParquetFileConstants.DATE_TIME_FORMAT)
        ) \
        .withColumn(
            "courseEnrolledTimestamp",
            date_format(col("enrolled_date"), ParquetFileConstants.DATE_TIME_FORMAT)
        ) \
        .withColumn("lastContentAccessTimestamp", lit("Not Available")) \
        .withColumn("userRating", lit("Not Available")) \
        .withColumn("live_cbp_plan_mandate", lit(False)) \
        .withColumn("batchID", lit("Not Available")) \
        .withColumn("issuedCertificateCount", size(col("issued_certificates"))) \
        .withColumn(
            "certificate_generated",
            expr("CASE WHEN issuedCertificateCount > 0 THEN 'Yes' ELSE 'No' END")
        ) \
        .withColumn(
            "certificateGeneratedOn",
            when(col("issued_certificates").isNull(), "")
            .otherwise(
                col("issued_certificates")[size(col("issued_certificates")) - 1].getItem("lastIssuedOn")
            )
        ) \
        .withColumn(
            "firstCompletedOn",
            when(col("issued_certificates").isNull(), "")
            .otherwise(
                when(size(col("issued_certificates")) > 0,
                     col("issued_certificates")[0].getItem("lastIssuedOn"))
                .otherwise("")
            )
        ) \
        .withColumn(
            "certificateID",
            when(col("issued_certificates").isNull(), "")
            .otherwise(
                col("issued_certificates")[size(col("issued_certificates")) - 1].getItem("identifier")
            )
        ) \
        .withColumn("Report_Last_Generated_On", lit(currentDateTime)) \
        .na.fill(0, subset=["courseProgress", "issuedCertificateCount"]) \
        .na.fill("", subset=["certificateGeneratedOn"]) \
        
    exportDFToParquet(externalEnrolmentDF,ParquetFileConstants.EXTERNAL_ENROLMENT_COMPUTED_PARQUET_FILE)

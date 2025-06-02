import sys
from pathlib import Path
from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, explode_outer, when, expr, concat_ws, rtrim, lit, unix_timestamp,coalesce,regexp_replace
)
from pyspark.sql.types import LongType


sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckutil import schemas
from constants.ParquetFileConstants import ParquetFileConstants

def preComputeUser(spark: SparkSession) -> DataFrame:
    userRawDF = spark.read.parquet(ParquetFileConstants.USER_PARQUET_FILE)

    # Select and rename base fields
    userDF = userRawDF.select(
        col("id").alias("userID"),
        col("firstname").alias("firstName"),
        col("lastname").alias("lastName"),
        col("maskedemail").alias("maskedEmail"),
        col("maskedphone").alias("maskedPhone"),
        col("rootorgid").alias("userOrgID"),
        col("status").alias("userStatus"),
        col("profiledetails").alias("userProfileDetails"),
        col("createddate").alias("userCreatedTimestamp"),
        col("updateddate").alias("userUpdatedTimestamp"),
        col("createdby").alias("userCreatedBy")
    )

    # Handle nulls
    userDF = userDF.na.fill("", subset=["userOrgID", "firstName", "lastName"])
    userDF = userDF.na.fill("{}", subset=["userProfileDetails"])

    # Parse JSON profileDetails string
    userDF = userDF.withColumn("profileDetails", from_json(col("userProfileDetails"), profileDetailsSchema))

    # Explode and extract nested fields
    userDF = userDF \
        .withColumn("personalDetails", col("profileDetails.personalDetails")) \
        .withColumn("employmentDetails", col("profileDetails.employmentDetails")) \
        .withColumn("professionalDetails", explode_outer(col("profileDetails.professionalDetails"))) \
        .withColumn("userVerified", coalesce(col("profileDetails.verifiedKarmayogi"), lit(False))) \
        .withColumn("userMandatoryFieldsExists", col("profileDetails.mandatoryFieldsExists")) \
        .withColumn("userProfileImgUrl", col("profileDetails.profileImageUrl")) \
        .withColumn("userProfileStatus", col("profileDetails.profileStatus")) \
        .withColumn("userPhoneVerified", expr("LOWER(personalDetails.phoneVerified) = 'true'")) \
        .withColumn("fullName", rtrim(concat_ws(" ", col("firstName"), col("lastName"))))

    # Handle `additionalProperties` fallback
    userDF = userDF.withColumn(
        "additionalProperties",
        when(col("profileDetails.additionalProperties").isNotNull(), col("profileDetails.additionalProperties"))
        .otherwise(col("profileDetails.additionalPropertis"))
    )

    # Drop now-unnecessary JSON fields
    userDF = userDF.drop("profileDetails", "userProfileDetails")

    # Convert timestamp fields (assuming this function exists)
    userDF = timestampStringToLong(userDF, ["userCreatedTimestamp", "userUpdatedTimestamp"])
    exportDFToParquet(userDF,ParquetFileConstants.USER_COMPUTED_PARQUET_FILE)


def exportDFToParquet(df,outputFile):
    df.write.parquet(outputFile)

def timestampStringToLong(df: DataFrame, column_names: list, format: str = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") -> DataFrame:
    """
    Converts ISO timestamp string columns to long (epoch milliseconds).
    
    Args:
        df (DataFrame): Input DataFrame.
        column_names (list): List of column names (timestamp strings).
        format (str): Timestamp format. Default is ISO-8601 with 'Z'.

    Returns:
        DataFrame: Modified DataFrame with long timestamps.
    """
    for col_name in column_names:
        df = df.withColumn(
            col_name,
            (unix_timestamp(col(col_name), format) * 1000).cast("long")
        )
    return df
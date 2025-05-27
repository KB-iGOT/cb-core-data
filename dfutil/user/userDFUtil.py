import sys
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, explode_outer, when, expr, concat_ws, rtrim, lit, unix_timestamp,coalesce,regexp_replace
)
from pyspark.sql.types import LongType


sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckutil import schemas
from ParquetFileConstants import ParquetFileConstants

def getUserOrgHierarchy(spark: SparkSession) -> DataFrame:
    """
    Process user organization hierarchy data from parquet file
    
    Args:
        spark: SparkSession instance
    
    Returns:
        DataFrame: Processed user organization hierarchy DataFrame
    """
    
    # Read parquet file
    user_org_hierarchy = spark.read.parquet(ParquetFileConstants.USER_ORG_HIERARCHY_COMPUTED_PARQUET_FILE)
    
    # Parse profiledetails JSON and extract designation from professionalDetails
    profile_details_schema = schemas.makeProfileDetailsSchema(
        additionalProperties=True, 
        professionalDetails=True
    )
    
    # Select and transform columns
    result_df = user_org_hierarchy.select(
        # User columns
        col("id").alias("userID"),
        col("firstname").alias("firstName"), 
        col("lastname").alias("lastName"),
        col("maskedemail").alias("maskedEmail"),
        col("maskedphone").alias("maskedPhone"),
        col("rootorgid").alias("userOrgID"),
        col("status").alias("userStatus"),
        col("createddate").alias("userCreatedTimestamp"),
        col("updateddate").alias("userUpdatedTimestamp"),
        col("profiledetails").alias("userProfileDetails"),
        col("createdby").alias("userCreatedBy"),

        # Organization columns   
        # col("id_1").alias("orgID"),
        # col("orgname").alias("orgName"),
        # col("status_1").alias("orgStatus"),
        # col("createddate_1").alias("orgCreatedDate"),
        # col("organisationtype").alias("orgType"),
        # col("organisationsubtype").alias("orgSubType"),

        # col("id_1").alias("userOrgID"),
        col("orgname").alias("userOrgName"),
        col("status_1").alias("userOrgStatus"),
        col("createddate_1").alias("userOrgCreatedDate"),
        col("organisationtype").alias("userOrgType"),
        col("organisationsubtype").alias("userOrgSubType"),

        # Hierarchy columns
        col("department").alias("dept_name"),
        col("ministry").alias("ministry_name")
    ).na.fill("", ["userOrgID", "firstName", "lastName", "userOrgName"]) \
    .na.fill("{}", ["userProfileDetails"]) \
    .withColumn("profileDetails", from_json(col("userProfileDetails"), profile_details_schema)) \
    .withColumn("personalDetails", col("profileDetails.personalDetails")) \
    .withColumn("employmentDetails", col("profileDetails.employmentDetails")) \
    .withColumn("professionalDetails", explode_outer(col("profileDetails.professionalDetails"))) \
    .withColumn("designation", coalesce(col("professionalDetails.designation"), lit(""))) \
    .withColumn("userVerified", 
        when(col("profileDetails.verifiedKarmayogi").isNull(), False)
        .otherwise(col("profileDetails.verifiedKarmayogi"))) \
    .withColumn("userMandatoryFieldsExists", col("profileDetails.mandatoryFieldsExists")) \
    .withColumn("userProfileImgUrl", col("profileDetails.profileImageUrl")) \
    .withColumn("userProfileStatus", col("profileDetails.profileStatus")) \
    .withColumn("userPhoneVerified", expr("LOWER(personalDetails.phoneVerified) = 'true'")) \
    .withColumn("fullName", rtrim(concat_ws(" ", col("firstName"), col("lastName"))))
    
    # Try to add additionalProperties (handle potential typo in column name)
    try:
        result_df = result_df.withColumn("additionalProperties", col("profileDetails.additionalPropertis"))
    except:
        result_df = result_df.withColumn("additionalProperties", col("profileDetails.additionalProperties"))
    
    # Drop intermediate columns
    result_df = result_df.drop("profileDetails", "userProfileDetails")

    # result_df = result_df \
    # .withColumn("personalDetails", col("personalDetails").cast("string")) \
    # .withColumn("employmentDetails", col("employmentDetails").cast("string")) \
    # .withColumn("professionalDetails", col("professionalDetails").cast("string")) \
    # .withColumn("additionalProperties", col("additionalProperties").cast("string"))
    
    # # Convert timestamps to long (Unix timestamp)
    result_df = result_df \
       .withColumn("userCreatedTimestamp", 
                regexp_replace(col("userCreatedTimestamp"), r":(\d{3})\+", r".$1+")) \
    .withColumn("userUpdatedTimestamp", 
                regexp_replace(col("userUpdatedTimestamp"), r":(\d{3})\+", r".$1+")) \
    .withColumn("userOrgCreatedDate", 
                regexp_replace(col("userOrgCreatedDate"), r":(\d{3})\+", r".$1+")) \
    .withColumn("userCreatedTimestamp", 
                regexp_replace(col("userCreatedTimestamp"), r"\+0000$", r"Z")) \
    .withColumn("userUpdatedTimestamp", 
                regexp_replace(col("userUpdatedTimestamp"), r"\+0000$", r"Z")) \
    .withColumn("userOrgCreatedDate", 
                regexp_replace(col("userOrgCreatedDate"), r"\+0000$", r"Z")) \
    .withColumn("userCreatedTimestamp", unix_timestamp(col("userCreatedTimestamp")).cast(LongType())) \
    .withColumn("userUpdatedTimestamp", unix_timestamp(col("userUpdatedTimestamp")).cast(LongType())) \
    .withColumn("userOrgCreatedDate", unix_timestamp(col("userOrgCreatedDate")).cast(LongType()))
    
    return result_df

def userCourseRatingDataframe(spark):
    df = spark.read.parquet(ParquetFileConstants.RATING_PARQUET_FILE).select(
    col("activityid").alias("courseID"),
    col("userid").alias("userID"),
    col("rating").alias("userRating"),
    col("activitytype").alias("cbpType"),
    col("createdon").alias("createdOn")
    )
    return df
import sys
from pathlib import Path
from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    sum,collect_list,col, from_json, explode_outer, when, expr, concat_ws, rtrim, lit, unix_timestamp,coalesce,regexp_replace
)
from pyspark.sql.types import LongType


sys.path.append(str(Path(__file__).resolve().parents[2]))
from util import schemas
from constants.ParquetFileConstants import ParquetFileConstants

def preComputeUser(spark: SparkSession) -> DataFrame:
    profileDetailsSchema = schemas.makeProfileDetailsSchema(False,True,True)
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
    

    roleRawDF = spark.read.parquet(ParquetFileConstants.ROLE_PARQUET_FILE)
    roleRawDF = roleRawDF \
        .withColumnRenamed("userID", "rowUserID") \
        .groupBy("rowUserID") \
        .agg(concat_ws(", ", collect_list("role")).alias("role"))

    userDF = userDF.join(
        roleRawDF,
        userDF["userID"] == roleRawDF["rowUserID"]
    )
    userDF = userDF.drop("rowUserID")


    karma_df = spark.read.parquet(ParquetFileConstants.USER_KARMA_POINTS_PARQUET_FILE)

    # Group by 'userid', aggregate 'points', and rename columns
    karma_df = karma_df.groupBy(col("userid").alias("karmaUserID")) \
           .agg(sum(col("points")).alias("total_points"))

    userDF = userDF.join(
        karma_df,
        userDF["userID"] == karma_df["karmaUserID"]
    )
    userDF = userDF.drop("karmaUserID")
    exportDFToParquet(userDF,ParquetFileConstants.USER_SELECT_PARQUET_FILE)
    # userDF.
    


def preComputeOrgWithHierarchy(spark: SparkSession):
     orgRawDF = spark.read.parquet(ParquetFileConstants.ORG_PARQUET_FILE)
     
     org_df = orgRawDF \
        .select(
            col("id").alias("orgID"),
            col("orgname").alias("orgName"),
            col("status").alias("orgStatus"),
            col("createddate").alias("orgCreatedDate"),
            col("organisationtype").alias("orgType"),
            col("organisationsubtype").alias("orgSubType")
        ) \
        .na.fill({"orgName": ""})

     org_computed_df = timestampStringToLong(org_df, ["orgCreatedDate"])
     org_hierarch_raw_df = spark.read.parquet(ParquetFileConstants.ORG_HIERARCHY_PARQUET_FILE)
     org_hierarch_computed_df = org_hierarch_raw_df \
        .select(
            col("mdo_id").alias("userOrgID"),
            col("department").alias("dept_name"),
            col("ministry").alias("ministry_name")
        )
     exportDFToParquet(org_computed_df, ParquetFileConstants.ORG_SELECT_PARQUET_FILE)
     exportDFToParquet(org_hierarch_computed_df, ParquetFileConstants.ORG_HIERARCHY_SELECT_PARQUET_FILE)

     org_merged_df = org_computed_df.join(
        org_hierarch_computed_df,
        org_computed_df["orgID"] == org_hierarch_computed_df["userOrgID"],
        "left"
     )
     org_merged_df = org_merged_df.drop("userOrgID")
     exportDFToParquet(org_merged_df, ParquetFileConstants.ORG_COMPUTED_PARQUET_FILE)

def preComputeOrgHierarchyWithUser(spark: SparkSession):
    org_merged_df = spark.read.parquet(ParquetFileConstants.ORG_COMPUTED_PARQUET_FILE+f"/**.parquet")
    user_merged_df = spark.read.parquet(ParquetFileConstants.USER_SELECT_PARQUET_FILE+f"/**.parquet")
    user_org_merged_df = user_merged_df.join(
        org_merged_df,
        user_merged_df["userOrgID"] == org_merged_df["orgID"]
    )
    exportDFToParquet(user_org_merged_df,ParquetFileConstants.USER_ORG_COMPUTED_FILE)

def exportDFToParquet(df,outputFile):
   df.write.mode("overwrite").option("compression", "snappy").parquet(outputFile)

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
import sys
from pathlib import Path
from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    sum,collect_list,col, from_json, explode_outer, when, expr, concat_ws, rtrim, lit, unix_timestamp,coalesce,regexp_replace,bround,countDistinct
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
        .withColumn("fullName", rtrim(concat_ws(" ", col("firstName"), col("lastName")))) \
        .withColumn("designation", coalesce(col("professionalDetails.designation"), lit(""))) \
        .withColumn("group", coalesce(col("professionalDetails.group"), lit(""))) \
        .withColumn("userPrimaryEmail", col("personalDetails.primaryEmail")) \
        .withColumn("userMobile", col("personalDetails.mobile"))

    # Handle `additionalProperties` fallback
    userDF = userDF.withColumn(
        "additionalProperties",
        when(col("profileDetails.additionalProperties").isNotNull(), col("profileDetails.additionalProperties"))
        .otherwise(col("profileDetails.additionalPropertis"))
    ) \
    .withColumn("Tag", concat_ws(", ", col("additionalProperties.tag")))

    # Drop now-unnecessary JSON fields
    userDF = userDF.drop("profileDetails", "userProfileDetails")

    # Convert timestamp fields (assuming this function exists)
    userDF = timestampStringToLong(userDF, ["userCreatedTimestamp", "userUpdatedTimestamp"])    
    exportDFToParquet(userDF,ParquetFileConstants.USER_SELECT_PARQUET_FILE)
    
    roleRawDF = spark.read.parquet(ParquetFileConstants.ROLE_PARQUET_FILE)
    roleRawDF = roleRawDF \
        .withColumnRenamed("userID", "rowUserID") \
        .groupBy("rowUserID") \
        .agg(concat_ws(", ", collect_list("role")).alias("role"))

    userDF = userDF.join(roleRawDF, userDF["userID"] == roleRawDF["rowUserID"], how="left").drop("rowUserID")
    print(f"User Role DF Count: {userDF.count()}")
    userDF = userDF.drop("rowUserID")


    karma_df = spark.read.parquet(ParquetFileConstants.USER_KARMA_POINTS_PARQUET_FILE)
    karma_df = karma_df.groupBy(col("userid").alias("karmaUserID")) \
        .agg(sum(col("points")).alias("total_points"))

    userDF = userDF.join(karma_df, userDF["userID"] == karma_df["karmaUserID"], how="left").drop("karmaUserID")

    weekly_claps_df = spark.read.parquet(ParquetFileConstants.CLAPS_PARQUET_FILE) \
        .withColumnRenamed("userid", "userID") \
        .withColumnRenamed("total_claps", "weekly_claps_day_before_yesterday") \
        .select("userID", "weekly_claps_day_before_yesterday")

    userDF = userDF.join(weekly_claps_df, on="userID", how="left")
    userDF = userDF.drop("weeklyClaspUserID")
    print(f"User Clap DF Count: {userDF.count()}")



    exportDFToParquet(userDF, ParquetFileConstants.USER_COMPUTED_PARQUET_FILE)
    return userDF
    


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
     ).drop("userOrgID")
     exportDFToParquet(org_merged_df, ParquetFileConstants.ORG_COMPUTED_PARQUET_FILE)

def preComputeOrgHierarchyWithUser(spark: SparkSession):
    org_merged_df = spark.read.parquet(ParquetFileConstants.ORG_COMPUTED_PARQUET_FILE+f"/**.parquet")
    org_merged_df = org_merged_df.select(col("orgID").alias("usermergedOrgID"),
      col("orgName").alias("userOrgName"),
      col("orgStatus").alias("userOrgStatus"),
      col("orgCreatedDate").alias("userOrgCreatedDate"),
      col("orgType").alias("userOrgType"),
      col("orgSubType").alias("userOrgSubType"),
      col("dept_name"),
      col("ministry_name"))
    user_merged_df = spark.read.parquet(ParquetFileConstants.USER_SELECT_PARQUET_FILE+f"/**.parquet")
    user_org_merged_df = user_merged_df.join(
        org_merged_df,
        user_merged_df["userOrgID"] == org_merged_df["usermergedOrgID"]
    ).drop("usermergedOrgID")
    exportDFToParquet(user_org_merged_df,ParquetFileConstants.USER_ORG_COMPUTED_FILE)

def appendContentDurationCompletionForEachUser(spark: SparkSession, user_master_df: DataFrame, user_enrolment_df: DataFrame, content_duration_df: DataFrame) -> DataFrame:
    userdf_with_enrolment_counts = user_enrolment_df \
        .join(content_duration_df, user_enrolment_df["courseid"] == content_duration_df["content_id"], how="left") \
        .groupBy("userID") \
        .agg(
            countDistinct(
                when(col("user_consumption_status").isin("not-started", "in-progress", "completed"), col("content_id"))
            ).alias("total_content_enrolments"),
            countDistinct(
                when((col("user_consumption_status") == "completed") & col("certificateID").isNotNull(), col("content_id"))
            ).alias("total_content_completions"),
            sum(
                when(
                    (col("user_consumption_status") == "completed") &
                    col("certificateID").isNotNull() &
                    (col("category") == "Course"),
                    coalesce(col("courseDuration"), lit(0.0))
                )
            ).alias("total_content_duration")
        ) \
        .withColumn("total_content_duration", bround(col("total_content_duration") / 3600.0, 2))

    user_enrolment_master_df = user_master_df.join(userdf_with_enrolment_counts, on="userID", how="left")
    return user_enrolment_master_df


def appendEventDurationCompletionForEachUser(spark: SparkSession, user_enrolment_df: DataFrame) -> DataFrame:
    """
    Appends event enrolment, completion, and learning hours (with certificates) to user enrolment DataFrame.
    """
    user_event_details_df = spark.read.parquet(ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE) \
        .withColumnRenamed("user_id", "userID") \
        .groupBy("userID") \
        .agg(
            countDistinct(
                when(col("status").isin("not-started", "in-progress", "completed"), col("event_id"))
            ).alias("total_event_enrolments"),
            countDistinct(
                when(col("status") == "completed", col("event_id"))
            ).alias("total_event_completions"),
            sum(
                when((col("status") == "completed") & col("certificate_id").isNotNull(), col("event_duration_seconds"))
            ).alias("total_event_learning_hours_with_certificates")
        ).withColumn("total_event_learning_hours_with_certificates", bround(col("total_event_learning_hours_with_certificates") / 3600.0, 2))

    print(user_event_details_df.count())
    user_event_details_df.printSchema()

    user_enrolment_df = user_enrolment_df.join(user_event_details_df, on="userID", how="left")
    return user_enrolment_df


def exportDFToParquet(df: DataFrame, outputFile: str):
    """
    Writes the DataFrame to Parquet file using snappy compression.
    """
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

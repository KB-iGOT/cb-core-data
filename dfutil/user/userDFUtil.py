import sys
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import *




# ==============================
# 1. Configuration and Constants
# ==============================

# Ensure the parent directory is in sys.path for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckutil import duckutil, datautil,schemas  # Assuming duckutil is in the parent directory
from constants.ParquetFileConstants import ParquetFileConstants
from constants.QueryConstants import QueryConstants

def getUserOrgHierarchy(spark: SparkSession):
    user_org_with_hierarchy = spark.read.parquet(ParquetFileConstants.USER_ORG_HIERARCHY_COMPUTED_PARQUET_FILE)
    print(user_org_with_hierarchy.count())
    profile_details_schema = make_profile_details_schema(professional_details=True)

    parsed_df = user_org_with_hierarchy.withColumn(
        "profileDetailsParsed", F.from_json(F.col("profiledetails"), profile_details_schema)
    )
    print(parsed_df)
    user_org_with_hierarchy_df = (
        parsed_df
        .withColumn("designation", F.coalesce(F.col("profileDetailsParsed.professionalDetails.designation"), F.lit("")))
        .withColumn("group", F.coalesce(F.col("profileDetailsParsed.professionalDetails.group"), F.lit("")))
        .withColumn("userPrimaryEmail", F.col("profileDetailsParsed.personalDetails.primaryEmail"))
        .withColumn("userMobile", F.col("profileDetailsParsed.personalDetails.mobile"))
        .withColumn("fullName", F.rtrim(F.concat_ws(" ", F.col("firstName"), F.col("lastName"))))
        .withColumn('userStatus',F.col('status'))
        .withColumn('userOrgID',F.col('rootorgid'))
        .withColumn('ministry_name',F.col('ministry'))
        .withColumn('dept_name',F.col('deptname'))
        .withColumn('userOrgName',F.col('orgname'))
        .select(
            "userID", "fullName", "userStatus", "userPrimaryEmail", "userMobile",
            "userOrgID", "ministry_name", "dept_name", "userOrgName", "designation", "group"
        )
    )
    return user_org_with_hierarchy_df


def make_profile_details_schema(
    competencies=False,
    additional_properties=False,
    professional_details=False
):
    fields = [
        StructField("verifiedKarmayogi", BooleanType(), True),
        StructField("mandatoryFieldsExists", BooleanType(), True),
        StructField("profileImageUrl", StringType(), True),
        StructField("personalDetails", schemas.personal_details_schema, True),
        StructField("employmentDetails", schemas.employment_details_schema, True),
        StructField("profileStatus", StringType(), True)
    ]

    if competencies:
        fields.append(StructField("competencies", ArrayType(schemas.profile_competency_schema), True))

    if additional_properties:
        fields.append(StructField("additionalProperties", schemas.additional_properties_schema, True))
        fields.append(StructField("additionalPropertis", schemas.additional_properties_schema, True))  # Preserving typo?

    if professional_details:
        fields.append(StructField("professionalDetails", schemas.professional_details_schema, True))

    return StructType(fields)
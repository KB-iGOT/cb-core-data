import sys
from pathlib import Path
from dfutil.user.userDFUtil import exportDFToParquet
from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    explode,sum,collect_list,col, from_json, explode_outer, when, expr, concat_ws, rtrim, lit, unix_timestamp,coalesce,regexp_replace
)
from pyspark.sql.types import LongType


sys.path.append(str(Path(__file__).resolve().parents[3]))
from util import schemas
from constants.ParquetFileConstants import ParquetFileConstants


def preComputeACBPData(spark):
    acbp_df = spark.read.parquet(ParquetFileConstants.ACBP_PARQUET_FILE)
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    acbp_select_df = acbp_df \
        .select(
            col("id").alias("acbpID"),
            col("orgid").alias("userOrgID"),
            col("draftdata"),
            col("status").alias("acbpStatus"),
            col("createdby").alias("acbpCreatedBy"),
            col("name").alias("cbPlanName"),
            col("assignmenttype").alias("assignmentType"),
            col("assignmenttypeinfo").alias("assignmentTypeInfo"),
            col("enddate").alias("completionDueDate").cast('string'),
            col("publishedat").alias("allocatedOn").cast('string'),
            col("createdat").cast('string'),
            col('updatedat').cast('string'),
            col("contentlist").alias("acbpCourseIDList")
        ) \
        .na.fill({"cbPlanName": ""})
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    acbp_select_df.printSchema()
    exportDFToParquet(acbp_select_df,ParquetFileConstants.ACBP_SELECT_FILE)
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    # Draft data
    draft_cbp_data = acbp_select_df.filter((col("acbpStatus") == "DRAFT") & col("draftdata").isNotNull()) \
        .select("acbpID", "userOrgID", "draftdata", "acbpStatus", "acbpCreatedBy") \
        .withColumn("draftData", from_json(col("draftdata"), schemas.cbplan_draft_data_schema)) \
        .withColumn("cbPlanName", col("draftData.name")) \
        .withColumn("assignmentType", col("draftData.assignmentType")) \
        .withColumn("assignmentTypeInfo", col("draftData.assignmentTypeInfo")) \
        .withColumn("completionDueDate", col("draftData.endDate").cast("string")) \
        .withColumn("allocatedOn", lit("not published")) \
        .withColumn("acbpCourseIDList", col("draftData.contentList")) \
        .drop("draftData")
    draft_cbp_data.printSchema()
    # Non-draft data
    non_draft_cbp_data = acbp_select_df.filter(col("acbpStatus") != "DRAFT")
    draft_cbp_data = draft_cbp_data.withColumn("draftdata", lit(None).cast("string"))
    # Union the two
    final_df = non_draft_cbp_data.unionByName(draft_cbp_data)
    # Cast any 'timestamp_ntz' columns to string to avoid Parquet INT96 error
    for field in final_df.schema.fields:
        if hasattr(field.dataType, "typeName") and field.dataType.typeName() == "timestamp_ntz":
            final_df = final_df.withColumn(field.name, col(field.name).cast("string"))
    print_nested_schema(final_df)
    exportDFToParquet(acbp_df,ParquetFileConstants.ACBP_SELECT_FILE)
    explodeAcbpData(spark, final_df)

def explodeAcbpData(spark,acbp_df):
    
    selectColumns = ["userID", "fullName", "userPrimaryEmail", "userMobile", "designation", "group", "userOrgID", "ministry_name", "dept_name", "userOrgName", "userStatus", "acbpID",
      "assignmentType", "completionDueDate", "allocatedOn", "acbpCourseIDList","acbpStatus", "acbpCreatedBy","cbPlanName"]
    user_df = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
    acbp_custom_user_df = acbp_df \
        .filter(col("assignmentType") == "CustomUser") \
        .withColumn("userID", explode(col("assignmentTypeInfo"))) \
        .join(user_df, ["userID", "userOrgID"], "left")
    print(f"ACBP Custom User DataFrame Count: {acbp_custom_user_df.count()}")

    # Designation
    acbp_designation_df = acbp_df \
        .filter(col("assignmentType") == "Designation") \
        .withColumn("designation", explode(col("assignmentTypeInfo"))) \
        .join(user_df, ["userOrgID", "designation"], "left")
    print(f"ACBP Designation DataFrame Count: {acbp_designation_df.count()}")
    # AllUser
    acbp_all_user_df = acbp_df \
        .filter(col("assignmentType") == "AllUser") \
        .join(user_df, ["userOrgID"], "left")
    print(f"ACBP All User DataFrame Count: {acbp_all_user_df.count()}")
    # Union the three DataFrames
    dfs = [acbp_custom_user_df, acbp_designation_df, acbp_all_user_df]
    selected_dfs = [df.select([col(c) for c in selectColumns]) for df in dfs]
    acbp_allotment_df = selected_dfs[0]

    
    print_nested_schema(acbp_allotment_df)
    exportDFToParquet(acbp_allotment_df,ParquetFileConstants.ACBP_COMPUTED_FILE)


def exportDFToParquet(df,outputFile):
   df_cleaned = cast_ntz_to_string(df)
   df_cleaned.write.mode("overwrite").option("compression", "snappy").parquet(outputFile)

def cast_ntz_to_string(df):
    for field in df.schema.fields:
        if field.dataType.simpleString() == "timestamp_ntz":
            df = df.withColumn(field.name, col(field.name).cast("string"))
    return df

def print_nested_schema(df, prefix=""):
    for field in df.schema.fields:
        dt = field.dataType
        name = prefix + field.name
        if isinstance(dt, StructType):
            print_nested_schema(df.select(f"{name}.*"), prefix=name + ".")
        else:
            print(f"{name}: {dt}")


import sys
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType



# ==============================
# 1. Configuration and Constants
# ==============================

# Ensure the parent directory is in sys.path for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckutil import duckutil, datautil  # Assuming duckutil is in the parent directory
from constants.ParquetFileConstants import ParquetFileConstants
from constants.QueryConstants import QueryConstants

def getACBPDetailsDF(spark: SparkSession, cbplan_draft_data_schema: StructType):
     acbp_df = spark.read.parquet(ParquetFileConstants.ACBP_PARQUET_FILE)
    
    # Load and select fields
     df = (
        acbp_df
        .select(
            F.col("id").alias("acbpID"),
            F.col("orgid").alias("userOrgID"),
            F.col("draftdata"),
            F.col("status").alias("acbpStatus"),
            F.col("createdby").alias("acbpCreatedBy"),
            F.col("name").alias("cbPlanName"),
            F.col("assignmenttype").alias("assignmentType"),
            F.col("assignmenttypeinfo").alias("assignmentTypeInfo"),
            F.col("enddate").alias("completionDueDate"),
            F.col("publishedat").alias("allocatedOn"),
            F.col("contentlist").alias("acbpCourseIDList"),
        )
        .na.fill({"cbPlanName": ""})
    )

    # Draft ACBP Data
     draft_cbp_data = (
        df.filter((F.col("acbpStatus") == "DRAFT") & F.col("draftdata").isNotNull())
        .select("acbpID", "userOrgID", "draftdata", "acbpStatus", "acbpCreatedBy")
        .withColumn("draftData", F.from_json(F.col("draftdata"), cbplan_draft_data_schema))
        .withColumn("cbPlanName", F.col("draftData.name"))
        .withColumn("assignmentType", F.col("draftData.assignmentType"))
        .withColumn("assignmentTypeInfo", F.col("draftData.assignmentTypeInfo"))
        .withColumn("completionDueDate", F.col("draftData.endDate"))
        .withColumn("allocatedOn", F.lit("not published"))
        .withColumn("acbpCourseIDList", F.col("draftData.contentList"))
        .drop("draftData","drafdata")
    )

    # Live & Retired ACBP Data
     non_draft_cbp_data = df.filter(F.col("acbpStatus") != "DRAFT").drop("draftdata")
     print(non_draft_cbp_data)
     print("####################################")
     print(draft_cbp_data)
    # Union
     final_df = non_draft_cbp_data.unionByName(draft_cbp_data)

     return final_df


def exploded_acbp_details(acbp_df: DataFrame, user_data_df: DataFrame, columns: list) -> DataFrame:
    # CustomUser
    acbp_custom_user_allotment_df = (
        acbp_df
        .filter(F.col("assignmentType") == "CustomUser")
        .withColumn("userID", F.explode(F.col("assignmentTypeInfo")))
        .join(user_data_df, on=["userID", "userOrgID"], how="left")
    )
    print("ACBP CUSTOM USER ******************************")
    # Designation
    acbp_designation_allotment_df = (
        acbp_df
        .filter(F.col("assignmentType") == "Designation")
        .withColumn("designation", F.explode(F.col("assignmentTypeInfo")))
        .join(user_data_df, on=["userOrgID", "designation"], how="left")
    )
    print("ACBP DESIGNATION ******************************")
    # AllUser
    acbp_all_user_allotment_df = (
        acbp_df
        .filter(F.col("assignmentType") == "AllUser")
        .join(user_data_df, on=["userOrgID"], how="left")
    )
    print("ACBP ALL USER ******************************")
    # Union all three
    acbp_allotment_df = (
        acbp_custom_user_allotment_df.select(*[F.col(c) for c in columns])
        .unionByName(acbp_designation_allotment_df.select(*[F.col(c) for c in columns]))
        .unionByName(acbp_all_user_allotment_df.select(*[F.col(c) for c in columns]))
    )

    return acbp_allotment_df 

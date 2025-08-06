import findspark
findspark.init()
import sys
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import bround, col, broadcast, concat_ws, coalesce, lit, when, from_unixtime
from pyspark.sql.functions import col, lit, coalesce, concat_ws, when, broadcast, get_json_object, rtrim
from pyspark.sql.functions import col, from_json, explode_outer, coalesce, lit ,format_string,count
from pyspark.sql.types import StructType, ArrayType, StringType, BooleanType, StructField
from pyspark.sql.types import MapType, StringType, StructType, StructField,FloatType,LongType, DateType,IntegerType
from pyspark.sql.functions import col, when, size, lit, expr, unix_timestamp, date_format, from_json, current_timestamp, to_date, round, explode, to_utc_timestamp, from_utc_timestamp,to_timestamp,sum as spark_sum
from pyspark.sql.functions import col, desc, row_number
from pyspark.sql.window import Window

from datetime import datetime
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))
from dfutil.content import contentDFUtil

from dfutil.dfexport import dfexportutil
from jobs.config import get_environment_config
from jobs.default_config import create_config

from constants.ParquetFileConstants import ParquetFileConstants

class ACBPModel:    
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.ACBPModel"
        
    def name(self):
        return "ACBPModel"
    
    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

    def process_data(self, spark,config):
        try:
            today = self.get_date()
            currentDateTime = date_format(current_timestamp(), ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT)
            primary_categories = ["Course", "Program", "Blended Program", "Curated Program", "Standalone Assessment"]
            
            userOrgDF = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE).select("userID", "fullName", "userStatus", "userPrimaryEmail", "userMobile", "userOrgID", "ministry_name", "dept_name", "userOrgName", "designation", "group")
            contentHierarchyDF = spark.read.parquet(ParquetFileConstants.CONTENT_HIERARCHY_SELECT_PARQUET_FILE)
            allCourseProgramESDF = spark.read.parquet(ParquetFileConstants.ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE).filter(col("category").isin(primary_categories))

            allCourseProgramDetailsDF = contentDFUtil.allCourseProgramDetailsWithCompetenciesJsonDataFrame(allCourseProgramESDF,contentHierarchyDF,spark.read.parquet(ParquetFileConstants.ORG_SELECT_PARQUET_FILE)).drop("competenciesJson")

            enrolmentDF = spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE)

            acbpAllEnrolmentDF = spark.read.parquet(ParquetFileConstants.ACBP_COMPUTED_FILE) \
                                 .withColumn("courseID", explode(col("acbpCourseIDList"))) \
                                .join(allCourseProgramDetailsDF, ["courseID"], "left") \
                                .join(enrolmentDF, ["courseID", "userID"], "left") \
                                .na.drop(subset=["userID", "courseID"]) \
                                .drop("acbpCourseIDList")
            
            cbPlanWarehouseDF = acbpAllEnrolmentDF \
                .select(
                    "userOrgID", "acbpCreatedBy", "acbpID", "cbPlanName", 
                    "assignmentType", "userID", "designation", "courseID",
                    "allocatedOn", "completionDueDate", "acbpStatus"
                ) \
                .withColumn(
                    "allotment_to", 
                    F.when(col("assignmentType") == "CustomUser", col("userID"))
                    .when(col("assignmentType") == "Designation", col("designation"))  
                    .when(col("assignmentType") == "AllUser", F.lit("All Users"))
                    .otherwise(F.lit("No Records"))
                ) \
                .withColumn("data_last_generated_on", F.lit(currentDateTime)) \
                .select(
                    col("userOrgID").alias("org_id"),
                    col("acbpCreatedBy").alias("created_by"),
                    col("acbpID").alias("cb_plan_id"),
                    col("cbPlanName").alias("plan_name"),
                    col("assignmentType").alias("allotment_type"),
                    col("allotment_to"),
                    col("courseID").alias("content_id"),
                    date_format(col("allocatedOn"), ParquetFileConstants.DATE_TIME_FORMAT).alias("allocated_on"),
                    date_format(col("completionDueDate"), ParquetFileConstants.DATE_TIME_FORMAT).alias("due_by"),
                    col("acbpStatus").alias("status"),
                    col("data_last_generated_on")
                ) \
                .dropDuplicates() \
                .orderBy("org_id", "created_by", "plan_name")
        

            window_spec = Window.partitionBy("userID", "courseID").orderBy(desc("completionDueDate"))
        
            acbpEnrolmentDF = acbpAllEnrolmentDF \
            .where(col("acbpStatus") == "Live") \
            .withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")
        
            # kafkaDispatch(timestamped_df, conf.acbpEnrolmentTopic)

            ministry_is_empty = (col("ministry_name").isNull()) | (col("ministry_name") == "")
            dept_is_empty = (col("dept_name").isNull()) | (col("dept_name") == "")
            
            # Process all transformations in a single chain to minimize passes
            enrolmentReportDF = acbpEnrolmentDF \
                .filter(col("userStatus").cast("int") == 1) \
                .select(
                    # Select only needed columns early to reduce data shuffling
                    "fullName", "userPrimaryEmail", "userMobile", "userOrgName", "group", 
                    "designation", "ministry_name", "dept_name", "courseName", 
                    "userOrgID", "dbCompletionStatus", "courseCompletedTimestamp", 
                    "allocatedOn", "completionDueDate"
                ) \
                .withColumn(
                    "currentProgress",
                    F.when(col("dbCompletionStatus") == 2, "Completed")
                    .when(col("dbCompletionStatus") == 1, "In Progress")
                    .when(col("dbCompletionStatus") == 0, "Not Started")
                    .otherwise("Not Enrolled")
                ) \
                .withColumn("courseCompletedTimestamp", date_format(col("courseCompletedTimestamp"), ParquetFileConstants.DATE_FORMAT)) \
                .withColumn("allocatedOn", date_format(col("allocatedOn"), ParquetFileConstants.DATE_FORMAT)) \
                .withColumn("completionDueDate", date_format(col("completionDueDate"), ParquetFileConstants.DATE_FORMAT)) \
                .withColumn("MDO_Name", col("userOrgName")) \
                .withColumn(
                    "Ministry",
                    when(ministry_is_empty, col("userOrgName")).otherwise(col("ministry_name"))
                ) \
                .withColumn(
                    "Department",
                    when(
                        (col("userOrgName").isNotNull()) & 
                        (col("ministry_name") != col("userOrgName")) & 
                        dept_is_empty,
                        col("userOrgName")
                    ).otherwise(col("dept_name"))
                ) \
                .withColumn(
                    "Organization",
                    when(
                        (col("ministry_name") != col("userOrgName")) & 
                        (col("dept_name") != col("userOrgName")),
                        col("userOrgName")
                    ).otherwise(lit(""))
                ) \
                .select(
                    col("fullName").alias("Name"),
                    col("userPrimaryEmail").alias("Email"),
                    col("userMobile").alias("Phone"),
                    col("MDO_Name"),
                    col("group").alias("Group"),
                    col("designation").alias("Designation"),
                    col("Ministry"),
                    col("Department"),
                    col("Organization"),
                    col("courseName").alias("Name of CBP Allocated Course"),
                    col("allocatedOn").alias("Allocated On"),
                    col("currentProgress").alias("Current Progress"),
                    col("completionDueDate").alias("Due Date of Completion"),
                    col("courseCompletedTimestamp").alias("Actual Date of Completion"),
                    col("userOrgID").alias("mdoid"),
                    lit(currentDateTime).alias("Report_Last_Generated_On")
                ) \
                .fillna("").cache()
            


            print("📝 Writing CSV reports...")
            dfexportutil.write_single_csv_duckdb(enrolmentReportDF, f"{config.localReportDir}/{config.acbpReportPath}/{today}/CBPEnrollmentReport/CBPEnrollmentReport.csv",  f"{config.localReportDir}/temp/cbp-enrolment-report/{today}")
            dfexportutil.write_csv_per_mdo_id_duckdb(
                enrolmentReportDF, 
                f"{config.localReportDir}/{config.acbpMdoEnrolmentReportPath}/{today}", 
                'mdoid',
                f"{config.localReportDir}/temp/cbp-enrolment-report/{today}"
            )

            userSummaryReportDF = acbpEnrolmentDF \
                .filter(
                    (col("userStatus").cast("int") == 1)
                ) \
                .groupBy(
                    "userID", "fullName", "userPrimaryEmail", "userMobile", 
                    "designation", "group", "userOrgID", "ministry_name", 
                    "dept_name", "userOrgName"
                ) \
                .agg(
                    count("courseID").alias("allocatedCount"),
                    spark_sum(F.when(col("dbCompletionStatus") == 2, 1).otherwise(0)).alias("completedCount"),
                    spark_sum(
                        F.when(
                            (col("dbCompletionStatus") == 2) & 
                            (col("courseCompletedTimestamp").cast(LongType()) < 
                            (col("completionDueDate").cast(LongType()) + 86400)), 
                            1
                        ).otherwise(0)
                    ).alias("completedBeforeDueDateCount")
                ) \
                .select(
                    col("fullName").alias("Name"),
                    col("userPrimaryEmail").alias("Email"),
                    col("userMobile").alias("Phone"),
                    col("userOrgName").alias("MDO_Name"),
                    col("group").alias("Group"),
                    col("designation").alias("Designation"),
                    when(
                        (col("ministry_name").isNull()) | (col("ministry_name") == ""), 
                        col("userOrgName")
                    ).otherwise(col("ministry_name")).alias("Ministry"),
                    
                    when(
                        (col("userOrgName").isNotNull()) & 
                        (col("ministry_name") != col("userOrgName")) & 
                        ((col("dept_name").isNull()) | (col("dept_name") == "")),
                        col("userOrgName")
                    ).otherwise(col("dept_name")).alias("Department"),
                    when(
                        (col("ministry_name") != col("userOrgName")) & 
                        (col("dept_name") != col("userOrgName")),
                        col("userOrgName")
                    ).otherwise(lit("")).alias("Organization"),               
                    col("allocatedCount").alias("Number of CBP Courses Allocated"),
                    col("completedCount").alias("Number of CBP Courses Completed"),
                    col("completedBeforeDueDateCount").alias("Number of CBP Courses Completed within due date"),      
                    col("userOrgID").alias("mdoid"),
                    lit(currentDateTime).alias("Report_Last_Generated_On"))

            dfexportutil.write_single_csv_duckdb(userSummaryReportDF, f"{config.localReportDir}/{config.acbpReportPath}/{today}/CBPUserSummaryReport/CBPUserSummaryReport.csv", f"{config.localReportDir}/temp/cbp-summary-report/{today}")
            dfexportutil.write_csv_per_mdo_id_duckdb(
                userSummaryReportDF, 
                f"{config.localReportDir}/{config.acbpMdoSummaryReportPath}/{today}", 
                'mdoid',
                f"{config.localReportDir}/temp/cbp-summary-report/{today}"
            )

            print("📦 Writing warehouse data...")
            cbPlanWarehouseDF.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(f"{config.warehouseReportDir}/{config.dwCBPlanTable}")
            print("✅ Processing completed successfully!")

        except Exception as e:
            print(f"❌ Error occurred during ACBPModel processing: {str(e)}")
            raise e
            sys.exit(1)

def main():
    # Initialize Spark Session with optimized settings for caching
    spark = SparkSession.builder \
        .appName("User Enrolment Report Model - Cached") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "30g") \
        .config("spark.driver.memory", "25g") \
        .config("spark.executor.memoryFraction", "0.7") \
        .config("spark.storage.memoryFraction", "0.2") \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    # Create model instance

    config_dict = get_environment_config()
    config = create_config(config_dict)
    
    start_time = datetime.now()
    print(f"[START] ACBPModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    model = ACBPModel()
    model.process_data(spark,config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] ACBPModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()
# Example usage:
if __name__ == "__main__":
   main()
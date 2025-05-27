import sys
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import bround, col, broadcast, concat_ws, coalesce, lit, when, from_unixtime
from pyspark.sql.functions import col, lit, coalesce, concat_ws, when, broadcast, get_json_object, rtrim
from pyspark.sql.functions import col, from_json, explode_outer, coalesce, lit ,format_string
from pyspark.sql.types import StructType, ArrayType, StringType, BooleanType, StructField
from pyspark.sql.types import MapType, StringType, StructType, StructField,FloatType,LongType, DateType
from pyspark.sql.functions import col, when, size, lit, expr, unix_timestamp, date_format, from_json, current_timestamp, to_date, round, explode, to_utc_timestamp, from_utc_timestamp,to_timestamp,sum as spark_sum

from datetime import datetime
import sys


# ==============================
# 1. Configuration and Constants
# ==============================

# Ensure the parent directory is in sys.path for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckutil import duckutil,datautil,storageutil,schemas  # Assuming duckutil is in the parent directory
from dfutil.content import contentDFUtil  # Assuming duckutil is in the parent directory
from dfutil.enrolment import enrolmentDF
from dfutil.user import userDFUtil


from constants.QueryConstants import QueryConstants
from ParquetFileConstants import ParquetFileConstants

class UserEnrolmentModel:
    """
    Python implementation of KCM (Knowledge and Competency Management) Model
    """
    
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.kcm.UserEnrolmentModel"
        
    def name(self):
        return "UserEnrolmentModel"
    
    @staticmethod
    def get_date():
        """Get current date in required format"""
        return datetime.now().strftime("%Y-%m-%d")
    
    @staticmethod
    def current_date_time():
        """Get current datetime in required format"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    @staticmethod
    def duration_format(df, in_col, out_col=None):
        """
        Format duration from seconds to HH:MM:SS format
        
        Args:
            df: Input DataFrame
            in_col: Input column name containing duration in seconds
            out_col: Output column name (optional, defaults to in_col)
        
        Returns:
            DataFrame with formatted duration column
        """
        out_col_name = out_col if out_col is not None else in_col
        
        return df.withColumn(out_col_name,
            when(col(in_col).isNull(), lit(""))
            .otherwise(
                format_string("%02d:%02d:%02d",
                    expr(f"{in_col} / 3600").cast("int"),
                    expr(f"{in_col} % 3600 / 60").cast("int"),
                    expr(f"{in_col} % 60").cast("int")
                )
            )
        )

    
    def process_data(self, spark):
        try:
            duckdb_conn = duckutil.initialize_duckdb()
            today = self.get_date()
            dateTimeFormat = "yyyy-MM-dd HH:mm:ss"
            datetime_with_ampm_format = "yyyy-MM-dd HH:mm:ss a"
            dateFormat = "yyyy-MM-dd"
            dateTimeWithMilliSecFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS"

            # Get current datetime with the specified format
            currentDateTime = date_format(current_timestamp(), datetime_with_ampm_format)
           
            # Define primary categories to filter
            primaryCategories = ["Course", "Program", "Blended Program", "CuratedCollections", "Curated Program"]

            masterEnrolment=enrolmentDF.process_enrollment_master(spark, primaryCategories)
            userOrgHierarchy = userDFUtil.getUserOrgHierarchy(spark)
            userRatingDF= userDFUtil.userCourseRatingDataframe(spark)

            # Read master enrolment data
            # userOrgHierarchy.printSchema()
            # masterEnrolment.printSchema()

            masterEnrolmentWithUser=masterEnrolment.join(userOrgHierarchy, ("userID"), "left")

            completionPercentageDF = contentDFUtil.withCompletionPercentageColumn(masterEnrolmentWithUser)
            oldCompletionsDF = contentDFUtil.withOldCompletionStatusColumn(completionPercentageDF)
            allCourseProgramCompletionWithDetailsDF=contentDFUtil.withUserCourseCompletionStatusColumn(oldCompletionsDF)
            
            allCourseProgramCompletionWithDetailsDFWithRating = allCourseProgramCompletionWithDetailsDF.join(userRatingDF, ["courseID", "userID"], "left")

            df =  self.duration_format(allCourseProgramCompletionWithDetailsDFWithRating, "courseDuration") \
            .withColumn("completedOn", date_format(col("courseCompletedTimestamp"), dateTimeFormat)) \
            .withColumn("enrolledOn", date_format(col("courseEnrolledTimestamp"), dateTimeFormat)) \
            .withColumn("firstCompletedOn", date_format(col("firstCompletedOn"), dateTimeFormat)) \
            .withColumn("lastContentAccessTimestamp", date_format(col("lastContentAccessTimestamp"), dateTimeFormat)) \
            .withColumn("courseLastPublishedOn", to_date(col("courseLastPublishedOn"), dateFormat)) \
            .withColumn("courseBatchStartDate", to_date(col("courseBatchStartDate"), dateFormat)) \
            .withColumn("courseBatchEndDate", to_date(col("courseBatchEndDate"), dateFormat)) \
            .withColumn("completionPercentage", round(col("completionPercentage"), 2)) \
            .withColumn("Tag", concat_ws(", ", col("additionalProperties.tag"))) \
            .withColumn("Report_Last_Generated_On", currentDateTime) \
            .withColumn("Certificate_Generated", expr("CASE WHEN issuedCertificateCount > 0 THEN 'Yes' ELSE 'No' END")) \
            .withColumn("ArchivedOn", expr("CASE WHEN courseStatus == 'Retired' THEN lastStatusChangedOn ELSE '' END")) \
            .withColumn("ArchivedOn", to_date(col("ArchivedOn"), dateFormat)) \
            .withColumn("Certificate_ID", col("certificateID")) \
            .dropDuplicates(["userID", "courseID", "batchID"])

            # df.printSchema()


            # try:
            #     # This might work if the underlying data source has statistics
            #     row_count = spark.sql("SELECT COUNT(*) FROM temp_table").collect()[0][0]
            #     print(f"Total rows: {row_count:,}")
            # except:
            #     # Fallback to regular count
            #     row_count = masterEnrolmentWithUser.count()
            #     print(f"Total rows: {row_count:,}")        
            # # Show the final schema
            # storageutil.generate_report(
            #     masterEnrolmentWithUser, 
            #     'reports/standalone-reports/en', 
            #     file_name='enrolment.csv',
            #     output_format=storageutil.OutputFormat.CSV)
            # print("Final DataFrame Schema with Completion Metrics:")
            # # final_df.printSchema()
            # masterEnrolmentWithUser.show(1, False)     

            marketPlaceContent =  spark.read.parquet(ParquetFileConstants.EXTERNAL_CONTENT_PARQUET_FILE).withColumn("parsed_data", from_json(col("cios_data"), schemas.cios_data_schema)) \
            .select(
                col("content_id"),
                col("parsed_data.content.name").alias("courseName"),
                col("parsed_data.content.duration").alias("courseDuration"),
                col("parsed_data.content.lastUpdatedOn").alias("courseLastPublishedOn"),
                col("parsed_data.content.contentPartner.id").alias("courseOrgID"),
                col("parsed_data.content.contentPartner.contentPartnerName").alias("courseOrgName"),
                lit("External Content").alias("category"),
                lit("LIVE").alias("courseStatus")
            )
            marketPlaceEnrolmentsDF= spark.read.parquet(ParquetFileConstants.EXTERNAL_COURSE_ENROLMENTS_PARQUET_FILE).withColumnRenamed("courseid", "content_id")
                    # Assuming you have these DataFrames available
        
            marketPlaceContentEnrolmentsDF = self.duration_format(marketPlaceContent, "courseDuration") \
                .join(marketPlaceEnrolmentsDF, ["content_id"], "inner") \
                .withColumn("courseCompletedTimestamp", date_format(col("completedon"), dateTimeFormat)) \
                .withColumn("courseEnrolledTimestamp", date_format(col("enrolled_date"), dateTimeFormat)) \
                .withColumn("lastContentAccessTimestamp", lit("Not Available")) \
                .withColumn("userRating", lit("Not Available")) \
                .withColumn("live_cbp_plan_mandate", lit(False)) \
                .withColumn("batchID", lit("Not Available")) \
                .withColumn("issuedCertificateCount", size(col("issued_certificates"))) \
                .withColumn("certificate_generated", 
                            expr("CASE WHEN issuedCertificateCount > 0 THEN 'Yes' ELSE 'No' END")) \
                .withColumn("certificateGeneratedOn", 
                            when(col("issued_certificates").isNull(), "") \
                            .otherwise(col("issued_certificates") \
                                    .getItem(size(col("issued_certificates")) - 1) \
                                    .getItem("lastIssuedOn"))) \
                .withColumn("firstCompletedOn", 
                            when(col("issued_certificates").isNull(), "") \
                            .otherwise(when(size(col("issued_certificates")) > 0, 
                                        col("issued_certificates") \
                                        .getItem(0) \
                                        .getItem("lastIssuedOn")) \
                                    .otherwise(""))) \
                .withColumn("certificateID", 
                            when(col("issued_certificates").isNull(), "") \
                            .otherwise(col("issued_certificates") \
                                    .getItem(size(col("issued_certificates")) - 1) \
                                    .getItem("identifier"))) \
                .withColumn("Report_Last_Generated_On", currentDateTime) \
                .withColumnRenamed("userid", "userID") \
                .withColumnRenamed("content_id", "courseID") \
                .withColumnRenamed("progress", "courseProgress") \
                .withColumnRenamed("status", "dbCompletionStatus") \
                .fillna(0, ["courseProgress", "issuedCertificateCount"]) \
                .fillna("", ["certificateGeneratedOn"])
            
            marketPlaceEnrolmentsWithUserDetailsDF = marketPlaceContentEnrolmentsDF.join(userOrgHierarchy, ["userID"], "left").withColumn("Tag", concat_ws(", ", col("additionalProperties.tag")))
            # marketPlaceEnrolmentsWithUserDetailsDF.printSchema()

            acbpDF = contentDFUtil.acbpDetailsDF(spark).where(col("acbpStatus") == "Live")

            # Define select columns
            select_columns = ["userID", "designation", "userOrgID", "acbpID", "assignmentType", "acbpCourseIDList", "acbpStatus", "userStatus"]

            # Get ACBP allotment DataFrame
            acbpAllotmentDF = contentDFUtil.exploded_acbp_details(acbpDF, userOrgHierarchy, select_columns)
            acbpAllEnrolmentDF = acbpAllotmentDF \
                .withColumn("courseID", explode(col("acbpCourseIDList"))).withColumn("liveCBPlan", lit(True)) \
                .select(col("userOrgID"),col("courseID"),col("userID"),col("designation"),col("liveCBPlan")) \
            
            
            enrolmentWithACBP = df.join(acbpAllEnrolmentDF, ["userID", "userOrgID", "courseID"], "left") \
                .withColumn("live_cbp_plan_mandate", when(col("liveCBPlan").isNull(), False).otherwise(col("liveCBPlan")))
            
            # enrolmentWithACBP.printSchema()

            fullReportDF = enrolmentWithACBP \
                .withColumn("MDO_Name", col("userOrgName")) \
                .withColumn("Ministry", 
                            when(col("ministry_name").isNull(), col("userOrgName"))
                            .otherwise(col("ministry_name"))) \
                .withColumn("Department", 
                            when((col("Ministry").isNotNull()) & 
                                (col("Ministry") != col("userOrgName")) & 
                                ((col("dept_name").isNull()) | (col("dept_name") == "")), 
                                col("userOrgName"))
                            .otherwise(col("dept_name"))) \
                .withColumn("Organization", 
                            when((col("Ministry") != col("userOrgName")) & 
                                (col("Department") != col("userOrgName")), 
                                col("userOrgName"))
                            .otherwise(lit(""))) \
                .select(
                    col("userID"),
                    col("userOrgID"),
                    col("courseID"),
                    col("courseOrgID"),
                    col("fullName").alias("Full_Name"),
                    col("professionalDetails.designation").alias("Designation"),
                    col("personalDetails.primaryEmail").alias("Email"),
                    col("personalDetails.mobile").alias("Phone_Number"),
                    col("MDO_Name"),
                    col("professionalDetails.group").alias("Group"),
                    col("Tag"),
                    col("Ministry"),
                    col("Department"),
                    col("Organization"),
                    col("courseOrgName").alias("Content_Provider"),
                    col("courseName").alias("Content_Name"),
                    col("category").alias("Content_Type"),
                    col("courseDuration").alias("Content_Duration"),
                    col("batchID").alias("Batch_Id"),
                    col("courseBatchName").alias("Batch_Name"),
                    col("courseBatchStartDate").alias("Batch_Start_Date"),
                    col("courseBatchEndDate").alias("Batch_End_Date"),
                    col("enrolledOn").alias("Enrolled_On"),
                    col("userCourseCompletionStatus").alias("Status"),
                    col("completionPercentage").alias("Content_Progress_Percentage"),
                    col("courseLastPublishedOn").alias("Last_Published_On"),
                    col("ArchivedOn").alias("Content_Retired_On"),
                    col("completedOn").alias("Completed_On"),
                    col("Certificate_Generated"),
                    col("userRating").alias("User_Rating"),
                    col("personalDetails.gender").alias("Gender"),
                    col("personalDetails.category").alias("Category"),
                    col("additionalProperties.externalSystem").alias("External_System"),
                    col("additionalProperties.externalSystemId").alias("External_System_Id"),
                    col("userOrgID").alias("mdoid"),
                    col("issuedCertificateCount"),
                    col("courseStatus"),
                    col("courseResourceCount").alias("resourceCount"),
                    col("courseProgress").alias("resourcesConsumed"),
                    round(expr("CASE WHEN courseResourceCount = 0 THEN 0.0 ELSE 100.0 * courseProgress / courseResourceCount END"), 2).alias("rawCompletionPercentage"),
                    col("Certificate_ID"),
                    col("Report_Last_Generated_On"),
                    col("userStatus"),
                    col("live_cbp_plan_mandate").alias("Live_CBP_Plan_Mandate")
                ) \
                .dropDuplicates(["userID", "Batch_Id", "courseID"]) \
                .coalesce(1)
            
            fullReportDF.printSchema()
            
            report_path = f"standalone-reports/user-enrollment-report/{today}"
            
            # Create MDO Marketplace Report
            mdoMarketplaceReport = marketPlaceEnrolmentsWithUserDetailsDF \
                .withColumn("MDO_Name", col("userOrgName")) \
                .withColumn("Ministry", 
                            when(col("ministry_name").isNull(), col("userOrgName"))
                            .otherwise(col("ministry_name"))) \
                .withColumn("Department", 
                            when((col("Ministry").isNotNull()) & 
                                (col("Ministry") != col("userOrgName")) & 
                                ((col("dept_name").isNull()) | (col("dept_name") == "")), 
                                col("userOrgName"))
                            .otherwise(col("dept_name"))) \
                .withColumn("Organization", 
                            when((col("Ministry") != col("userOrgName")) & 
                                (col("Department") != col("userOrgName")), 
                                col("userOrgName"))
                            .otherwise(lit(""))) \
                .select(
                    col("fullName").alias("Full_Name"),
                    col("professionalDetails.designation").alias("Designation"),
                    col("personalDetails.primaryEmail").alias("Email"),
                    col("personalDetails.mobile").alias("Phone_Number"),
                    col("MDO_Name"),
                    col("professionalDetails.group").alias("Group"),
                    col("Tag"),
                    col("Ministry"),
                    col("Department"),
                    col("Organization"),
                    col("courseOrgName").alias("Content_Provider"),
                    col("courseName").alias("Content_Name"),
                    col("category").alias("Content_Type"),
                    col("courseDuration").alias("Content_Duration"),
                    col("batchID").alias("Batch_Id"),
                    lit("Not Available").alias("Batch_Name"),
                    lit(None).cast(DateType()).alias("Batch_Start_Date"),
                    lit(None).cast(DateType()).alias("Batch_End_Date"),
                    col("courseEnrolledTimestamp").alias("Enrolled_On"),
                    when(col("dbCompletionStatus").isNull(), "not-enrolled")
                    .when(col("dbCompletionStatus") == 0, "not-started")
                    .when(col("dbCompletionStatus") == 1, "in-progress")
                    .otherwise("completed")
                    .alias("Status"),
                    col("completionpercentage").alias("Content_Progress_Percentage"),
                    to_date(col("courseLastPublishedOn"), dateFormat).alias("Last_Published_On"),
                    lit(None).cast(DateType()).alias("Content_Retired_On"),
                    col("courseCompletedTimestamp").alias("Completed_On"),
                    col("certificate_generated").alias("Certificate_Generated"),
                    col("userRating").alias("User_Rating"),
                    col("personalDetails.gender").alias("Gender"),
                    lit("External Content").alias("category"),
                    col("additionalProperties.externalSystem").alias("External_System"),
                    col("additionalProperties.externalSystemId").alias("External_System_Id"),
                    col("userOrgID").alias("mdoid"),
                    col("certificateID").alias("Certificate_ID"),
                    col("Report_Last_Generated_On"),
                    col("userStatus"),
                    col("live_cbp_plan_mandate").alias("Live_CBP_Plan_Mandate")
                )
            
            # Create MDO Platform Report by selecting columns from full report
            mdoPlatformReport = fullReportDF.select(
                col("Full_Name"),
                col("Designation"),
                col("Email"),
                col("Phone_Number"),
                col("MDO_Name"),
                col("Group"),
                col("Tag"),
                col("Ministry"),
                col("Department"),
                col("Organization"),
                col("Content_Provider"),
                col("Content_Name"),
                col("Content_Type"),
                col("Content_Duration"),
                col("Batch_Id"),
                col("Batch_Name"),
                col("Batch_Start_Date"),
                col("Batch_End_Date"),
                col("Enrolled_On"),
                F.col("Status"),
                col("Content_Progress_Percentage"),
                col("Last_Published_On"),
                col("Content_Retired_On"),
                col("Completed_On"),
                col("Certificate_Generated"),
                col("User_Rating"),
                col("Gender"),
                col("Category"),
                col("External_System"),
                col("External_System_Id"),
                col("mdoid"),
                col("Certificate_ID"),
                col("Report_Last_Generated_On"),
                F.col("userStatus"),
                col("Live_CBP_Plan_Mandate")
            )
            
            # Filter both DataFrames before union
            filtered_platform = mdoPlatformReport.filter(col("userStatus").cast("int") == 1)
            filtered_marketplace = mdoMarketplaceReport.filter(col("userStatus").cast("int") == 1)

            # Union and generate report
            # storageutil.generate_report(
            #     filtered_platform.union(filtered_marketplace).drop("userStatus").coalesce(1),
            #     report_path, 
            #     "mdoid", 
            #     "ConsumptionReport"
            # )

    #             if (conf.reportSyncEnable) {
    #   syncReports(s"${conf.localReportDir}/${reportPath}", reportPath)
    # }

            # Marketplace Warehouse DataFrame
            marketPlaceWarehouseDF = marketPlaceEnrolmentsWithUserDetailsDF \
                .withColumn("certificate_generated_on",
                            date_format(
                                from_utc_timestamp(
                                    to_utc_timestamp(
                                        to_timestamp(col("certificateGeneratedOn"), dateTimeWithMilliSecFormat), 
                                        "UTC"
                                    ), 
                                    "IST"
                                ), 
                                dateTimeFormat
                            )) \
                .withColumn("data_last_generated_on", currentDateTime) \
                .withColumn("firstCompletedOn", date_format(col("firstCompletedOn"), dateTimeFormat)) \
                .select(
                    col("userID").alias("user_id"),
                    col("batchID").alias("batch_id"),
                    col("courseID").alias("content_id"),
                    col("courseEnrolledTimestamp").alias("enrolled_on"),
                    col("completionpercentage").alias("content_progress_percentage"),
                    col("courseProgress").alias("resource_count_consumed"),
                    when(col("dbCompletionStatus").isNull(), "not-enrolled")
                    .when(col("dbCompletionStatus") == 0, "not-started")
                    .when(col("dbCompletionStatus") == 1, "in-progress")
                    .otherwise("completed")
                    .alias("user_consumption_status"),
                    col("firstCompletedOn").alias("first_completed_on"),
                    col("firstCompletedOn").alias("first_certificate_generated_on"),
                    col("courseCompletedTimestamp").alias("last_completed_on"),
                    col("certificate_generated_on").alias("last_certificate_generated_on"),
                    col("lastContentAccessTimestamp").alias("content_last_accessed_on"),
                    col("certificate_generated").alias("certificate_generated"),
                    col("issuedCertificateCount").alias("number_of_certificate"),
                    col("userRating").alias("user_rating"),
                    col("certificateID").alias("certificate_id"),
                    col("live_cbp_plan_mandate"),
                    col("data_last_generated_on")
                ) \
                .dropDuplicates(["user_id", "batch_id", "content_id"])
            
            marketPlaceWarehouseDF.printSchema()
            # Platform Warehouse DataFrame
            platformWarehouseDF = enrolmentWithACBP \
                .withColumn("certificate_generated_on",
                            date_format(
                                from_utc_timestamp(
                                    to_utc_timestamp(
                                        to_timestamp(col("certificateGeneratedOn"), dateTimeWithMilliSecFormat), 
                                        "UTC"
                                    ), 
                                    "IST"
                                ), 
                                dateTimeFormat
                            )) \
                .withColumn("data_last_generated_on", currentDateTime) \
                .select(
                    col("userID").alias("user_id"),
                    col("batchID").alias("batch_id"),
                    col("courseID").alias("content_id"),
                    col("enrolledOn").alias("enrolled_on"),
                    col("completionPercentage").alias("content_progress_percentage"),
                    col("courseProgress").alias("resource_count_consumed"),
                    col("userCourseCompletionStatus").alias("user_consumption_status"),
                    col("firstCompletedOn").alias("first_completed_on"),
                    col("firstCompletedOn").alias("first_certificate_generated_on"),
                    col("completedOn").alias("last_completed_on"),
                    col("certificate_generated_on").alias("last_certificate_generated_on"),
                    col("lastContentAccessTimestamp").alias("content_last_accessed_on"),
                    col("Certificate_Generated").alias("certificate_generated"),
                    col("issuedCertificateCount").alias("number_of_certificate"),
                    col("userRating").alias("user_rating"),
                    col("Certificate_ID").alias("certificate_id"),
                    col("live_cbp_plan_mandate"),
                    col("data_last_generated_on")
                ) \
                .dropDuplicates(["user_id", "batch_id", "content_id"])
            
            # Union both DataFrames
            warehouseDF = platformWarehouseDF.union(marketPlaceWarehouseDF)

            karmaPointsData =spark.read.parquet(ParquetFileConstants.USER_KARMA_POINTS_PARQUET_FILE) \
                .select(
                    col("userid").alias("user_id"),
                    col("context_id").alias("content_id"),
                    col("points")
                ) \
                .groupBy(col("user_id"), col("content_id")) \
                .agg(spark_sum(col("points")).alias("karma_points"))

            warehouseDFwithKarmaPoints = warehouseDF \
                .join(karmaPointsData, ["user_id", "content_id"], "left") \
                .fillna(0, ["karma_points"]) \
                .select(
                    col("user_id"),
                    col("batch_id"),
                    col("content_id"),
                    col("enrolled_on"),
                    col("content_progress_percentage"),
                    col("resource_count_consumed"),
                    col("user_consumption_status"),
                    col("first_completed_on"),
                    col("first_certificate_generated_on"),
                    col("last_completed_on"),
                    col("last_certificate_generated_on"),
                    col("content_last_accessed_on"),
                    col("certificate_generated"),
                    col("number_of_certificate"),
                    col("user_rating"),
                    col("certificate_id"),
                    col("live_cbp_plan_mandate"),
                    col("karma_points"),
                    col("data_last_generated_on")
                )
            
            # Write to warehouse cache
            # storageutil.generate_report(
            #     warehouseDFwithKarmaPoints.coalesce(1), 
            #    "warehouse_pq/user_enrolments/",
            #     output_format=storageutil.OutputFormat.PARQUET)
        
        except Exception as e:
            print(f"Error occurred during UserEnrolmentModel processing: {str(e)}")
            raise e  # Re-raise the exception for better debugging
            sys.exit(1)
    

# Example usage:
if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("User Report Model") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "16g") \
        .config("spark.driver.memory", "16g") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    # Create model instance
    start_time = datetime.now()
    print(f"[START] UserEnrolmentModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    model = UserEnrolmentModel()
    model.process_data(spark=spark)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] UserEnrolmentModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
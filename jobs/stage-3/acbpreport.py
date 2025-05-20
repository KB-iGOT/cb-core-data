from sqlite3 import Date
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import Window


# ==============================
# 1. Configuration and Constants
# ==============================

# Ensure the parent directory is in sys.path for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckutil import duckutil,datautil  # Assuming duckutil is in the parent directory
from dfutil.content import contentDF


def main():
     today = Date()

     duckdb_conn = duckutil.initialize_duckdb()
    # obtain and save user org data
    # userDataDF = datautil.getUserDataFrame(duckdb_conn)
     #orgDF = datautil.getOrgDataFrame(duckdb_conn)
     #acbpAllEnrolmentDF = datautil.getAcbpDetailsDF(duckdb_conn).df()

     #hierarchyDF = datautil.contentHierarchyDataFrame(Seq("Course", "Program", "Blended Program", "Curated Program", "Standalone Assessment")).df() 
     allCourseProgramESDF = datautil.getAllCourseProgramESDataFrame().df()
     allCourseProgramDetailsWithCompDF = contentDF.all_course_program_competency_dataframe(allCourseProgramESDF, hierarchyDF, orgDF).df()
     allCourseProgramDetailsDF = contentDF.all_course_program_details_dataframe(allCourseProgramDetailsWithCompDF)
     acbpAllotmentDF = datautil.getExplodedACBPDetailsDataFrame()
     userCourseProgramEnrolmentDF = datautil.get_user_course_program_completion_data_frame()

      #replace content list with names of the courses instead of ids
     acbpAllEnrolmentDF = (
          acbpAllotmentDF
          .withColumn("courseID", F.explode(F.col("acbpCourseIDList")))
          .join(allCourseProgramDetailsDF, on="courseID", how="left")
          .join(userCourseProgramEnrolmentDF, on=["courseID", "userID"], how="left")
          .na.drop(subset=["userID", "courseID"])
          .drop("acbpCourseIDList")
     )
     cb_plan_warehouse_df = datautil.fetch_cb_plan_warehouse_df(acbpAllEnrolmentDF)

     #acbpAllotmentDF = datautil.getExplodedACBPDetailsDataFrame()
     

     # Define Window Spec for Priority Selection
     priority_window = Window.partitionBy("userID", "courseID").orderBy(F.col("completionDueDate").desc())

     # Filter for "Live" Status and Apply Priority Logic
     acbpEnrolmentDF = (
          acbpAllEnrolmentDF
          .filter(F.col("acbpStatus") == "Live")
          .withColumn("row_number", F.row_number().over(priority_window))
          .filter(F.col("row_number") == 1)
          .drop("row_number")
     )

     
     enrolmentReportDF = fetch_enrolment_data_df(acbpEnrolmentDF)
    
     # Optimized Enrollment Report
     (enrolmentReportDFFiltered,enrolmentReportDFMDO) = filter_enrolment_data_df(acbpEnrolmentDF)

     userSummaryReportDF = optimise_user_summary_report(acbpEnrolmentDF)
    
     #generateReport(enrolmentReportDFFiltered, f"{reportPath}/CBPEnrollmentReport", fileName="CBPEnrollmentReport")
     #generateReport(enrolmentReportDFMDO, mdoReportPath, fileName="CBPEnrollmentReport")

     #  Optional Sync Reports
     #if conf.reportSyncEnable:
          #syncReports(f"{conf.localReportDir}/{reportPath}", mdoReportPath)

     #  Optimized User Summary Report
     

     columnsToKeepInSummaryReport = [col for col in userSummaryReportDF.columns if col != "status"]

     #generateReport(userSummaryReportDFFiltered, f"{reportPath}/CBPUserSummaryReport", fileName="CBPUserSummaryReport")
     #generateReport(userSummaryReportDFMDO, mdoSummaryReportPath, fileName="CBPUserSummaryReport")

     # Optional Sync Reports - New and Old Path
     #if conf.reportSyncEnable:
     #    syncReports(f"{conf.localReportDir}/{reportPath}", mdoSummaryReportPath)

     # Sync Reports to Local Directory
     #syncReports(f"{conf.localReportDir}/{reportPath}", reportPath)
     #     print(len(acbpAllEnrolmentDF))


def fetch_cb_plan_warehouse_df(acbpAllEnrolmentDF):
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cbPlanWarehouseDF = (
        acbpAllEnrolmentDF
        .withColumn(
            "allotment_to",
            F.when(F.col("assignmentType") == "CustomUser", F.col("userID"))
            .when(F.col("assignmentType") == "Designation", F.col("designation"))
            .when(F.col("assignmentType") == "AllUser", F.lit("All Users"))
            .otherwise(F.lit("No Records"))
        )
        .withColumn("data_last_generated_on", F.lit(current_timestamp))
        .select(
            F.col("userOrgID").alias("org_id"),
            F.col("acbpCreatedBy").alias("created_by"),
            F.col("acbpID").alias("cb_plan_id"),
            F.col("cbPlanName").alias("plan_name"),
            F.col("assignmentType").alias("allotment_type"),
            "allotment_to",
            F.col("courseID").alias("content_id"),
            F.date_format(F.col("allocatedOn"), "yyyy-MM-dd HH:mm:ss").alias("allocated_on"),
            F.date_format(F.col("completionDueDate"), "yyyy-MM-dd").alias("due_by"),
            F.col("acbpStatus").alias("status"),
            "data_last_generated_on"
        )
        .drop_duplicates()
        .orderBy("org_id", "created_by", "plan_name")
    )

    return cbPlanWarehouseDF

def fetch_enrolment_data_df(acbpEnrolmentDF):
     date_format = "yyyy-MM-dd"

     # Building the Enrolment Report DataFrame
     enrolmentReportDataDF = (
          acbpEnrolmentDF
          .withColumn(
               "currentProgress",
               F.expr(
                    """
                    CASE 
                         WHEN dbCompletionStatus = 2 THEN 'Completed' 
                         WHEN dbCompletionStatus = 1 THEN 'In Progress' 
                         WHEN dbCompletionStatus = 0 THEN 'Not Started' 
                         ELSE 'Not Enrolled' 
                    END
                    """
               )
          )
          .withColumn("courseCompletedTimestamp", F.date_format("courseCompletedTimestamp", date_format))
          .withColumn("allocatedOn", F.date_format("allocatedOn", date_format))
          .withColumn("completionDueDate", F.date_format("completionDueDate", date_format))
          .na.fill("")
     )
     return enrolmentReportDataDF

def filter_enrolment_data_df(enrolmentReportDF):
     columnsToKeepInEnrolmentReport = [col for col in enrolmentReportDF.columns if col != "status"]

     enrolmentReportDFFiltered = (
          enrolmentReportDF
          .filter(F.col("status").cast("int") == 1)
          .select(*columnsToKeepInEnrolmentReport)
          .drop("mdoid")
          .coalesce(1)
     )
     enrolmentReportDFMDO = (
          enrolmentReportDF
          .filter(F.col("status").cast("int") == 1)
          .select(*columnsToKeepInEnrolmentReport)
          .coalesce(1)
     )
     return (enrolmentReportDFFiltered,enrolmentReportDFMDO)
    
def optimise_user_summary_report(acbpEnrolmentDF):
     userSummaryDataDF = (
          acbpEnrolmentDF
          .withColumn("completionDueDateLong", (F.col("completionDueDate") + F.expr("INTERVAL 24 HOURS")).cast(LongType()))
          .withColumn("courseCompletedTimestampLong", F.col("courseCompletedTimestamp").cast(LongType()))
          .groupBy(
               "userID", "fullName", "userPrimaryEmail", "userMobile",
               "designation", "group", "userOrgID", "ministry_name",
               "dept_name", "userOrgName", "userStatus"
          )
          .agg(
               F.count("courseID").alias("allocatedCount"),
               F.sum(F.when(F.col("dbCompletionStatus") == 2, 1).otherwise(0)).alias("completedCount"),
               F.sum(F.when(
                    (F.col("dbCompletionStatus") == 2) & 
                    (F.col("courseCompletedTimestampLong") < F.col("completionDueDateLong")), 1
               ).otherwise(0)).alias("completedBeforeDueDateCount")
          )
     )

     userSummaryReportDF = (
          userSummaryDataDF
          .withColumn("MDO_Name", F.col("userOrgName"))
          .withColumn(
               "Ministry",
               F.when(F.col("ministry_name").isNull() | (F.col("ministry_name") == ""), F.col("userOrgName"))
               .otherwise(F.col("ministry_name"))
          )
          .withColumn(
               "Department",
               F.when(
                    (F.col("Ministry").isNotNull()) & 
                    (F.col("Ministry") != F.col("userOrgName")) & 
                    (F.col("dept_name").isNull() | (F.col("dept_name") == "")),
                    F.col("userOrgName")
               ).otherwise(F.col("dept_name"))
          )
          .withColumn(
               "Organization",
               F.when(
                    (F.col("Ministry") != F.col("userOrgName")) & 
                    (F.col("Department") != F.col("userOrgName")),
                    F.col("userOrgName")
               ).otherwise(F.lit(""))
          )
          .select(
               F.col("fullName").alias("Name"),
               F.col("userPrimaryEmail").alias("Email"),
               F.col("userMobile").alias("Phone"),
               F.col("MDO_Name"),
               F.col("group").alias("Group"),
               F.col("designation").alias("Designation"),
               F.col("Ministry"),
               F.col("Department"),
               F.col("Organization"),
               F.col("allocatedCount").alias("Number of CBP Courses Allocated"),
               F.col("completedCount").alias("Number of CBP Courses Completed"),
               F.col("completedBeforeDueDateCount").alias("Number of CBP Courses Completed within due date"),
               F.col("userStatus").alias("status"),
               F.col("userOrgID").alias("mdoid")
          )
          .withColumn("Report_Last_Generated_On", F.current_timestamp())
     )
     return userSummaryReportDF

if __name__ == "__main__":
    main()
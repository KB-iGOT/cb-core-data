import findspark

findspark.init()

import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
from pyspark.sql.functions import (col, lower, when, lit, expr, concat_ws, explode_outer, from_json, to_date,
                                   current_timestamp, date_format, round, coalesce, broadcast, size, map_keys,
                                   map_values)
from datetime import datetime
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))

# Reusable imports from userReport structure
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.enrolment import enrolmentDFUtil
from dfutil.content import contentDFUtil
from dfutil.dfexport import dfexportutil
from jobs.config import get_environment_config
from jobs.default_config import create_config

class BlendedModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.BlendedModel"

    def name(self):
        return "BlendedModel"

    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

    def process_data(self, spark,config):
        try:
            start_time = time.time()
            today = self.get_date()
            primary_categories = ["Blended Program"]
            print("📊 Loading and filtering data...")
            spark = SparkSession.getActiveSession()
            spark.conf.set("spark.sql.adaptive.enabled", "true")
            spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

            userOrgDF = (spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
                         .select("userID", "fullName", "userGender", "userCategory", "maskedPhone",
                                 "maskedEmail", "userPrimaryEmail", "userMobile", "userStatus",
                                 "designation", "group", "Tag", "ministry_name", "dept_name",
                                 "userOrgID", "userOrgName")
                         .repartition(col("userID"))
                         .cache())

            blendedProgramDF = (spark.read.parquet(ParquetFileConstants.ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE)
                                .select("courseID", "courseName", "category", "courseStatus",
                                        "courseLastPublishedOn", "courseOrgID", "courseChannel")
                                .filter((col("category").isin(primary_categories)) &
                                        (col("courseStatus") == "Live") &
                                        col("courseLastPublishedOn").isNotNull())
                                .repartition(col("courseID"))  # Partition by join key
                                .cache())

            contentHierarchyDF = (spark.read.parquet(ParquetFileConstants.CONTENT_HIERARCHY_SELECT_PARQUET_FILE)
                                  .select("identifier", "hierarchy")
                                  .filter(col("hierarchy").isNotNull() & (col("hierarchy") != ""))
                                  .withColumn("hierarchy_parsed",
                                              from_json(col("hierarchy"), self.get_hierarchy_schema()))
                                  .select("identifier", "hierarchy_parsed")
                                  .filter(col("hierarchy_parsed").isNotNull())
                                  .cache())

            enrolmentDF = (spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE)
                           .select("userID", "courseID", "batchID", "courseEnrolledTimestamp",
                                   "lastContentAccessTimestamp", "courseCompletedTimestamp",
                                   "courseBatchName", "courseProgress", "courseBatchStartDate",
                                   "courseBatchEndDate", "courseBatchCreatedBy", "issuedCertificateCount",
                                   "courseContentStatus", "courseBatchAttrs")
                           .repartition(col("courseID"), col("userID"))  # Partition by both join keys
                           .cache())

            bpAllChildrenDF = (self.extract_bp_children(blendedProgramDF, contentHierarchyDF)
                               .select("bpID", "bpChildID", "bpChildName", "bpChildCategory",
                                       "bpChildDuration", "bpChildResourceCount")
                               .persist())  # Use persist() for better control

            bpEnrolmentDF = (enrolmentDF
                             .join(blendedProgramDF.select("courseID"),
                                   on="courseID", how="inner")  # Use on parameter for better optimization
                             .select(
                col("userID"),
                col("courseID").alias("bpID"),
                col("batchID").alias("bpBatchID"),
                col("courseEnrolledTimestamp").alias("bpEnrolledTimestamp"),
                col("lastContentAccessTimestamp").alias("bpChildLastContentAccessTimestamp"),
                col("courseCompletedTimestamp").alias("bpChildCompletedTimestamp"),
                col("courseBatchName").alias("bpBatchName"),
                col("courseProgress").alias("bpProgress"),
                col("courseBatchStartDate").alias("bpBatchStartDate"),
                col("courseBatchEndDate").alias("bpBatchEndDate"),
                col("courseBatchCreatedBy").alias("bpBatchCreatedBy"),
                col("issuedCertificateCount").alias("bpIssuedCertificateCount"),
                col("courseContentStatus").alias("bpContentStatus"),
                col("courseBatchAttrs"))
                             .cache())

            bpChildStatusDF = (bpEnrolmentDF
                               .filter(col("bpContentStatus").isNotNull() & (size(col("bpContentStatus")) > 0))
                               .withColumn("bpChildID", explode_outer(map_keys(col("bpContentStatus"))))
                               .withColumn("bpChildStatus", col("bpContentStatus")[col("bpChildID")])
                               .select(
                col("userID"), col("bpID"), col("bpBatchID"), col("bpBatchName"),
                col("bpBatchStartDate"), col("bpBatchEndDate"), col("bpBatchCreatedBy"),
                col("bpEnrolledTimestamp"), col("bpChildLastContentAccessTimestamp"),
                col("bpChildCompletedTimestamp"), col("bpProgress"), col("bpIssuedCertificateCount"),
                col("courseBatchAttrs"), col("bpChildID"), col("bpChildStatus"))
                               .cache())

            bpUserDF = (bpChildStatusDF
                        .join(userOrgDF, on="userID", how="inner")
                        .select(
                # User info
                "userID", "fullName", "userGender", "userCategory", "maskedPhone",
                "maskedEmail", "userPrimaryEmail", "userMobile", "userOrgID",
                "userOrgName", "designation", "group", "userStatus", "ministry_name",
                "dept_name", "Tag",
                # BP info
                "bpBatchID", "bpBatchName", "bpBatchStartDate", "bpBatchEndDate",
                "bpBatchCreatedBy", "bpEnrolledTimestamp", "bpChildLastContentAccessTimestamp",
                "bpChildID", "bpChildCompletedTimestamp", "bpProgress",
                "bpIssuedCertificateCount", "bpChildStatus", "courseBatchAttrs")
                        .cache())

            bpWithChildMetaDF = (bpUserDF
                                 .join(bpAllChildrenDF, on="bpChildID", how="left")
                                 .cache())

            bpBatchSessionDetailsDF = (self.process_batch_session_data(bpEnrolmentDF)
                                       .cache())

            programInfoDF = blendedProgramDF.select(
                col("courseID"),
                col("courseName").alias("bpName"),
                col("courseOrgID").alias("bpOrgID"),
                col("courseChannel").alias("Provider_Name"))

            # Second join: coordinator info
            coordinatorDF = userOrgDF.select(
                col("userID").alias("bpBatchCreatedBy"),
                col("fullName").alias("Program_Coordinator_Name"))

            finalResDF = (bpWithChildMetaDF
                          .join(programInfoDF, bpWithChildMetaDF["bpID"] == programInfoDF["courseID"], how="left")
                          .join(coordinatorDF, on="bpBatchCreatedBy", how="left")
                          .join(bpBatchSessionDetailsDF,
                                (bpWithChildMetaDF["bpID"] == bpBatchSessionDetailsDF["bpSessionID"]) &
                                (bpWithChildMetaDF["bpBatchID"] == bpBatchSessionDetailsDF["bpBatchSessionID"]))
                          .withColumn("Component_Mode",
                                      when(lower(col("bpChildCategory")) == "offline session", "Offline")
                                      .otherwise(""))
                          .withColumn("bpChildAttendanceStatus",
                                      when(col("bpChildStatus") == 2, "Attended")
                                      .otherwise("Not Attended"))
                          .withColumn("Offline_Attendance_Status",
                                      when(col("Component_Mode") == "Offline", col("bpChildAttendanceStatus"))
                                      .otherwise(""))
                          .withColumn("Component_Progress_Percentage",
                                      when(col("bpChildStatus") == 2, lit(100.0))
                                      .when((col("bpProgress") == 0) | (col("bpChildResourceCount") == 0) |
                                            (col("bpChildStatus") == 0), lit(0.0))
                                      .otherwise(round(100.0 * col("bpProgress") / col("bpChildResourceCount"), 2)))
                          .withColumn("Offline_Session_Date",
                                      when(col("Component_Mode") == "Offline", col("bpBatchSessionStartDate"))
                                      .otherwise(lit(None)))
                          .withColumn("Offline_Session_Start_Time",
                                      when(col("Component_Mode") == "Offline", col("bpBatchSessionStartTime"))
                                      .otherwise(lit(None)))
                          .withColumn("Offline_Session_End_Time",
                                      when(col("Component_Mode") == "Offline", col("bpBatchSessionEndTime"))
                                      .otherwise(lit(None)))
                          .withColumn("Certificate_Generated",
                                      when(col("bpIssuedCertificateCount") > 0, "Yes")
                                      .otherwise("No"))
                          .withColumn("Status",
                                      when(col("bpChildStatus") == 2, "Completed")
                                      .otherwise("Not Completed"))
                          .withColumn("Report_Last_Generated_On", current_timestamp())
                          )

            base_columns = [
                col("fullName").alias("Name"),
                col("userPrimaryEmail").alias("Email"),
                col("userMobile").alias("Phone_Number"),
                col("userOrgName").alias("MDO_Name"),
                col("designation").alias("Designation"),
                col("group").alias("Group"),
                col("userGender").alias("Gender"),
                col("userCategory").alias("Category"),
                col("Tag"),
                col("ministry_name").alias("Ministry"),
                col("dept_name").alias("Department"),
                col("userOrgName").alias("Organization"),
                col("Provider_Name"),
                col("bpName").alias("Program_Name"),
                col("bpBatchName").alias("Batch_Name"),
                col("bpBatchLocation").alias("Batch_Location"),
                col("bpBatchStartDate").alias("Batch_Start_Date"),
                col("bpBatchEndDate").alias("Batch_End_Date"),
                col("bpEnrolledTimestamp").alias("Enrolled_On"),
                col("bpChildName").alias("Component_Name"),
                col("bpChildCategory").alias("Component_Type"),
                col("Component_Mode"),
                col("Status"),
                col("bpChildDuration").alias("Component_Duration"),
                col("Component_Progress_Percentage"),
                col("bpChildCompletedTimestamp").alias("Component_Completed_On"),
                col("bpChildLastContentAccessTimestamp").alias("Last_Accessed_On"),
                col("Offline_Session_Date"),
                col("Offline_Session_Start_Time"),
                col("Offline_Session_End_Time"),
                col("Offline_Attendance_Status"),
                col("bpBatchSessionFacilators").alias("Instructor(s)_Name"),
                col("Program_Coordinator_Name"),
                col("Certificate_Generated"),
                col("Report_Last_Generated_On")
            ]

            # MDO Report
            mdoReportDF = (finalResDF.filter(col("userStatus") == 1)
                           .select(*(base_columns + [col("userOrgID").alias("mdoid")]))
                           .cache())

            # CBP Report
            cbpReportDF = (finalResDF
                           .select(*(base_columns + [col("bpOrgID").alias("mdoid")]))
                           .distinct()
                           .cache())

            dfexportutil.write_csv_per_mdo_id_duckdb(
                mdoReportDF,
                f"{config.localReportDir}/{config.blendedReportPath}-mdo/{today}",
                'mdoid',
                f"{config.localReportDir}/temp/blended-program-report-mdo/{today}",
                )

            dfexportutil.write_csv_per_mdo_id_duckdb(
                cbpReportDF,
                f"{config.localReportDir}/{config.blendedReportPath}-cbp/{today}",
                'mdoid',
                f"{config.localReportDir}/temp/blended-program-report-cbp/{today}",
                )

            print("🏗️ Creating warehouse data...")

            warehouseDF = (finalResDF
                           .select(
                col("userID").alias("user_id"),
                col("bpID").alias("content_id"),
                col("bpBatchID").alias("batch_id"),
                col("bpBatchLocation").alias("batch_location"),
                col("bpChildName").alias("component_name"),
                col("bpChildID").alias("component_id"),
                col("bpChildCategory").alias("component_type"),
                col("Component_Mode").alias("component_mode"),
                col("Status").alias("component_status"),
                col("bpChildDuration").alias("component_duration"),
                col("Component_Progress_Percentage").alias("component_progress_percentage"),
                col("bpChildCompletedTimestamp").alias("component_completed_on"),
                col("bpChildLastContentAccessTimestamp").alias("last_accessed_on"),
                col("Offline_Session_Date").alias("offline_session_date"),
                col("Offline_Session_Start_Time").alias("offline_session_start_time"),
                col("Offline_Session_End_Time").alias("offline_session_end_time"),
                col("Offline_Attendance_Status").alias("offline_attendance_status"),
                col("bpBatchSessionFacilators").alias("instructors_name"),
                col("Program_Coordinator_Name").alias("program_coordinator_name"),
                current_timestamp().alias("data_last_generated_on"))
                           .repartition(200)  # Optimize partitioning for warehouse
                           .sortWithinPartitions("user_id", "content_id"))  # Sort for better compression

            (warehouseDF
             .write
             .mode("overwrite")
             .option("compression", "snappy")
             .parquet(f"{config.warehouseReportDir}/{config.dwBPEnrollmentsTable}"))

            cleanup_order = [
                warehouseDF, cbpReportDF, mdoReportDF, finalResDF, bpBatchSessionDetailsDF,
                bpWithChildMetaDF, bpUserDF, bpChildStatusDF, bpEnrolmentDF,
                bpAllChildrenDF, enrolmentDF, contentHierarchyDF, blendedProgramDF, userOrgDF
            ]

            for df in cleanup_order:
                try:
                    df.unpersist()
                except:
                    pass  # Some DataFrames might not be cached

            total_time = time.time() - start_time
            print(
                f"\n✅ Optimized Blended Program Report generation completed in {total_time:.2f} seconds ({total_time / 60:.1f} minutes)")


        except Exception as e:
            print(f"❌ Error: {str(e)}")
            raise

    def extract_bp_children(self, bp_ids_df, parsed_hierarchy_df):
        """Extract blended program children matching Scala logic"""
        from pyspark.sql.functions import col, explode_outer

        # Join and explode first level children (equivalent to currentDF in Scala)
        current_df = bp_ids_df.join(parsed_hierarchy_df, bp_ids_df["courseID"] == parsed_hierarchy_df["identifier"],
                                    "left").withColumn("bpChild",
                                                       explode_outer(col("hierarchy_parsed.children"))).select(
            col("courseID").alias("bpID"),
            col("bpChild.*"))

        # Case 1: Not Course or Course Unit → pick directly (level1DirectDF)
        level1_direct_df = current_df.filter(
            (~col("primaryCategory").isin("Course", "Course Unit")) &
            col("identifier").isNotNull()
        ).select(
            col("bpID"),
            col("identifier").alias("bpChildID"),
            col("name").alias("bpChildName"),
            col("primaryCategory").alias("bpChildCategory"),
            col("duration").alias("bpChildDuration"),
            col("leafNodesCount").alias("bpChildResourceCount"))

        # Case 2: Course → Course Unit → LR (courseDF)
        course_df = current_df.filter(
            (col("primaryCategory") == "Course") &
            col("children").isNotNull()
        ).withColumn("courseUnit", explode_outer(col("children"))).select(
            col("bpID"),
            col("courseUnit.*"))

        # Non-Course Unit children within Course
        non_course_unit_in_course_df = course_df.filter(
            (col("primaryCategory") != "Course Unit") &
            col("identifier").isNotNull()).select(
            col("bpID"),
            col("identifier").alias("bpChildID"),
            col("name").alias("bpChildName"),
            col("primaryCategory").alias("bpChildCategory"),
            col("duration").alias("bpChildDuration"),
            col("leafNodesCount").alias("bpChildResourceCount"))

        # Course Unit children within Course → Learning Resource
        course_unit_in_course_df = course_df.filter(
            (col("primaryCategory") == "Course Unit") &
            col("children").isNotNull()).withColumn("courseUnitChild", explode_outer(col("children"))).select(
            col("bpID"),
            col("courseUnitChild.identifier").alias("bpChildID"),
            col("courseUnitChild.name").alias("bpChildName"),
            col("courseUnitChild.primaryCategory").alias("bpChildCategory"),
            col("courseUnitChild.duration").alias("bpChildDuration"),
            col("courseUnitChild.leafNodesCount").alias("bpChildResourceCount"))

        # Case 3: Course Unit → LR (courseUnitDF)
        course_unit_df = current_df.filter(
            (col("primaryCategory") == "Course Unit") &
            col("children").isNotNull()
        ).withColumn("unitChild", explode_outer(col("children"))).select(
            col("bpID"),
            col("unitChild.identifier").alias("bpChildID"),
            col("unitChild.name").alias("bpChildName"),
            col("unitChild.primaryCategory").alias("bpChildCategory"),
            col("unitChild.duration").alias("bpChildDuration"),
            col("unitChild.leafNodesCount").alias("bpChildResourceCount"))

        # Union all cases
        return level1_direct_df.unionByName(non_course_unit_in_course_df).unionByName(
            course_unit_in_course_df).unionByName(
            course_unit_df)

    def process_batch_session_data(self, enrolment_df):
        session_details_schema = ArrayType(StructType([
            StructField("sessionId", StringType(), True),
            StructField("sessionType", StringType(), True),
            StructField("startDate", StringType(), True),
            StructField("startTime", StringType(), True),
            StructField("endTime", StringType(), True),
            StructField("facilatorDetails", ArrayType(StructType([
                StructField("name", StringType(), True)
            ])), True)
        ]))

        batch_attrs_schema = StructType([
            StructField("batchLocationDetails", StringType(), True),
            StructField("currentBatchSize", StringType(), True),
            StructField("sessionDetails_v2", session_details_schema, True)
        ])

        bpBatchSessionDF = enrolment_df.select(
            col("bpID").alias("bpSessionID"),
            col("bpBatchID").alias("bpBatchSessionID"),
            col("courseBatchAttrs")
        ).distinct() \
            .withColumn("bpBatchAttrs_parsed",
                        when(col("courseBatchAttrs").isNotNull() & (col("courseBatchAttrs") != "{}"),
                             from_json(col("courseBatchAttrs"), batch_attrs_schema))
                        .otherwise(lit(None))) \
            .withColumn("bpBatchLocation", col("bpBatchAttrs_parsed.batchLocationDetails")) \
            .withColumn("bpBatchCurrentSize", col("bpBatchAttrs_parsed.currentBatchSize")) \
            .withColumn("bpBatchSessionDetails", explode_outer(col("bpBatchAttrs_parsed.sessionDetails_v2"))) \
            .withColumn("bpBatchSessionType", col("bpBatchSessionDetails.sessionType")) \
            .withColumn("bpBatchSessionFacilators", concat_ws(", ", col("bpBatchSessionDetails.facilatorDetails.name"))) \
            .withColumn("bpBatchSessionStartDate", col("bpBatchSessionDetails.startDate")) \
            .withColumn("bpBatchSessionStartTime", col("bpBatchSessionDetails.startTime")) \
            .withColumn("bpBatchSessionEndTime", col("bpBatchSessionDetails.endTime")) \
            .drop("courseBatchAttrs", "bpBatchAttrs_parsed", "bpBatchSessionDetails")

        return bpBatchSessionDF

    def get_hierarchy_schema(self):
        level3 = StructType([
            StructField("identifier", StringType(), True),
            StructField("name", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("duration", StringType(), True),
            StructField("primaryCategory", StringType(), True),
            StructField("leafNodesCount", IntegerType(), True),
            StructField("contentType", StringType(), True),
            StructField("objectType", StringType(), True),
            StructField("showTimer", StringType(), True),
            StructField("allowSkip", StringType(), True)
        ])

        level2 = StructType([
            StructField("identifier", StringType(), True),
            StructField("name", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("duration", StringType(), True),
            StructField("primaryCategory", StringType(), True),
            StructField("leafNodesCount", IntegerType(), True),
            StructField("contentType", StringType(), True),
            StructField("objectType", StringType(), True),
            StructField("showTimer", StringType(), True),
            StructField("allowSkip", StringType(), True),
            StructField("children", ArrayType(level3), True)
        ])

        level1 = StructType([
            StructField("identifier", StringType(), True),
            StructField("name", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("duration", StringType(), True),
            StructField("primaryCategory", StringType(), True),
            StructField("leafNodesCount", IntegerType(), True),
            StructField("contentType", StringType(), True),
            StructField("objectType", StringType(), True),
            StructField("showTimer", StringType(), True),
            StructField("allowSkip", StringType(), True),
            StructField("children", ArrayType(level2), True)
        ])

        return StructType([
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("reviewStatus", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("duration", StringType(), True),
            StructField("primaryCategory", StringType(), True),
            StructField("leafNodesCount", IntegerType(), True),
            StructField("leafNodes", ArrayType(StringType()), True),
            StructField("publish_type", StringType(), True),
            StructField("isExternal", BooleanType(), True),
            StructField("contentType", StringType(), True),
            StructField("objectType", StringType(), True),
            StructField("userConsent", StringType(), True),
            StructField("visibility", StringType(), True),
            StructField("createdOn", StringType(), True),
            StructField("lastUpdatedOn", StringType(), True),
            StructField("lastPublishedOn", StringType(), True),
            StructField("lastSubmittedOn", StringType(), True),
            StructField("lastStatusChangedOn", StringType(), True),
            StructField("createdFor", ArrayType(StringType()), True),
            StructField("children", ArrayType(level1), True),
            StructField("competencies_v3", StringType(), True)
        ])


def main():
    # Initialize Spark Session with optimized settings for caching
    spark = SparkSession.builder \
        .appName("Blended Program Report Model - Cached") \
        .config("spark.executor.memory", "25") \
        .config("spark.driver.memory", "25g") \
        .config("spark.sql.shuffle.partitions", "64") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.shuffle.io.connectionTimeout", "300s") \
        .config("spark.shuffle.io.maxRetries", "20") \
        .config("spark.shuffle.io.retryWait", "10s") \
        .getOrCreate()
    # Create model instance
    start_time = datetime.now()
    print(f"[START] BlendedModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    config_dict = get_environment_config()
    config = create_config(config_dict)
    model = BlendedModel()
    model.process_data(spark,config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] BlendedModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()

if __name__ == "__main__":
    main()

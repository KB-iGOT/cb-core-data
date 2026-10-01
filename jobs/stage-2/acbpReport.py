import findspark

findspark.init()
import sys
import time
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import bround, col, broadcast, concat_ws, split, coalesce, lit, when, from_unixtime, regexp_replace
from pyspark.sql.functions import col, lit, coalesce, concat_ws, create_map, when, broadcast, get_json_object, rtrim
from pyspark.sql.functions import col, trim, array_join, from_json, explode_outer, coalesce, lit, format_string, count, countDistinct
from pyspark.sql.types import StructType, ArrayType, StringType, BooleanType, StructField
from pyspark.sql.types import MapType, StringType, StructType, StructField, FloatType, LongType, DateType, IntegerType
from pyspark.sql.functions import col, when, size, lit, expr, unix_timestamp, date_format, from_json, current_timestamp, \
    to_date, round, explode, to_utc_timestamp, from_utc_timestamp, to_timestamp, sum as spark_sum
from pyspark.sql.functions import col, desc, row_number, udf
from pyspark.sql.window import Window
from itertools import chain
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

    def process_data(self, spark, config):
        try:
            start_time = time.time()
            stage_start = time.time()
            today = self.get_date()
            currentDateTime = date_format(current_timestamp(), ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT)
            primary_categories = ["Course", "Program", "Blended Program", "Curated Program", "Standalone Assessment"]

            print("📥 Reading source data...")
            userOrgDF = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE).select("userID",
                                                                                               "fullName",
                                                                                               "userStatus",
                                                                                               "userPrimaryEmail",
                                                                                               "userMobile",
                                                                                               "userOrgID",
                                                                                               "ministry_name",
                                                                                               "dept_name",
                                                                                               "userOrgName",
                                                                                               "designation", "group",
                                                                                               "additionalProperties.externalSystem",
                                                                                               "additionalProperties.externalSystemId")

            contentHierarchyDF = spark.read.parquet(ParquetFileConstants.CONTENT_HIERARCHY_SELECT_PARQUET_FILE)
            allCourseProgramESDF = spark.read.parquet(
                ParquetFileConstants.ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE).filter(
                col("category").isin(primary_categories))

            allCourseProgramDetailsDF = contentDFUtil.allCourseProgramDetailsWithCompetenciesJsonDataFrame(
                allCourseProgramESDF, contentHierarchyDF,
                spark.read.parquet(ParquetFileConstants.ORG_SELECT_PARQUET_FILE)).drop("competenciesJson")

            enrolmentDF = spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE).filter(col('enrolment_status') == 'enrolled')

            acbpAllEnrolDF = spark.read.parquet(ParquetFileConstants.ACBP_COMPUTED_FILE)

            acbpAllEnrolmentDF = (acbpAllEnrolDF \
                                  .withColumn("courseID", explode(col("acbpCourseIDList"))) \
                                  .withColumn("courseID", regexp_replace(col("courseID"), r"^\s*\[|\]\s*$|\s+", "")) \
                                  .join(allCourseProgramDetailsDF, ["courseID"], "left") \
                                  .join(enrolmentDF, ["courseID", "userID"], "left") \
                                  .na.drop(subset=["userID", "courseID"]) \
                                  .drop("acbpCourseIDList") \
                                  )

            # Assignment type mapping
            assignment_type_mapping = {
                'rootorgid': 'mdo_id',
                'user': 'user',
                'customuser': 'user',
                'alluser': 'user',
                'designation': 'designation',
                'cadre': 'cadre',
                'group': 'groups',
                'batch': 'cadre_batch',
                'service': 'civil_services',
                'isprofileverified': 'is_verified_karmayogi',
                'isoncentraldeputation': 'is_on_central_deputation'
            }

            mapping_expr = create_map([lit(x) for x in chain(*assignment_type_mapping.items())])

            acbpSelectEnrolmentDF = spark.read.parquet(ParquetFileConstants.ACBP_SELECT_FILE) \
                .withColumn("courseID", explode(col("acbpCourseIDList"))) \
                .join(allCourseProgramDetailsDF, ["courseID"], "left") \
                .drop("acbpCourseIDList") \
                .withColumn("assignmentTypeInfo",
                            array_join(
                                F.transform(
                                    split(col("assignmentTypeInfo"), "\\|"),
                                    lambda seg: array_join(
                                        F.transform(
                                            from_json(seg, ArrayType(StringType())),
                                            lambda v: F.concat(lit('"'), v, lit('"'))),", ")), "|")) \
                .withColumn("assignmentTypeInfo", when(col("assignmentType") == "alluser", lit("AllUser")).otherwise(col("assignmentTypeInfo"))) \
                .withColumn("assignmentType", array_join(F.transform(split(col("assignmentType"), "\\|"), lambda x: mapping_expr[trim(x)]), "|"))

            cbPlanWarehouseDF = acbpSelectEnrolmentDF \
                .select(
                "orgID", "acbpCreatedBy", "acbpID", "cbPlanName", "isapar",
                "assignmentType", "assignmentTypeInfo", "courseID",
                "allocatedOn", "completionDueDate", "acbpStatus"
            ) \
                .withColumn("data_last_generated_on", lit(currentDateTime)) \
                .select(
                col("orgID").alias("org_id"),
                col("acbpCreatedBy").alias("created_by"),
                col("acbpID").alias("cb_plan_id"),
                col("cbPlanName").alias("plan_name"),
                col("assignmentType").alias("allotment_type"),
                col("assignmentTypeInfo").alias("allotment_to"),
                col("courseID").alias("content_id"),
                date_format(col("allocatedOn"), ParquetFileConstants.DATE_TIME_FORMAT).alias("allocated_on"),
                date_format(col("completionDueDate"), ParquetFileConstants.DATE_TIME_FORMAT).alias("due_by"),
                col("acbpStatus").alias("status"),
                col("isapar"),
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

            ministry_is_empty = (col("ministry_name").isNull()) | (col("ministry_name") == "")
            dept_is_empty = (col("dept_name").isNull()) | (col("dept_name") == "")

            enrolmentReportDF = acbpEnrolmentDF \
                .filter(col("userStatus").cast("int") == 1) \
                .select(
                "fullName", "userPrimaryEmail", "userMobile", "userOrgName", "group",
                "designation", "ministry_name", "dept_name", "cadreName", "civilServiceType", "civilServiceName",
                "cadreBatch", "organised_service", "courseName", "isapar",
                "userOrgID", "dbCompletionStatus", "courseCompletedTimestamp",
                "allocatedOn", "completionDueDate", "employeeCode"
            ) \
                .withColumn(
                "currentProgress",
                F.when(col("dbCompletionStatus") == 2, "Completed")
                .when(col("dbCompletionStatus") == 1, "In Progress")
                .when(col("dbCompletionStatus") == 0, "Not Started")
                .otherwise("Not Enrolled")
            ) \
                .withColumn("courseCompletedTimestamp",
                            date_format(col("courseCompletedTimestamp"), ParquetFileConstants.DATE_FORMAT)) \
                .withColumn("allocatedOn", date_format(col("allocatedOn"), ParquetFileConstants.DATE_FORMAT)) \
                .withColumn("completionDueDate",
                            date_format(col("completionDueDate"), ParquetFileConstants.DATE_FORMAT)) \
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
                col("employeeCode").alias("EmployeeId"),
                col("MDO_Name"),
                col("group").alias("Group"),
                col("designation").alias("Designation"),
                col("Ministry"),
                col("Department"),
                col("Organization"),
                col("cadreName").alias("Cadre"),
                col("civilServiceType").alias("Civil Service Type"),
                col("civilServiceName").alias("Civil Services"),
                col("cadreBatch").alias("Cadre Batch"),
                col("organised_service").alias("Is From Organised Service of Govt"),
                col("courseName").alias("Name of CBP Allocated Course"),
                col("isapar"),
                col("allocatedOn").alias("Allocated On"),
                col("currentProgress").alias("Current Progress"),
                col("completionDueDate").alias("Due Date of Completion"),
                col("courseCompletedTimestamp").alias("Actual Date of Completion"),
                col("userOrgID").alias("mdoid"),
                lit(currentDateTime).alias("Report_Last_Generated_On")
            ) \
                .fillna("")

            print(f"Stage 1: Complete - source data + enrolment report built (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            # ================================================================
            # CHANGED: this job now writes PLAIN parquet only - no in-job
            # Polars/DuckDB CSV export at all. Calling Polars from inside this
            # Spark job (as a previous draft did) kept Spark's reserved
            # executor/driver memory alive the whole time Polars' own
            # partition_by() memory spike was happening - the exact
            # contention risk already fixed for courseBasedAssessmentReport.
            # The per-org CSV split now happens entirely in the separate,
            # standalone polars_csv_export.py process, run AFTER this job's
            # Spark session has fully stopped.
            #
            # This path is STABLE (not cleaned up by this job) - keep it in
            # sync with the matching entry in polars_csv_export.py's
            # get_reports() registry.
            # ================================================================
            print("📝 Writing plain parquet for enrollment report...")
            enrolmentReportPlainPath = f"{config.localReportDir}/temp/cbp-enrolment-report-plain/{today}"
            enrolmentReportDF.write.mode("overwrite").option("compression", "snappy").parquet(enrolmentReportPlainPath)

            enrolmentReportDF.write.mode("overwrite").option("compression", "snappy").parquet(
                f"{config.warehouseReportDir}/cbp_enrollments")
            print(f"Stage 2: Complete - enrollment plain parquet + warehouse write (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            ######################################################
            # creating data for apar enrollment report for sahil
            #######################################################

            print("📝 Start Apar enrollment report data...")

            kcmDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwKcmDictionaryTable}")
            kcmMappingDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwKcmContentTable}")

            kcmMappingDF = kcmMappingDF.join(kcmDF, kcmDF.competency_area_id == kcmMappingDF.competency_area_id, "left").select(
                col("course_id"),
                kcmMappingDF["competency_area_id"],
                col("competency_area")
            ).distinct()

            km = kcmMappingDF.alias("km")
            kc = (
                kcmDF
                .alias("kc")
                .withColumnRenamed("competency_area", "kc_competency_area")
            )

            resultDF = (
                km.join(kc, kc.competency_area_id == km.competency_area_id, "left")
                .select(
                    km.course_id,
                    km.competency_area_id,
                    kc.kc_competency_area.alias("competency_area")
                )
                .distinct()
                .groupBy("course_id")
                .agg(
                    F.concat_ws(", ", F.collect_set("competency_area")).alias("competency_areas")
                )
            )


            print("📝 Preparing Apar enrollment report data...")

            userAdditionalProperties = userOrgDF.select("userID", "externalSystem","externalSystemId")

            aparEnrolmentData = acbpAllEnrolmentDF.where((col("acbpStatus") == "Live") & (col("isapar") == True)) \
                .join(userAdditionalProperties, "userID", "left") \
                .join(resultDF, acbpAllEnrolmentDF.courseID == resultDF.course_id, "left") \
                .withColumn(
                "content_duration",
                F.when(F.col("courseDuration").isNull(), None)
                .when(F.col("courseDuration") == 0, "00:00:00")
                .otherwise(
                    F.format_string(
                        "%02d:%02d:%02d",
                        (F.col("courseDuration") / 3600).cast("int"),
                        ((F.col("courseDuration") % 3600) / 60).cast("int"),
                        (F.col("courseDuration") % 60).cast("int")
                    )
                )
            ) \
                .filter((col("userStatus").cast("int") == 1) & (col("isApar") == True)) \
                .select(
                col("userID").alias("user_id"),
                col("fullName").alias("name"),
                col("employeeCode").alias("EmployeeId"),
                col("userOrgID").alias("mdo_id"),
                col("userOrgName").alias("mdo_name"),
                col("courseID").alias("content_id"),
                col("courseName").alias("content_name"),
                col("courseStatus").alias("content_status"),
                col("content_duration"),
                col("courseCategory").alias("content_type"),
                col("userMobile").alias("phone"),
                col("userPrimaryEmail").alias("email"),
                col('isApar').alias("is_apar"),
                when(((col("courseProgress").isNotNull()) & (col("courseProgress") > 0)), col("courseProgress")).otherwise(0).alias("content_progress_percentage"),
                col("externalSystem").alias("external_system"),
                col("externalSystemId").alias("external_system_id"),
                col("competency_areas").alias("competency_type"),
                lit(None).cast("string").alias("parichay_id"),
                col("allocatedOn").cast("timestamp").alias("assigned_on")).dropDuplicates(["user_id", "content_id"])

            resultDF.unpersist()
            kcmMappingDF.unpersist()
            kcmDF.unpersist()
            print("✅ Apar enrollment report data prepared successfully!")
            print(f"Stage 3: Complete - Apar enrollment data prepared (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            ######################################################
            # end of apar enrollment report for sahil
            ######################################################

            ministry_is_empty = (col("ministry_name").isNull()) | (col("ministry_name") == "")
            dept_is_empty = (col("dept_name").isNull()) | (col("dept_name") == "")

            cleanDF = acbpEnrolmentDF \
                .filter(col("userStatus").cast("int") == 1) \
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
            )

            userSummaryReportDF = cleanDF \
                .groupBy(
                "userID", "fullName", "userPrimaryEmail", "userMobile",
                "designation", "cadreName", "civilServiceType", "civilServiceName",
                "cadreBatch", "organised_service", "group", "userOrgID",
                "Ministry", "Department", "Organization", "employeeCode"
            ) \
                .agg(
                countDistinct("courseID").alias("allocatedCount"),
                spark_sum(when(col("dbCompletionStatus") == 2, 1).otherwise(0)).alias("completedCount"),
                spark_sum(
                    when(
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
                col("employeeCode").alias("EmployeeId"),
                col("Ministry"),
                col("Department"),
                col("Organization"),
                col("group").alias("Group"),
                col("designation").alias("Designation"),
                col("cadreName").alias("Cadre"),
                col("civilServiceType").alias("Civil Service Type"),
                col("civilServiceName").alias("Civil Services"),
                col("cadreBatch").alias("Cadre Batch"),
                col("organised_service").alias("Is From Organised Service of Govt"),
                col("allocatedCount").alias("Number of CBP Courses Allocated"),
                col("completedCount").alias("Number of CBP Courses Completed"),
                col("completedBeforeDueDateCount").alias("Number of CBP Courses Completed within due date"),
                col("userOrgID").alias("mdoid"),
                lit(currentDateTime).alias("Report_Last_Generated_On")
            )

            # Same reasoning as the enrollment report above - plain parquet
            # only, CSV split handled by the separate polars_csv_export.py run.
            print("📝 Writing plain parquet for user summary report...")
            summaryReportPlainPath = f"{config.localReportDir}/temp/cbp-summary-report-plain/{today}"
            userSummaryReportDF.write.mode("overwrite").option("compression", "snappy").parquet(summaryReportPlainPath)
            print(f"Stage 4: Complete - user summary plain parquet written (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            print("📦 Writing warehouse data...")
            # coalesce(1) removed - was forcing this write through a single
            # task regardless of available parallelism, same validated fix
            # as courseBasedAssessmentReport (there: 496.68s -> 27.48s).
            cbPlanWarehouseDF.write.mode("overwrite").option("compression", "snappy").parquet(
                f"{config.warehouseReportDir}/{config.dwCBPlanTable}")
            print(f"Stage 5: Complete - cbPlanWarehouseDF written (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()
            print("✅ Processing completed successfully!")

            print("📝 Writing Apar enrollment parquet report for warehouse...")
            # coalesce(1) removed here too - this was the write that showed
            # up in Spark UI as a single task blocking 6 pending stages for
            # 2.3+ minutes (Job 59, "0/1 (1 running)").
            aparEnrolmentData.write.mode("overwrite").option("compression", "snappy").parquet(
                f"{config.warehouseReportDir}/{config.dwAparCBPEnrollmentTable}")
            print(f"Stage 6: Complete - aparEnrolmentData written (⏱ {time.time() - stage_start:.2f}s)")
            print("✅ Apar enrollment parquet report written successfully!")

            total_time = time.time() - start_time
            print(f"\n✅ ACBPModel processing completed in {total_time:.2f} seconds ({total_time / 60:.1f} minutes)")
            print("   Next step: run polars_csv_export.py to produce the per-org CSVs from the plain parquet(s) above.")

        except Exception as e:
            print(f"❌ Error occurred during ACBPModel processing: {str(e)}")
            raise e


def main():
    # Reverted from local-cluster back to local[*] - measured no benefit for
    # this job (25:14 min on local[*] vs 25:08 min on local-cluster[4,8,40000],
    # essentially unchanged). Confirms this job's bottleneck is the un-batched
    # DuckDB path and the coalesce(1) writes, not GC/spill - same conclusion
    # reached for dashboardSync. local-cluster's executor-isolation/GC benefit
    # doesn't pay for its own coordination + serialization overhead when the
    # real cost lives elsewhere.
    spark = SparkSession.builder \
        .appName("ACBP Report") \
        .config("spark.sql.shuffle.partitions", "120") \
        .config("spark.driver.memory", "120g") \
        .config("spark.driver.memoryOverhead", "20g") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    config_dict = get_environment_config()
    config = create_config(config_dict)

    start_time = datetime.now()
    print(f"[START] ACBPModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    model = ACBPModel()
    model.process_data(spark, config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] ACBPModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()


if __name__ == "__main__":
    main()

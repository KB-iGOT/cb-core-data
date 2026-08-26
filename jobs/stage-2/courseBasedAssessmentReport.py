import findspark
findspark.init()

import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
from pyspark.sql.window import Window
from pyspark.sql.functions import (col, row_number, countDistinct, current_timestamp, date_format, broadcast,
                                   unix_timestamp, when, lit, concat_ws, from_unixtime, format_string, expr,
                                   upper, coalesce, concat, trim, exists, split)
from datetime import datetime
from pyspark.sql import functions as F
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))

from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.assessment import assessmentDFUtil
from dfutil.content import contentDFUtil
from dfutil.dfexport import dfexportutil
from jobs.default_config import create_config
from jobs.config import get_environment_config


class CourseBasedAssessmentModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.CourseBasedAssessmentModel"

    def name(self):
        return "CourseBasedAssessmentModel"

    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

    def process_data(self, spark, config):
        try:
            start_time = time.time()
            stage_start = time.time()  # CHANGED: added for per-stage timing
            today = self.get_date()
            currentDateTime = date_format(current_timestamp(), ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT)

            print("Stage 1: Loading assessment data...")
            assessmentDF = spark.read.parquet(ParquetFileConstants.ALL_ASSESSMENT_COMPUTED_PARQUET_FILE) \
                .filter(
                col("assessCategory").isin("Course", "Standalone Assessment", "Blended Program", "Curated Program"))
            hierarchyDF = spark.read.parquet(ParquetFileConstants.HIERARCHY_PARQUET_FILE)
            organizationDF = spark.read.parquet(ParquetFileConstants.ORG_COMPUTED_PARQUET_FILE)
            print(f"Stage 1: Complete (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            print("Stage 2: Adding hierarchy information...")
            assWithHierarchyData = assessmentDFUtil.add_hierarchy_column(
                assessmentDF, hierarchyDF, id_col="assessID", as_col="data", spark=spark,
                children=True, competencies=True, l2_children=True)
            print(f"Stage 2: Complete (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            print("Stage 3: Transforming assessment data...")
            assessWithHierarchyDF = assessmentDFUtil.transform_assessment_data(assWithHierarchyData, organizationDF)
            assessWithDetailsDF = assessWithHierarchyDF.drop("children")
            print(f"Stage 3: Complete (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            assessChildrenDF = assessmentDFUtil.assessment_children_dataframe(assessWithHierarchyDF)
            print(f"Stage 4: Complete (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            print("Stage 5: Processing user assessment data...")
            userAssessmentDF = spark.read.parquet(ParquetFileConstants.USER_ASSESSMENT_PARQUET_FILE) \
                .filter(col("assessUserStatus") == "SUBMITTED") \
                .withColumn("assessStartTime", col("assessStartTimestamp").cast("long")) \
                .withColumn("assessEndTime", col("assessEndTimestamp").cast("long"))

            windowBasic = Window.partitionBy("userID", "assessChildID").orderBy(col("assessOverallResult").desc())
            windowSectional = Window.partitionBy("userID", "assessChildID").orderBy(col("assessTotalSectionMarks").desc())

            userAssessmentDF = userAssessmentDF \
                .withColumn("rn",
                            when(col("assessTotalSectionMarks").isNull(), row_number().over(windowBasic))
                            .otherwise(row_number().over(windowSectional))) \
                .filter(col("rn") == 1).drop("rn")
            print(f"Stage 5: Complete (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            userAssessChildrenDF = assessmentDFUtil.user_assessment_children_dataframe(userAssessmentDF, assessChildrenDF)

            categories = ["Course", "Program", "Blended Program", "Standalone Assessment", "Curated Program"]
            allCourseProgramDetailsWithCompDF = assessmentDFUtil.all_course_program_details_with_competencies_json_dataframe(
                spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE)
                    .filter(col("category").isin(categories)), hierarchyDF, organizationDF, spark)
            allCourseProgramDetailsDF = allCourseProgramDetailsWithCompDF.drop("competenciesJson")
            print(f"Stage 6: Complete (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            print("Stage 7: Adding rating information...")
            allCourseProgramDetailsWithRatingDF = assessmentDFUtil.all_course_program_details_with_rating_df(
                allCourseProgramDetailsDF,
                spark.read.parquet(ParquetFileConstants.RATING_SUMMARY_COMPUTED_PARQUET_FILE))

            userAssessChildrenDetailsDF = assessmentDFUtil.user_assessment_children_details_dataframe(
                userAssessChildrenDF, assessWithDetailsDF, allCourseProgramDetailsWithRatingDF,
                spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE))

            retakesDF = userAssessChildrenDetailsDF.groupBy("assessChildID", "userID").agg(
                countDistinct("assessStartTime").alias("retakes"))

            windowSpec = Window.partitionBy("assessChildID", "userID").orderBy(col("assessEndTimestamp").desc())
            userAssessChildDataLatestDF = userAssessChildrenDetailsDF.withColumn(
                "rowNum", row_number().over(windowSpec)).filter(F.col("rowNum") == 1).drop("rowNum") \
                .join(retakesDF.select("assessChildID", "userID", "retakes"), ["assessChildID", "userID"], "left")

            finalDF = userAssessChildDataLatestDF.withColumn(
                "userAssessmentDuration",
                unix_timestamp("assessEndTimestamp") - unix_timestamp("assessStartTimestamp")) \
                .withColumn("Pass", when(col("assessPass") == 1, "Yes").otherwise("No")) \
                .withColumn("assessPercentage",
                            when(col("assessPassPercentage").isNotNull(), col("assessPassPercentage"))
                            .otherwise(lit("Need to pass in all sections"))) \
                .withColumn("assessment_type",
                            when(col("assessPrimaryCategory").isNotNull(), col("assessPrimaryCategory"))
                            .when(col("assessCategory") == "Standalone Assessment", col("assessCategory"))
                            .otherwise(lit(""))) \
                .withColumn("assessment_course_name",
                            when(col("assessment_type") == "Course Assessment", col("assessName")).otherwise(lit(""))) \
                .withColumn("Total_Score_Calculated",
                            when(col("assessMaxQuestions").isNotNull(), col("assessMaxQuestions") * 1)) \
                .withColumn("course_id", lit(col("assessID"))) \
                .withColumn("Tags", concat_ws(", ", col("tag")))

            finalFormattedDF = self.duration_format(finalDF, "assessExpectedDuration") \
                .withColumnRenamed("assessExpectedDuration", "totalAssessmentDuration")

            fullReportNewDF = finalFormattedDF.withColumn("MDO_Name", col("userOrgName")) \
                .withColumn("Ministry", when(col("ministry_name").isNull(), col("userOrgName")).otherwise(col("ministry_name"))) \
                .withColumn("Department", when((col("Ministry").isNotNull()) & (col("Ministry") != col("userOrgName")) &
                                               ((col("dept_name").isNull()) | (col("dept_name") == "")),
                                               col("userOrgName")).otherwise(col("dept_name"))) \
                .withColumn("Organisation",
                            when((col("Ministry") != col("userOrgName")) & (col("Department") != col("userOrgName")),
                                 col("userOrgName")).otherwise(lit(""))) \
                .withColumn("Report_Last_Generated_On", currentDateTime) \
                .select(
                col("userID"), col("assessChildID").alias("assessment_id"), col("assessID"), col("assessOrgID"),
                col("userOrgID"), col("fullName"), col("userStatus").alias("status"),
                col("professionalDetails.designation").alias("Designation"),
                col("personalDetails.primaryEmail").alias("E mail"),
                col("personalDetails.mobile").alias("Phone Number"),
                col("MDO_Name"), col("employmentDetails.employeeCode").alias("Employee_Id"),
                col("professionalDetails.group").alias("Group"), col("Tags"),
                col("Ministry"), col("Department"), col("Organisation"), col("role").alias("Roles"),
                col("assessChildName").alias("assessment_name"), col("assessment_type"),
                col("assessOrgName").alias("assessment_content_provider"),
                date_format(from_unixtime(col("assessLastPublishedOn")), ParquetFileConstants.DATE_FORMAT).alias("assessment_publish_date"),
                col("assessment_course_name").alias("course_name"), col("course_id"),
                col("totalAssessmentDuration").alias("assessment_duration"),
                date_format(from_unixtime(col("assessEndTime")), ParquetFileConstants.DATE_FORMAT).alias("last_attempted_date"),
                col("assessOverallResult").alias("latest_percentage_achieved"), col("assessPercentage"), col("Pass"),
                col("assessMaxQuestions").alias("total_questions"), col("assessIncorrect").alias("incorrect_count"),
                col("assessBlank").alias("unattempted_questions"), col("retakes"), col("assessEndTime"),
                col("userOrgID").alias("mdoid"), col("Report_Last_Generated_On"))
            print(f"Stage 7: Complete (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            oldAssessmentDetailsDF = spark.read.parquet(ParquetFileConstants.OLD_ASSESSMENT_COMPUTED_PARQUET_FILE)

            fullReportOldDF = oldAssessmentDetailsDF \
                .withColumn("MDO_Name", col("userOrgName")) \
                .withColumn("Ministry", when(col("ministry_name").isNull(), col("userOrgName")).otherwise(col("ministry_name"))) \
                .withColumn("Department", when(
                (col("Ministry").isNotNull()) & (col("Ministry") != col("userOrgName")) & (
                        col("dept_name").isNull() | (col("dept_name") == "")),
                col("userOrgName")).otherwise(col("dept_name"))) \
                .withColumn("Organisation",
                            when((col("Ministry") != col("userOrgName")) & (col("Department") != col("userOrgName")),
                                 col("userOrgName")).otherwise(lit(""))) \
                .withColumn("Report_Last_Generated_On", currentDateTime) \
                .select(
                col("userID"), col("source_id").alias("assessment_id"), col("courseOrgID"), col("assessChildID"),
                col("userOrgID"), col("fullName"), col("userStatus"), col("designation").alias("Designation"),
                col("userPrimaryEmail").alias("E mail"), col("userMobile").alias("Phone Number"), col("MDO_Name"),
                col("employmentDetails.employeeCode").alias("Employee_Id"), col("group").alias("Group"),
                col("tag").alias("Tags"), col("Ministry"), col("Department"), col("Organisation"),
                lit(None).cast(StringType()).alias("Roles"), col("source_title").alias("assessment_name"),
                col("assessment_type"), col("courseOrgID").alias("assessment_content_provider"),
                col("assessment_publish_date"), col("courseName").alias("course_name"),
                col("courseID").alias("course_id"), col("assessment_duration"), col("last_attempted_date"),
                col("result_percent").alias("latest_percentage_achieved"), col("pass_percent").alias("assessPercentage"),
                col("Pass"), col("total_questions"), col("incorrect_count"),
                col("not_answered_count").alias("unattempted_questions"), col("retakes"), col("assessEndTime"),
                col("userOrgID").alias("mdoid"), col("Report_Last_Generated_On"))

            fullReportDF = fullReportNewDF.union(fullReportOldDF).dropDuplicates(["userID", "assessment_id", "course_id"])
            # CHANGED: force materialization now, once, so it's genuinely cached
            # rather than being recomputed at every downstream action.
            fullReportDF = fullReportDF.cache()
            _fr_count = fullReportDF.count()
            print(f"Stage 9: Complete - fullReportDF built and cached ({_fr_count:,} rows) (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            mdoReportDF = fullReportDF.filter(col("status") == 1).select(
                col("userID").alias("User ID"), col("fullName").alias("Full Name"), col("Designation"),
                col("E mail"), col("Phone Number"), col("MDO_Name"), col("Group"), col("Tags"),
                col("Ministry"), col("Department"), col("Organisation"), col("Roles"), col("Employee_Id"),
                col("assessment_name").alias("Assessment Name"), col("assessment_type").alias("Assessment Type"),
                col("assessment_content_provider").alias("Assessment/Content Provider"),
                col("assessment_publish_date").alias("Assessment Publish Date"), col("course_name").alias("Course Name"),
                col("course_id").alias("Course ID"), col("assessment_duration").alias("Assessment Duration"),
                col("last_attempted_date").alias("Last Attempted Date"),
                col("latest_percentage_achieved").alias("Latest Percentage Achieved"),
                col("assessPercentage").alias("Cut off Percentage"), col("Pass"),
                col("total_questions").alias("Total Questions"), col("incorrect_count").alias("No of Incorrect Responses"),
                col("unattempted_questions").alias("Unattempted Questions"), col("retakes").alias("No of Retakes"),
                col("mdoid"), col("Report_Last_Generated_On")
            )

            mdoReportDF = mdoReportDF.withColumn(
                "is_non_govt_user",
                when(
                    (trim(upper(coalesce(col("Designation"), lit("")))) == "VOLUNTEER") |
                    coalesce(exists(split(coalesce(col("Roles"), lit("")), ","), lambda r: trim(upper(r)) == "VOLUNTEER"), lit(False)),
                    lit(True)
                ).otherwise(lit(False))
            ).cache()
            # CHANGED: force materialization now, once - THIS is the fix that
            # prevents the expensive join/window chain from being recomputed
            # 2-3 more times later at collect(), CSV export, and parquet write.
            _mdo_count = mdoReportDF.count()
            print(f"Stage 10: Complete - mdoReportDF classified and cached ({_mdo_count:,} rows) (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            govt_part_df = mdoReportDF.filter(~col("is_non_govt_user")).drop("is_non_govt_user", "Roles")
            non_govt_part_df = mdoReportDF.filter(col("is_non_govt_user")).drop("is_non_govt_user", "Roles")

            govt_org_ids = set(
                row.mdoid for row in govt_part_df.select("mdoid").distinct().collect() if row.mdoid is not None
            )
            non_govt_org_ids = set(
                row.mdoid for row in non_govt_part_df.select("mdoid").distinct().collect() if row.mdoid is not None
            )
            both_org_ids = govt_org_ids & non_govt_org_ids

            print(f"Orgs with only Govt users: {len(govt_org_ids - both_org_ids)}")
            print(f"Orgs with only Non-Govt users: {len(non_govt_org_ids - both_org_ids)}")
            print(f"Orgs with BOTH Govt and Non-Govt users (2 reports each): {len(both_org_ids)}")
            print(f"Stage 11: Complete - Govt/Non-Govt org id sets determined (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            both_org_ids_list = list(both_org_ids)
            non_govt_part_df = non_govt_part_df.withColumn(
                "mdoid",
                when(col("mdoid").isin(both_org_ids_list), concat(col("mdoid"), lit("_non_govt")))
                .otherwise(col("mdoid"))
            )

            finalAssessmentDF = spark.read.parquet(ParquetFileConstants.FINAL_ASSESSMENT_PARQUET_FILE)
            finalAssessmentDF = finalAssessmentDF.join(userAssessmentDF,
                                                       finalAssessmentDF["Identifier"] == userAssessmentDF["assessChildID"], "inner") \
                .withColumn("cut_off_percentage", col("assessPassPercentage").cast("float")) \
                .withColumn("time_spent_by_the_user",
                            unix_timestamp("assessEndTimestamp") - unix_timestamp("assessStartTimestamp")) \
                .withColumn("data_last_generated_on", currentDateTime) \
                .select(col("identifier").alias("assessment_id"), col("userID").alias("user_id"),
                        col("courseID").alias("content_id"), col("name").alias("assessment_name"),
                        col("assessPrimaryCategory").alias("assessment_type"),
                        col("assessExpectedDuration").alias("assessment_duration"), col("time_spent_by_the_user"),
                        date_format(from_unixtime(col("assessEndTime")), ParquetFileConstants.DATE_FORMAT).alias("completion_date"),
                        col("assessOverallResult").alias("score_achieved"), col("assessMaxQuestions").alias("overall_score"),
                        col("cut_off_percentage"), col("assessMaxQuestions").alias("total_question"),
                        col("assessIncorrect").alias("number_of_incorrect_responses"), lit(0).alias("number_of_retakes"),
                        when(col("assessPass") == 1, "Yes").otherwise("No").alias("pass"), col("data_last_generated_on"))

            finalAssessmentDF = self.duration_format(finalAssessmentDF, "assessment_duration")
            finalAssessmentDF = self.duration_format(finalAssessmentDF, "time_spent_by_the_user")
            print(f"Stage 12: Complete - finalAssessmentDF built (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            warehouseDF = fullReportDF.withColumn("data_last_generated_on", currentDateTime) \
                .withColumn("cut_off_percentage", col("assessPercentage").cast("float")) \
                .withColumn("pass", when(col("Pass") == "Yes", "Yes").otherwise("No")) \
                .select(
                col("userID").alias("user_id"), col("course_id").alias("content_id"), col("assessment_id"),
                col("assessment_name").alias("assessment_name"), col("assessment_type").alias("assessment_type"),
                col("assessment_duration").alias("assessment_duration"),
                col("assessment_duration").alias("time_spent_by_the_user"),
                date_format(from_unixtime(col("assessEndTime")), ParquetFileConstants.DATE_FORMAT).alias("completion_date"),
                col("latest_percentage_achieved").alias("score_achieved"), col("total_questions").alias("overall_score"),
                col("cut_off_percentage"), col("total_questions").alias("total_question"),
                col("incorrect_count").alias("number_of_incorrect_responses"), col("retakes").alias("number_of_retakes"),
                col("pass"), col("data_last_generated_on"))

            warehouseDF = warehouseDF.unionByName(finalAssessmentDF)

            assessmentMinPassDF = spark.read.parquet(f"{config.baseCachePath}/esCourseAssessment")
            assessMinPassDF = assessmentMinPassDF.filter(
                col('minimumPassPercentage').isNotNull() & (col('minimumPassPercentage') > 0)) \
                .select("identifier", "minimumPassPercentage")

            warehouseDF = warehouseDF.join(broadcast(assessMinPassDF),
                                           warehouseDF.assessment_id == assessMinPassDF.identifier, "left"
                                           ).select(warehouseDF["*"], assessMinPassDF["minimumPassPercentage"])

            warehouseDF = warehouseDF.join(assessmentDF, warehouseDF["content_id"] == assessmentDF["assessID"], "left") \
                .withColumn("assessment_sub_type",
                            when(col("assessCourseCategory") == "Comprehensive Assessment Program",
                                 "Comprehensive Assessment Program").otherwise(col("assessment_type"))) \
                .withColumn("cut_off_percentage",
                            when(col("minimumPassPercentage").isNotNull(), col("minimumPassPercentage").cast("float"))
                            .otherwise(col("cut_off_percentage"))) \
                .select(
                col("user_id"), col("content_id"), col("assessment_id"), col("assessment_name"),
                col("assessment_type"), col("assessment_sub_type"), col("assessment_duration"),
                col("time_spent_by_the_user"), col("completion_date"), col("score_achieved"), col("overall_score"),
                col("cut_off_percentage"), col("total_question"), col("number_of_incorrect_responses"),
                col("number_of_retakes"), col("pass"), col("data_last_generated_on"))

            w = Window.partitionBy("user_id", "content_id", "assessment_id").orderBy(
                col("score_achieved").desc(), col("completion_date").desc())

            warehouseDF = (warehouseDF.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn"))
            # CHANGED: force materialization now, once, before it gets used
            # three separate times below (CSV exports x2 + final parquet write).
            warehouseDF = warehouseDF.cache()
            _wh_count = warehouseDF.count()
            print(f"Stage 13: Complete - warehouseDF built and cached ({_wh_count:,} rows) (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            cba_out_path = f"{config.localReportDir}/{config.cbaReportPath}/{today}"

            if govt_org_ids:
                print(f"➡️  Writing GOVT course-based assessment report -> {cba_out_path}")
                dfexportutil.write_csv_per_mdo_id_duckdb(
                    govt_part_df, cba_out_path, 'mdoid',
                    f"{config.localReportDir}/temp/cba-report/{today}",
                    csv_filename=config.cbaReport
                )
            else:
                print("ℹ️  No Govt users found in this run — skipping Govt CSV write.")

            if non_govt_org_ids:
                print(f"➡️  Writing NON-GOVT course-based assessment report -> {cba_out_path}")
                dfexportutil.write_csv_per_mdo_id_duckdb(
                    non_govt_part_df, cba_out_path, 'mdoid',
                    f"{config.localReportDir}/temp/cba-report-non-govt/{today}",
                    csv_filename=config.cbaReport
                )
            else:
                print("ℹ️  No Non-Govt (VOLUNTEER) users found in this run — skipping Non-Govt CSV write.")
            print(f"Stage 14: Complete - CSV export written (⏱ {time.time() - stage_start:.2f}s)")
            stage_start = time.time()

            print("Stage 15: Starting warehouse parquet write...")
            (warehouseDF.coalesce(1)
             .write
             .mode("overwrite")
             .option("compression", "snappy")
             .parquet(f"{config.warehouseReportDir}/{config.dwAssessmentTable}"))
            print(f"Stage 15: Complete - warehouse parquet written (⏱ {time.time() - stage_start:.2f}s)")

            mdoReportDF.unpersist()
            fullReportDF.unpersist()
            warehouseDF.unpersist()

            total_time = time.time() - start_time
            print(f"\n✅ Optimized Course Based Assessment Report generation completed in {total_time:.2f} seconds ({total_time / 60:.1f} minutes)")

        except Exception as e:
            print(f"❌ Error: {str(e)}")
            raise

    def duration_format(self, df, in_col, out_col=None):
        out_col = out_col or in_col
        return df.withColumn(
            out_col,
            when(col(in_col).isNull(), lit(""))
            .otherwise(
                format_string(
                    "%02d:%02d:%02d",
                    expr(f"{in_col} / 3600").cast("int"),
                    expr(f"{in_col} % 3600 / 60").cast("int"),
                    expr(f"{in_col} % 60").cast("int")
                )
            )
        )


def main():
    spark = SparkSession.builder \
        .appName("Course Based Assessment Report Model - Cached") \
        .master("local-cluster[4, 8, 40000]") \
        .config("spark.executor.memory", "38g") \
        .config("spark.driver.memory", "16g") \
        .config("spark.driver.maxResultSize", "3g") \
        .config("spark.sql.shuffle.partitions", "128") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    start_time = datetime.now()
    try:
        print(f"[START] Course based assessment processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        config_dict = get_environment_config()
        config = create_config(config_dict)
        model = CourseBasedAssessmentModel()
        model.process_data(spark, config)
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"[END] Course based assessment completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[INFO] Total duration: {duration}")
    except Exception as e:
        print(f"[ERROR] Course based assessment job failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
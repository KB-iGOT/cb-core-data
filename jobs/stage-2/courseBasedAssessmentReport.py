import findspark
findspark.init()

import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
from pyspark.sql.window import Window
from pyspark.sql.functions import (col, row_number, countDistinct, current_timestamp, date_format, broadcast, unix_timestamp, when, lit, concat_ws, from_unixtime, format_string, expr)
from datetime import datetime
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))

# Reusable imports from userReport structure
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

    def process_data(self, spark,config):
        try:
            start_time = time.time()
            today = self.get_date()
            currentDateTime = current_timestamp()

            print("Stage 1: Loading assessment data...")
            assessmentDF = spark.read.parquet(ParquetFileConstants.ALL_ASSESSMENT_COMPUTED_PARQUET_FILE).filter(col("assessCategory").isin("Standalone Assessment"))
            hierarchyDF = spark.read.parquet(ParquetFileConstants.HIERARCHY_PARQUET_FILE)
            organizationDF = spark.read.parquet(ParquetFileConstants.ORG_COMPUTED_PARQUET_FILE)

            print("Stage 1: Complete")

            # Stage 2: Add Hierarchy Information
            print("Stage 2: Adding hierarchy information...")
            assWithHierarchyData = assessmentDFUtil.add_hierarchy_column(
            assessmentDF,
            hierarchyDF,
            id_col="assessID",
            as_col="data",
            spark=spark,
            children=True,
            competencies=True,
            l2_children=True )
            print("Stage 2: Complete")

            print("Stage 3: Transforming assessment data...")
            assessWithHierarchyDF = assessmentDFUtil.transform_assessment_data(assWithHierarchyData, organizationDF)
            assessWithDetailsDF = assessWithHierarchyDF.drop("children")
            print("Stage 3: Complete")

            assessChildrenDF = assessmentDFUtil.assessment_children_dataframe(assessWithHierarchyDF)
            print("Stage 4: Complete")
            print("Stage 5: Processing user assessment data...")
            userAssessmentDF = spark.read.parquet(ParquetFileConstants.USER_ASSESSMENT_PARQUET_FILE)\
            .filter(col("assessUserStatus") == "SUBMITTED")\
            .withColumn("assessStartTime", col("assessStartTimestamp").cast("long"))\
            .withColumn("assessEndTime", col("assessEndTimestamp").cast("long"))
            userAssessChildrenDF = assessmentDFUtil.user_assessment_children_dataframe(userAssessmentDF, assessChildrenDF)
            categories = ["Course", "Program", "Blended Program", "Curated Program", "Moderated Course",
                      "Standalone Assessment", "CuratedCollections"]
            allCourseProgramDetailsWithCompDF = assessmentDFUtil.all_course_program_details_with_competencies_json_dataframe(spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE)\
                .filter(col("category").isin(categories)), hierarchyDF, organizationDF, spark)
            print("All Course Program Details with Competencies JSON DataFrame Schema:")
            allCourseProgramDetailsDF = allCourseProgramDetailsWithCompDF.drop("competenciesJson")
            print("Stage 6: Complete")

            # Stage 7: Add Rating Information
            print("Stage 7: Adding rating information...")
            allCourseProgramDetailsWithRatingDF = assessmentDFUtil.all_course_program_details_with_rating_df(allCourseProgramDetailsDF,
            spark.read.parquet(ParquetFileConstants.RATING_SUMMARY_COMPUTED_PARQUET_FILE))
            userAssessChildrenDetailsDF = assessmentDFUtil.user_assessment_children_details_dataframe(userAssessChildrenDF, assessWithDetailsDF, allCourseProgramDetailsWithRatingDF, spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE))
            print("User Assessment Children DataFrame Schema:")
            retakesDF = userAssessChildrenDetailsDF.groupBy("assessChildID", "userID").agg(countDistinct("assessStartTime").alias("retakes"))

            # Step 2: Get latest entry per (assessChildID, userID) using row_number()
            windowSpec = Window.partitionBy("assessChildID", "userID").orderBy(col("assessEndTimestamp").desc())

            userAssessChildDataLatestDF = userAssessChildrenDetailsDF.withColumn("rowNum", row_number().over(windowSpec)).filter(col("rowNum") == 1).drop("rowNum")\
              .join(broadcast(retakesDF), ["assessChildID", "userID"], "left")

            finalDF = userAssessChildDataLatestDF.withColumn("userAssessmentDuration", unix_timestamp("assessEndTimestamp") - unix_timestamp("assessStartTimestamp")) \
                .withColumn("Pass", when(col("assessPass") == 1, "Yes").otherwise("No")) \
                .withColumn("assessPercentage", when(col("assessPassPercentage").isNotNull(), col("assessPassPercentage")).otherwise(lit("Need to pass in all sections"))) \
                .withColumn("assessment_type", when(col("assessCategory") == "Standalone Assessment", col("assessCategory")).
                            when(col("assessPrimaryCategory").isNotNull(), col("assessPrimaryCategory")).otherwise(lit(""))) \
                .withColumn("assessment_course_name", when(col("assessment_type") == "Course Assessment", col("assessName")).otherwise(lit(""))) \
                .withColumn("Total_Score_Calculated", when(col("assessMaxQuestions").isNotNull(), col("assessMaxQuestions") * 1)) \
                .withColumn("course_id", when(col("assessCategory") == "Standalone Assessment", lit("")).otherwise(col("assessID"))) \
                .withColumn("Tags", concat_ws(", ", col("tag")))
            finalFormattedDF = self.duration_format(finalDF,"assessExpectedDuration")\
                .withColumnRenamed("assessExpectedDuration", "totalAssessmentDuration")
            fullReportNewDF = finalFormattedDF.withColumn("MDO_Name", col("userOrgName")) \
                .withColumn("Ministry", when(col("ministry_name").isNull(), col("userOrgName")).otherwise(col("ministry_name"))) \
                .withColumn("Department", when((col("Ministry").isNotNull()) & (col("Ministry") != col("userOrgName")) &
                                               ((col("dept_name").isNull()) | (col("dept_name") == "")),
                                               col("userOrgName")).otherwise(col("dept_name"))) \
                .withColumn("Organisation", when((col("Ministry") != col("userOrgName")) & (col("Department") != col("userOrgName")),
                                 col("userOrgName")).otherwise(lit(""))) \
                .withColumn("Report_Last_Generated_On", currentDateTime) \
                .select(
                col("userID"),
                col("assessChildID").alias("assessment_id"),
                col("assessID"),
                col("assessOrgID"),
                col("userOrgID"),
                col("fullName"),
                col("userStatus").alias("status"),
                col("professionalDetails.designation").alias("Designation"),
                col("personalDetails.primaryEmail").alias("E mail"),
                col("personalDetails.mobile").alias("Phone Number"),
                col("MDO_Name"),
                col("professionalDetails.group").alias("Group"),
                col("Tags"),
                col("Ministry"),
                col("Department"),
                col("Organisation"),
                col("assessChildName").alias("assessment_name"),
                col("assessment_type"),
                col("assessOrgName").alias("assessment_content_provider"),
                from_unixtime(col("assessLastPublishedOn").cast("long"), "yyyy-MM-dd").alias("assessment_publish_date"),
                col("assessment_course_name").alias("course_name"),
                col("course_id"),
                col("totalAssessmentDuration").alias("assessment_duration"),
                from_unixtime(col("assessEndTime")).alias("last_attempted_date"),
                col("assessOverallResult").alias("latest_percentage_achieved"),
                col("assessPercentage"),
                col("Pass"),
                col("assessMaxQuestions").alias("total_questions"),
                col("assessIncorrect").alias("incorrect_count"),
                col("assessBlank").alias("unattempted_questions"),
                col("retakes"),
                col("assessEndTime"),
                col("userOrgID").alias("mdoid"),
                col("Report_Last_Generated_On"))
            oldAssessmentDetailsDF = spark.read.parquet(ParquetFileConstants.OLD_ASSESSMENT_COMPUTED_PARQUET_FILE)
            fullReportOldDF =  oldAssessmentDetailsDF\
                .withColumn("MDO_Name", col("userOrgName"))\
                .withColumn("Ministry", when(col("ministry_name").isNull(), col("userOrgName")).otherwise(col("ministry_name"))) \
                .withColumn("Department", when((col("Ministry").isNotNull()) & (col("Ministry") != col("userOrgName")) & (col("dept_name").isNull() | (col("dept_name") == "")),
                    col("userOrgName")).otherwise(col("dept_name"))) \
                .withColumn("Organisation", when((col("Ministry") != col("userOrgName")) & (col("Department") != col("userOrgName")), col("userOrgName")).otherwise(lit(""))) \
                .withColumn("Report_Last_Generated_On", current_timestamp()) \
                .select(
                    col("userID"),
                    col("source_id").alias("assessment_id"),
                    col("courseOrgID"),
                    col("assessChildID"),
                    col("userOrgID"),
                    col("fullName"),
                    col("userStatus"),
                    col("designation").alias("Designation"),
                    col("userPrimaryEmail").alias("E mail"),
                    col("userMobile").alias("Phone Number"),
                    col("MDO_Name"),
                    col("group").alias("Group"),
                    col("tag").alias("Tags"),
                    col("Ministry"),
                    col("Department"),
                    col("Organisation"),
                    col("source_title").alias("assessment_name"),
                    col("assessment_type"),
                    col("courseOrgID").alias("assessment_content_provider"),
                    col("assessment_publish_date"),
                    col("courseName").alias("course_name"),
                    col("courseID").alias("course_id"),
                    col("assessment_duration"),
                    col("last_attempted_date"),
                    col("result_percent").alias("latest_percentage_achieved"),
                    col("pass_percent").alias("assessPercentage"),
                    col("Pass"),
                    col("total_questions"),
                    col("incorrect_count"),
                    col("not_answered_count").alias("unattempted_questions"),
                    col("retakes"),
                    col("assessEndTime"),
                    col("userOrgID").alias("mdoid"),
                    col("Report_Last_Generated_On"))
            fullReportDF = fullReportNewDF.union(fullReportOldDF)
            mdoReportDF = fullReportDF.filter(col("status") == 1).select(
                col("userID").alias("User ID"),
                col("fullName").alias("Full Name"),
                col("Designation"),
                col("E mail"),
                col("Phone Number"),
                col("MDO_Name"),
                col("Group"),
                col("Tags"),
                col("status"),
                col("Ministry"),
                col("Department"),
                col("Organisation"),
                col("assessment_name").alias("Assessment Name"),
                col("assessment_type").alias("Assessment Type"),
                col("assessment_content_provider").alias("Assessment/Content Provider"),
                col("assessment_publish_date").alias("Assessment Publish Date"),
                col("course_name").alias("Course Name"),
                col("course_id").alias("Course ID"),
                col("assessment_duration").alias("Assessment Duration"),
                col("last_attempted_date").alias("Last Attempted Date"),
                col("latest_percentage_achieved").alias("Latest Percentage Achieved"),
                col("assessPercentage").alias("Cut off Percentage"),
                col("Pass"),
                col("total_questions").alias("Total Questions"),
                col("incorrect_count").alias("No of Incorrect Responses"),
                col("unattempted_questions").alias("Unattempted Questions"),
                col("retakes").alias("No of Retakes"),
                col("mdoid"),
                col("Report_Last_Generated_On")
            )

            warehouseDF = fullReportDF.withColumn("data_last_generated_on", currentDateTime)\
                .withColumn("cut_off_percentage", col("assessPercentage").cast("float")) \
                .select(
                    col("userID").alias("user_id"),
                    col("course_id").alias("content_id"),
                    col("assessment_id"),
                    col("assessment_name").alias("assessment_name"),
                    col("assessment_type").alias("assessment_type"),
                    col("assessment_duration").alias("assessment_duration"),
                    col("assessment_duration").alias("time_spent_by_the_user"),
                    date_format(from_unixtime(col("assessEndTime")), ParquetFileConstants.DATE_FORMAT).alias("completion_date"),
                    col("latest_percentage_achieved").alias("score_achieved"),
                    col("total_questions").alias("overall_score"),
                    col("cut_off_percentage"),
                    col("total_questions").alias("total_question"),
                    col("incorrect_count").alias("number_of_incorrect_responses"),
                    col("retakes").alias("number_of_retakes"),
                    col("data_last_generated_on"))
            # mdo_orgids = mdoReportDF.select("mdoid").distinct().collect()
            # mdo_orgid_list = [row.mdoid for row in mdo_orgids]

            # print(f"📊 Writing MDO reports for {len(mdo_orgid_list)} organizations...")
            dfexportutil.write_csv_per_mdo_id_duckdb(
                mdoReportDF,
                f"{config.localReportDir}/{config.cbaReportPath}/{today}",
                'mdoid',
                f"{config.localReportDir}/temp/cba-report/{today}",
               )

            (warehouseDF.coalesce(1)
             .write
             .mode("overwrite")
             .option("compression", "snappy")
             .parquet(f"{config.warehouseReportDir}/{config.dwAssessmentTable}"))
            total_time = time.time() - start_time
            print(
                f"\n✅ Optimized Course Based Assessment Report generation completed in {total_time:.2f} seconds ({total_time / 60:.1f} minutes)")

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
            StructField("contentTyCourseBasedAssessmentModelpe", StringType(), True),
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
        .appName("Course Based Assessment Report Model - Cached") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "18g") \
        .config("spark.driver.memory", "18g") \
        .config("spark.executor.memoryFraction", "0.7") \
        .config("spark.storage.memoryFraction", "0.2") \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    # Create model instance
    start_time = datetime.now()
    print(f"[START] Course based assessment processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    config_dict = get_environment_config()
    config = create_config(config_dict)
    model = CourseBasedAssessmentModel()
    model.process_data(spark, config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] Course based assessment completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")

if __name__ == "__main__":
    main()

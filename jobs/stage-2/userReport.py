import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, when, sum, bround, broadcast, coalesce, lit,
    current_timestamp, date_format, from_unixtime, concat_ws
)

# Add parent directory to sys.path for importing project-specific modules
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Import reusable utilities from project
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.enrolment.acbp import acbpDFUtil
from dfutil.enrolment import enrolmentDFUtil
from dfutil.content import contentDFUtil

# Initialize the Spark Session with tuning configurations
spark = SparkSession.builder \
    .appName("UserReportGenerator") \
    .config("spark.executor.memory", "12g") \
    .config("spark.driver.memory", "10g") \
    .config("spark.sql.shuffle.partitions", "64") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()


def processUserReport():
    """
    Generates a complete user learning and enrolment report by:
    1. Loading user, enrolment, and content data.
    2. Computing learning durations from events and content.
    3. Enriching user profiles with calculated fields.
    4. Returning a final DataFrame for reporting or export.
    
    Includes error handling and progress logging.
    """

    try:
        print("\n=== Step 1: Load User Master Data ===")
        user_master_df = spark.read.parquet(ParquetFileConstants.USER_COMPUTED_PARQUET_FILE)
        print(f"✅ User Master Count: {user_master_df.count()}")

        print("\n=== Step 2: Load Enrolment Data ===")
        user_enrolment_df = spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE)
        user_enrolment_df.printSchema()
        print(f"✅ Enrolment Count: {user_enrolment_df.count()}")

        print("\n=== Step 3: Load Course Content Duration Data ===")
        content_duration_df = (
            spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE)
            .filter(col("category") == "Course")
            .select(
                col("courseID").alias("content_id"),
                col("courseDuration").cast("double"),
                col("category")
            )
        )
        print(f"✅ Course Duration Rows: {content_duration_df.count()}")

        print("\n=== Step 4: Add User Learning Status from Enrolments ===")
        user_enrolment_df = user_enrolment_df.withColumn(
            "user_consumption_status",
            when(col("dbCompletionStatus").isNull(), "not-enrolled")
            .when(col("dbCompletionStatus") == 0, "not-started")
            .when(col("dbCompletionStatus") == 1, "in-progress")
            .otherwise("completed")
        )

        print("\n=== Step 5: Append Content Learning Duration to User Data ===")
        user_enrolment_master_df = userDFUtil.appendContentDurationCompletionForEachUser(
            spark, user_master_df, user_enrolment_df, content_duration_df
        )
        print(f"✅ User Data with Content Duration: {user_enrolment_master_df.count()}")

        print("\n=== Step 6: Append Event Learning Metrics ===")
        user_complete_data = userDFUtil.appendEventDurationCompletionForEachUser(
            spark, user_enrolment_master_df
        )
        print(f"✅ User Data with Event Duration: {user_complete_data.count()}")

        print("\n=== Step 7: Add Derived Columns for Reporting ===")
        user_complete_data = user_complete_data \
            .withColumn("Tag", concat_ws(", ", col("additionalProperties.tag"))) \
            .withColumn("Total_Learning_Hours",
                        coalesce(col("total_event_learning_hours_with_certificates"), lit(0)) +
                        coalesce(col("total_content_duration"), lit(0))) \
            .withColumn("weekly_claps_day_before_yesterday",
                        when(col("weekly_claps_day_before_yesterday").isNull() |
                             (col("weekly_claps_day_before_yesterday") == ""),
                             lit(0)).otherwise(col("weekly_claps_day_before_yesterday")))

        print("\n=== Step 8: Format and Select Final Columns ===")
        dateTimeFormat = "yyyy-MM-dd HH:mm:ss"
        currentDateTime = current_timestamp()

        user_complete_df = user_complete_data \
            .withColumn("marked_as_not_my_user", when(col("userProfileStatus") == "NOT-MY-USER", lit(True)).otherwise(lit(False))) \
            .withColumn("data_last_generated_on", currentDateTime) \
            .withColumn("is_verified_karmayogi", when(col("userProfileStatus") == "VERIFIED", lit(True)).otherwise(lit(False))) \
            .select(
                col("userID").alias("user_id"),
                col("userOrgID").alias("mdo_id"),
                col("userStatus").alias("status"),
                coalesce(col("total_points"), lit(0)).alias("no_of_karma_points"),
                col("fullName").alias("full_name"),
                col("professionalDetails.designation").alias("designation"),
                col("personalDetails.primaryEmail").alias("email"),
                col("personalDetails.mobile").alias("phone_number"),
                col("professionalDetails.group").alias("groups"),
                col("Tag").alias("tag"),
                col("userProfileStatus").alias("profile_status"),
                date_format(from_unixtime(col("userCreatedTimestamp")), dateTimeFormat).alias("user_registration_date"),
                col("role").alias("roles"),
                col("personalDetails.gender").alias("gender"),
                col("personalDetails.category").alias("category"),
                col("marked_as_not_my_user"),
                col("is_verified_karmayogi"),
                col("userCreatedBy").alias("created_by_id"),
                col("additionalProperties.externalSystem").alias("external_system"),
                col("additionalProperties.externalSystemId").alias("external_system_id"),
                col("weekly_claps_day_before_yesterday"),
                coalesce(col("total_event_learning_hours_with_certificates"), lit(0)).alias("total_event_learning_hours"),
                coalesce(col("total_content_duration"), lit(0)).alias("total_content_learning_hours"),
                coalesce(col("Total_Learning_Hours"), lit(0)).alias("total_learning_hours"),
                col("employmentDetails.employeeCode").alias("employee_id"),
                col("data_last_generated_on")
            )

        print(f"✅ Final Report Row Count: {user_complete_df.count()}")
        user_complete_df.printSchema()

        # Optional: Save the output
        # user_complete_df.write.mode("overwrite").parquet("/your/output/path")

    except Exception as e:
        print("\n❌ ERROR: Exception occurred during report generation")
        print(str(e))


def main():
    """
    Entry point for the report generation script.
    Calls the processing function.
    """
    print(">>> 🔁 Starting User Report Generation <<<")
    processUserReport()
    print(">>> ✅ Completed User Report Generation <<<")


if __name__ == "__main__":
    main()

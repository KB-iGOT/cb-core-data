import findspark

findspark.init()
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, coalesce, lit,
    current_timestamp, date_format, from_unixtime, concat_ws,from_json,explode,trim,length,first
)
import os
import time
from datetime import datetime

# Add parent directory to sys.path for importing project-specific modules
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Import reusable utilities from project
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from util import schemas
from dfutil.dfexport import dfexportutil
from jobs.default_config import create_config
from jobs.config import get_environment_config

# Initialize Spark
spark = SparkSession.builder \
    .appName("UserReportGenerator") \
    .config("spark.executor.memory", "32g") \
    .config("spark.driver.memory", "10g") \
    .config("spark.sql.shuffle.partitions", "64") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

print("✅ Spark Session initialized")


def processUserReport(config):
    """
    User Report Generation with minimal traceable steps
    """

    try:
        start_time = time.time()
        today = datetime.now().strftime("%Y-%m-%d")
        currentDateTime = date_format(current_timestamp(), ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT)

        # Step 1: Load User Master Data
        print("📊 Step 1: Loading User Master Data...")
        user_master_df = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
        print("✅ Step 1 Complete")

        # Step 2: Load Enrolment Data
        print("📚 Step 2: Loading Enrolment Data...")
        user_enrolment_df = spark.read.parquet(ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE)
        print("✅ Step 2 Complete")

        # Step 3: Load Content Duration
        print("📖 Step 3: Loading Content Duration Data...")
        content_duration_df = (
            spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE)
            .filter(col("category") == "Course")
            .select(
                col("courseID").alias("content_id"),
                col("courseDuration").cast("double"),
                col("category")
            )
        )
        print("✅ Step 3 Complete")

        # Step 4: Add User Status Classification
        print("🏷️ Step 4: Classifying User Status...")
        print("✅ Step 4 Complete")

        # Step 5: Join User and Content Data
        print("🔗 Step 5: Joining User and Content Data...")
        user_enrolment_master_df = userDFUtil.appendContentDurationCompletionForEachUser(
            spark, user_master_df, user_enrolment_df, content_duration_df
        )
        print("✅ Step 5 Complete")

        # Step 6: Add Event Metrics
        print("📊 Step 6: Adding Event Metrics...")
        user_complete_data = userDFUtil.appendEventDurationCompletionForEachUser(
            spark, user_enrolment_master_df
        )
        print("✅ Step 6 Complete")

        # Step 7: Create Derived Columns
        print("✨ Step 7: Creating Derived Columns...")
        user_complete_data = user_complete_data \
            .withColumn("Tag", concat_ws(", ", col("additionalProperties.tag"))) \
            .withColumn("Total_Learning_Hours",
                        coalesce(col("total_event_learning_hours_with_certificates"), lit(0)) +
                        coalesce(col("total_content_duration"), lit(0))) \
            .withColumn("weekly_claps_day_before_yesterday",
                        when(col("weekly_claps_day_before_yesterday").isNull() |
                             (col("weekly_claps_day_before_yesterday") == ""),
                             lit(0)).otherwise(col("weekly_claps_day_before_yesterday")))
        print("✅ Step 7 Complete")

        # Step 8: Final Column Selection
        print("🎯 Step 8: Final Column Selection...")

        mdoWiseReportDF = user_complete_data.filter(col("userStatus").cast("int") == 1) \
            .withColumn("Report_Last_Generated_On", currentDateTime) \
            .withColumn("Total_Enrolments",
                        coalesce(col("total_event_enrolments"), lit(0)) +
                        coalesce(col("total_content_enrolments"), lit(0))) \
            .withColumn("Total_Completions",
                        coalesce(col("total_event_completions"), lit(0)) +
                        coalesce(col("total_content_completions"), lit(0))) \
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
            from_unixtime(col("userCreatedTimestamp") / 1000, ParquetFileConstants.DATE_FORMAT).alias(
                "User_Registration_Date"),
            col("role").alias("Roles"),
            col("personalDetails.gender").alias("Gender"),
            col("personalDetails.category").alias("Category"),
            col("additionalProperties.externalSystem").alias("External_System"),
            col("additionalProperties.externalSystemId").alias("External_System_Id"),
            col("employmentDetails.employeeCode").alias("Employee_Id"),
            from_unixtime(col("userOrgCreatedDate") / 1000, ParquetFileConstants.DATE_FORMAT).alias("MDO_Created_On"),
            col("userProfileStatus").alias("Profile_Status"),
            col("weekly_claps_day_before_yesterday"),
            coalesce(col("total_points"), lit(0)).alias("Karma_Points"),
            coalesce(col("total_event_enrolments"), lit(0)).alias("Event_Enrolments"),
            coalesce(col("total_event_completions"), lit(0)).alias("Event_Completions"),
            coalesce(col("total_event_learning_hours_with_certificates"), lit(0)).alias("Event_Learning_Hours"),
            coalesce(col("total_content_enrolments"), lit(0)).alias("Course_Enrolments"),
            coalesce(col("total_content_completions"), lit(0)).alias("Course_Completions"),
            coalesce(col("total_content_duration"), lit(0)).alias("Course_Learning_Hours"),
            coalesce(col("Total_Enrolments"), lit(0)).alias("Total_Enrolments"),
            coalesce(col("Total_Completions"), lit(0)).alias("Total_Completions"),
            coalesce(col("Total_Learning_Hours"), lit(0)).alias("Total_Learning_Hours"),
            col("Report_Last_Generated_On"),
            col("userOrgID").alias("mdoid")
        )

        dfexportutil.write_csv_per_mdo_id(mdoWiseReportDF, f"{config.localReportDir}/{config.userReportPath}/{today}",
                                          'mdoid', csv_filename=config.userReport)

        warehouseDF = user_complete_data \
            .withColumn("marked_as_not_my_user",
                        when(col("userProfileStatus") == "NOT-MY-USER", lit(True)).otherwise(lit(False))) \
            .withColumn("data_last_generated_on", currentDateTime) \
            .withColumn("is_verified_karmayogi",
                        when(col("userProfileStatus") == "VERIFIED", lit(True)).otherwise(lit(False))) \
            .select(
            col("userID").alias("user_id"),
            col("userOrgID").alias("mdo_id"),
            col("userStatus").alias("status"),
            coalesce(col("total_points"), lit(0)).alias("no_of_karma_points"),
            col("fullName").alias("full_name"),
            col("professionalDetails.designation").alias("designation"),
            col("personalDetails.primaryEmail").alias("email"),
            col("personalDetails.mobile").alias("phone_number"),
            col("personalDetails.pincode").alias("pincode"),
            col("professionalDetails.group").alias("groups"),
            col("Tag").alias("tag"),
            col("userProfileStatus").alias("profile_status"),
            date_format(from_unixtime(col("userCreatedTimestamp") / 1000), ParquetFileConstants.DATE_TIME_FORMAT).alias(
                "user_registration_date"),
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
            col("cadreName").alias("cadre"),
            col("civilServiceType").alias("civil_service_type"),
            col("civilServiceName").alias("civil_services"),
            col("cadreBatch").alias("cadre_batch"),
            col("organised_service").alias("is_from_organised_service_of_govt"),
            col("data_last_generated_on")
        )
        print("✅ Step 8 Complete")

        # Step 9: Export Data
        print("📁 Step 9: Exporting Warehouse Data...")
        warehouseDF.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(
            f"{config.warehouseReportDir}/{config.dwUserTable}")
        print("✅ Step 9 Complete")


        # Step 10: Process User Extended Profile Data
        print("🔍 Step 10: Processing User Extended Profile Data...")
        # Load user extended profile data
        user_extended_profile_df = (
            spark.read.parquet(ParquetFileConstants.USER_EXTENDED_PROFILE_FILE)  # You'll need to define this constant
            .filter(col("contexttype") == "orgAdditionalProperties")
            .withColumnRenamed("userid", "userID")
            .withColumn("contextData", from_json(col("contextdata"), schemas.context_data_schema))
            .select(
                col("userID"),
                col("contexttype").alias("contextType"),
                col("contextData"),
                col("contextData.organisationId").alias("mdo_id")
            )
        )
        
        # Step 1: Explode customFieldValues to get individual attribute-value pairs
        exploded_df = (
            user_extended_profile_df
            .withColumn("customField", explode(col("contextData.customFieldValues")))
            .select(
                col("userID"),
                col("mdo_id"),
                col("customField.attributeName").alias("attribute_name"),
                col("customField.value").alias("attribute_value")
            )
            .filter(
                col("attribute_name").isNotNull() & 
                col("attribute_value").isNotNull()
            )
        )
        
        # Write to warehouse tables
        exploded_df.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(
            f"{config.warehouseReportDir}/userCustomFields"
        )
        
        # Cache the exploded data for reuse
        exploded_cached = exploded_df.cache()
        print("✅ Step 10 Complete")
        
        # Step 11: Create MDO-wise Slim Data for Custom Reports
        print("📋 Step 11: Creating MDO-wise Custom Reports...")
        
        mdo_wise_slim = (
            user_complete_data
            .filter(col("userStatus").cast("int") == 1)
            .select(
                col("userID"),
                col("fullName").alias("Full_Name"),
                col("professionalDetails.designation").alias("Designation"),
                col("personalDetails.primaryEmail").alias("Email"),
                col("personalDetails.mobile").alias("Phone_Number"),
                col("userOrgName").alias("MDO_Name"),
                col("professionalDetails.group").alias("Group"),
                col("Tag"),
                when(col("ministry_name").isNull(), col("userOrgName"))
                    .otherwise(col("ministry_name")).alias("Ministry"),
                when(
                    (col("ministry_name").isNotNull()) &
                    (col("ministry_name") != col("userOrgName")) &
                    ((col("dept_name").isNull()) | (col("dept_name") == "")),
                    col("userOrgName")
                ).otherwise(col("dept_name")).alias("Department"),
                when(
                    (col("ministry_name") != col("userOrgName")) &
                    (col("dept_name") != col("userOrgName")),
                    col("userOrgName")
                ).otherwise(lit("")).alias("Organization"),
                from_unixtime(col("userCreatedTimestamp") / 1000, ParquetFileConstants.DATE_FORMAT).alias("User_Registration_Date"),
                col("role").alias("Roles"),
                col("personalDetails.gender").alias("Gender"),
                col("personalDetails.category").alias("Category"),
                col("additionalProperties.externalSystem").alias("External_System"),
                col("additionalProperties.externalSystemId").alias("External_System_Id"),
                col("employmentDetails.employeeCode").alias("Employee_Id"),
                from_unixtime(col("userOrgCreatedDate") / 1000, ParquetFileConstants.DATE_FORMAT).alias("MDO_Created_On"),
                col("userProfileStatus").alias("Profile_Status"),
                col("weekly_claps_day_before_yesterday"),
                coalesce(col("total_points"), lit(0)).alias("Karma_Points"),
                coalesce(col("total_event_enrolments"), lit(0)).alias("Event_Enrolments"),
                coalesce(col("total_event_completions"), lit(0)).alias("Event_Completions"),
                coalesce(col("total_event_learning_hours_with_certificates"), lit(0)).alias("Event_Learning_Hours"),
                coalesce(col("total_content_enrolments"), lit(0)).alias("Course_Enrolments"),
                coalesce(col("total_content_completions"), lit(0)).alias("Course_Completions"),
                coalesce(col("total_content_duration"), lit(0)).alias("Course_Learning_Hours"),
                (coalesce(col("total_event_enrolments"), lit(0)) + 
                 coalesce(col("total_content_enrolments"), lit(0))).alias("Total_Enrolments"),
                (coalesce(col("total_event_completions"), lit(0)) + 
                 coalesce(col("total_content_completions"), lit(0))).alias("Total_Completions"),
                coalesce(col("Total_Learning_Hours"), lit(0)).alias("Total_Learning_Hours"),
                lit(currentDateTime).alias("Report_Last_Generated_On"),
                col("userOrgID").alias("mdoid")
            )
        )
        
        base_out = f"standalone-reports/user-custom-report/{today}"
        
        # Get list of organization IDs
        org_ids = [row.mdo_id for row in exploded_cached.select("mdo_id").distinct().collect()]
        org_ids.sort()
        
        print(f"Processing {len(org_ids)} organizations for custom reports...")
        
        for org_id in org_ids:
            print(f"  Processing organization: {org_id}")
            
            # Filter data for current organization
            org_data = exploded_cached.filter(col("mdo_id") == org_id)
            
            # Get attribute names for this organization, sanitized
            attribute_rows = (
                org_data
                .select(trim(col("attribute_name")).alias("n"))
                .filter((col("n").isNotNull()) & (length(col("n")) > 0))
                .distinct()
                .collect()
            )
            attribute_names = sorted([row.n for row in attribute_rows])
            
            # Pivot only on these attributes → columns limited to this org's customs
            if attribute_names:
                # Create pivot with specific values
                pivoted = (
                    org_data
                    .groupBy("userID")
                    .pivot("attribute_name")
                    .agg(first("attribute_value"))
                )
            else:
                # No customs → keep ids only
                pivoted = org_data.select("userID").distinct()
            
            # Join with MDO-wise slim data
            joined = (
                pivoted
                .join(mdo_wise_slim, ["userID"], "left")
                .withColumn("mdoid", lit(org_id))
            )
            
            # Define fixed columns order
            fixed_cols = [
                "userID", "Full_Name", "Designation", "Email", "Phone_Number", "MDO_Name", "Group", "Tag",
                "Ministry", "Department", "Organization", "User_Registration_Date", "Roles", "Gender",
                "Category", "External_System", "External_System_Id", "Employee_Id", "MDO_Created_On",
                "Profile_Status", "weekly_claps_day_before_yesterday", "Karma_Points", "Event_Enrolments",
                "Event_Completions", "Event_Learning_Hours", "Course_Enrolments", "Course_Completions",
                "Course_Learning_Hours", "Total_Enrolments", "Total_Completions", "Total_Learning_Hours",
                "Report_Last_Generated_On", "mdoid"
            ]
            
            # Combine fixed and dynamic columns
            all_columns = fixed_cols + attribute_names
            
            # Select columns that exist in the dataframe
            existing_columns = joined.columns
            final_columns = [c for c in all_columns if c in existing_columns]
            
            ordered = joined.select(*[col(c) for c in final_columns])
            
            # Write one file per org using existing CSV writing function
            out_path = f"{config.localReportDir}/{base_out}/mdoid={org_id}"
            csv_file_path = f"{out_path}/UserCustomReport.csv"
            
            # Create directory if it doesn't exist
            os.makedirs(out_path, exist_ok=True)
            
            # Use existing function to write single CSV per organization
            dfexportutil.write_single_csv_duckdb(
                df=ordered.coalesce(1),
                output_path=csv_file_path,
                parquet_tmp_path=f"{out_path}/temp_parquet_{org_id}",
                keep_parquets=False
            )
        
        exploded_cached.unpersist()
        
        print("✅ Step 11 Complete")
        print(f"Custom reports generated for {len(org_ids)} organizations")

        # Performance Summary
        total_duration = time.time() - start_time
        print(f"\n📊 Processing Summary:")
        print(f"⏱️ Total duration: {total_duration:.2f} seconds ({total_duration / 60:.1f} minutes)")
        print(f"🎯 Status: Success")

    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        raise


def main():
    config_dict = get_environment_config()
    config = create_config(config_dict)
    processUserReport(config)
    print("🏆 User Report Generation completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
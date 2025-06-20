import sys
import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, max as spark_max, 
    expr, current_timestamp, broadcast
)

# Add parent directory to sys.path for importing project-specific modules
sys.path.append(str(Path(__file__).resolve().parents[2]))

from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.dfexport import dfexportutil
from dfutil.assessment import assessmentdfUtil
from dfutil.content import contentDFUtil

# Initialize Spark Session
print("Initializing Spark Session...")
spark = SparkSession.builder \
    .appName("AssessmentReportGenerator") \
    .config("spark.executor.memory", "12g") \
    .config("spark.driver.memory", "10g") \
    .config("spark.sql.shuffle.partitions", "64") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

print("Spark Session initialized successfully")

def process_assessment_report():
    """
    Assessment Report Generation with minimal logging for performance
    """
    total_start_time = time.time()
    
    try:
        # Stage 1: Load Assessment Data
        print("Stage 1: Loading assessment data...")
        assessmentDF = assessmentdfUtil.assessment_es_dataframe(spark)
        hierarchyDF = spark.read.parquet(ParquetFileConstants.HIERARCHY_PARQUET_FILE)
        organizationDF = spark.read.parquet(ParquetFileConstants.ORG_COMPUTED_PARQUET_FILE)
        print("Stage 1: Complete")

        # Stage 2: Add Hierarchy Information
        print("Stage 2: Adding hierarchy information...")
        assWithHierarchyData = assessmentdfUtil.add_hierarchy_column(
            assessmentDF,
            hierarchyDF,
            id_col="assessID",
            as_col="data",
            spark=spark,
            children=True,
            competencies=True,
            l2_children=True
        )
        print("Stage 2: Complete")

        # Stage 3: Transform Assessment Data
        print("Stage 3: Transforming assessment data...")
        assessWithHierarchyDF = assessmentdfUtil.transform_assessment_data(assWithHierarchyData, organizationDF)
        assessWithDetailsDF = assessWithHierarchyDF.drop("children")
        print("Stage 3: Complete")

        # Stage 4: Process Assessment Children
        print("Stage 4: Processing assessment children...")
        assessChildrenDF = assessmentdfUtil.assessment_children_dataframe(assessWithHierarchyDF)
        print("Stage 4: Complete")

        # Stage 5: Process User Assessment Data
        print("Stage 5: Processing user assessment data...")
        userAssessmentDF = spark.read.parquet(ParquetFileConstants.USER_ASSESSMENT_PARQUET_FILE) 
        userAssessChildrenDF = assessmentdfUtil.user_assessment_children_dataframe(userAssessmentDF, assessChildrenDF)
        print("User Assessment Children DataFrame Schema:")
        categories = ["Course","Program","Blended Program","Curated Program","Moderated Course","Standalone Assessment","CuratedCollections"]
        allCourseProgramDetailsWithCompDF = assessmentdfUtil.all_course_program_details_with_competencies_json_dataframe(contentDFUtil.AllCourseProgramESDataFrame(spark,categories), hierarchyDF, organizationDF, spark)
        print("All Course Program Details with Competencies JSON DataFrame Schema:")
        allCourseProgramDetailsDF = allCourseProgramDetailsWithCompDF.drop("competenciesJson")
        print("Stage 6: Complete")

        # Stage 7: Add Rating Information
        print("Stage 7: Adding rating information...")
        allCourseProgramDetailsWithRatingDF = assessmentdfUtil.all_course_program_details_with_rating_df(
            allCourseProgramDetailsDF,
            spark.read.parquet(ParquetFileConstants.RATING_SUMMARY_COMPUTED_PARQUET_FILE)
        )
        print("Stage 7: Complete")

        # Stage 8: Generate User Assessment Details
        print("Stage 8: Generating user assessment details...")
        userAssessChildrenDetailsDF = assessmentdfUtil.user_assessment_children_details_dataframe(
            userAssessChildrenDF, 
            assessWithDetailsDF,
            allCourseProgramDetailsWithRatingDF, 
            spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
        )
        print("Stage 8: Complete")

        # Stage 9: Process Final Report Data
        print("Stage 9: Processing final report data...")
        
        # Get latest attempt per user per child assessment
        latest = userAssessChildrenDetailsDF.groupBy("assessChildID", "userID").agg(
            spark_max("assessEndTimestamp").alias("assessEndTimestamp"),
            expr("COUNT(*)").alias("noOfAttempts")
        )
        
        # Define status expressions
        case_expr = """
            CASE 
                WHEN assessPass = 1 AND assessUserStatus = 'SUBMITTED' THEN 'Pass' 
                WHEN assessPass = 0 AND assessUserStatus = 'SUBMITTED' THEN 'Fail' 
                ELSE 'N/A' 
            END
        """
        completion_status_expr = """
            CASE 
                WHEN assessUserStatus = 'SUBMITTED' THEN 'Completed' 
                ELSE 'In progress' 
            END
        """
        
        # Join and transform data
        original_df = userAssessChildrenDetailsDF.join(
            broadcast(latest),
            on=["assessChildID", "userID", "assessEndTimestamp"],
            how="inner"
        ).withColumn("Assessment_Status", expr(case_expr)) \
        .withColumn("Overall_Status", expr(completion_status_expr)) \
        .withColumn("Report_Last_Generated_On", current_timestamp()) \
        .dropDuplicates(["userID", "assessID"]) \
        .select(
            col("userID").alias("User_ID"),
            col("fullName").alias("Full_Name"),
            col("assessName").alias("Assessment_Name"),
            col("Overall_Status"),
            col("Assessment_Status"),
            col("assessPassPercentage").alias("Percentage_Of_Score"),
            col("noOfAttempts").alias("Number_of_Attempts"),
            col("maskedEmail").alias("Email"),
            col("userStatus").alias("status"),
            col("maskedPhone").alias("Phone"),
            col("assessOrgID").alias("mdoid"),
            col("Report_Last_Generated_On")
        ).coalesce(1)
        
        print("Stage 9: Complete")

        # Stage 10: Generate Final Report
        print("Stage 10: Generating final report...")
        
        # Filter active users and prepare final dataset
        columns_to_keep = [c for c in original_df.columns if c != "status"]
        final_df = original_df.filter(col("status").cast("int") == 1).select([col(c) for c in columns_to_keep])

        # Export report
        dfexportutil.write_csv_per_mdo_id(final_df, f"{'reports'}/assessment", 'mdoid')
        print("Stage 10: Complete")

        # Performance Summary
        total_duration = time.time() - total_start_time
        print(f"Total processing time: {total_duration:.2f} seconds ({total_duration/60:.1f} minutes)")

    except Exception as e:
        print(f"Error occurred during processing: {str(e)}")
        raise

def main():
    """
    Main function to execute assessment report generation
    """
    print("Starting Assessment Report Generation...")
    start_time = time.time()
    
    process_assessment_report()
    
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"Assessment report generation completed in {total_time:.2f} seconds")

if __name__ == "__main__":
    main()
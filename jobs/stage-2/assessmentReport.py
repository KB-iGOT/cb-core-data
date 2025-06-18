import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr,max as spark_max, 
    expr, current_timestamp, broadcast
)
# Add parent directory to sys.path for importing project-specific modules
sys.path.append(str(Path(__file__).resolve().parents[2]))

from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.dfexport import dfexportutil
from dfutil.assessment import assessmentdfUtil


# Initialize the Spark Session with tuning configurations
spark = SparkSession.builder \
    .appName("UserReportGenerator") \
    .config("spark.executor.memory", "12g") \
    .config("spark.driver.memory", "10g") \
    .config("spark.sql.shuffle.partitions", "64") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

def processAssessmentReport():
    """
    Generates a complete Assessment report by:
    Includes error handling and progress logging.
    """
    try:
        print("\n=== Step 1: Load assessment data frame from ES ===")
        assessmentDF = assessmentdfUtil.assessment_es_dataframe(spark)
        hierarchyDF = spark.read.parquet(ParquetFileConstants.HIERARCHY_PARQUET_FILE)
        organizationDF = spark.read.parquet(ParquetFileConstants.ORG_COMPUTED_PARQUET_FILE)
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
        assessWithHierarchyDF = assessmentdfUtil.transform_assessment_data(assWithHierarchyData, organizationDF)
        assessWithDetailsDF = assessWithHierarchyDF.drop("children")


        assessChildrenDF = assessmentdfUtil.assessment_children_dataframe(assessWithHierarchyDF)
        userAssessmentDF = spark.read.parquet(ParquetFileConstants.USER_ASSESSMENT_PARQUET_FILE) 
        userAssessChildrenDF = assessmentdfUtil.user_assessment_children_dataframe(userAssessmentDF, assessChildrenDF)
        print("User Assessment Children DataFrame Schema:")
        allCourseProgramDetailsWithCompDF = assessmentdfUtil.all_course_program_details_with_competencies_json_dataframe(spark.read.parquet(ParquetFileConstants.ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE), hierarchyDF, organizationDF, spark)
        print("All Course Program Details with Competencies JSON DataFrame Schema:")
        allCourseProgramDetailsDF = allCourseProgramDetailsWithCompDF.drop("competenciesJson")
        print("All Course Program Details DataFrame Schema:")
        print("Course Rating DataFrame Schema:")
        allCourseProgramDetailsWithRatingDF = assessmentdfUtil.all_course_program_details_with_rating_df(
            allCourseProgramDetailsDF,
            spark.read.parquet(ParquetFileConstants.RATING_SUMMARY_COMPUTED_PARQUET_FILE))
        print("All Course Program Details with Rating DataFrame Schema:")
        userOrgDF = userDFUtil.user_org_dataframe(spark.read.parquet(ParquetFileConstants.ORG_COMPUTED_PARQUET_FILE),spark.read.parquet(ParquetFileConstants.USER_COMPUTED_PARQUET_FILE))
        print("User Organization DataFrame Schema:")
        userAssessChildrenDetailsDF = assessmentdfUtil.user_assessment_children_details_dataframe(userAssessChildrenDF, assessWithDetailsDF,
        allCourseProgramDetailsWithRatingDF, userOrgDF)
        print("User Assessment Children Details DataFrame Schema:")
        # Step 1: Group to get latest attempt per user per child assessment
        latest = userAssessChildrenDetailsDF.groupBy("assessChildID", "userID").agg(
            spark_max("assessEndTimestamp").alias("assessEndTimestamp"),
            expr("COUNT(*)").alias("noOfAttempts")
        )

        # Step 2: CASE expressions for status columns
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

        # Step 3: Join with original DF and apply transformations
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

        # Step 4: Filter out inactive users and generate report
        columns_to_keep = [c for c in original_df.columns if c != "status"]
        final_df = original_df.filter(col("status").cast("int") == 1).select([col(c) for c in columns_to_keep])

        print(f"✅ Final Report Row Count: {final_df.count()}")
        #final_df.printSchema()
        dfexportutil.write_csv_per_mdo_id(final_df,f"{'reports'}/assessment", 'mdoid')

    except Exception as e:
        print(f"Error occurred: {e}")

def main():
    """
    Entry point for the report generation script.
    Calls the processing function.
    """
    print(">>> 🔁 Starting Assessment Report Generation <<<")
    processAssessmentReport()
    print(">>> ✅ Completed Assessment Report Generation <<<")


if __name__ == "__main__":
    main()
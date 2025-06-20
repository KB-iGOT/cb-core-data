import sys
from pathlib import Path
from pyspark.sql import SparkSession

# Add root directory to sys.path for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Custom module imports
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.enrolment.acbp import acbpDFUtil
from dfutil.enrolment import enrolmentDFUtil
from dfutil.content import contentDFUtil

def initialize_spark():
    """
    Initializes and returns a SparkSession with configured settings.
    """
    return SparkSession.builder \
        .appName("MySparkApp") \
        .config("spark.executor.memory", "12g") \
        .config("spark.driver.memory", "10g") \
        .config("spark.sql.shuffle.partitions", "64") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

def run_stage(name: str, func, spark):
    """
    Runs a stage with logging and exception handling.

    Parameters:
    - name: Name of the stage
    - func: Callable function for the stage
    - spark: SparkSession object
    """
    print(f"""
    ##########################################################
    ###             Starting Stage: {name}
    ##########################################################
    """)
    try:
        result = func(spark)
        if hasattr(result, "count"):
            print(f"[✓] Stage '{name}' completed. Record Count: {result.count()}")
        else:
            print(f"[✓] Stage '{name}' completed.")
    except Exception as e:
        print(f"[✗] Stage '{name}' failed with error: {e}")

def main():
    """
    Main function to run all ETL pre-computation stages.
    """
    spark = initialize_spark()

    # Run each ETL stage with logging
    run_stage("Org Hierarchy Computation", userDFUtil.preComputeOrgWithHierarchy, spark)
    run_stage("Content Ratings & Summary", contentDFUtil.preComputeRatingAndSummaryDataFrame, spark)
    run_stage("Content Master Data", contentDFUtil.preComputeContentDataFrame, spark)
    run_stage("External Content", contentDFUtil.preComputeExternalContentDataFrame, spark)
    run_stage("User Profile Computation", userDFUtil.preComputeUser, spark)
    run_stage("Enrolment Master Data", enrolmentDFUtil.preComputeEnrolment, spark)
    run_stage("External Enrolment", enrolmentDFUtil.preComputeExternalEnrolment, spark)
    run_stage("Org-User Mapping with Hierarchy", userDFUtil.preComputeOrgHierarchyWithUser, spark)
    run_stage("ACBP Enrolment Computation", acbpDFUtil.preComputeACBPData, spark)

    print("\n🚀 All stages completed successfully.")

if __name__ == "__main__":
    main()

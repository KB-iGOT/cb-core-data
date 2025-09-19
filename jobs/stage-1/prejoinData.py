import findspark
findspark.init()
import sys
from pathlib import Path
from pyspark.sql import SparkSession
import time
from datetime import datetime

# Add root directory to sys.path for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Custom module imports
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.enrolment.acbp import acbpDFUtil
from dfutil.enrolment import enrolmentDFUtil
from dfutil.content import contentDFUtil
from dfutil.assessment import assessmentDFUtil

def initialize_spark():
    """
    Initializes and returns a SparkSession with optimized configuration
    """
    print("Initializing Spark Session...")
    
    spark = SparkSession.builder \
        .appName("DataProcessing_Pipeline") \
        .config("spark.executor.memory", "42g") \
        .config("spark.driver.memory", "18g") \
        .config("spark.sql.shuffle.partitions", "64") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    
    print("Spark Session initialized successfully")
    return spark

def run_stage(name: str, func, spark):
    """
    Executes a processing stage with minimal logging for performance
    
    Parameters:
    - name: Name of the processing stage
    - func: The processing function to execute
    - spark: The SparkSession instance
    """
    
    print(f"Stage: {name} - Starting...")
    start_time = time.time()
    
    try:
        result = func(spark)
        duration = time.time() - start_time
        
        print(f"Stage: {name} - Complete ({duration:.2f}s)")
        return result
            
    except Exception as e:
        duration = time.time() - start_time
        print(f"Stage: {name} - Failed after {duration:.2f}s - Error: {str(e)}")
        raise e

def main():
    """
    Main data processing pipeline execution
    """
    
    print("Starting Data Processing Pipeline...")
    
    # Initialize Spark session
    spark = initialize_spark()
    
    # Track overall progress
    total_start_time = time.time()
    completed_stages = 0
    
    # Define processing stages
    processing_stages = [
        ("Org Hierarchy Computation", userDFUtil.preComputeOrgWithHierarchy),
        ("Content Ratings & Summary", contentDFUtil.preComputeRatingAndSummaryDataFrame),
        ("All Course/Program (ES)", contentDFUtil.preComputeAllCourseProgramESDataFrame),
        ("Content Master Data", contentDFUtil.preComputeContentDataFrame),
        ("Content Hierarchy Computation", contentDFUtil.precomputeContentHierarchyDataFrame),
        ("Assessment Master Data", assessmentDFUtil.precomputeAssessmentEsDataframe),
        ("External Content", contentDFUtil.preComputeExternalContentDataFrame),
        ("User Profile Computation", userDFUtil.preComputeUser),
        ("Enrolment Master Data", enrolmentDFUtil.preComputeEnrolment),
        ("External Enrolment", enrolmentDFUtil.preComputeExternalEnrolment),
        ("Org-User Mapping with Hierarchy", userDFUtil.preComputeOrgHierarchyWithUser),
        ("Enrolment Warehouse", enrolmentDFUtil.preComputeUserEnrolmentWarehouseData),
        ("User Warehouse", userDFUtil.preComputeUserWarehouseData),
        ("Course Warehouse", contentDFUtil.preComputeContentWarehouseData),
        ("ACBP Enrolment Computation", acbpDFUtil.preComputeACBPData),
        ("Old Assessment Data", assessmentDFUtil.precomputeOldAssessmentDataframe),

    ]
    total_stages = len(processing_stages)
    
    try:
        for stage_name, stage_function in processing_stages:
            run_stage(stage_name, stage_function, spark)
            completed_stages += 1
            
            # Progress update
            progress = (completed_stages / total_stages) * 100
            print(f"Progress: {completed_stages}/{total_stages} stages completed ({progress:.0f}%)")
        
        # Pipeline completion summary
        total_duration = time.time() - total_start_time
        
        print(f"""
Pipeline Execution Summary:
✅ Stages Completed: {completed_stages}/{total_stages}
⏱️  Total Duration: {total_duration/60:.1f} minutes
🎯 Success Rate: 100%
📊 Status: All stages completed successfully
        """)
        
    except Exception as e:
        total_duration = time.time() - total_start_time
        print(f"""
Pipeline Execution Failed:
❌ Failed at stage: {completed_stages + 1}/{total_stages}
⏱️  Duration before failure: {total_duration/60:.1f} minutes
🚨 Error: {str(e)}
        """)
        raise
    
    finally:
        print("Data Processing Pipeline finished")
        spark.stop()

if __name__ == "__main__":
    main()
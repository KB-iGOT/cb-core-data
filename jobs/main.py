import sys
from pathlib import Path
from pyspark.sql import SparkSession
import importlib
import time
from datetime import datetime

sys.path.append(str(Path(__file__).resolve().parents[1]))
stage_1 = importlib.import_module('jobs.stage-1.prejoinData')
stage_2a = importlib.import_module('jobs.stage-2.userReport') 
stage_2b = importlib.import_module('jobs.stage-2.assessmentReport')

def execute_all_stages():
    """
    Executes all pipeline stages with minimal logging for optimal performance
    """
    
    print("Starting Multi-Stage Data Pipeline...")
    print(f"Execution started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    total_start_time = time.time()
    
    try:
        # Stage 1: Data Preparation
        print("\nStage 1: Data Preparation - Starting...")
        stage1_start = time.time()
        
        stage_1.main()
        
        stage1_duration = time.time() - stage1_start
        print(f"Stage 1: Data Preparation - Complete ({stage1_duration:.2f}s)")
        
        # Stage 2A: User Analytics
        print("\nStage 2A: User Analytics - Starting...")
        stage2a_start = time.time()
        
        stage_2a.main()
        
        stage2a_duration = time.time() - stage2a_start
        print(f"Stage 2A: User Analytics - Complete ({stage2a_duration:.2f}s)")
        
        # Stage 2B: Assessment Analytics
        print("\nStage 2B: Assessment Analytics - Starting...")
        stage2b_start = time.time()
        
        stage_2b.main()
        
        stage2b_duration = time.time() - stage2b_start
        print(f"Stage 2B: Assessment Analytics - Complete ({stage2b_duration:.2f}s)")
        
        # Pipeline Summary
        total_duration = time.time() - total_start_time
        
        print(f"\nPipeline Execution Summary:")
        print(f"Stage 1 Duration: {stage1_duration:.2f}s ({stage1_duration/60:.1f} min)")
        print(f"Stage 2A Duration: {stage2a_duration:.2f}s ({stage2a_duration/60:.1f} min)")
        print(f"Stage 2B Duration: {stage2b_duration:.2f}s ({stage2b_duration/60:.1f} min)")
        print(f"Total Duration: {total_duration:.2f}s ({total_duration/60:.1f} min)")
        print(f"Execution completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("Status: All stages completed successfully")
        
        return {
            "status": "SUCCESS",
            "total_duration": total_duration,
            "stage_durations": {
                "data_preparation": stage1_duration,
                "user_analytics": stage2a_duration,
                "assessment_analytics": stage2b_duration
            },
            "completion_time": datetime.now().isoformat()
        }
        
    except Exception as e:
        total_duration = time.time() - total_start_time
        print(f"\nPipeline execution failed after {total_duration:.2f}s")
        print(f"Error: {str(e)}")
        print(f"Failed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        raise e

if __name__ == "__main__":
    
    print(f"Pipeline execution time: {datetime.now().strftime('%A, %B %d, %Y - %H:%M:%S')}")
    
    # Execute the pipeline
    result = execute_all_stages()
    
    print(f"\nPipeline execution completed successfully")
    print(f"Total processing time: {result['total_duration']:.2f} seconds")
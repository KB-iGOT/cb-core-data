import sys
from pathlib import Path
import os
import time

# Ensure the parent directory is in sys.path for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckutil import duckutil  # Assuming duckutil is in the parent directory
from constants.ParquetFileConstants import ParquetFileConstants
from constants.QueryConstants import QueryConstants

# ==============================
# 1. Configuration and Constants
# ==============================

def main():
    duck_conn = duckutil.initialize_duckdb("12GB")
    print("[INFO] DuckDB connection initialized.")

    prefetchUserOrgData(duck_conn)
    prejoinUserOrgRoleData(duck_conn)
    prejoinEnrolmentCourseData(duck_conn)
    prejoinUserOrgEnrolmentData(duck_conn)
    print("[INFO] DuckDB connection closed.")

def prefetchUserOrgData(duckdb_conn):
   prefetchDataAndOutputToComputeFile(duckdb_conn,QueryConstants.PRE_FETCH_USER_ORG_DATA,
        ParquetFileConstants.USER_ORG_COMPUTED_PARQUET_FILE,"User & Org")

def prejoinUserOrgRoleData(duckdb_conn):
   prefetchDataAndOutputToComputeFile(duckdb_conn,QueryConstants.PRE_FETCH_USER_ORG_ROLE_DATA,
        ParquetFileConstants.USER_ORG_ROLE_COMPUTED_PARQUET_FILE,"User, Org & Role")

def prejoinEnrolmentCourseData(duckdb_conn):
    prefetchDataAndOutputToComputeFile(duckdb_conn,QueryConstants.PREFETCH_ENROLMENT_WITH_COURSE_DATA,
            ParquetFileConstants.COURSE_PROGRAM_ENROLMENT_COMPUTED_FILE,"Enrolment & Course Data")

def prejoinUserOrgEnrolmentData(duckdb_conn):
    prefetchDataAndOutputToComputeFile(duckdb_conn,QueryConstants.PREFETCH_ENROLMENT_WITH_COURSE_DATA_USER_ORG_ROLE_DATA,
            ParquetFileConstants.USER_ORG_COURSE_PROGRAM_ENROLMENT_COMPUTED_FILE,"User Org Enrolment & Course Data")

def prefetchDataAndOutputToComputeFile(duckdb_conn,query,output,category):
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    print("\n[INFO] Prefetching {category} Data...")
    print(output)
    start_time = time.time()

    
    duckutil.executeQuery(duckdb_conn, f"""
    COPY ({query}) TO '{output}' 
    (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    print(f"[SUCCESS] User data fetched and saved to {output}.")
    fetchPrintDFCount(duckdb_conn,output,category)
    
    print(f"[INFO] User Data Prefetch Completed in {round(time.time() - start_time, 2)} seconds.")

def fetchPrintDFCount(duckdb_conn,tableName,type):
    queryToExecute = f"SELECT COUNT(*) AS count FROM read_parquet('{tableName}')"
    userDFCount = duckutil.executeQuery(duckdb_conn, queryToExecute).df()
    print(f"[INFO] {type} Record Count: {userDFCount.iloc[0, 0]}")


if __name__ == "__main__":
    main()

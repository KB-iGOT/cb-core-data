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

    prefetchUserData(duck_conn)
    prejoinUserOrgData(duck_conn)
    prejoinuserorgroledata(duck_conn)
    prejoinACBPData(duck_conn)
    prejoinusercourseprogramenrollment(duck_conn)

    print("[INFO] DuckDB connection closed.")

def prefetchUserData(duckdb_conn):
    print("\n[INFO] Prefetching User Data...")
    start_time = time.time()

    query = QueryConstants.PRE_FETCH_USER_DATA
    duckutil.executeQuery(duckdb_conn, f"""
    COPY ({query}) TO '{ParquetFileConstants.USER_COMPUTED_PARQUET_FILE}' 
    (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    print(f"[SUCCESS] User data fetched and saved to {ParquetFileConstants.USER_COMPUTED_PARQUET_FILE}.")
    fetchPrintDFCount(duckdb_conn,ParquetFileConstants.USER_COMPUTED_PARQUET_FILE,"User")
    
    print(f"[INFO] User Data Prefetch Completed in {round(time.time() - start_time, 2)} seconds.")

def prejoinACBPData(duckdb_conn):
    print("\n[INFO] Prefetching ACBP Data...")
    start_time = time.time()

    query = QueryConstants.PRE_FETCH_ACBP_DETAILS
    duckutil.executeQuery(duckdb_conn, f"""
    COPY ({query}) TO '{ParquetFileConstants.ACBP_COMPUTED_PARQUET_FILE}' 
    (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    
    print(f"[SUCCESS] ACBP data fetched and saved to {ParquetFileConstants.ACBP_COMPUTED_PARQUET_FILE}.")
    
    fetchPrintDFCount(duckdb_conn,ParquetFileConstants.ACBP_COMPUTED_PARQUET_FILE,"ACBP")

    print(f"[INFO] ACBP Data Prefetch Completed in {round(time.time() - start_time, 2)} seconds.")

def prejoinUserOrgData(duckdb_conn):
    print("\n[INFO] Prefetching User-Org Data...")
    start_time = time.time()

    query = QueryConstants.PRE_FETCH_ALL_USER_ORG_DATA
    duckutil.executeQuery(duckdb_conn, f"""
    COPY ({query}) TO '{ParquetFileConstants.USER_ORG_COMPUTED_PARQUET_FILE}' 
    (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    print(f"[SUCCESS] User-Org data fetched and saved to {ParquetFileConstants.USER_ORG_COMPUTED_PARQUET_FILE}.")
    
    fetchPrintDFCount(duckdb_conn,ParquetFileConstants.USER_ORG_COMPUTED_PARQUET_FILE,"User and Org")
  

    print(f"[INFO] User-Org Data Prefetch Completed in {round(time.time() - start_time, 2)} seconds.")

def prejoinuserorgroledata(duckdb_conn):
    print("\n[INFO] Prefetching User-Org-Role Data...")
    start_time = time.time()

    query = QueryConstants.PRE_FETCH_USER_ORG_ROLE_DATA
    duckutil.executeQuery(duckdb_conn, f"""
    COPY ({query}) TO '{ParquetFileConstants.USER_ORG_ROLE_COMPUTED_PARQUET_FILE}' 
    (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    
    print(f"[SUCCESS] User-Org-Role data fetched and saved to {ParquetFileConstants.USER_ORG_ROLE_COMPUTED_PARQUET_FILE}.")
    
    fetchPrintDFCount(duckdb_conn,ParquetFileConstants.USER_ORG_ROLE_COMPUTED_PARQUET_FILE,"User, Org & Role")

    print(f"[INFO] User-Org-Role Data Prefetch Completed in {round(time.time() - start_time, 2)} seconds.")

def prejoinusercourseprogramenrollment(duckdb_conn):
    print("\n[INFO] Prefetching User-Org-Role Data...")
    start_time = time.time()
    query = QueryConstants.PRE_FETCH_ENROLLMENT_DATA
    duckutil.executeQuery(duckdb_conn, f"""
        COPY ({query}) TO '{ParquetFileConstants.USER_COURSE_PROGRAM_ENROLMENT_FILE}' 
        (FORMAT PARQUET, COMPRESSION ZSTD);
        """)
    print(f"[SUCCESS] User-Org-Role data fetched and saved to {ParquetFileConstants.USER_COURSE_PROGRAM_ENROLMENT_FILE}.")
    
    fetchPrintDFCount(duckdb_conn,ParquetFileConstants.USER_COURSE_PROGRAM_ENROLMENT_FILE,"User, Course Program Enrolment")
    print(f"[INFO] User-Org-Role Data Prefetch Completed in {round(time.time() - start_time, 2)} seconds.")

def fetchPrintDFCount(duckdb_conn,tableName,type):
    queryToExecute = f"SELECT COUNT(*) AS count FROM read_parquet('{tableName}')"
    userDFCount = duckutil.executeQuery(duckdb_conn, queryToExecute).df()
    print(f"[INFO] {type} Record Count: {userDFCount.iloc[0, 0]}")


if __name__ == "__main__":
    main()

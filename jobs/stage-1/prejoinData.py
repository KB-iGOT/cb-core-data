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

# Ensure the parent directory is in sys.path for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckutil import duckutil  # Assuming duckutil is in the parent directory

def main():
    duck_conn = duckutil.initialize_duckdb()
    prejoinACBPData(duck_conn)

def prejoinACBPData(duckdb_conn):
    query = QueryConstants.FETCH_ACBP_DETAILS
    duckutil.executeQuery(duckdb_conn,f"""
    COPY ({query}) TO '{ParquetFileConstants.ACBP_COMPUTED_PARQUET_FILE}' 
    (FORMAT PARQUET, COMPRESSION ZSTD);
    """).df()
    queryToExecute = QueryConstants.FETCH_COMPUTED_ACBP_DETAILS
    acbpComputedDF = duckutil.executeQuery(duckdb_conn,queryToExecute).df()
    print("Combined Data Frame For ACBP Comouted Data",len(acbpComputedDF))

def prejoinUserData(duckdb_conn):
    print("Hello")


if __name__ == "__main__":
    main()
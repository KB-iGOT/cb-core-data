import duckdb
import time
import os
from pathlib import Path
from constants.ParquetFileConstants import ParquetFileConstants


def log_step(start_time, step_name):
    now = time.perf_counter()
    print(f"[{step_name}] completed in {now - start_time:.2f} seconds")
    return now

def initialize_duckdb(memory_limit="8GB"):
    OUTPUT_DIR = Path(__file__).resolve().parents[1] / "duckFolder/temp_duckdb"
    conn = duckdb.connect()
    conn.execute(f"PRAGMA memory_limit='{memory_limit}';")
    conn.execute("PRAGMA temp_directory = '{OUTPUT_DIR}';")
    conn.execute(f"PRAGMA max_temp_directory_size='150GB'")
    conn.execute("LOAD icu;") 
    #conn.execute("PRAGMA mmap = true;")
    conn.execute(f"PRAGMA threads={os.cpu_count()}")
    return conn

def close_duck_db(duckdb_conn):
    duckdb_conn.close()

def executeQuery(duckdb_conn,query):
    return duckdb_conn.execute(query)

def chunkedQueryExecution(duckdb_conn, table1, table2, output):
    # Register Parquet files as physical DuckDB tables
    duckdb_conn.execute(f"""
        CREATE TABLE t1 AS
        SELECT * FROM read_parquet('{table1}')
    """)
    duckdb_conn.execute(f"""
        CREATE TABLE t2 AS
        SELECT * FROM read_parquet('{table2}')
    """)

    batch_size = 1_000_000
    offset = 0

    total_rows = duckdb_conn.execute("SELECT COUNT(*) FROM t1").fetchone()[0]
    print(f"Total rows in content_program: {total_rows}")

    batch_num = 1
    output_path = Path(output)

    while offset < total_rows:
        print(f"Processing batch {batch_num}...")

        query = f"""
            SELECT tab1.*, 
                   tab2.*
            FROM (
                SELECT * FROM t1
                LIMIT {batch_size} OFFSET {offset}
            ) AS tab1
            LEFT JOIN t2 tab2
            ON tab1.userID = tab2.userID
        """

        batch_output_file = output_path.with_stem(output_path.stem + f"_batch_{batch_num}")

        duckdb_conn.execute(f"""
            COPY (
                {query}
            ) TO '{batch_output_file}' (FORMAT PARQUET);
        """)

        offset += batch_size
        batch_num += 1

    print("All batches processed.")

    # Register Parquet files as virtual tables
    duckdb_conn.execute(f"""
        CREATE TABLE t1 AS
        SELECT * FROM read_parquet('{table1}')
    """)

    duckdb_conn.execute(f"""
        CREATE TABLE t2 AS
        SELECT * FROM read_parquet('{table2}')
    """)

    # Create an iterator over batches (adjust batch size as needed)
    batch_size = 10000000
    offset = 0

    # Get total row count from content_program
    total_rows = duckdb_conn.execute(f"SELECT COUNT(*) FROM t1").fetchone()[0]
    print(f"Total rows in content_program: {total_rows}")

    batch_num = 1
    while offset < total_rows:
        print(f"Processing batch {batch_num}...")

        # Execute chunked join with LIMIT + OFFSET
        query = f"""
            SELECT tab1.*, 
                tab2.*
            FROM (
                SELECT * FROM t1
                LIMIT {batch_size} OFFSET {offset}
            ) AS tab1
            LEFT JOIN t2 tab2
            ON tab1.userID = tab2.userID
        """

        duckdb_conn.execute(f"""
        COPY (
            {query}
        ) TO '{output}' (FORMAT PARQUET);
        """)

        #result_df = duckdb_conn.execute(query).fetchdf()

        # Process or save the result
        # Example: write to file
        #result_df.to_parquet(f"{output}")

        # Move to next batch
        offset += batch_size
        batch_num += 1

    print("All batches processed.")
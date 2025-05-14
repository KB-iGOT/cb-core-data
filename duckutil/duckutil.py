import duckdb
import time
import os
from pathlib import Path


def log_step(start_time, step_name):
    now = time.perf_counter()
    print(f"[{step_name}] completed in {now - start_time:.2f} seconds")
    return now

def initialize_duckdb(memory_limit="8GB"):
    conn = duckdb.connect()
    conn.execute(f"PRAGMA memory_limit='{memory_limit}'")
    conn.execute("PRAGMA temp_directory = 'temp_duckdb';")
    conn.execute(f"PRAGMA max_temp_directory_size='50GB'")
    conn.execute("LOAD icu;") 
    #conn.execute("PRAGMA mmap = true;")
    conn.execute(f"PRAGMA threads={os.cpu_count()}")
    return conn

def close_duck_db(duckdb_conn):
    duckdb_conn.close()

def executeQuery(duckdb_conn,query):
    return duckdb_conn.execute(query)
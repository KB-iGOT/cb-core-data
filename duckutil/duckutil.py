import duckdb
import time
import os
from pathlib import Path

    
BASE_OUTPUT_DIR = Path(__file__).resolve().parents[1] / 'duckFolder'
duckdb_dir = Path(BASE_OUTPUT_DIR)
BASE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
output_file = duckdb_dir / f"warehouse_combined_data.duckdb"

def log_step(start_time, step_name):
    now = time.perf_counter()
    print(f"[{step_name}] completed in {now - start_time:.2f} seconds")
    return now

def initialize_duckdb(memory_limit="8GB"):
    conn = duckdb.connect(Path(output_file))
    conn.execute(f"PRAGMA memory_limit='{memory_limit}'")
    conn.execute("PRAGMA temp_directory = 'temp_duckdb';")
    conn.execute(f"PRAGMA max_temp_directory_size='30GB'")
    conn.execute("LOAD icu;") 
    #conn.execute("PRAGMA mmap = true;")
    conn.execute(f"PRAGMA threads={os.cpu_count()}")
    return conn

def close_duck_db(duckdb_conn):
    duckdb_conn.close()

def executeQuery(duckdb_conn,query):
    return duckdb_conn.execute(query)
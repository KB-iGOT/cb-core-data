from sqlite3 import Date
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import Window


# ==============================
# 1. Configuration and Constants
# ==============================

# Ensure the parent directory is in sys.path for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckutil import duckutil,datautil  # Assuming duckutil is in the parent directory
from dfutil.content import contentDF


def main():

     duckdb_conn = duckutil.initialize_duckdb()
     acbp_df = datautil.getAcbpDetailsDataFrame(duckdb_conn)
     print(len(acbp_df.df()))
    

if __name__ == "__main__":
    main()
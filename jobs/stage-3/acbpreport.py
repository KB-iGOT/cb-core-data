import sys
from pathlib import Path
import pandas as pd


# ==============================
# 1. Configuration and Constants
# ==============================

# Ensure the parent directory is in sys.path for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckutil import duckutil,datautil  # Assuming duckutil is in the parent directory


def main():
     duckdb_conn = duckutil.initialize_duckdb()
    # obtain and save user org data
    # orgDF, userDF, userOrgDF = datautil.getOrgUserDataFrames(duckdb_conn, True)
     acbpAllEnrolmentDF = datautil.getAcbpDetailsDF(duckdb_conn).df()
     hierarchyDF = datautil.contentHierarchyDataFrame().df() 
     allCourseProgramESDF = datautil.fetchCourseProgramDataFrame()
     datautil.orgCompleteHierarchyDataFrame()
     print(len(acbpAllEnrolmentDF))
    
if __name__ == "__main__":
    main()
import sys
from pathlib import Path
import os
import time

# ==============================
# 1. Configuration and Constants
# ==============================

# Ensure the parent directory is in sys.path for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckutil import duckutil  # Assuming duckutil is in the parent directory
from constants.ParquetFileConstants import ParquetFileConstants
from constants.QueryConstants import QueryConstants


def getOrgUserDataFrames(duckdb_conn):
    """Returns organization, user, and joined data as DuckDB DataFrames."""
    try:
        orgDF = getOrgDataFrame(duckdb_conn)
        userDF = getUserDataFrame(duckdb_conn)
        orgUserDF = getOrgUserDataFrame(duckdb_conn)
        return orgDF, userDF, orgUserDF
    finally:
        duckutil.close_duck_db(duckdb_conn)


def getOrgDataFrame(duckdb_conn):
    """Reads and processes organization data."""
    parquet_file_path = ParquetFileConstants.ORG_COMPLETE_HIERARCHY_PARQUET_FILE

    if not Path(parquet_file_path).exists():
        raise FileNotFoundError(f"Organization file not found: {parquet_file_path}")

    query = QueryConstants.FETCH_ALL_ORG_DATA
    return duckutil.executeQuery(duckdb_conn, query).fetchdf()


def getUserDataFrame(duckdb_conn, isActive: bool = False):
    """Reads and processes user data with JSON parsing."""

    query = QueryConstants.FETCH_ALL_ACTIVE_USERS if isActive else QueryConstants.FETCH_ALL_USERS
    
    return duckutil.executeQuery(duckdb_conn, query)


def getOrgUserDataFrame(duckdb_conn):
    """Joins organization and user data using a LEFT JOIN."""

    query = QueryConstants.FETCH_ALL_USER_ORG_ACTIVE_DATA
    return duckutil.executeQuery(duckdb_conn, query)

def getOrgUserRoleDataFrame(duckdb_conn):
     """Joins organization and user data using a LEFT JOIN."""

def getAcbpDetailsDataFrame(duckdb_conn):
    """Fetch ACBPDetails"""
    queryToExecute = QueryConstants.FETCH_COMPUTED_ACBP_DETAILS
    return duckutil.executeQuery(duckdb_conn,queryToExecute)

def getExplodedACBPDetailsDataFrame(duckdb_conn):
    """ Fetch Exploded ACBPDetails with union of Custom User, Designation & All User Based ACBP"""

def getContentHierarchyDataFrame(duckdb_conn):
    queryToExecute = QueryConstants.FETCH_CONTENT_ID_BY_HIERARCHY
    return duckutil.executeQuery(duckdb_conn,queryToExecute)

def getAllCourseProgramESDataFrame(duckdb_conn):
    queryToExecute = QueryConstants.FETCH_CONTENT_ID_BY_HIERARCHY
    return duckutil.executeQuery(duckdb_conn,queryToExecute)

def getOrgCompleteHierarchyDataFrame(duckdb_conn):
    queryToExecute = QueryConstants.FETCH_ALL_ORG_HIERARCHY
    return duckutil.executeQuery(duckdb_conn,queryToExecute)

def getUserOrgRoleDataFrame(duckdb_conn):
     """Joins user,organization and role data using a LEFT JOIN."""
     queryToExecute = QueryConstants.FETCH_USER_ORG_ROLE_DATA
     return duckutil.executeQuery(duckdb_conn,queryToExecute)

def getRoleCountDataFrame(duckdb_conn):
    """Count Role For Data Frame"""
    queryToExecute  = QueryConstants.FETCH_ROLE_COUNT
    return duckutil.executeQuery(duckdb_conn,queryToExecute)

def getOrgRoleCountDataFrame(duckdb_conn):
    """Count Org User Role Count For Data Frame"""
    queryToExecute  = QueryConstants.FETCH_USER_ORG_ROLE_COUNT_ACTIVE_DATA
    return duckutil.executeQuery(duckdb_conn,queryToExecute)

def getOrgUserCountDataFrame(duckdb_conn):
    """Count Org User Count"""
    queryToExecute  = QueryConstants.FETCH_ACTIVE_ORG_USER_COUNT
    return duckutil.executeQuery(duckdb_conn,queryToExecute)

def getElasticSearchCourseProgramDataFrame(duckdb_conn,primaryCategories):
    queryToExecute = "select * from read_parquet('{ParquetFileConstants.ESCONTENT_PARQUET_FILE}') where primaryCategory in [{primaryCategories}]"
    return duckutil.executeQuery(duckdb_conn,queryToExecute)




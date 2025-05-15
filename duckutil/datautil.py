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

def org_complete_hierarchy_dataframe(duckdb_conn):
    queryToExecute = "select * from read_parquet('{ParquetFileConstants.ORG_COMPLETE_HIERARCHY_PARQUET_FILE}')"
    return duckutil.executeQuery(duckdb_conn,queryToExecute)

def roleCountDataFrame(duckdb_conn):
    queryToExecute = "select role,COUNT(DISTINCT userID) AS count from read_parquet('{ParquetFileConstants.USER_ORG_ROLE_COMPUTED_PARQUET_FILE}') where userStatus = 1 and userOrgstatus = 1 GROUP BY ROLE"
    return duckutil.executeQuery(duckdb_conn,queryToExecute)

def userOrgCountDataFrame(duckdb_conn):
    queryToExecute = "SELECT userOrgID AS orgID, userOrgName AS orgName, role, COUNT(DISTINCT userID) AS count FROM userOrgRole WHERE userStatus = 1 AND userOrgStatus = 1 GROUP BY userOrgID, userOrgName, role;"
    return duckutil.executeQuery(duckdb_conn,queryToExecute)

#Problem In this query
def orgUserCountDataFrame(duckdb_conn):
    queryToExecute = "SELECT orgID, orgName, COUNT(DISTINCT userID) AS registeredCount, 10000 AS totalCount FROM orgUserDF GROUP BY orgID, orgName;"
    return duckutil.executeQuery(duckdb_conn,queryToExecute)

def content_hierarchy_dataframe(duckdb_conn):
    queryToExecute ="select identifier from read_parquet('{ParquetFileConstants.ORG_HIERARCHY_PARQUET_FILE}')"
    return duckutil.executeQuery(duckdb_conn,queryToExecute)

def course_rating_summary_dataframe(duckdb_conn):
    queryToExecute = """ 
    SELECT 
        activityid AS courseID,
        LOWER(activitytype) AS categoryLower,
        sum_of_total_ratings AS ratingSum,
        total_number_of_ratings AS ratingCount,
        sum_of_total_ratings / total_number_of_ratings AS ratingAverage,
        totalcount1stars AS count1Star,
        totalcount2stars AS count2Star,
        totalcount3stars AS count3Star,
        totalcount4stars AS count4Star,
        totalcount5stars AS count5Star
    FROM 
        ratingSummary
    WHERE 
        total_number_of_ratings > 0;
    """
    duckutil.executeQuery(duckdb_conn, queryToExecute)
    
    # Fetching the created summary table for further use
    summary_df = duckutil.executeQuery(duckdb_conn, "SELECT * FROM rating_summary;").df()
    
    print(f"Rating Summary DataFrame loaded with {len(summary_df)} rows.")
    return summary_df

def org_complete_hierarchy_dataframe(duckdb_conn):
    queryToExecute = "SELECT mdo_id AS userOrgID,department AS dept_name,ministry AS ministry_name FROM orgHierarchy;"
    return duckutil.executeQuery(duckdb_conn,queryToExecute)

def user_course_rating_dataframe(duckdb_conn):
    queryToExecute = "SELECT activityid AS courseID,userid AS userID,rating AS userRating,activitytype AS cbpType,createdon AS createdOn FROM rating;"
    return duckutil.executeQuery(duckdb_conn,queryToExecute)

def course_batch_data_frame(duckdb_conn):
     queryToExecute ="""SELECT courseid AS courseID,batchid AS batchID,name AS courseBatchName,
        createdby AS courseBatchCreatedBy,start_date AS courseBatchStartDate,
        end_date AS courseBatchEndDate,
        COALESCE(batch_attributes, '{}') AS courseBatchAttrs
        FROM 
        batch;"""
     return duckutil.executeQuery(duckdb_conn,queryToExecute)
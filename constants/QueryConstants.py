import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[0]))
from ParquetFileConstants import ParquetFileConstants

class QueryConstants:

    #Static Constants for each PreJoin Stage
    PRE_FETCH_USER_ORG_DATA = f"""
    SELECT 
        u.*,
        o.*
    FROM 
        read_parquet('{ParquetFileConstants.USER_PARQUET_FILE}') AS u
    LEFT JOIN 
        read_parquet('{ParquetFileConstants.ORG_PARQUET_FILE}') AS o 
    ON 
        u.regorgid = o.id
    """

    PRE_FETCH_USER_ORG_ROLE_DATA = f"""
        SELECT 
        u.*,
        o.*,
        r.roles
        FROM 
        read_parquet('{ParquetFileConstants.USER_PARQUET_FILE}') AS u
        LEFT JOIN 
        read_parquet('{ParquetFileConstants.ORG_PARQUET_FILE}') AS o
        ON u.regorgid = o.id
        LEFT JOIN LATERAL (
        SELECT ARRAY_AGG(role) AS roles
        FROM read_parquet('{ParquetFileConstants.ROLE_PARQUET_FILE}') AS r
        WHERE r.userid = u.userID
        ) AS r ON TRUE
    """
    
    PREFETCH_ENROLMENT_WITH_COURSE_DATA = f"""
        SELECT en.*,con.* FROM 
        read_parquet('{ParquetFileConstants.ENROLMENT_PARQUET_FILE}') AS en
        LEFT JOIN
        read_parquet('{ParquetFileConstants.ESCONTENT_PARQUET_FILE}') AS con
        ON en.courseid = con.identifier
    """

    PREFETCH_ENROLMENT_WITH_COURSE_DATA_USER_ORG_ROLE_DATA = f"""
        select cpe.*,uo.userid,uo.email,uo.emailverified,uo.firstname,uo.lastname,
        uo.roles,uo.regorgid,uo.orgname,uo.status,uo.organisationtype,uo.organisationsubtype FROM 
        read_parquet('{ParquetFileConstants.COURSE_PROGRAM_ENROLMENT_COMPUTED_FILE}') AS cpe
        LEFT JOIN
        read_parquet('{ParquetFileConstants.USER_ORG_COMPUTED_PARQUET_FILE}') AS uo
        ON cpe.userID = uo.userID
    """
    


  
 
 
    #Static Constants for each Entity Parquet File
    FETCH_ALL_USERS = f"""
        select * from read_parquet('{ParquetFileConstants.USER_COMPUTED_PARQUET_FILE}')
    """

    FETCH_ALL_ACTIVE_USERS = f"""
        select * from read_parquet('{ParquetFileConstants.USER_COMPUTED_PARQUET_FILE}') where userStatus == 1
    """

    FETCH_ALL_ORG_DATA = f"""
        SELECT 
            id AS orgID,
            orgname AS orgName,
            COALESCE(status, '') AS orgStatus,
            CAST(strftime('%s', orgCreatedDate) AS BIGINT) AS orgCreatedDate,
            organisationtype AS orgType,
            organisationsubtype AS orgSubType
        FROM read_parquet('{ParquetFileConstants.ORG_PARQUET_FILE}')
    """

    FETCH_ALL_USER_ORG_ACTIVE_DATA = f"""
        select * from read_parquet('{ParquetFileConstants.USER_ORG_COMPUTED_PARQUET_FILE}')
        where u.userStatus == 1
    """

    FETCH_ALL_USER_ORG_ACTIVE_DATA_GROUP_BY_ORG = f"""
        SELECT 
            u.*,
            u.professionaldetails.designation AS designation
            o.orgID AS userOrgID,
            o.orgName AS userOrgName,
            o.orgStatus AS userOrgStatus,
            o.orgCreatedDate AS userOrgCreatedDate,
            o.orgType AS userOrgType,
            o.orgSubType AS userOrgSubType
        FROM 
            read_parquet('{ParquetFileConstants.USER_PARQUET_FILE}') AS u
        LEFT JOIN 
            read_parquet('{ParquetFileConstants.ORG_PARQUET_FILE}') AS o 
        ON 
            u.userOrgID = o.orgID where u.userStatus == 1
        
    """
    
    FETCH_COMPUTED_ACBP_DETAILS = f"""
        select * from read_parquet('{ParquetFileConstants.ACBP_COMPUTED_PARQUET_FILE}')
    """
    
    FETCH_CONTENT_ID_BY_HIERARCHY = f"""
        SELECT 
        identifier, hierarchy 
        FROM read_parquet('{ParquetFileConstants.HIERARCHY_PARQUET_FILE}');
    """
    
    FETCH_ALL_ORG_HIERARCHY = f"""
        select * from read_parquet('{ParquetFileConstants.ORG_PARQUET_FILE}')
    """

    FETCH_USER_ORG_ROLE_DATA = f"""
        SELECT 
        u.userID, 
        u.userStatus,
        u.userOrgID, 
        o.orgName AS userOrgName, 
        o.orgStatus AS userOrgStatus,
        r.roleID,
        r.roleName,
        r.roleType
        FROM 
        read_parquet('{ParquetFileConstants.USER_PARQUET_FILE}') AS u -- Replace with actual path
        LEFT JOIN 
        read_parquet('{ParquetFileConstants.ORG_PARQUET_FILE}') AS o -- Replace with actual path
        ON u.userOrgID = o.orgID
        LEFT JOIN 
        read_parquet('{ParquetFileConstants.ROLE_PARQUET_FILE}') AS r -- Replace with actual path
        ON u.userID = r.userID;
        """
    
    FETCH_ROLE_COUNT = f"""
        SELECT 
        role, 
        COUNT(DISTINCT userID) AS count
        FROM 
        read_parquet('output/user_combined_data.parquet') AS u
        LEFT JOIN 
        read_parquet('output/org_combined_data.parquet') AS o 
        ON 
        u.userOrgID = o.orgID
        LEFT JOIN 
        read_parquet('output/role_combined_data.parquet') AS r 
        ON 
        u.userID = r.userID
        WHERE 
        u.userStatus = 1 
        AND o.orgStatus = 1
        GROUP BY 
        role;
    """

    FETCH_USER_ORG_ROLE_COUNT_ACTIVE_DATA = f"""
        SELECT 
        u.userOrgID AS orgID,
        o.orgName AS orgName,
        r.roleName AS role,
        COUNT(DISTINCT u.userID) AS count
        FROM 
        read_parquet('{ParquetFileConstants.USER_PARQUET_FILE}') AS u 
        LEFT JOIN 
        read_parquet('{ParquetFileConstants.ORG_PARQUET_FILE}') AS o 
        ON 
        u.userOrgID = o.orgID
        LEFT JOIN 
        read_parquet('{ParquetFileConstants.ROLE_PARQUET_FILE}') AS r 
        ON 
        u.userID = r.userID
        WHERE 
        u.userStatus = 1 
        AND o.orgStatus = 1
        GROUP BY 
        u.userOrgID, 
        o.orgName, 
        r.roleName;
        """
    
    FETCH_ACTIVE_ORG_USER_COUNT =f"""
            WITH active_users AS (
            SELECT 
            * from read_parquet('{ParquetFileConstants.USER_COMPUTED_PARQUET_FILE}')
            WHERE status = 1),

            -- Join with Active Organization Data
            org_user_counts AS (
            SELECT 
            o.orgID,
            o.orgName,
            COUNT(DISTINCT u.userID) AS registeredCount,
            10000 AS totalCount -- Fixed value as per your requirement
            FROM 
            read_parquet('{ParquetFileConstants.ORG_PARQUET_FILE}') AS o
            LEFT JOIN 
            active_users AS u 
            ON 
            o.orgID = u.orgID
            GROUP BY 
            o.orgID, o.orgName
            )

            -- Final Output
            SELECT 
            orgID, 
            orgName, 
            registeredCount, 
            totalCount
            FROM 
            org_user_counts;
    """
    
    
    def main():
        print("Defined Static Parquet File Constants:")

    if __name__ == "__main__":
        main()
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[0]))
from ParquetFileConstants import ParquetFileConstants

class QueryConstants:

    #Static Constants for each PreJoin Stage
    PRE_FETCH_ALL_USER_ORG_DATA = f"""
    SELECT 
        u.*,
        o.id AS userOrgID,
        o.orgname AS userOrgName,
        o.status AS userOrgStatus,
        o.createddate AS userOrgCreatedDate,
        o.organisationtype AS userOrgType,
        o.organisationsubtype AS userOrgSubType
    FROM 
        read_parquet('{ParquetFileConstants.USER_PARQUET_FILE}') AS u
    LEFT JOIN 
        read_parquet('{ParquetFileConstants.ORG_PARQUET_FILE}') AS o 
    ON 
        u.regorgid = o.id
    """

    PRE_FETCH_ACBP_DETAILS = f"""
        -- Step 1: Load ACBP Data
        WITH acbp_data AS (
        SELECT 
            id AS acbpID,
            orgid AS userOrgID,
            draftdata,
            status AS acbpStatus,
            createdby AS acbpCreatedBy,
            COALESCE(name, '') AS cbPlanName,
            assignmenttype AS assignmentType,
            assignmenttypeinfo AS assignmentTypeInfo,
            enddate AS completionDueDate,
            publishedat AS allocatedOn,
            contentlist AS acbpCourseIDList
        FROM 
            read_parquet('{ParquetFileConstants.ACBP_PARQUET_FILE}')
        ),
        -- Step 2: Extract DRAFT data with simple pattern matching instead of JSON functions
        draft_cbp_data AS (
        SELECT 
            acbpID,
            userOrgID,
            draftdata,
            acbpStatus,
            acbpCreatedBy,
            CASE 
                WHEN draftdata IS NULL THEN ''
                WHEN LEFT(draftdata, 1) = '{{' THEN 
                    -- Only attempt JSON extraction if it starts with a curly brace
                    COALESCE(TRY_CAST(json_extract(draftdata, '$.name') AS VARCHAR), '')
                ELSE draftdata -- Use the text directly if it doesn't look like JSON
            END AS cbPlanName,
            CASE 
                WHEN draftdata IS NULL THEN ''
                WHEN LEFT(draftdata, 1) = '{{' THEN 
                    COALESCE(TRY_CAST(json_extract(draftdata, '$.assignmentType') AS VARCHAR), '')
                ELSE '' 
            END AS assignmentType,
            CASE 
                WHEN draftdata IS NULL THEN ''
                WHEN LEFT(draftdata, 1) = '{{' THEN 
                    COALESCE(TRY_CAST(json_extract(draftdata, '$.assignmentTypeInfo') AS VARCHAR), '')
                ELSE '' 
            END AS assignmentTypeInfo,
            CASE 
                WHEN draftdata IS NULL THEN ''
                WHEN LEFT(draftdata, 1) = '{{' THEN 
                    COALESCE(TRY_CAST(json_extract(draftdata, '$.endDate') AS VARCHAR), '')
                ELSE '' 
            END AS completionDueDate,
            'not published' AS allocatedOn,
            CASE 
                WHEN draftdata IS NULL THEN '[]'
                WHEN LEFT(draftdata, 1) = '{{' THEN 
                    COALESCE(TRY_CAST(json_extract(draftdata, '$.contentList') AS VARCHAR), '[]')
                ELSE '[]' 
            END AS acbpCourseIDList
        FROM 
            acbp_data
        WHERE 
            acbpStatus = 'DRAFT'
        ),
        -- Step 3: Non-DRAFT (Live and Retire) data
        non_draft_cbp_data AS (
        SELECT * 
        FROM acbp_data 
        WHERE acbpStatus <> 'DRAFT'
        )
        -- Step 4: Union DRAFT and Non-DRAFT Data
        SELECT * FROM non_draft_cbp_data
        UNION ALL
        SELECT * FROM draft_cbp_data
        """

    PRE_FETCH_USER_ORG_ROLE_DATA = f"""
        SELECT 
        u.userID, 
        u.status,
        u.regorgid, 
        o.orgname AS userOrgName, 
        o.status AS userOrgStatus,
        r.userid,
        r.role
        FROM 
        read_parquet('{ParquetFileConstants.USER_PARQUET_FILE}') AS u -- Replace with actual path
        LEFT JOIN 
        read_parquet('{ParquetFileConstants.ORG_PARQUET_FILE}') AS o -- Replace with actual path
        ON u.regorgid = o.id
        LEFT JOIN 
        read_parquet('{ParquetFileConstants.ROLE_PARQUET_FILE}') AS r -- Replace with actual path
        ON u.userID = r.userid
        """

    PRE_FETCH_USER_DATA =f"""
        SELECT 
            id AS userID,
            COALESCE(firstname, '') AS firstName,
            COALESCE(lastname, '') AS lastName,
            maskedemail AS maskedEmail,
            maskedphone AS maskedPhone,
            COALESCE(rootorgid, '') AS userOrgID,
            status AS userStatus,
            COALESCE(profiledetails, '{{}}') AS userProfileDetails,
            createddate AS userCreatedTimestamp,
            updateddate AS userUpdatedTimestamp,
            createdby AS userCreatedBy,
            
            -- Extract JSON fields only once for optimization
            JSON(profiledetails) AS profileDetails,
            
            -- Parsing JSON fields directly
            profileDetails->>'personalDetails' AS personalDetails,
            profileDetails->>'employmentDetails' AS employmentDetails,
            profileDetails->>'professionalDetails' AS professionalDetails,
            
            -- Extracting userVerified (handling null as false)
            COALESCE(CAST(profileDetails->>'verifiedKarmayogi' AS BOOLEAN), FALSE) AS userVerified,
            profileDetails->>'mandatoryFieldsExists' AS userMandatoryFieldsExists,
            profileDetails->>'profileImageUrl' AS userProfileImgUrl,
            profileDetails->>'profileStatus' AS userProfileStatus,
            
            -- Checking if phone is verified
            LOWER(COALESCE(profileDetails->>'personalDetails.phoneVerified', 'false')) = 'true' AS userPhoneVerified,
            
            -- Concatenating full name
            RTRIM(CONCAT_WS(' ', COALESCE(firstname, ''), COALESCE(lastname, ''))) AS fullName

        FROM read_parquet('{ParquetFileConstants.USER_PARQUET_FILE}')
    """
    
    PRE_FETCH_ENROLLMENT_DATA=f"""
        SELECT 
            userid AS userID,
            courseid AS courseID,
            batchid AS batchID,
            progress AS courseProgress,
            status AS dbCompletionStatus,
            contentstatus AS courseContentStatus,
            
            
           -- Convert to LONG (UNIX Timestamp)
            EPOCH(CAST(completedon AS TIMESTAMP)) AS courseCompletedTimestamp,
            EPOCH(CAST(enrolled_date AS TIMESTAMP)) AS courseEnrolledTimestamp,
            EPOCH(CAST(lastcontentaccesstime AS TIMESTAMP)) AS lastContentAccessTimestamp,

            -- Issued Certificate Metrics
            COALESCE(array_length(issued_certificates), 0) AS issuedCertificateCount,
            CASE WHEN array_length(issued_certificates) > 0 THEN 1 ELSE 0 END AS issuedCertificateCountPerContent,
            
            -- Certificate Details
            CASE 
                WHEN issued_certificates IS NULL THEN '' 
                ELSE issued_certificates[array_length(issued_certificates) - 1]->>'lastIssuedOn' 
            END AS certificateGeneratedOn,
            CASE 
                WHEN issued_certificates IS NULL THEN '' 
                WHEN array_length(issued_certificates) > 0 THEN issued_certificates[0]->>'lastIssuedOn' 
                ELSE '' 
            END AS firstCompletedOn,
            CASE 
                WHEN issued_certificates IS NULL THEN '' 
                ELSE issued_certificates[array_length(issued_certificates) - 1]->>'identifier' 
            END AS certificateID,
            *

        FROM 
            read_parquet('{ParquetFileConstants.ENROLMENT_PARQUET_FILE}')
        WHERE 
            active = TRUE
    """
    
    #Static Constants for each PreJoin Parquet File
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

    FETCH_ALL_USER_ORG_DATA = f"""
    select * from read_parquet('{ParquetFileConstants.USER_ORG_COMPUTED_PARQUET_FILE}')
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
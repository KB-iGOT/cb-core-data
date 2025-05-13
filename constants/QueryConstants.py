import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[0]))
from ParquetFileConstants import ParquetFileConstants

class QueryConstants:

    FETCH_ALL_USERS = f"""
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

    FETCH_ALL_ACTIVE_USERS = f"""
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

        FROM read_parquet('{ParquetFileConstants.USER_PARQUET_FILE}') where userStatus == 1
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
        SELECT 
            u.*,
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
            u.userOrgID = o.orgID
    """
    
    FETCH_ALL_USER_ORG_ACTIVE_DATA = f"""
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
    
    FETCH_ACBP_DETAILS = f"""
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

    FETCH_COMPUTED_ACBP_DETAILS = f"""
        select * from read_parquet('{ParquetFileConstants.ACBP_COMPUTED_PARQUET_FILE}')
    """
    
    def main():
        print("Defined Static Parquet File Constants:")

    if __name__ == "__main__":
        main()
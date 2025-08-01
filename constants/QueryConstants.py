from constants.ParquetFileConstants import ParquetFileConstants

class QueryConstants:
    
    ORG_BASED_DESIGNATION_LIST = f"""SELECT  
                                    userOrgID,  
                                    STRING_AGG(DISTINCT COALESCE(designation, professionalDetails.designation)) as org_designations  
                                FROM read_parquet('{ParquetFileConstants.USER_ORG_COMPUTED_FILE}/**.parquet')  
                                WHERE COALESCE(designation, professionalDetails.designation) IS NOT NULL  
                                GROUP BY userOrgID"""

    ORG_USER_COUNT_DATAFRAME_QUERY = f"""
                                    SELECT 
                                    userOrgID AS orgID,
                                    userOrgName AS orgName,
                                    COUNT(userID) AS registeredCount,
                                    10000 AS totalCount
                                    FROM read_parquet('{ParquetFileConstants.USER_ORG_COMPUTED_FILE}/**.parquet')
                                    WHERE userOrgID IS NOT NULL 
                                    AND userStatus = 1      -- Filter for ACTIVE USERS ONLY
                                    AND userOrgStatus = 1   -- Filter for ACTIVE ORGS ONLY
                                    GROUP BY userOrgID, userOrgName
                                    ORDER BY registeredCount DESC"""
    

    TOP_10_LEARNERS_BY_MDO_QUERY = F"""
                                    WITH ranked_users AS (
                                        SELECT
                                            *,
                                            RANK() OVER (PARTITION BY userOrgID ORDER BY total_points DESC) AS rank
                                        FROM read_parquet('{ParquetFileConstants.USER_ORG_COMPUTED_FILE}/**.parquet')
                                        WHERE total_points IS NOT NULL
                                    ),
                                    top10_by_org AS (
                                        SELECT * FROM ranked_users WHERE rank <= 10
                                    ),
                                    json_ready AS (
                                        SELECT
                                            userOrgID,
                                            -- build JSON string manually, escaping strings properly
                                            FORMAT(
                                                '{{{{"userID":"{{}}", "fullName":"{{}}", "userOrgName":"{{}}", "designation":"{{}}", "userProfileImgUrl":{{}}, "total_points":{{}}, "rank":{{}}}}}}',
                                                userID,
                                                fullName,
                                                userOrgName,
                                                COALESCE(designation, ''),
                                                CASE WHEN userProfileImgUrl IS NULL THEN 'null' ELSE '"' || userProfileImgUrl || '"' END,
                                                total_points,
                                                rank
                                            ) AS json_details_str
                                        FROM top10_by_org
                                    )
                                    SELECT
                                        userOrgID,
                                        -- Aggregate as an array of JSON strings and wrap in JSON object
                                        json_object('top_learners', list(json_details_str)) AS top_learners
                                    FROM json_ready
                                    GROUP BY userOrgID;"""


    ORG_BASED_MDO_LEADER_COUNT = f"""WITH exploded_roles AS (
                                SELECT 
                                    userID,
                                    userOrgID,
                                    TRIM(role_value.unnest) AS role  -- extract string from struct before trimming
                                FROM read_parquet('{ParquetFileConstants.USER_ORG_COMPUTED_FILE}/**.parquet'),
                                    UNNEST(STRING_SPLIT(role, ',')) AS role_value
                                WHERE userStatus = 1 AND userOrgStatus = 1
                            ),
                            filtered_roles AS (
                                SELECT DISTINCT userOrgID
                                FROM exploded_roles
                                WHERE role IN ('MDO_ADMIN', 'MDO_LEADER')
                            )
                            SELECT COUNT(*) AS org_with_admin_or_leader_count
                            FROM filtered_roles;"""

    ORG_BASED_MDO_ADMIN_COUNT = f"""WITH exploded_roles AS (
                                SELECT 
                                    userID,
                                    userOrgID,
                                    TRIM(unnested_role.unnest) AS role
                                FROM read_parquet('{ParquetFileConstants.USER_ORG_COMPUTED_FILE}/**.parquet'),
                                    UNNEST(STRING_SPLIT(role, ',')) AS unnested_role
                                WHERE userStatus = 1 AND userOrgStatus = 1
                            ),
                            filtered_roles AS (
                                SELECT DISTINCT userOrgID
                                FROM exploded_roles
                                WHERE role = 'MDO_ADMIN'
                            )
                            SELECT COUNT(*) AS org_with_admin_count
                            FROM filtered_roles;"""


    USER_COUNT_BY_ORG = f"""Select  
                                userOrgID,  
                                count(*)  
                                from read_parquet('{ParquetFileConstants.USER_COMPUTED_PARQUET_FILE}/**.parquet')
                                WHERE userStatus = 1   
                                GROUP BY userOrgID  
                                order by count(*) DESC"""
    
    USER_REGISTERED_YESTERDAY=f"""SELECT  count(*) as count
                                    FROM read_parquet('{ParquetFileConstants.USER_COMPUTED_PARQUET_FILE}/**.parquet')
                                    WHERE userStatus = 1   
                                    AND userCreatedTimestamp >= extract(epoch from date_trunc('day', current_timestamp - interval '1 day')) * 1000
                                    AND userCreatedTimestamp < extract(epoch from date_trunc('day', current_timestamp)) * 1000;"""

    # Fixed: Added missing comma and corrected path
    COURSE_COUNT_BY_STATUS_GROUP_BY_ORG = f"""SELECT 
                                            main.courseOrgID,
                                            main.category,
                                            COUNT(*) AS totalCourseCount,
                                            COUNT(CASE WHEN main.courseStatus = 'Live' THEN 1 END) AS liveCourseCount,
                                            COUNT(CASE WHEN LOWER(main.courseStatus) = 'draft' THEN 1 END) AS draftCourseCount,
                                            COUNT(CASE WHEN main.courseStatus = 'Review' THEN 1 END) AS reviewCourseCount,
                                            COUNT(CASE WHEN main.courseStatus = 'Retired' THEN 1 END) AS retiredCourseCount,
                                            COUNT(CASE WHEN main.courseStatus = 'Review' AND main.courseReviewStatus = 'Reviewed' THEN 1 END) AS pendingPublishCourseCount,
                                            AVG(ratings.ratingAverage) AS avgRating
                                        FROM 
                                            read_parquet('{ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE}/**.parquet') AS main
                                        LEFT JOIN 
                                            (
                                                SELECT 
                                                    courseID,
                                                    LOWER(category) AS categoryLower,
                                                    ratingAverage
                                                FROM 
                                                    read_parquet('{ParquetFileConstants.RATING_SUMMARY_COMPUTED_PARQUET_FILE}/**.parquet')
                                            ) AS ratings
                                        ON 
                                            main.courseID = ratings.courseID AND LOWER(main.category) = ratings.categoryLower
                                        GROUP BY 
                                            main.courseOrgID, main.category
                                        ORDER BY 
                                            totalCourseCount DESC;"""

    # Fixed: Moved WHERE clause to correct position and changed == to =
    AVG_COURSE_RATING_BY_COURSE_ORG = f"""SELECT courseOrgID,  
                                        COUNT(*) AS totalCourseCount,  
                                        AVG(rating) AS avgRating  
                                        FROM read_parquet('{ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE}/**.parquet')  
                                        WHERE category = 'Course'  
                                        GROUP BY courseOrgID  
                                        ORDER BY avgRating DESC"""
    
    # Fixed: Moved WHERE clause to correct position and changed == to =
    AVG_COURSE_RATING_ACROSS_PLATFORM = f"""SELECT  
                                            AVG(rating) AS avgRating  
                                            FROM read_parquet('{ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE}/**.parquet')  
                                            WHERE category = 'Course'"""
    
    # Fixed: Moved WHERE clause to correct position and changed == to =
    AVG_MODERATED_COURSE_RATING_BY_COURSE_ORG = f"""SELECT courseOrgID,  
                                        COUNT(*) AS totalCourseCount,  
                                        AVG(rating) AS avgRating  
                                        FROM read_parquet('{ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE}/**.parquet')  
                                        WHERE category = 'Course'  
                                        GROUP BY courseOrgID  
                                        ORDER BY avgRating DESC"""
    
    UNIQUE_USERS_ENROLLED_BY_STATUS = f"""SELECT  
                                        COUNT(DISTINCT userID) AS totalUsers,  
                                        COUNT(DISTINCT CASE WHEN dbCompletionStatus = 0 THEN userID END) AS notStartedUsers,  
                                        COUNT(DISTINCT CASE WHEN dbCompletionStatus = 1 THEN userID END) AS inProgressUsers,  
                                        COUNT(DISTINCT CASE WHEN dbCompletionStatus = 2 THEN userID END) AS completedUsers  
                                    FROM read_parquet('{ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE}/**.parquet')"""

    DASHBOARD_ENROLMENTS_BY_STATUS = f"""SELECT  
                                COUNT(*) AS totalEnrolments,  
                                COUNT(CASE WHEN dbCompletionStatus = 0 THEN 1 END) AS notStartedEnrolments,  
                                COUNT(CASE WHEN dbCompletionStatus = 1 THEN 1 END) AS inProgressEnrolments,  
                                COUNT(CASE WHEN dbCompletionStatus = 2 THEN 1 END) AS completedEnrolments  
                            FROM read_parquet('{ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE}/**.parquet')"""

    # These lists should now work correctly
    ORG_BASED_LIST = [ORG_BASED_DESIGNATION_LIST, ORG_USER_COUNT_DATAFRAME_QUERY,
                     ORG_BASED_MDO_LEADER_COUNT, ORG_BASED_MDO_ADMIN_COUNT]
    USER_BASED_LIST = [USER_COUNT_BY_ORG]
    COURSE_BASED_LIST = [COURSE_COUNT_BY_STATUS_GROUP_BY_ORG, AVG_COURSE_RATING_BY_COURSE_ORG, 
                        AVG_COURSE_RATING_ACROSS_PLATFORM, AVG_MODERATED_COURSE_RATING_BY_COURSE_ORG]
    ENROLMENT_BASED_LIST = [UNIQUE_USERS_ENROLLED_BY_STATUS, DASHBOARD_ENROLMENTS_BY_STATUS]

    
def main():
    print("Defined Static Parquet File Constants:")
    print(f"ORG_BASED_LIST contains {len(QueryConstants.ORG_BASED_LIST)} queries")
    print(f"USER_BASED_LIST contains {len(QueryConstants.USER_BASED_LIST)} queries")
    print(f"COURSE_BASED_LIST contains {len(QueryConstants.COURSE_BASED_LIST)} queries")
    print(f"ENROLMENT_BASED_LIST contains {len(QueryConstants.ENROLMENT_BASED_LIST)} queries")

if __name__ == "__main__":
    main()
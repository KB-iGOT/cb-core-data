from constants.ParquetFileConstants import ParquetFileConstants
from datetime import datetime, timedelta, timezone
import pytz

class QueryConstants:

    currentDate = datetime.now().date()
    istOffset = timezone(timedelta(hours=5, minutes=30))
    previousDayStartTime = datetime.combine(currentDate - timedelta(days=1), datetime.min.time()).replace(tzinfo=istOffset)
    previousDayEndTime = (datetime.combine(currentDate, datetime.min.time()) - timedelta(seconds=1)).replace(tzinfo=istOffset)
    previousDayStartTimeString = previousDayStartTime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "+0530"
    previousDayEndTimeString = previousDayEndTime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "+0530"
    
    # Epoch calculations
    twentyFourHoursAgoEpochMillisTime = int(datetime.combine(currentDate - timedelta(days=1), datetime.min.time()).replace(tzinfo=istOffset).timestamp())
    previousDayEndEpochMillis = int((datetime.combine(currentDate, datetime.min.time()) - timedelta(seconds=1)).replace(tzinfo=istOffset).timestamp()) * 1000
    twentyFourHoursAgoEpochMillis = int(datetime.combine(currentDate - timedelta(days=1), datetime.min.time()).replace(tzinfo=istOffset).timestamp()) * 1000
    twelveMonthsAgoEpochMillis = int(datetime.combine(currentDate - timedelta(days=365), datetime.min.time()).replace(tzinfo=istOffset).timestamp())
    print(twentyFourHoursAgoEpochMillis)
    print(twelveMonthsAgoEpochMillis)
    print(previousDayEndEpochMillis)

    def get_epoch_for_ist_datetime(date_str):
        """Convert IST datetime string to epoch seconds"""
        ist = pytz.timezone('Asia/Kolkata')
        dt = datetime.strptime(date_str.strip("'"), '%Y-%m-%d %H:%M:%S')
        dt_ist = ist.localize(dt)
        return int(dt_ist.timestamp())

 # Note: Replace these date values with your actual NLW dates
    NLW_START_DATE = "'2024-01-15 00:00:00'"
    NLW_END_DATE = "'2024-01-21 23:59:59'"
    # Calculate epochs
    nlw_start_epoch = get_epoch_for_ist_datetime(NLW_START_DATE)
    nlw_end_epoch = get_epoch_for_ist_datetime(NLW_END_DATE)
    
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


    # ORG_BASED_MDO_LEADER_COUNT = f"""WITH exploded_roles AS (
    #                             SELECT 
    #                                 userID,
    #                                 userOrgID,
    #                                 TRIM(role_value.unnest) AS role  -- extract string from struct before trimming
    #                             FROM read_parquet('{ParquetFileConstants.USER_ORG_COMPUTED_FILE}/**.parquet'),
    #                                 UNNEST(STRING_SPLIT(role, ',')) AS role_value
    #                             WHERE userStatus = 1 AND userOrgStatus = 1
    #                         ),
    #                         filtered_roles AS (
    #                             SELECT DISTINCT userOrgID
    #                             FROM exploded_roles
    #                             WHERE role IN ('MDO_ADMIN', 'MDO_LEADER')
    #                         )
    #                         SELECT COUNT(*) AS org_with_admin_or_leader_count
    #                         FROM filtered_roles;"""

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


    # USER_COUNT_BY_ORG = f"""Select  
    #                             userOrgID,  
    #                             count(*)  
    #                             from read_parquet('{ParquetFileConstants.USER_COMPUTED_PARQUET_FILE}/**.parquet')
    #                             WHERE userStatus = 1   
    #                             GROUP BY userOrgID  
    #                             order by count(*) DESC"""
    
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
    EXTERNAL_CONTENT_PROCESSED = f"""
        WITH external_processed AS (
            SELECT *,
                CASE 
                    WHEN issued_certificates IS NULL THEN ''
                    ELSE issued_certificates[array_length(issued_certificates, 1)].identifier
                END AS certificateID,
                status AS dbCompletionStatus,
                userid AS userID,
                CASE 
                    WHEN issued_certificates IS NULL THEN ''
                    WHEN array_length(issued_certificates, 1) > 0 THEN issued_certificates[1].lastIssuedOn
                    ELSE ''
                END AS firstCompletedOn
            FROM read_parquet('{ParquetFileConstants.EXTERNAL_COURSE_ENROLMENTS_PARQUET_FILE}')  
        )
        SELECT * FROM external_processed
        """
        
        # External content completed yesterday (handles multiple timestamp formats)
    EXTERNAL_CONTENT_COMPLETED_YESTERDAY = f"""
    WITH external_processed AS (
        SELECT *,
            CASE 
                WHEN issued_certificates IS NULL THEN ''
                ELSE issued_certificates[array_length(issued_certificates, 1)].identifier
            END AS certificateID,
            status AS dbCompletionStatus,
            userid AS userID,
            CASE 
                WHEN issued_certificates IS NULL THEN ''
                WHEN array_length(issued_certificates, 1) > 0 THEN issued_certificates[1].lastIssuedOn
                ELSE ''
            END AS firstCompletedOn
        FROM read_parquet('{ParquetFileConstants.EXTERNAL_COURSE_ENROLMENTS_PARQUET_FILE}')  
    ),
    processed_with_epochs AS (
        SELECT *,
            CASE 
                WHEN firstCompletedOn IS NULL OR firstCompletedOn = '' OR LENGTH(firstCompletedOn) = 0 THEN NULL
                -- Try format with +0000 timezone
                WHEN firstCompletedOn LIKE '%+0000' THEN TRY_CAST(epoch(strptime(firstCompletedOn, '%Y-%m-%dT%H:%M:%S.%f+0000')) AS BIGINT)
                -- Try format with Z timezone  
                WHEN firstCompletedOn LIKE '%Z' THEN TRY_CAST(epoch(strptime(firstCompletedOn, '%Y-%m-%dT%H:%M:%S.%fZ')) AS BIGINT)
                -- Try generic timezone format
                ELSE TRY_CAST(epoch(strptime(firstCompletedOn, '%Y-%m-%dT%H:%M:%S.%f%z')) AS BIGINT)
            END AS firstCompletedEpoch
        FROM external_processed
    )
    SELECT COUNT(*) as external_certificate_issued_yesterday_count
    FROM processed_with_epochs
    WHERE firstCompletedEpoch IS NOT NULL
        AND firstCompletedEpoch BETWEEN {int(previousDayStartTime.timestamp())} AND {int(previousDayEndTime.timestamp())}
    """

    # Complete base data with all filter categories
    BASE_DATA_COMPLETE = f"""
    WITH base_data AS (
        SELECT *,
            -- Base eligibility categories
            CASE 
                WHEN courseStatus IN ('Live', 'Retired') AND userStatus = 1 THEN 'live_retired_content'
                ELSE 'other'
            END AS live_retired_content_eligible,
            
            CASE 
                WHEN category IN ('Course', 'Program', 'Blended Program', 'CuratedCollections', 'Curated Program') 
                        AND courseStatus IN ('Live', 'Retired') 
                        AND userStatus = 1 THEN 'live_retired_enrolment'
                ELSE 'other'
            END AS live_retired_enrolment_eligible,
            
            CASE 
                WHEN category = 'Course' 
                        AND courseStatus IN ('Live', 'Retired') 
                        AND userOrgID IS NOT NULL THEN 'live_retired_course'
                ELSE 'other'
            END AS live_retired_course_eligible,
            
            CASE 
                WHEN category IN ('Course', 'Program') 
                        AND courseStatus IN ('Live', 'Retired') 
                        AND userStatus = 1 THEN 'live_retired_course_program'
                ELSE 'other'
            END AS live_retired_course_program_eligible,
            
            CASE 
                WHEN category IN ('Course', 'Program', 'Blended Program', 'CuratedCollections', 'Standalone Assessment', 'Curated Program') 
                        AND courseStatus IN ('Live', 'Retired') 
                        AND userStatus = 1 THEN 'live_retired_course_program_excluding_moderated'
                ELSE 'other'
            END AS live_retired_course_program_excluding_moderated_eligible,
            
            CASE 
                WHEN category IN ('Course', 'Moderated Course') 
                        AND courseStatus IN ('Live', 'Retired') 
                        AND userStatus = 1 THEN 'live_retired_course_moderated'
                ELSE 'other'
            END AS live_retired_course_moderated_eligible,
            
            -- Live-only filters
            CASE 
                WHEN category IN ('Course', 'Program') 
                        AND courseStatus = 'Live' 
                        AND userStatus = 1 THEN 'live_course_program'
                ELSE 'other'
            END AS live_course_program_eligible,
            
            CASE 
                WHEN category IN ('Course', 'Program', 'Blended Program', 'CuratedCollections', 'Standalone Assessment', 'Curated Program') 
                        AND courseStatus = 'Live' 
                        AND userStatus = 1 THEN 'live_course_program_excluding_moderated'
                ELSE 'other'
            END AS live_course_program_excluding_moderated_eligible,
            
            CASE 
                WHEN category IN ('Course', 'Moderated Course') 
                        AND courseStatus = 'Live' 
                        AND userStatus = 1 THEN 'live_course_moderated'
                ELSE 'other'
            END AS live_course_moderated_eligible,
            
            -- Completion status categories
            CASE 
                WHEN dbCompletionStatus = 0 THEN 'not_started'
                WHEN dbCompletionStatus = 1 THEN 'in_progress' 
                WHEN dbCompletionStatus = 2 THEN 'completed'
                ELSE 'unknown'
            END AS completion_category,
            
            -- Yesterday completion filter
            CASE 
                WHEN category IN ('Course', 'Program') 
                        AND courseStatus IN ('Live', 'Retired') 
                        AND userStatus = 1 
                        AND dbCompletionStatus = 2 
                        AND COALESCE(
                TRY_CAST(courseCompletedTimestamp AS BIGINT),
                CAST(epoch(courseCompletedTimestamp) AS BIGINT)
            ) >= {twentyFourHoursAgoEpochMillisTime} THEN 'completed_yesterday'
                ELSE 'other'
            END AS completed_yesterday_category,
            
            -- 12 months enrollment filter
            CASE 
                WHEN category = 'Course' 
                        AND courseStatus IN ('Live', 'Retired') 
                        AND userStatus = 1 
                        AND COALESCE(
                TRY_CAST(courseEnrolledTimestamp AS BIGINT),
                CAST(epoch(courseEnrolledTimestamp) AS BIGINT)
            ) >= {twelveMonthsAgoEpochMillis}
            THEN 'enrolled_last_12_months'
                ELSE 'other'
            END AS enrolled_last_12_months_category,
            
            -- Certificate generation filter
            CASE 
                WHEN certificateGeneratedOn IS NOT NULL AND certificateGeneratedOn != '' THEN 'certificate_generated'
                ELSE 'no_certificate'
            END AS certificate_category,
            
            -- UDF equivalent for epoch conversion (handles multiple timestamp formats)
            CASE 
                WHEN firstCompletedOn IS NULL OR firstCompletedOn = '' OR LENGTH(firstCompletedOn) = 0 THEN 0
                -- Try format with +0000 timezone
                WHEN firstCompletedOn LIKE '%+0000' THEN COALESCE(TRY_CAST(epoch(strptime(firstCompletedOn, '%Y-%m-%dT%H:%M:%S.%f+0000')) AS BIGINT), 0)
                -- Try format with Z timezone  
                WHEN firstCompletedOn LIKE '%Z' THEN COALESCE(TRY_CAST(epoch(strptime(firstCompletedOn, '%Y-%m-%dT%H:%M:%S.%fZ')) AS BIGINT), 0)
                -- Try generic timezone format
                ELSE COALESCE(TRY_CAST(epoch(strptime(firstCompletedOn, '%Y-%m-%dT%H:%M:%S.%f%z')) AS BIGINT), 0)
            END AS epoch_seconds
            
        FROM read_parquet('{ParquetFileConstants.ENROLMENT_CONTENT_USER_COMPUTED_PARQUET_FILE}/**.parquet')  
    )
    """

    # Overall metrics query - FIXED: All metrics in one query
    OVERALL_METRICS = BASE_DATA_COMPLETE + f"""
    SELECT 
        -- Basic enrollment counts (liveRetiredCourseEnrolmentDF equivalent)
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course') as enrolment_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course') as enrolment_unique_user_count,
        
        -- Status-based counts from course enrollment
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'not_started') as not_started_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'not_started') as not_started_unique_user_count,
        
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category IN ('in_progress', 'completed')) as started_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category IN ('in_progress', 'completed')) as started_unique_user_count,
        
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'in_progress') as in_progress_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'in_progress') as in_progress_unique_user_count,
        
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'completed') as completed_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'completed') as completed_unique_user_count,
        
        -- Landing page completed (course program)
        COUNT(*) FILTER (WHERE live_retired_course_program_eligible = 'live_retired_course_program' AND completion_category = 'completed') as landing_page_completed_count,
        
        -- Yesterday completions 
        COUNT(*) FILTER (WHERE completed_yesterday_category = 'completed_yesterday') as landing_page_completed_yesterday_count,
        
        -- Content enrollment and completion
        COUNT(*) FILTER (WHERE live_retired_enrolment_eligible = 'live_retired_enrolment') as content_enrolment_count,
        COUNT(*) FILTER (WHERE live_retired_enrolment_eligible = 'live_retired_enrolment' AND completion_category = 'completed') as content_completed_count,
        
        -- Content completed
        COUNT(*) FILTER (WHERE live_retired_content_eligible = 'live_retired_content' AND completion_category = 'completed') as live_retired_content_completed_count,
        
        -- Yesterday content completed count - FIXED: All in one query
        COUNT(*) FILTER (WHERE live_retired_enrolment_eligible = 'live_retired_enrolment'
                                AND completion_category = 'completed'
                                AND epoch_seconds > 0
                                AND epoch_seconds * 1000 >= {twentyFourHoursAgoEpochMillis} 
                                AND epoch_seconds * 1000 <= {previousDayEndEpochMillis}) as landing_page_content_completed_yesterday_count
        
    FROM base_data
    """
    
    # External content metrics (matches Scala exactly)
    EXTERNAL_CONTENT_METRICS = f"""
    SELECT 
        COUNT(*) as external_content_enrolment_count,
        COUNT(*) FILTER (WHERE status = 2) as external_content_completed_count
    FROM read_parquet('{ParquetFileConstants.EXTERNAL_COURSE_ENROLMENTS_PARQUET_FILE}')
    """
    
    # Live course program enrollment counts by courseID (exact match)
    LIVE_COURSE_PROGRAM_ENROLMENT_COUNTS = BASE_DATA_COMPLETE + """
    SELECT 
        courseID,
        COUNT(*) as enrolmentCount
    FROM base_data 
    WHERE live_course_program_eligible = 'live_course_program'
    GROUP BY courseID
    """
    
    # MDO-wise comprehensive metrics (all operations in one query)
    MDO_WISE_COMPREHENSIVE = BASE_DATA_COMPLETE + f"""
    SELECT 
        userOrgID,
        
        -- Course enrollment metrics (liveRetiredCourseEnrolmentByMDODF)
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course') as course_enrolment_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course') as course_enrolment_unique_user_count,
        
        -- Content enrollment metrics (liveRetiredContentEnrolmentByMDODF)
        COUNT(*) FILTER (WHERE live_retired_content_eligible = 'live_retired_content') as content_enrolment_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_content_eligible = 'live_retired_content') as content_enrolment_unique_user_count,
        
        -- 12 months enrollment (liveRetiredCourseEnrolmentsInLast12MonthsByMDODF)
        COUNT(*) FILTER (WHERE enrolled_last_12_months_category = 'enrolled_last_12_months') as enrolled_last_12_months_count,
        COUNT(DISTINCT userID) FILTER (WHERE enrolled_last_12_months_category = 'enrolled_last_12_months') as active_users_last_12_months,
        
        -- Status-based counts
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'not_started') as not_started_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'not_started') as not_started_unique_user_count,
        
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category IN ('in_progress', 'completed')) as started_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category IN ('in_progress', 'completed')) as started_unique_user_count,
        
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'in_progress') as in_progress_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'in_progress') as in_progress_unique_user_count,
        
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'completed') as completed_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'completed') as completed_unique_user_count
        
    FROM base_data
    WHERE userOrgID IS NOT NULL
    GROUP BY userOrgID
    """
    
    # CBP-wise comprehensive metrics (all operations in one query)
    CBP_WISE_COMPREHENSIVE = BASE_DATA_COMPLETE + """
    SELECT 
        courseOrgID,
        
        -- Course enrollment metrics (liveRetiredCourseEnrolmentByCBPDF)
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course') as course_enrolment_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course') as course_enrolment_unique_user_count,
        
        -- Content completion (liveRetiredContentCompletedByCBPDF)
        COUNT(*) FILTER (WHERE live_retired_content_eligible = 'live_retired_content' AND completion_category = 'completed') as content_completed_count,
        
        -- Course + Moderated course enrollment (liveRetiredCourseModeratedCourseEnrolmentByCBPDF)
        COUNT(*) FILTER (WHERE live_retired_course_moderated_eligible = 'live_retired_course_moderated') as course_moderated_course_enrolment_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_moderated_eligible = 'live_retired_course_moderated') as course_moderated_course_enrolment_unique_user_count,
        
        -- Content enrollment (liveRetiredContentEnrolmentByCBPDF)
        COUNT(*) FILTER (WHERE live_retired_content_eligible = 'live_retired_content') as content_enrolment_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_content_eligible = 'live_retired_content') as content_enrolment_unique_user_count,
        
        -- Status-based counts
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'not_started') as not_started_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'not_started') as not_started_unique_user_count,
        
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category IN ('in_progress', 'completed')) as started_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category IN ('in_progress', 'completed')) as started_unique_user_count,
        
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'in_progress') as in_progress_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'in_progress') as in_progress_unique_user_count,
        
        COUNT(*) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'completed') as completed_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_eligible = 'live_retired_course' AND completion_category = 'completed') as completed_unique_user_count,
        
        -- Certificate generation metrics (certificateGeneratedByCBPDF)
        COUNT(*) FILTER (WHERE live_retired_content_eligible = 'live_retired_content' AND certificate_category = 'certificate_generated') as certificates_generated_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_content_eligible = 'live_retired_content' AND certificate_category = 'certificate_generated') as certificates_generated_unique_user_count,
        
        -- Course + Moderated course certificates (courseModeratedCourseCertificateGeneratedByCBPDF)
        COUNT(*) FILTER (WHERE live_retired_course_moderated_eligible = 'live_retired_course_moderated' AND certificate_category = 'certificate_generated') as course_moderated_course_certificates_generated_count,
        COUNT(DISTINCT userID) FILTER (WHERE live_retired_course_moderated_eligible = 'live_retired_course_moderated' AND certificate_category = 'certificate_generated') as course_moderated_course_certificates_generated_unique_user_count
        
    FROM base_data
    WHERE courseOrgID IS NOT NULL
    GROUP BY courseOrgID
    """
    
    # Top courses by organization (competencies) - exact match to Scala logic
    TOP_COURSES_BY_ORG = BASE_DATA_COMPLETE + """,
    course_counts AS (
        SELECT 
            courseOrgID,
            courseID,
            COUNT(*) as course_count
        FROM base_data
        WHERE live_retired_content_eligible = 'live_retired_content'
        GROUP BY courseOrgID, courseID
    ),
    ranked_courses AS (
        SELECT 
            courseOrgID,
            courseID,
            course_count,
            ROW_NUMBER() OVER (PARTITION BY courseOrgID ORDER BY course_count DESC) as rank
        FROM course_counts
    )
    SELECT 
        courseOrgID,
        STRING_AGG(courseID, ',' ORDER BY rank) as courseIDs
    FROM ranked_courses
    GROUP BY courseOrgID
    """

    # ==================== National Learning Week Base Dates ====================
   
    
    # ==================== NLW Enrollment Metrics ====================
    
    # Event enrollments during NLW
    NLW_EVENT_ENROLLMENTS = f"""
    SELECT COUNT(event_id) as event_count
    FROM read_parquet('{ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE}') 
    WHERE enrolled_on_datetime >= {NLW_START_DATE} 
      AND enrolled_on_datetime <= {NLW_END_DATE}
    """
    
    # Content enrollments during NLW (using epoch timestamps)
    NLW_CONTENT_ENROLLMENTS = f"""
    SELECT COUNT(*) as content_count
    FROM read_parquet('{ParquetFileConstants.ENROLMENT_CONTENT_USER_COMPUTED_PARQUET_FILE}/**.parquet') 
    WHERE courseStatus IN ('Live', 'Retired')
    AND userStatus=1 AND COALESCE(
                TRY_CAST(courseEnrolledTimestamp AS BIGINT),
                CAST(epoch(courseEnrolledTimestamp) AS BIGINT)
            ) >= {nlw_start_epoch}
      AND COALESCE(
                TRY_CAST(courseEnrolledTimestamp AS BIGINT),
                CAST(epoch(courseEnrolledTimestamp) AS BIGINT)
            ) <= {nlw_end_epoch}
    """
    
    # Total NLW enrollments (combine both)
    TOTAL_NLW_ENROLLMENTS = f"""
    WITH event_enroll AS (
        SELECT COUNT(event_id) as count
        FROM read_parquet('{ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE}') 
        WHERE enrolled_on_datetime >= {NLW_START_DATE} 
          AND enrolled_on_datetime <= {NLW_END_DATE}
    ),
    content_enroll AS (
        SELECT COUNT(*) as count
        FROM read_parquet('{ParquetFileConstants.ENROLMENT_CONTENT_USER_COMPUTED_PARQUET_FILE}/**.parquet') 
        WHERE courseStatus IN ('Live', 'Retired')
        AND userStatus=1 AND courseEnrolledTimestamp >= epoch({NLW_START_DATE}::TIMESTAMPTZ AT TIME ZONE 'Asia/Kolkata')
          AND courseEnrolledTimestamp <= epoch({NLW_END_DATE}::TIMESTAMPTZ AT TIME ZONE 'Asia/Kolkata')
    )
    SELECT (e.count + c.count) as total_enrollment_nlw_count
    FROM event_enroll e, content_enroll c
    """
    

    
    # ==================== Certificate Generation - Yesterday ====================
    
    # Content certificates generated yesterday
    CONTENT_CERTIFICATES_YESTERDAY = f"""
    WITH yesterday_range AS (
        SELECT 
            (CURRENT_DATE - INTERVAL '1 day')::DATE + TIME '00:00:00' AT TIME ZONE 'Asia/Kolkata' as start_time,
            (CURRENT_DATE - INTERVAL '1 day')::DATE + TIME '23:59:59' AT TIME ZONE 'Asia/Kolkata' as end_time
    )
    SELECT COUNT(*) as certificate_count
    FROM read_parquet('{ParquetFileConstants.ENROLMENT_CONTENT_USER_COMPUTED_PARQUET_FILE}/**.parquet') l, yesterday_range y
    WHERE l.courseStatus IN ('Live', 'Retired')
        AND l.userStatus=1 AND l.certificateGeneratedOn >= strftime(y.start_time, '%Y-%m-%dT%H:%M:%S%z')
      AND l.certificateGeneratedOn <= strftime(y.end_time, '%Y-%m-%dT%H:%M:%S%z')
    """
    
    # Event certificates generated yesterday
    EVENT_CERTIFICATES_YESTERDAY = f"""
    WITH yesterday_range AS (
        SELECT 
            (CURRENT_DATE - INTERVAL '1 day')::DATE + TIME '00:00:00' as start_time,
            (CURRENT_DATE - INTERVAL '1 day')::DATE + TIME '23:59:59' as end_time
    )
    SELECT COUNT(DISTINCT certificate_id) as event_certificate_count
    FROM read_parquet('{ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE}') e, yesterday_range y
    WHERE e.status = 'completed'
      AND e.certificate_id IS NOT NULL
      AND e.enrolled_on_datetime >= strftime(y.start_time, '%Y-%m-%d %H:%M:%S')
      AND e.enrolled_on_datetime <= strftime(y.end_time, '%Y-%m-%d %H:%M:%S')
    """
    
    # Total certificates generated yesterday
    TOTAL_CERTIFICATES_YESTERDAY = f"""
    WITH yesterday_range AS (
        SELECT 
            (CURRENT_DATE - INTERVAL '1 day')::DATE + TIME '00:00:00' AT TIME ZONE 'Asia/Kolkata' as start_time,
            (CURRENT_DATE - INTERVAL '1 day')::DATE + TIME '23:59:59' AT TIME ZONE 'Asia/Kolkata' as end_time
    ),
    content_certs AS (
        SELECT COUNT(*) as count
        FROM read_parquet('{ParquetFileConstants.ENROLMENT_CONTENT_USER_COMPUTED_PARQUET_FILE}/**.parquet') l, yesterday_range y
        WHERE l.courseStatus IN ('Live', 'Retired')
        AND l.userStatus=1 AND l.certificateGeneratedOn >= strftime(y.start_time, '%Y-%m-%dT%H:%M:%S%z')
          AND l.certificateGeneratedOn <= strftime(y.end_time, '%Y-%m-%dT%H:%M:%S%z')
    ),
    event_certs AS (
        SELECT COUNT(DISTINCT certificate_id) as count
        FROM read_parquet('{ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE}') e, yesterday_range y
        WHERE e.status = 'completed'
          AND e.certificate_id IS NOT NULL
          AND e.enrolled_on_datetime >= strftime(y.start_time, '%Y-%m-%d %H:%M:%S')
          AND e.enrolled_on_datetime <= strftime(y.end_time, '%Y-%m-%d %H:%M:%S')
    )
    SELECT (c.count + e.count) as total_certificates_yesterday
    FROM content_certs c, event_certs e
    """
    
    # ==================== Certificate Generation - NLW ====================
    
    # Content certificates generated during NLW
    CONTENT_CERTIFICATES_NLW = f"""
    SELECT COUNT(*) as certificate_count,
           COUNT(DISTINCT userID) as unique_user_count
    FROM read_parquet('{ParquetFileConstants.ENROLMENT_CONTENT_USER_COMPUTED_PARQUET_FILE}/**.parquet') 
    WHERE l.courseStatus IN ('Live', 'Retired')
        AND l.userStatus=1 AND certificateGeneratedOn >= strftime({NLW_START_DATE}::TIMESTAMPTZ AT TIME ZONE 'Asia/Kolkata', '%Y-%m-%dT%H:%M:%S%z')
      AND certificateGeneratedOn <= strftime({NLW_END_DATE}::TIMESTAMPTZ AT TIME ZONE 'Asia/Kolkata', '%Y-%m-%dT%H:%M:%S%z')
    """

    
    # Total certificates generated during NLW
    TOTAL_CERTIFICATES_NLW = f"""
    WITH content_certs AS (
        SELECT COUNT(*) as count
        FROM read_parquet('{ParquetFileConstants.ENROLMENT_CONTENT_USER_COMPUTED_PARQUET_FILE}/**.parquet') 
        WHERE l.courseStatus IN ('Live', 'Retired')
        AND l.userStatus=1 AND certificateGeneratedOn >= strftime({NLW_START_DATE}::TIMESTAMPTZ AT TIME ZONE 'Asia/Kolkata', '%Y-%m-%dT%H:%M:%S%z')
          AND certificateGeneratedOn <= strftime({NLW_END_DATE}::TIMESTAMPTZ AT TIME ZONE 'Asia/Kolkata', '%Y-%m-%dT%H:%M:%S%z')
    ),
    event_certs AS (
        SELECT COUNT(DISTINCT certificate_id) as count
        FROM read_parquet('{ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE}') 
        WHERE status = 'completed'
          AND certificate_id IS NOT NULL
          AND enrolled_on_datetime >= {NLW_START_DATE}
    )
    SELECT (c.count + e.count) as total_certificates_nlw
    FROM content_certs c, event_certs e
    """
    
    # ==================== Events Published ====================
    
    # Total events published (distinct count)
    TOTAL_EVENTS_PUBLISHED = f"""
    SELECT COUNT(DISTINCT event_id) as events_published_count
    FROM read_parquet('{ParquetFileConstants.EVENT_PARQUET_FILE}')
    """
    
    # ==================== Learning Hours - NLW ====================
    
    # Event learning hours by user during NLW
    NLW_EVENT_LEARNING_HOURS_BY_USER = f"""
    SELECT 
        user_id,
        ROUND(SUM(CASE WHEN duration >= 180 THEN event_duration_seconds ELSE 0 END) / 3600.0, 2) as totalLearningHours
    FROM read_parquet('{ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE}') 
    WHERE duration IS NOT NULL
      AND enrolled_on_datetime >= {NLW_START_DATE}
      AND enrolled_on_datetime <= {NLW_END_DATE}
    GROUP BY user_id
    """
    
    # Content learning hours by user during NLW
    NLW_CONTENT_LEARNING_HOURS_BY_USER = f"""
    SELECT 
        userID,
        ROUND(SUM(courseDuration) / 3600.0, 2) as totalLearningHours
    FROM cbpCompletionWithDetails 
    WHERE courseCompletedTimestamp >= epoch({NLW_START_DATE}::TIMESTAMPTZ AT TIME ZONE 'Asia/Kolkata')
      AND courseCompletedTimestamp <= epoch({NLW_END_DATE}::TIMESTAMPTZ AT TIME ZONE 'Asia/Kolkata')
      AND userID != ''
      AND dbCompletionStatus = 2
      AND certificateID IS NOT NULL
    GROUP BY userID
    """
    
    # ==================== NLW Certificates by User ====================
    
    # Content certificates by user during NLW
    NLW_CONTENT_CERTIFICATES_BY_USER = f"""
    SELECT 
        userID,
        COUNT(*) as certificate_count
    FROM read_parquet('{ParquetFileConstants.ENROLMENT_CONTENT_USER_COMPUTED_PARQUET_FILE}/**.parquet') 
    WHERE l.courseStatus IN ('Live', 'Retired')
        AND l.userStatus=1 AND certificateGeneratedOn >= strftime({NLW_START_DATE}::TIMESTAMPTZ AT TIME ZONE 'Asia/Kolkata', '%Y-%m-%dT%H:%M:%S%z')
      AND certificateGeneratedOn <= strftime({NLW_END_DATE}::TIMESTAMPTZ AT TIME ZONE 'Asia/Kolkata', '%Y-%m-%dT%H:%M:%S%z')
    GROUP BY userID
    """
    
    # Event certificates by user during NLW
    NLW_EVENT_CERTIFICATES_BY_USER = f"""
    SELECT 
        user_id,
        COUNT(*) as certificate_count
    FROM read_parquet('{ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE}') 
    WHERE status = 'completed'
      AND certificate_id IS NOT NULL
      AND enrolled_on_datetime >= {NLW_START_DATE}
    GROUP BY user_id
    """
    
    # ==================== Top Content by Completion ====================
    
    # Top 5 content by completion by course org
    TOP_5_CONTENT_BY_COMPLETION_BY_ORG = f"""
    WITH ranked_content AS (
        SELECT 
            courseID,
            courseName,
            courseOrgID,
            COUNT(userID) as enrolledCount,
            SUM(CASE WHEN dbCompletionStatus = 2 THEN 1 ELSE 0 END) as completedCount,
            ROW_NUMBER() OVER (PARTITION BY courseOrgID ORDER BY SUM(CASE WHEN dbCompletionStatus = 2 THEN 1 ELSE 0 END) DESC) as rowNum
        FROM read_parquet('{ParquetFileConstants.ENROLMENT_CONTENT_USER_COMPUTED_PARQUET_FILE}')
        WHERE courseStatus IN ('Live', 'Retired')
        AND userStatus=1
        GROUP BY courseID, courseName, courseOrgID
    )
    SELECT 
        courseOrgID,
        JSON_GROUP_ARRAY(
            JSON_OBJECT(
                'rowNum', rowNum,
                'courseID', courseID,
                'courseName', courseName,
                'courseOrgID', courseOrgID,
                'completedCount', completedCount
            )
        ) as jsonData
    FROM ranked_content
    WHERE rowNum <= 5
    GROUP BY courseOrgID
    """
    
    # ==================== Competency Coverage ====================
    
    # Competency coverage by org
    COMPETENCY_COVERAGE_BY_ORG = """
    WITH content_competencies AS (
        SELECT 
            courseID,
            courseOrgID,
            competency_area_id,
            competency_theme_id,
            competency_sub_theme_id
        FROM competencyContentMapping
        WHERE competency_area_id IS NOT NULL
    ),
    area_counts AS (
        SELECT 
            courseOrgID,
            competency_area_id,
            COUNT(DISTINCT competency_theme_id) as area_count
        FROM content_competencies
        GROUP BY courseOrgID, competency_area_id
    ),
    total_counts AS (
        SELECT 
            courseOrgID,
            COALESCE(COUNT(DISTINCT competency_theme_id), 0) as total_count
        FROM content_competencies
        GROUP BY courseOrgID
    ),
    mapped_areas AS (
        SELECT 
            courseOrgID,
            area_count,
            CASE 
                WHEN competency_area_id = 'COMAREA-000003' THEN 'Functional'
                WHEN competency_area_id = 'COMAREA-000001' THEN 'Behavioural'
                WHEN competency_area_id = 'COMAREA-000002' THEN 'Domain'
                ELSE competency_area_id
            END as mapped_area_id
        FROM area_counts
    )
    SELECT 
        m.courseOrgID,
        JSON_OBJECT(
            'total', t.total_count,
            'area_count_map', JSON_GROUP_OBJECT(m.mapped_area_id, m.area_count)
        ) as jsonData
    FROM mapped_areas m
    JOIN total_counts t ON m.courseOrgID = t.courseOrgID
    GROUP BY m.courseOrgID, t.total_count
    """
    
    # ==================== Course Statistics ====================
    
    # Courses enrolled in at least once (live courses only)
    COURSES_ENROLLED_AT_LEAST_ONCE = """
    SELECT 
        COUNT(DISTINCT courseID) as courses_enrolled_count,
        STRING_AGG(DISTINCT courseID, ',') as course_id_list
    FROM liveRetiredCourseEnrolment 
    WHERE courseStatus = 'Live'
      AND courseID != ''
    """
    
    # Courses completed at least once (live courses only)
    COURSES_COMPLETED_AT_LEAST_ONCE = """
    SELECT 
        COUNT(DISTINCT courseID) as courses_completed_count,
        STRING_AGG(DISTINCT courseID, ',') as course_id_list
    FROM liveRetiredCourseCompleted 
    WHERE courseStatus = 'Live'
      AND courseID != ''
    """
    
    # ==================== MDO-wise Analytics ====================
    
    # Certificates generated by MDO
    CERTIFICATES_BY_MDO = """
    SELECT 
        userOrgID,
        COUNT(*) as certificate_count,
        COUNT(DISTINCT userID) as unique_user_count
    FROM certificateGenerated
    GROUP BY userOrgID
    """
    
    # Top 5 MDO by completion
    TOP_5_MDO_BY_COMPLETION = """
    SELECT 
        userOrgID,
        userOrgName,
        COUNT(courseID) as completedCount
    FROM liveRetiredCourseCompleted
    GROUP BY userOrgID, userOrgName
    ORDER BY completedCount DESC
    LIMIT 5
    """
    
    # Trending events by MDO (top 20 per MDO)
    TRENDING_EVENTS_BY_MDO = f"""
    WITH event_counts AS (
        SELECT 
            u.userOrgID,
            e.event_id,
            COUNT(*) as event_count,
            DENSE_RANK() OVER (PARTITION BY u.userOrgID ORDER BY COUNT(*) DESC) as rank
        FROM read_parquet('{ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE}') e
        JOIN userOrg u ON e.user_id = u.userID
        JOIN read_parquet('{ParquetFileConstants.EVENT_PARQUET_FILE}') ed ON e.event_id = ed.event_id
        WHERE ed.event_status = 'Live'
        GROUP BY u.userOrgID, e.event_id
    )
    SELECT 
        userOrgID,
        STRING_AGG(event_id, ',') as events
    FROM event_counts
    WHERE rank <= 20
    GROUP BY userOrgID
    """
    
    # Featured events overall (top 20)
    FEATURED_EVENTS_OVERALL = f"""
    WITH event_counts AS (
        SELECT 
            e.event_id,
            COUNT(*) as event_count
        FROM read_parquet('{ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE}') e
        JOIN userOrg u ON e.user_id = u.userID
        JOIN read_parquet('{ParquetFileConstants.EVENT_PARQUET_FILE}') ed ON e.event_id = ed.event_id
        WHERE ed.event_status = 'Live'
        GROUP BY e.event_id
        ORDER BY event_count DESC
        LIMIT 20
    )
    SELECT STRING_AGG(event_id, ',') as events
    FROM event_counts
    """
    
    # ==================== Content Ratings ====================
    
    # Total ratings by course org
    TOTAL_RATINGS_BY_ORG = """
    SELECT 
        courseOrgID,
        COUNT(ratingCount) as totalRatings
    FROM ratedLiveContent
    GROUP BY courseOrgID
    """
    
    # Ratings spread by org
    RATINGS_SPREAD_BY_ORG = """
    SELECT 
        courseOrgID,
        JSON_OBJECT(
            'count5', SUM(count5Star),
            'count4', SUM(count4Star),
            'count3', SUM(count3Star),
            'count2', SUM(count2Star),
            'count1', SUM(count1Star)
        ) as jsonData
    FROM ratedLiveContent
    GROUP BY courseOrgID
    """

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
    
    # ==================== Configuration Variables ====================
    # Update these with your actual values
    NLW_START_DATE = "'2024-01-15 00:00:00'"
    NLW_END_DATE = "'2024-01-21 23:59:59'"
    PLATFORM_RATING_SURVEY_ID = "'YOUR_SURVEY_ID'"  # Replace with actual survey ID
    
    TOTAL_EVENT_ENROLLMENTS = f"""
    SELECT COUNT(event_id) as total_event_count
    FROM read_parquet('{ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE}')
    """
    
    EVENTS_PUBLISHED_COUNT = f"""
    SELECT COUNT(DISTINCT event_id) as events_published_count
    FROM read_parquet('{ParquetFileConstants.EVENT_PARQUET_FILE}')
    """
    
    # ==================== Certificate Generation - Yesterday ====================
    
   
    
    
    # ==================== Certificate Generation - NLW ====================
    
    EVENT_CERTIFICATES_NLW = f"""
    SELECT COUNT(DISTINCT certificate_id) as event_certificate_count
    FROM read_parquet('{ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE}') 
    WHERE status = 'completed'
      AND certificate_id IS NOT NULL
      AND enrolled_on_datetime >= {NLW_START_DATE}
    """
    
    # ==================== NLW Events Published ====================
    
    EVENTS_PUBLISHED_NLW = f"""
    SELECT COUNT(*) as events_published_nlw_count
    FROM read_parquet('{ParquetFileConstants.EVENT_PARQUET_FILE}')
    WHERE startDate >= date({NLW_START_DATE})
      AND startDate <= date({NLW_END_DATE})
      AND contentType = 'Event'
    """
    
    # ==================== Certificate Generation by User - NLW ====================
    
    NLW_CONTENT_CERTIFICATES_BY_USER = f"""
    WITH nlw_range AS (
        SELECT 
            strftime({NLW_START_DATE}::TIMESTAMPTZ AT TIME ZONE 'Asia/Kolkata', '%Y-%m-%dT%H:%M:%S%z') as start_time,
            strftime({NLW_END_DATE}::TIMESTAMPTZ AT TIME ZONE 'Asia/Kolkata', '%Y-%m-%dT%H:%M:%S%z') as end_time
    )
    SELECT 
        userID,
        COUNT(*) as count
    FROM read_parquet('{ParquetFileConstants.ENROLMENT_CONTENT_USER_COMPUTED_PARQUET_FILE}/**.parquet') l, nlw_range r
    WHERE l.courseStatus IN ('Live', 'Retired')
        AND l.userStatus=1 AND l.certificateGeneratedOn >= r.start_time
      AND l.certificateGeneratedOn <= r.end_time
    GROUP BY userID
    """
    
    NLW_EVENT_CERTIFICATES_BY_USER = f"""
    SELECT 
        user_id,
        COUNT(*) as count
    FROM read_parquet('{ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE}') 
    WHERE status = 'completed'
      AND certificate_id IS NOT NULL
      AND enrolled_on_datetime >= {NLW_START_DATE}
    GROUP BY user_id
    """
    
    
    # ==================== Top Content by Completion ====================
    
    TOP_COURSES_BY_COMPLETION_BY_ORG = """
    WITH course_stats AS (
        SELECT 
            courseOrgID,
            courseID,
            COUNT(DISTINCT userID) as user_enrolment_count
        FROM liveCourseProgramExcludingModeratedCompleted
        WHERE category = 'Course'
        GROUP BY courseOrgID, courseID
        ORDER BY user_enrolment_count DESC
    )
    SELECT 
        CONCAT(courseOrgID, ':courses') as key,
        STRING_AGG(courseID, ',') as sorted_courseIDs
    FROM course_stats
    GROUP BY courseOrgID
    """
    
    TOP_PROGRAMS_BY_COMPLETION_BY_ORG = """
    WITH program_stats AS (
        SELECT 
            courseOrgID,
            courseID,
            COUNT(DISTINCT userID) as user_enrolment_count
        FROM liveCourseProgramExcludingModeratedCompleted
        WHERE category = 'Program'
        GROUP BY courseOrgID, courseID
        ORDER BY user_enrolment_count DESC
    )
    SELECT 
        CONCAT(courseOrgID, ':programs') as key,
        STRING_AGG(courseID, ',') as sorted_courseIDs
    FROM program_stats
    GROUP BY courseOrgID
    """
    
    TOP_ASSESSMENTS_BY_COMPLETION_BY_ORG = """
    WITH assessment_stats AS (
        SELECT 
            courseOrgID,
            courseID,
            COUNT(DISTINCT userID) as user_enrolment_count
        FROM liveCourseProgramExcludingModeratedCompleted
        WHERE category = 'Standalone Assessment'
        GROUP BY courseOrgID, courseID
        ORDER BY user_enrolment_count DESC
    )
    SELECT 
        CONCAT(courseOrgID, ':assessments') as key,
        STRING_AGG(courseID, ',') as sorted_courseIDs
    FROM assessment_stats
    GROUP BY courseOrgID
    """
    
    # ==================== NPS Calculation ====================
    
    AVERAGE_NPS = f"""
    SELECT 
        ROUND(
            COALESCE(
                ((SUM(CASE WHEN rating IN (9, 10) THEN 1 ELSE 0 END) - 
                  SUM(CASE WHEN rating IN (0, 1, 2, 3, 4, 5, 6) THEN 1 ELSE 0 END)) * 1.0) 
                / NULLIF(COUNT(rating), 0) * 100, 
                0
            ), 1
        ) AS avgNps 
    FROM nps_upgraded_users_data 
    WHERE submitted = true 
      AND activityID = {PLATFORM_RATING_SURVEY_ID}
    """
    
    
    CORE_COMPETENCIES_BY_MDO = f"""
    WITH course_counts AS (
        SELECT 
            userOrgID,
            courseID,
            COUNT(*) as count
        FROM read_parquet('{ParquetFileConstants.ENROLMENT_CONTENT_USER_COMPUTED_PARQUET_FILE}')
        WHERE courseStatus IN ('Live', 'Retired')
        AND userStatus=1
        GROUP BY userOrgID, courseID
        ORDER BY count DESC
    )
    SELECT 
        userOrgID,
        STRING_AGG(courseID, ',') as courseIDs
    FROM course_counts
    GROUP BY userOrgID
    """

    
    # ==================== Monthly Active Users ====================
    
    AVERAGE_MONTHLY_ACTIVE_USERS = """
    SELECT COUNT(DISTINCT userID) as active_users_count
    FROM userActivity 
    WHERE activity_date >= CURRENT_DATE - INTERVAL '30 days'
    """
    
    # ==================== Moderated Course Certificates ====================
    
    MODERATED_COURSE_CERTIFICATES_YESTERDAY = """
    WITH yesterday_range AS (
        SELECT 
            strftime((CURRENT_DATE - INTERVAL '1 day')::DATE + TIME '00:00:00', '%Y-%m-%dT%H:%M:%S:00+0000') as start_time
    )
    SELECT COUNT(*) as certificate_count
    FROM liveRetiredCourseModeratedCourseEnrolment l, yesterday_range y
    WHERE l.certificateGeneratedOn IS NOT NULL 
      AND l.certificateGeneratedOn != ''
      AND l.certificateGeneratedOn >= y.start_time
    """

    # These lists should now work correctly
    ORG_BASED_LIST = [ORG_BASED_DESIGNATION_LIST, ORG_USER_COUNT_DATAFRAME_QUERY,ORG_BASED_MDO_ADMIN_COUNT]
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
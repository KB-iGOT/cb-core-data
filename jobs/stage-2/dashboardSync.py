import findspark
findspark.init()
import sys
from pathlib import Path
import pandas as pd
import duckdb
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import date_format, current_timestamp, unix_timestamp, udf,sum
from pyspark.sql import SparkSession
from dateutil import tz
from datetime import datetime, timedelta, timezone
import pytz

from datetime import datetime
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))

from dfutil.utils.redis import Redis
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.enrolment import enrolmentDFUtil
from constants.QueryConstants import QueryConstants
from dfutil.assessment import assessmentDFUtil
from jobs.default_config import create_config
from jobs.config import get_environment_config


class DashboardDuckDBExecutor:
    
    def __init__(self):
        self.conn = duckdb.connect()
        self.results = {}
    
    def execute_query(self,spark, query_name, query):
        try:
            print(f"🔄 Executing DuckDB query: {query_name}")
            result = self.conn.execute(query).fetchdf()
            return spark.createDataFrame(result)
        except Exception as e:
            print(f"❌ Error executing {query_name}: {str(e)}")
            self.results[query_name] = None
            return None
    
    def close(self):
        self.conn.close()


class DashboardSyncModel:    
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.DashboardSyncModel"
        self.duckdb_executor = DashboardDuckDBExecutor()
        
    def name(self):
        return "DashboardSyncModel"
    
    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")
    
    def getOrgUserMaps(self,org_user_count_df):
        orgRegisteredUserCountMap = {}
        orgTotalUserCountMap = {}
        orgNameMap = {}
        
        rows = org_user_count_df.collect()
        
        for row in rows:
            orgId = row["orgID"]  # or row.orgID
            orgRegisteredUserCountMap[orgId] = str(row["registeredCount"]) 
            orgTotalUserCountMap[orgId] = str(row["totalCount"])
            orgNameMap[orgId] = row["orgName"]
        
        return (orgRegisteredUserCountMap, orgTotalUserCountMap, orgNameMap)

    def dashboardRedisUpdates(self,spark,config):
        # orgWithMdoAdminLeaderCount = self.duckdb_executor.execute_query(spark,"orgWithMdoAdminLeaderCount", QueryConstants.ORG_BASED_MDO_LEADER_COUNT)
        # orgAdminLeaderCount = orgWithMdoAdminLeaderCount.collect()[0]["org_with_admin_or_leader_count"]
        # # Redis.update("dashboard_org_with_mdo_admin_leader_count", str(orgAdminLeaderCount))

        orgWithMdoAdminCount = self.duckdb_executor.execute_query(spark,"orgWithMdoAdminCount", QueryConstants.ORG_BASED_MDO_ADMIN_COUNT)
        orgAdminCount = orgWithMdoAdminCount.collect()[0]["org_with_admin_count"]
        # Redis.update("dashboard_org_with_mdo_admin_count", str(orgAdminCount))

        # activeUsersByMDODF = self.duckdb_executor.execute_query(spark,"activeUsersByMDODF", QueryConstants.USER_COUNT_BY_ORG)
        # # Redis.dispatchDataFrame("dashboard_user_count_by_user_org",activeUsersByMDODF, "userOrgID", "count",config)

        usersRegisteredYesterdayDF = self.duckdb_executor.execute_query(spark,"usersRegisteredYesterdayDF", QueryConstants.USER_REGISTERED_YESTERDAY)
        usersRegisteredYesterdayCount = usersRegisteredYesterdayDF.collect()[0]["count"]
        # Redis.update("dashboard_new_users_registered_yesterday", str(usersRegisteredYesterdayCount))

        allCourseProgramDetailsWithRatingDF = self.duckdb_executor.execute_query(spark,"allCrsPgmDF", QueryConstants.COURSE_COUNT_BY_STATUS_GROUP_BY_ORG)
        allCourseDF = allCourseProgramDetailsWithRatingDF.filter(F.col("category") == "Course").withColumn("avgRating",F.when(F.isnan("avgRating"), None).otherwise(F.col("avgRating")))
        allCourseModeratedCourseDF = allCourseProgramDetailsWithRatingDF.filter(F.col("category").isin("Course", "Moderated Course")).withColumn("avgRating",F.when(F.isnan("avgRating"), None).otherwise(F.col("avgRating")))
        liveContentDF = allCourseProgramDetailsWithRatingDF.filter(F.col("liveCourseCount") > 0).withColumn("avgRating",F.when(F.isnan("avgRating"), None).otherwise(F.col("avgRating")))

        # # Live Course Count (only Course)
        # liveCourseCountByCBPDF = allCourseDF.select(
        #     "courseOrgID", F.col("liveCourseCount").alias("count")
        # ).filter("count IS NOT NULL")
        # # Redis.dispatchDataFrame("dashboard_live_course_count_by_course_org", liveCourseCountByCBPDF, "courseOrgID", "count",config)

        # Live Course + Moderated Course
        liveCourseModeratedCourseByCBPDF = allCourseModeratedCourseDF.select(
            "courseOrgID", F.col("liveCourseCount").alias("count")
        ).filter("count IS NOT NULL")
        # Redis.dispatchDataFrame("dashboard_live_course_moderated_course_count_by_course_org", liveCourseModeratedCourseByCBPDF, "courseOrgID", "count",config)

        # Live Content Count (all categories)
        liveContentCountByCBPDF = liveContentDF.select(
            "courseOrgID", F.col("liveCourseCount").alias("count")
        ).filter("count IS NOT NULL")
        # Redis.dispatchDataFrame("dashboard_live_content_count_by_course_org", liveContentCountByCBPDF, "courseOrgID", "count",config)

        # Draft
        # draftCourseCountByCBPDF = allCourseDF.select(
        #     "courseOrgID", F.col("draftCourseCount").alias("count")
        # ).filter("count IS NOT NULL")
        # # Redis.dispatchDataFrame("dashboard_draft_course_count_by_course_org", draftCourseCountByCBPDF, "courseOrgID", "count",config)

        # Review
        # reviewCourseCountByCBPDF = allCourseDF.select(
        #     "courseOrgID", F.col("reviewCourseCount").alias("count")
        # ).filter("count IS NOT NULL")
        # # Redis.dispatchDataFrame("dashboard_review_course_count_by_course_org", reviewCourseCountByCBPDF, "courseOrgID", "count",config)

        # Retired
        # retiredCourseCountByCBPDF = allCourseDF.select(
        #     "courseOrgID", F.col("retiredCourseCount").alias("count")
        # ).filter("count IS NOT NULL")
        # # Redis.dispatchDataFrame("dashboard_retired_course_count_by_course_org", retiredCourseCountByCBPDF, "courseOrgID", "count",config)

        # Pending Publish
        # pendingPublishCourseCountByCBPDF = allCourseDF.select(
        #     "courseOrgID", F.col("pendingPublishCourseCount").alias("count")
        # ).filter("count IS NOT NULL")
        # # Redis.dispatchDataFrame("dashboard_pending_publish_course_count_by_course_org", pendingPublishCourseCountByCBPDF, "courseOrgID", "count",config)

        # --- Additional Metrics ---

        # Count of MDOs with live course
        # orgWithLiveCourseCount = allCourseDF.filter(F.col("liveCourseCount") > 0).select("courseOrgID").distinct().count()
        # # Redis.update("dashboard_cbp_with_live_course_count", str(orgWithLiveCourseCount),config)

        # Overall Avg Rating (Course only)
        # ratedCourseDF = allCourseDF.filter(F.col("avgRating").isNotNull())
        # if ratedCourseDF.count() > 0:
        #     avgRatingOverall = ratedCourseDF.agg(F.avg("avgRating")).first()[0]
            # Redis.update("dashboard_course_average_rating_overall", str(avgRatingOverall),config)

        # Avg Rating by courseOrgID (Course)
        # avgRatingByCBPDF = ratedCourseDF.select(
        #     "courseOrgID", F.col("avgRating").alias("ratingAverage")
        # )
        # Redis.dispatchDataFrame("dashboard_course_average_rating_by_course_org", avgRatingByCBPDF, "courseOrgID", "ratingAverage",config)

        # Avg Rating by courseOrgID (Course + Moderated Course)
        ratedCourseModeratedDF = allCourseModeratedCourseDF.filter(F.col("avgRating").isNotNull())
        courseModeratedCourseAvgRatingByCBPDF = ratedCourseModeratedDF.select(
            "courseOrgID", F.col("avgRating").alias("ratingAverage")
        )
        # Redis.dispatchDataFrame("dashboard_course_moderated_course_average_rating_by_course_org", courseModeratedCourseAvgRatingByCBPDF, "courseOrgID", "ratingAverage",config)

        ratedLiveContentDF = liveContentDF.filter(F.col("avgRating").isNotNull())
        avgContentRatingByCBPDF = ratedLiveContentDF.groupBy("courseOrgID").agg(
            F.avg("avgRating").alias("ratingAverage")
        )
        # Redis.dispatchDataFrame("dashboard_content_average_rating_by_course_org", avgContentRatingByCBPDF, "courseOrgID", "ratingAverage",config)

        primary_categories= ["Course","Program","Blended Program","Curated Program","Moderated Course","Standalone Assessment","CuratedCollections"]
        allCourseProgramCompletionWithDetailsDF = spark.read.parquet(ParquetFileConstants.ENROLMENT_CONTENT_USER_COMPUTED_PARQUET_FILE).filter(F.col("category").isin(primary_categories))

        # self.courseProgramRedisUpdateWithDuckDB(spark,config)

        # self.nlwAnalyticsUpdateWithDuckDB(spark,config)
   
        return
    

    def courseProgramRedisUpdateWithDuckDB(self,spark,config):
        externalYesterdayDF = self.duckdb_executor.execute_query(spark, "external_yesterday", QueryConstants.EXTERNAL_CONTENT_COMPLETED_YESTERDAY)
        externalCertificateIssuedYesterdayCount = externalYesterdayDF.collect()[0]["external_certificate_issued_yesterday_count"]
        
        # 2. Overall metrics
        overallMetricsDF = self.duckdb_executor.execute_query(spark, "overall_metrics", QueryConstants.OVERALL_METRICS)
        overallMetrics = overallMetricsDF.collect()[0]  # First row has main metrics
        yesterdayMetrics = overallMetricsDF.collect()[1] if overallMetricsDF.count() > 1 else overallMetrics  # Second row has yesterday content
        
        # 3. External content metrics  
        externalMetricsDF = self.duckdb_executor.execute_query(spark, "external_metrics", QueryConstants.EXTERNAL_CONTENT_METRICS)
        externalMetrics = externalMetricsDF.collect()[0]
        
        # 4. Live course program enrollment counts
        liveCourseProgramEnrolmentCountsDF = self.duckdb_executor.execute_query(spark, "live_course_program_enrolment_counts", QueryConstants.LIVE_COURSE_PROGRAM_ENROLMENT_COUNTS)
        
        # 5. MDO-wise comprehensive metrics
        mdoWiseMetricsDF = self.duckdb_executor.execute_query(spark, "mdo_wise_metrics", QueryConstants.MDO_WISE_COMPREHENSIVE)
        mdoWiseMetricsDF.printSchema()
        mdoWiseMetricsDF.show(2,False)
        
        # 6. CBP-wise comprehensive metrics
        # cbpWiseMetricsDF = self.duckdb_executor.execute_query(spark, "cbp_wise_metrics", QueryConstants.CBP_WISE_COMPREHENSIVE)
        
        # 7. Top courses by organization
        topCoursesByOrgDF = self.duckdb_executor.execute_query(spark, "top_courses_by_org", QueryConstants.TOP_COURSES_BY_ORG)

        # Extract values (exact match to Scala variable names)
        enrolmentCount = overallMetrics["enrolment_count"]
        enrolmentUniqueUserCount = overallMetrics["enrolment_unique_user_count"]
        notStartedCount = overallMetrics["not_started_count"]
        notStartedUniqueUserCount = overallMetrics["not_started_unique_user_count"]
        startedCount = overallMetrics["started_count"]
        startedUniqueUserCount = overallMetrics["started_unique_user_count"]
        inProgressCount = overallMetrics["in_progress_count"]
        inProgressUniqueUserCount = overallMetrics["in_progress_unique_user_count"]
        completedCount = overallMetrics["completed_count"]
        completedUniqueUserCount = overallMetrics["completed_unique_user_count"]
        landingPageCompletedCount = overallMetrics["landing_page_completed_count"]
        landingPageCompletedYesterdayCount = overallMetrics["landing_page_completed_yesterday_count"]
        landingPageContentCompletedYesterdayCount = yesterdayMetrics["landing_page_content_completed_yesterday_count"]
        contentEnrolmentCount = overallMetrics["content_enrolment_count"]
        contentCompletedCount = overallMetrics["content_completed_count"]
        
        externalContentEnrolmentCount = externalMetrics["external_content_enrolment_count"]
        externalContentCompletedCount = externalMetrics["external_content_completed_count"]

        # Print statements (exact match to Scala)
        print("dashboard_completed_count:" + str(completedCount))
        print("dashboard_content_completed_count:" + str(contentCompletedCount))
        print("dashboard_external_content_completed_count:" + str(externalContentCompletedCount))
        print("dashboard_enrolment_count:" + str(enrolmentCount))
        print("dashboard_content_enrolment_count:" + str(contentEnrolmentCount))
        print("dashboard_external_content_enrolment_count:" + str(externalContentEnrolmentCount))

        # Redis updates (uncommented - exact match to original)
        # Redis.update("dashboard_unique_users_enrolled_count", str(enrolmentUniqueUserCount),config)
        # Redis.update("dashboard_unique_users_not_started_count", str(notStartedUniqueUserCount),config)
        # Redis.update("dashboard_unique_users_started_count", str(startedUniqueUserCount),config)
        # Redis.update("dashboard_unique_users_in_progress_count", str(inProgressUniqueUserCount),config)
        # Redis.update("dashboard_unique_users_completed_count", str(completedUniqueUserCount),config)

        # Redis.update("dashboard_enrolment_count", str(contentEnrolmentCount + externalContentEnrolmentCount),config)
        # Redis.update("dashboard_not_started_count", str(notStartedCount),config)
        # Redis.update("dashboard_started_count", str(startedCount),config)
        # Redis.update("dashboard_in_progress_count", str(inProgressCount),config)
        # Redis.update("dashboard_completed_count", str(contentCompletedCount + externalContentCompletedCount),config)
        # Redis.update("lp_completed_count", str(landingPageCompletedCount),config)
        
        # Redis.dispatchDataFrame("live_course_program_enrolment_count", liveCourseProgramEnrolmentCountsDF, "courseID", "enrolmentCount",config)

        # # MDO-wise Redis dispatches (all operations covered)
        # Redis.dispatchDataFrame("dashboard_enrolment_count_by_user_org", 
        #                        mdoWiseMetricsDF.select("userOrgID", F.col("course_enrolment_count").alias("count")), 
        #                        "userOrgID", "count",config)
        # Redis.dispatchDataFrame("dashboard_enrolment_content_by_user_org", 
        #                        mdoWiseMetricsDF.select("userOrgID", F.col("content_enrolment_count").alias("count")), 
        #                        "userOrgID", "count",config)
        # Redis.dispatchDataFrame("dashboard_enrolment_unique_user_count_by_user_org", 
        #                        mdoWiseMetricsDF.select("userOrgID", F.col("course_enrolment_unique_user_count").alias("uniqueUserCount")), 
        #                        "userOrgID", "uniqueUserCount",config)
        # Redis.dispatchDataFrame("dashboard_active_users_last_12_months_by_org", 
        #                        mdoWiseMetricsDF.select("userOrgID", F.col("active_users_last_12_months").alias("uniqueUserCount")), 
        #                        "userOrgID", "uniqueUserCount",config)
        # Redis.dispatchDataFrame("dashboard_not_started_count_by_user_org", 
        #                        mdoWiseMetricsDF.select("userOrgID", F.col("not_started_count").alias("count")), 
        #                        "userOrgID", "count",config)
        # Redis.dispatchDataFrame("dashboard_not_started_unique_user_count_by_user_org", 
        #                        mdoWiseMetricsDF.select("userOrgID", F.col("not_started_unique_user_count").alias("uniqueUserCount")), 
        #                        "userOrgID", "uniqueUserCount",config)
        # Redis.dispatchDataFrame("dashboard_started_count_by_user_org", 
        #                        mdoWiseMetricsDF.select("userOrgID", F.col("started_count").alias("count")), 
        #                        "userOrgID", "count",config)
        # Redis.dispatchDataFrame("dashboard_started_unique_user_count_by_user_org", 
        #                        mdoWiseMetricsDF.select("userOrgID", F.col("started_unique_user_count").alias("uniqueUserCount")), 
        #                        "userOrgID", "uniqueUserCount",config)
        # Redis.dispatchDataFrame("dashboard_in_progress_count_by_user_org", 
        #                        mdoWiseMetricsDF.select("userOrgID", F.col("in_progress_count").alias("count")), 
        #                        "userOrgID", "count",config)
        # Redis.dispatchDataFrame("dashboard_in_progress_unique_user_count_by_user_org", 
        #                        mdoWiseMetricsDF.select("userOrgID", F.col("in_progress_unique_user_count").alias("uniqueUserCount")), 
        #                        "userOrgID", "uniqueUserCount",config)
        # Redis.dispatchDataFrame("dashboard_completed_count_by_user_org", 
        #                        mdoWiseMetricsDF.select("userOrgID", F.col("completed_count").alias("count")), 
        #                        "userOrgID", "count",config)
        # Redis.dispatchDataFrame("dashboard_completed_unique_user_count_by_user_org", 
        #                        mdoWiseMetricsDF.select("userOrgID", F.col("completed_unique_user_count").alias("uniqueUserCount")), 
        #                        "userOrgID", "uniqueUserCount",config)

        # # CBP-wise Redis dispatches (all operations covered)
        # Redis.dispatchDataFrame("dashboard_content_completed_count_by_course_org", 
        #                        cbpWiseMetricsDF.select("courseOrgID", F.col("content_completed_count").alias("count")), 
        #                        "courseOrgID", "count",config)
        # Redis.dispatchDataFrame("dashboard_enrolment_count_by_course_org", 
        #                        cbpWiseMetricsDF.select("courseOrgID", F.col("course_enrolment_count").alias("count")), 
        #                        "courseOrgID", "count",config)
        # Redis.dispatchDataFrame("dashboard_course_moderated_course_enrolment_count_by_course_org", 
        #                        cbpWiseMetricsDF.select("courseOrgID", F.col("course_moderated_course_enrolment_count").alias("count")), 
        #                        "courseOrgID", "count",config)
        # Redis.dispatchDataFrame("dashboard_enrolment_content_by_course_org", 
        #                        cbpWiseMetricsDF.select("courseOrgID", F.col("content_enrolment_count").alias("count")), 
        #                        "courseOrgID", "count",config)
        # Redis.dispatchDataFrame("dashboard_enrolment_unique_user_count_by_course_org", 
        #                        cbpWiseMetricsDF.select("courseOrgID", F.col("course_enrolment_unique_user_count").alias("uniqueUserCount")), 
        #                        "courseOrgID", "uniqueUserCount",config)
        # Redis.dispatchDataFrame("dashboard_not_started_count_by_course_org", 
        #                        cbpWiseMetricsDF.select("courseOrgID", F.col("not_started_count").alias("count")), 
        #                        "courseOrgID", "count",config)
        # Redis.dispatchDataFrame("dashboard_not_started_unique_user_count_by_course_org", 
        #                        cbpWiseMetricsDF.select("courseOrgID", F.col("not_started_unique_user_count").alias("uniqueUserCount")), 
        #                        "courseOrgID", "uniqueUserCount",config)
        # Redis.dispatchDataFrame("dashboard_started_count_by_course_org", 
        #                        cbpWiseMetricsDF.select("courseOrgID", F.col("started_count").alias("count")), 
        #                        "courseOrgID", "count",config)
        # Redis.dispatchDataFrame("dashboard_started_unique_user_count_by_course_org", 
        #                        cbpWiseMetricsDF.select("courseOrgID", F.col("started_unique_user_count").alias("uniqueUserCount")), 
        #                        "courseOrgID", "uniqueUserCount",config)
        # Redis.dispatchDataFrame("dashboard_in_progress_count_by_course_org", 
        #                        cbpWiseMetricsDF.select("courseOrgID", F.col("in_progress_count").alias("count")), 
        #                        "courseOrgID", "count",config)
        # Redis.dispatchDataFrame("dashboard_in_progress_unique_user_count_by_course_org", 
        #                        cbpWiseMetricsDF.select("courseOrgID", F.col("in_progress_unique_user_count").alias("uniqueUserCount")), 
        #                        "courseOrgID", "uniqueUserCount",config)
        # Redis.dispatchDataFrame("dashboard_completed_count_by_course_org", 
        #                        cbpWiseMetricsDF.select("courseOrgID", F.col("completed_count").alias("count")), 
        #                        "courseOrgID", "count",config)
        # Redis.dispatchDataFrame("dashboard_completed_unique_user_count_by_course_org", 
        #                        cbpWiseMetricsDF.select("courseOrgID", F.col("completed_unique_user_count").alias("uniqueUserCount")), 
        #                        "courseOrgID", "uniqueUserCount",config)
        
        # # Certificate Redis dispatches
        # Redis.dispatchDataFrame("dashboard_certificates_generated_count_by_course_org", 
        #                        cbpWiseMetricsDF.select("courseOrgID", F.col("certificates_generated_count").alias("count")), 
        #                        "courseOrgID", "count",config)
        # Redis.dispatchDataFrame("dashboard_course_moderated_course_certificates_generated_count_by_course_org", 
        #                        cbpWiseMetricsDF.select("courseOrgID", F.col("course_moderated_course_certificates_generated_count").alias("count")), 
        #                        "courseOrgID", "count",config)
        
        # # Competencies dispatch
        # Redis.dispatchDataFrame("dashboard_competencies_count_by_course_org", topCoursesByOrgDF, "courseOrgID", "courseIDs",config)

        print("Complete DuckDB analytics executed successfully!")
    
    def nlwAnalyticsUpdateWithDuckDB(self, spark,config):
        
        # ==================== NLW Enrollment Metrics ====================
        print("Calculating NLW enrollment metrics...")
        
        # Event enrollments during NLW
        event_enrollments_result = self.duckdb_executor.execute_query(
            spark, "nlw_event_enrollments", QueryConstants.NLW_EVENT_ENROLLMENTS
        )
        enrolment_event_nlw_count = event_enrollments_result.collect()[0]['event_count']
        
        # Content enrollments during NLW  
        content_enrollments_result = self.duckdb_executor.execute_query(
            spark, "nlw_content_enrollments", QueryConstants.NLW_CONTENT_ENROLLMENTS
        )
        enrolment_content_nlw_count = content_enrollments_result.collect()[0]['content_count']
        
        # Total NLW enrollments
        total_enrollment_nlw_count = enrolment_event_nlw_count + enrolment_content_nlw_count
        # Redis.update("dashboard_content_enrolment_nlw_count", str(total_enrollment_nlw_count),config)
        
        # Total event enrollments (all time)
        total_event_enrollments_result = self.duckdb_executor.execute_query(
            spark, "total_event_enrollments", QueryConstants.TOTAL_EVENT_ENROLLMENTS
        )
        total_event_enrolment_count = total_event_enrollments_result.collect()[0]['total_event_count']
        # Redis.update("dashboard_events_enrolment_count", str(total_event_enrolment_count),config)
        print(f"dashboard_events_enrolment_count: {total_event_enrolment_count}")
        
        # ==================== Events Published ====================
        events_published_result = self.duckdb_executor.execute_query(
            spark, "events_published_count", QueryConstants.EVENTS_PUBLISHED_COUNT
        )
        events_published_count = events_published_result.collect()[0]['events_published_count']
        # Redis.update("dashboard_events_published_count", str(events_published_count),config)
        print(f"dashboard_events_published_count: {events_published_count}")
        
        # ==================== Certificate Generation - Yesterday ====================
        print("Calculating yesterday's certificate metrics...")
        
        # Content certificates generated yesterday
        content_certs_yesterday_result = self.duckdb_executor.execute_query(
            spark, "content_certificates_yesterday", QueryConstants.CONTENT_CERTIFICATES_YESTERDAY
        )
        certificate_generated_yday_count = content_certs_yesterday_result.collect()[0]['certificate_count']
        
        # Event certificates generated yesterday
        event_certs_yesterday_result = self.duckdb_executor.execute_query(
            spark, "event_certificates_yesterday", QueryConstants.EVENT_CERTIFICATES_YESTERDAY
        )
        event_certificate_generated_yday_count = event_certs_yesterday_result.collect()[0]['event_certificate_count']
        
        # Update Redis for yesterday's certificates
        # Redis.update("dashboard_event_certificates_generated_yday_nlw_count", str(event_certificate_generated_yday_count),config)
        # Redis.update("dashboard_content_only_certificates_generated_yday_nlw_count", str(certificate_generated_yday_count),config)
        
        # Total certificates yesterday
        total_certificates_generated_yday_count = certificate_generated_yday_count + event_certificate_generated_yday_count
        # Redis.update("dashboard_content_certificates_generated_yday_nlw_count", str(total_certificates_generated_yday_count),config)
        
        print(f"dashboard_event_certificates_generated_yday_nlw_count: {event_certificate_generated_yday_count}")
        print(f"dashboard_content_only_certificates_generated_yday_nlw_count: {certificate_generated_yday_count}")
        print(f"dashboard_content_certificates_generated_yday_nlw_count: {total_certificates_generated_yday_count}")
        
        # ==================== Certificate Generation - NLW ====================
        print("Calculating NLW certificate metrics...")
        
        # Content certificates generated during NLW
        content_certs_nlw_result = self.duckdb_executor.execute_query(
            spark, "content_certificates_nlw", QueryConstants.CONTENT_CERTIFICATES_NLW
        )
        certificate_generated_in_nlw_count = content_certs_nlw_result.collect()[0]['certificate_count']
        
        # Event certificates generated during NLW
        event_certs_nlw_result = self.duckdb_executor.execute_query(
            spark, "event_certificates_nlw", QueryConstants.EVENT_CERTIFICATES_NLW
        )
        event_certificate_generated_nlw_count = event_certs_nlw_result.collect()[0]['event_certificate_count']
        
        # Total certificates issued in NLW
        total_certificates_issued_in_nlw = certificate_generated_in_nlw_count + event_certificate_generated_nlw_count
        # Redis.update("dashboard_content_certificates_generated_nlw_count", str(total_certificates_issued_in_nlw),config)
        # Redis.update("dashboard_events_completed_count", str(event_certificate_generated_nlw_count),config)
        print(f"dashboard_events_completed_count: {event_certificate_generated_nlw_count}")
        print(f"dashboard_content_certificates_generated_nlw_count: {total_certificates_issued_in_nlw}")
        
        # ==================== NLW Events Published ====================
        events_published_nlw_result = self.duckdb_executor.execute_query(
            spark, "events_published_nlw", QueryConstants.EVENTS_PUBLISHED_NLW
        )
        events_published_in_nlw_count = events_published_nlw_result.collect()[0]['events_published_nlw_count']
        # Redis.update("dashboard_events_published_nlw_count", str(events_published_in_nlw_count),config)
        print(f"dashboard_events_published_nlw_count: {events_published_in_nlw_count}")
        
        # ==================== Certificates by User - NLW (Cache Operations) ====================
        print("Processing certificate data by user...")
        
        # Content certificates by user during NLW
        nlw_content_certs_by_user = self.duckdb_executor.execute_query(
            spark, "nlw_content_certs_by_user", QueryConstants.NLW_CONTENT_CERTIFICATES_BY_USER
        )
        self.cache.write(nlw_content_certs_by_user.coalesce(1), "nlwContentCertificateGeneratedCount")
        self.pq_cache.write(nlw_content_certs_by_user.coalesce(1), "nlwContentCertificateGeneratedCount")
        
        # Event certificates by user during NLW
        nlw_event_certs_by_user = self.duckdb_executor.execute_query(
            spark, "nlw_event_certs_by_user", QueryConstants.NLW_EVENT_CERTIFICATES_BY_USER
        )
        self.cache.write(nlw_event_certs_by_user.coalesce(1), "nlwEventCertificateGeneratedCount")
        self.pq_cache.write(nlw_event_certs_by_user.coalesce(1), "nlwEventCertificateGeneratedCount")
        
        # ==================== Learning Hours by User - NLW ====================
        print("Processing learning hours data...")
        
        # Event learning hours by user
        event_learning_hours = self.duckdb_executor.execute_query(
            spark, "nlw_event_learning_hours", QueryConstants.NLW_EVENT_LEARNING_HOURS_BY_USER
        )
        self.cache.write(event_learning_hours, "nlwEventLearningHours")
        self.pq_cache.write(event_learning_hours, "nlwEventLearningHours")
        
        # Content learning hours by user
        content_learning_hours = self.duckdb_executor.execute_query(
            spark, "nlw_content_learning_hours", QueryConstants.NLW_CONTENT_LEARNING_HOURS_BY_USER
        )
        self.cache.write(content_learning_hours, "nlwContentLearningHours")
        self.pq_cache.write(content_learning_hours, "nlwContentLearningHours")
        
        # ==================== Top Content by Completion (dispatchDataFrame) ====================
        print("Processing top content by completion...")
        
        # Top courses by completion
        top_courses = self.duckdb_executor.execute_query(
            spark, "top_courses_by_org", QueryConstants.TOP_COURSES_BY_COMPLETION_BY_ORG
        )
        
        # Top programs by completion
        top_programs = self.duckdb_executor.execute_query(
            spark, "top_programs_by_org", QueryConstants.TOP_PROGRAMS_BY_COMPLETION_BY_ORG
        )
        
        # Top assessments by completion
        top_assessments = self.duckdb_executor.execute_query(
            spark, "top_assessments_by_org", QueryConstants.TOP_ASSESSMENTS_BY_COMPLETION_BY_ORG
        )
        
        # Combine all top content
        combined_top_content = top_courses.union(top_programs).union(top_assessments)
        # Redis.dispatch("dashboard_top_10_courses_by_completion_by_course_org", 
                                    #    combined_top_content, "key", "sorted_courseIDs",config)
        
        # ==================== Average NPS ====================
        print("Calculating average NPS...")
        
        nps_result = self.duckdb_executor.execute_query(
            spark, "average_nps", QueryConstants.AVERAGE_NPS
        )
        avg_nps = nps_result.collect()[0]['avgNps'] if nps_result.count() > 0 else 0.0
        # Redis.update("dashboard_nps_across_platform", str(avg_nps),config)
        
        # ==================== Competency Coverage ====================
        print("Processing competency coverage...")
        
        competency_coverage = self.duckdb_executor.execute_query(
            spark, "competency_coverage", QueryConstants.COMPETENCY_COVERAGE_BY_ORG
        )
        # Redis.dispatch("dashboard_competency_coverage_by_org", 
                                    #    competency_coverage, "courseOrgID", "jsonData",config)
        
        # ==================== Monthly Active Users ====================
        print("Calculating monthly active users...")
        
        monthly_active_result = self.duckdb_executor.execute_query(
            spark, "monthly_active_users", QueryConstants.AVERAGE_MONTHLY_ACTIVE_USERS
        )
        average_monthly_active_users_count = monthly_active_result.collect()[0]['active_users_count']
        # Redis.update("dashboard_average_monthly_active_users_last_30_days", str(average_monthly_active_users_count),config)
        
        # ==================== Moderated Course Certificates ====================
        moderated_certs_result = self.duckdb_executor.execute_query(
            spark, "moderated_certificates_yesterday", QueryConstants.MODERATED_COURSE_CERTIFICATES_YESTERDAY
        )
        moderated_cert_count = moderated_certs_result.collect()[0]['certificate_count']
        # Redis.update("dashboard_course_moderated_course_certificates_generated_yesterday_count", str(moderated_cert_count),config)
        
        # ==================== Course Statistics ====================
        print("Processing course statistics...")
        
        # Courses enrolled in at least once
        courses_enrolled_result = self.duckdb_executor.execute_query(
            spark, "courses_enrolled", QueryConstants.COURSES_ENROLLED_AT_LEAST_ONCE
        )
        enrolled_data = courses_enrolled_result.collect()[0]
        courses_enrolled_count = enrolled_data['courses_enrolled_count']
        courses_enrolled_id_list = enrolled_data['course_id_list']
        
        # Redis.update("dashboard_courses_enrolled_in_at_least_once", str(courses_enrolled_count),config)
        # Redis.update("dashboard_courses_enrolled_in_at_least_once_id_list", courses_enrolled_id_list or "",config)
        
        # Courses completed at least once
        courses_completed_result = self.duckdb_executor.execute_query(
            spark, "courses_completed", QueryConstants.COURSES_COMPLETED_AT_LEAST_ONCE
        )
        completed_data = courses_completed_result.collect()[0]
        courses_completed_count = completed_data['courses_completed_count']
        courses_completed_id_list = completed_data['course_id_list']
        
        # Redis.update("dashboard_courses_completed_at_least_once", str(courses_completed_count),config)
        # Redis.update("dashboard_courses_completed_at_least_once_id_list", courses_completed_id_list or "",config)
        
        # ==================== MDO-wise Analytics ====================
        print("Processing MDO-wise analytics...")
        
        # Certificates generated by MDO
        certificates_by_mdo = self.duckdb_executor.execute_query(
            spark, "certificates_by_mdo", QueryConstants.CERTIFICATES_BY_MDO
        )
        # Redis.dispatch("dashboard_certificates_generated_count_by_user_org", 
                                    #    certificates_by_mdo, "userOrgID", "count",config)
        
        # Core competencies by MDO
        core_competencies_by_mdo = self.duckdb_executor.execute_query(
            spark, "core_competencies_by_mdo", QueryConstants.CORE_COMPETENCIES_BY_MDO
        )
        # Redis.dispatch("dashboard_core_competencies_by_user_org", 
                                    #    core_competencies_by_mdo, "userOrgID", "courseIDs",config)
        
        # Top 5 content by completion by org
        top_5_content = self.duckdb_executor.execute_query(
            spark, "top_5_content_by_completion", QueryConstants.TOP_5_CONTENT_BY_COMPLETION_BY_ORG
        )
        # Redis.dispatch("dashboard_top_5_content_by_completion_by_course_org", 
                                    #    top_5_content, "courseOrgID", "jsonData",config)
        
        # ==================== Content Ratings ====================
        print("Processing content ratings...")
        
        # Total ratings by org
        total_ratings = self.duckdb_executor.execute_query(
            spark, "total_ratings_by_org", QueryConstants.TOTAL_RATINGS_BY_ORG
        )
        # Redis.dispatch("dashboard_content_total_ratings_by_course_org", 
                                    #    total_ratings, "courseOrgID", "totalRatings",config)
        
        # Ratings spread by org
        ratings_spread = self.duckdb_executor.execute_query(
            spark, "ratings_spread_by_org", QueryConstants.RATINGS_SPREAD_BY_ORG
        )
        # Redis.dispatch("dashboard_content_ratings_spread_by_course_org", 
                                    #    ratings_spread, "courseOrgID", "jsonData",config)
        
        # ==================== Trending and Featured Events ====================
        print("Processing trending and featured events...")
        
        # Trending events by MDO
        trending_events = self.duckdb_executor.execute_query(
            spark, "trending_events_by_mdo", QueryConstants.TRENDING_EVENTS_BY_MDO
        )
        # Redis.dispatch("dashboard_trending_events_by_mdo", 
                                    #    trending_events, "userOrgID", "events",config)
        
        # Featured events overall
        featured_events_result = self.duckdb_executor.execute_query(
            spark, "featured_events_overall", QueryConstants.FEATURED_EVENTS_OVERALL
        )
        featured_events = featured_events_result.collect()[0]['events'] if featured_events_result.count() > 0 else ""
        # Redis.update("dashboard_overall_featured_events", featured_events,config)
        
        print("National Learning Week Analytics completed successfully!")

    def process_data(self, spark, config):
        try:
            today = self.get_date()
            currentDateTime = date_format(current_timestamp(), ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT)
            
            designationsDF = self.duckdb_executor.execute_query(spark,"designationsDF", QueryConstants.ORG_BASED_DESIGNATION_LIST)
            # Redis.dispatchDataFrame("dashboard_rolled_up_enrolment_content_count",designationsDF, "ministryID", "enrolmentCount",config)
            
            orgUserCountDF = self.duckdb_executor.execute_query(spark,"orgUserCountDF", QueryConstants.ORG_USER_COUNT_DATAFRAME_QUERY)
            orgRegisteredUserCountMap, orgTotalUserCountMap, orgNameMap=self.getOrgUserMaps(orgUserCountDF)
            import builtins

            activeOrgCount = len(orgNameMap)
            activeUserCount = builtins.sum(int(count) for count in orgRegisteredUserCountMap.values())
            
            # Redis.update(config.redisTotalRegisteredOfficerCountKey, str(activeUserCount))
            # Redis.update(config.redisTotalOrgCountKey, str(activeOrgCount))
            # Redis.dispatch(config.redisRegisteredOfficerCountKey, orgRegisteredUserCountMap,config)
            # Redis.dispatch(config.redisTotalOfficerCountKey, orgTotalUserCountMap,config)
            # Redis.dispatch(config.redisOrgNameKey, orgNameMap,config)

            top10LearnersByMDODF = self.duckdb_executor.execute_query(spark,"top10LearnersByMDODF", QueryConstants.TOP_10_LEARNERS_BY_MDO_QUERY)
            # Redis.dispatchDataFrame("dashboard_top_10_learners_on_kp_by_user_org",top10LearnersByMDODF, "userOrgID", "top_learners",config)


            self.dashboardRedisUpdates(spark,config)
            print("✅ Processing completed successfully!")

        except Exception as e:
            print(f"❌ Error occurred during DashboardSyncModel processing: {str(e)}")
            raise e
        finally:
            self.duckdb_executor.close()
            print("🔒 DuckDB connection closed")

def main():
    spark = SparkSession.builder \
        .appName("Dashboard Sync Model with DuckDB") \
        .config("spark.sql.shuffle.partitions", "32") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    config_dict = get_environment_config()
    config = create_config(config_dict)
    start_time = datetime.now()
    print(f"[START] DashboardSyncModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    model = DashboardSyncModel()
    model.process_data(spark, config)
    
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] DashboardSyncModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    
    spark.stop()

if __name__ == "__main__":
    main()
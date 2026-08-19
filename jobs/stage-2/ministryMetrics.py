import findspark
findspark.init()
import sys
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, countDistinct, count

from datetime import datetime
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))
from dfutil.utils import utils
from dfutil.utils.redis import Redis
from dfutil.utils import profiling
from jobs.default_config import create_config
from jobs.config import get_environment_config


from constants.ParquetFileConstants import ParquetFileConstants

JOB_NAME = "ministryMetrics"


class MinistryMetricsModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.MinistryMetricsModel"
        
    def name(self):
        return "MinistryMetricsModel"
    
    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")
    
    def process_data(self, spark,conf):
        try:
            print("📥 Loading base DataFrames...")
            enrolmentDF_path = ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE
            with profiling.phase(JOB_NAME, "read", "enrolmentDF", spark=spark) as m:
                enrolmentDF = spark.read.parquet(enrolmentDF_path)
                m["materialize"] = enrolmentDF
                m["input_mb"] = profiling.dir_size_mb(enrolmentDF_path)
            org_hierarchyDF_path = ParquetFileConstants.ORG_HIERARCHY_PARQUET_FILE
            with profiling.phase(JOB_NAME, "read", "org_hierarchyDF", spark=spark) as m:
                org_hierarchyDF = spark.read.parquet(org_hierarchyDF_path)
                m["materialize"] = org_hierarchyDF
                m["input_mb"] = profiling.dir_size_mb(org_hierarchyDF_path)
            ministryNamesDF = org_hierarchyDF.select(col("mdo_name").alias("ministry"), col("mdo_id").alias("ministryID"))
            userDF_path = ParquetFileConstants.USER_COMPUTED_PARQUET_FILE
            with profiling.phase(JOB_NAME, "read", "userDF", spark=spark) as m:
                userDF= spark.read.parquet(userDF_path) \
                .withColumnRenamed("userOrgID", "user_org_id") \
                          .withColumnRenamed("userID", "user_ID") \
                          .filter(col("userStatus") == 1)
                m["materialize"] = userDF
                m["input_mb"] = profiling.dir_size_mb(userDF_path)

            # Druid query for active users
            query = """SELECT DISTINCT(uid) as user_ID FROM "summary-events" WHERE dimensions_type='app' AND __time > CURRENT_TIMESTAMP - INTERVAL '24' HOUR"""
            with profiling.phase(JOB_NAME, "read", "usersLoggedInLast24HrsDF", spark=spark) as m:
                usersLoggedInLast24HrsDF = utils.druidDFOption(query, conf.sparkDruidRouterHost)
                m["materialize"] = usersLoggedInLast24HrsDF
            twentyFoutHrActiveUserDF = userDF.join(usersLoggedInLast24HrsDF, ["user_ID"], "inner")
            joined24HrActiveUserDF = twentyFoutHrActiveUserDF.join(
                org_hierarchyDF, 
                userDF["user_org_id"] == org_hierarchyDF["mdo_id"], 
                "left_outer"
            )

            # Active user count by ministry
            twentyFourHrActiveUserCountMinistryDF = (joined24HrActiveUserDF
                .groupBy("ministry")
                .agg(count("user_ID").alias("activeUserCount")))
            
            # Active user count by department
            twentyFourHrActiveUserCountDeptDF = (joined24HrActiveUserDF
                .groupBy("department")
                .agg(count("user_ID").alias("activeUserCount"))
                .select(col("department").alias("ministry"), col("activeUserCount")))
            
            # Active user count by organization
            twentyFourHrActiveUserCountOrgDF = (joined24HrActiveUserDF
                .groupBy("mdo_id")
                .agg(count("user_ID").alias("activeUserCount"))
                .select(col("mdo_id").alias("ministry"), col("activeUserCount")))
            
            # Union all active user counts
            twentyFourHrActiveUserCountDF = (twentyFourHrActiveUserCountMinistryDF
                .union(twentyFourHrActiveUserCountDeptDF)
                .union(twentyFourHrActiveUserCountOrgDF))
            
            # Join user and enrolment data
            joinUserDF = enrolmentDF.join(userDF, enrolmentDF["userID"] == userDF["user_ID"], "inner").drop(enrolmentDF["userID"])
            
            # Join with org_hierarchy data to get ministryID for all DF operations
            joinedWithMinistryIDDF = joinUserDF.join(
                org_hierarchyDF, 
                userDF["user_org_id"] == org_hierarchyDF["mdo_id"], 
                "left_outer"
            )
            
            # Certificate counts
            certificateMinistryDF = (joinedWithMinistryIDDF
                .groupBy("ministry")
                .agg(countDistinct("certificateID").alias("certificateCount")))
            
            certificateDeptDF = (joinedWithMinistryIDDF
                .groupBy("department")
                .agg(countDistinct("certificateID").alias("certificateCount"))
                .select(col("department").alias("ministry"), col("certificateCount")))
            
            certificateOrgDF = (joinedWithMinistryIDDF
                .groupBy("mdo_id")
                .agg(countDistinct("certificateID").alias("certificateCount"))
                .select(col("mdo_id").alias("ministry"), col("certificateCount")))
            
            certificateResultDF = certificateMinistryDF.union(certificateDeptDF).union(certificateOrgDF)
            
            # Enrolment counts
            enrolmentMinistrytDF = (joinedWithMinistryIDDF
                .groupBy("ministry")
                .agg(count(joinedWithMinistryIDDF["user_ID"]).alias("enrolmentCount")))
            
            enrolmentDeptDF = (joinedWithMinistryIDDF
                .groupBy("department")
                .agg(count("user_ID").alias("enrolmentCount"))
                .select(col("department").alias("ministry"), col("enrolmentCount")))
            
            enrolmentOrgDF = (joinedWithMinistryIDDF
                .groupBy("mdo_id")
                .agg(count("user_ID").alias("enrolmentCount"))
                .select(col("mdo_id").alias("ministry"), col("enrolmentCount")))
            
            enrolmentResultDF = enrolmentMinistrytDF.union(enrolmentDeptDF).union(enrolmentOrgDF)
            
            # User counts
            userCountMinistryDF = (userDF.join(org_hierarchyDF, userDF["user_org_id"] == org_hierarchyDF["mdo_id"], "left_outer")
                .groupBy("ministry")
                .agg(count("user_ID").alias("userCount")))
            
            userCountDeptDF = (userDF.join(org_hierarchyDF, userDF["user_org_id"] == org_hierarchyDF["mdo_id"], "left_outer")
                .groupBy("department")
                .agg(count("user_ID").alias("userCount"))
                .select(col("department").alias("ministry"), col("userCount")))
            
            userCountOrgDF = (userDF.join(org_hierarchyDF, userDF["user_org_id"] == org_hierarchyDF["mdo_id"], "left_outer")
                .groupBy("mdo_id")
                .agg(count("user_ID").alias("userCount"))
                .select(col("mdo_id").alias("ministry"), col("userCount")))
            
            userCountDF = userCountMinistryDF.union(userCountDeptDF).union(userCountOrgDF)
            
            # Final DataFrames with ministry IDs
            finalActiveUserCountDF = (twentyFourHrActiveUserCountDF
                .join(ministryNamesDF, ["ministry"], "inner")
                .select(col("ministryID"), coalesce(col("activeUserCount"), lit(0)).alias("activeUserCount")))
            
            finalCertificateCountDF = (certificateResultDF
                .join(ministryNamesDF, ["ministry"], "inner")
                .select(col("ministryID"), coalesce(col("certificateCount"), lit(0)).alias("certificateCount")))
            
            finalUserCountDF = (userCountDF
                .join(ministryNamesDF, ["ministry"], "inner")
                .select(col("ministryID"), coalesce(col("userCount"), lit(0)).alias("userCount")))
            
            finalEnrolmentCountDF = (enrolmentResultDF
                .join(ministryNamesDF, ["ministry"], "inner")
                .select(col("ministryID"), coalesce(col("enrolmentCount"), lit(0)).alias("enrolmentCount")))
            
            # finalActiveUserCountDF's lineage covers the 24-hour active-user branch:
            # the usersLoggedInLast24HrsDF/userDF/org_hierarchyDF joins, the three
            # ministry/department/org groupBys, their union, and the final join with
            # ministryNamesDF. None of that executes until forced - this phase's
            # materialize is what makes that real cost visible as "process" time
            # instead of silently landing inside the redis_write phase below.
            with profiling.phase(JOB_NAME, "process", "finalActiveUserCountDF", spark=spark) as m:
                m["materialize"] = finalActiveUserCountDF
            with profiling.phase(JOB_NAME, "redis_write", "finalActiveUserCountDF", spark=spark):
                Redis.dispatchDataFrame("dashboard_rolled_up_login_percent_last_24_hrs", finalActiveUserCountDF, "ministryID", "activeUserCount",conf=conf)

            # finalUserCountDF's lineage covers the user-count branch: the userDF/
            # org_hierarchyDF joins, the three ministry/department/org groupBys, their
            # union, and the final join with ministryNamesDF.
            with profiling.phase(JOB_NAME, "process", "finalUserCountDF", spark=spark) as m:
                m["materialize"] = finalUserCountDF
            with profiling.phase(JOB_NAME, "redis_write", "finalUserCountDF", spark=spark):
                Redis.dispatchDataFrame("dashboard_rolled_up_user_count", finalUserCountDF, "ministryID", "userCount",conf=conf)

            # finalCertificateCountDF's lineage covers the certificate-count branch:
            # the enrolmentDF/userDF/org_hierarchyDF joins (joinUserDF/
            # joinedWithMinistryIDDF), the three ministry/department/org groupBys,
            # their union, and the final join with ministryNamesDF.
            with profiling.phase(JOB_NAME, "process", "finalCertificateCountDF", spark=spark) as m:
                m["materialize"] = finalCertificateCountDF
            with profiling.phase(JOB_NAME, "redis_write", "finalCertificateCountDF", spark=spark):
                Redis.dispatchDataFrame("dashboard_rolled_up_certificates_generated_count", finalCertificateCountDF, "ministryID", "certificateCount",conf=conf)

            # finalEnrolmentCountDF's lineage covers the enrolment-count branch:
            # joinedWithMinistryIDDF (shared with the certificate branch above), the
            # three ministry/department/org groupBys, their union, and the final join
            # with ministryNamesDF.
            with profiling.phase(JOB_NAME, "process", "finalEnrolmentCountDF", spark=spark) as m:
                m["materialize"] = finalEnrolmentCountDF
            with profiling.phase(JOB_NAME, "redis_write", "finalEnrolmentCountDF", spark=spark):
                Redis.dispatchDataFrame("dashboard_rolled_up_enrolment_content_count",finalEnrolmentCountDF, "ministryID", "enrolmentCount",conf=conf)

        except Exception as e:
            print(f"❌ Error occurred during MinistryMetricsModel processing: {str(e)}")
            raise e
            sys.exit(1)

def main():
    # Initialize Spark Session with optimized settings for caching
    run_id = profiling.get_run_id()
    spark = SparkSession.builder \
        .appName(f'{JOB_NAME}_{run_id}') \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "20g") \
        .config("spark.driver.memory", "15g") \
        .config("spark.executor.memoryFraction", "0.7") \
        .config("spark.storage.memoryFraction", "0.2") \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", f"file://{profiling.event_log_dir()}") \
        .config("spark.eventLog.compress", profiling.event_log_compress()) \
        .getOrCreate()

    config_dict = get_environment_config()
    config = create_config(config_dict)
    start_time = datetime.now()
    print(f"[START] MinistryMetricsModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    model = MinistryMetricsModel()
    status, error_msg = "ok", None
    try:
        model.process_data(spark,config)
    except Exception as e:
        status, error_msg = "error", str(e)
        raise
    finally:
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"[END] MinistryMetricsModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[INFO] Total duration: {duration}")
        profiling.write_summary(JOB_NAME, duration.total_seconds(), status=status, error_msg=error_msg)
        spark.stop()

if __name__ == "__main__":
   main()
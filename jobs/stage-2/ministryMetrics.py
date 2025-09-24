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
from jobs.default_config import create_config
from jobs.config import get_environment_config


from constants.ParquetFileConstants import ParquetFileConstants

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
            enrolmentDF = spark.read.parquet(ParquetFileConstants.ENROLMENT_WAREHOUSE_COMPUTED_PARQUET_FILE)
            org_hierarchyDF = spark.read.parquet(ParquetFileConstants.ORG_HIERARCHY_PARQUET_FILE)
            ministryNamesDF = org_hierarchyDF.select(col("mdo_name").alias("ministry"), col("mdo_id").alias("ministryID"))
            userDF= spark.read.parquet(ParquetFileConstants.USER_COMPUTED_PARQUET_FILE) \
            .withColumnRenamed("userOrgID", "user_org_id") \
                      .withColumnRenamed("userID", "user_ID") \
                      .filter(col("userStatus") == 1)
            
            # Druid query for active users
            query = """SELECT DISTINCT(uid) as user_ID FROM "summary-events" WHERE dimensions_type='app' AND __time > CURRENT_TIMESTAMP - INTERVAL '24' HOUR"""
            usersLoggedInLast24HrsDF = utils.druidDFOption(query, conf.sparkDruidRouterHost)
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
            
            Redis.dispatchDataFrame("dashboard_rolled_up_login_percent_last_24_hrs", finalActiveUserCountDF, "ministryID", "activeUserCount",conf=conf)
            Redis.dispatchDataFrame("dashboard_rolled_up_user_count", finalUserCountDF, "ministryID", "userCount",conf=conf)
            Redis.dispatchDataFrame("dashboard_rolled_up_certificates_generated_count", finalCertificateCountDF, "ministryID", "certificateCount",conf=conf)
            Redis.dispatchDataFrame("dashboard_rolled_up_enrolment_content_count",finalEnrolmentCountDF, "ministryID", "enrolmentCount",conf=conf)

        except Exception as e:
            print(f"❌ Error occurred during MinistryMetricsModel processing: {str(e)}")
            raise e
            sys.exit(1)

def main():
    # Initialize Spark Session with optimized settings for caching
    spark = SparkSession.builder \
        .appName("Ministry Metrics") \
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
        .getOrCreate()
    
    config_dict = get_environment_config()
    config = create_config(config_dict)
    start_time = datetime.now()
    print(f"[START] MinistryMetricsModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    model = MinistryMetricsModel()
    model.process_data(spark,config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] MinistryMetricsModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()

if __name__ == "__main__":
   main()
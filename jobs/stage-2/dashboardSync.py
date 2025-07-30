import findspark
findspark.init()
import sys
from pathlib import Path
import pandas as pd
import duckdb
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import date_format, current_timestamp

from datetime import datetime
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))

from dfutil.utils.redis import Redis
from constants.ParquetFileConstants import ParquetFileConstants
from constants.QueryConstants import QueryConstants
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


    def process_data(self, spark, config):
        try:
            today = self.get_date()
            currentDateTime = date_format(current_timestamp(), ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT)
            
            designationsDF = self.duckdb_executor.execute_query(spark,"designationsDF", QueryConstants.ORG_BASED_DESIGNATION_LIST)
            # Redis.dispatchDataFrame("dashboard_rolled_up_enrolment_content_count",designationsDF, "ministryID", "enrolmentCount",config)
            
            orgUserCountDF = self.duckdb_executor.execute_query(spark,"orgUserCountDF", QueryConstants.ORG_USER_COUNT_DATAFRAME_QUERY)
            orgRegisteredUserCountMap, orgTotalUserCountMap, orgNameMap=self.getOrgUserMaps(orgUserCountDF)

            activeOrgCount = len(orgNameMap)
            activeUserCount = sum(int(count) for count in orgRegisteredUserCountMap.values())
            
            # Redis.update(config.redisTotalRegisteredOfficerCountKey, str(activeUserCount))
            # Redis.update(config.redisTotalOrgCountKey, str(activeOrgCount))
            # Redis.dispatch(config.redisRegisteredOfficerCountKey, orgRegisteredUserCountMap)
            # Redis.dispatch(config.redisTotalOfficerCountKey, orgTotalUserCountMap)
            # Redis.dispatch(config.redisOrgNameKey, orgNameMap)

            top10LearnersByMDODF = self.duckdb_executor.execute_query(spark,"top10LearnersByMDODF", QueryConstants.TOP_10_LEARNERS_BY_MDO_QUERY)
            # Redis.dispatchDataFrame("dashboard_top_10_learners_on_kp_by_user_org",top10LearnersByMDODF, "userOrgID", "top_learners",config)

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
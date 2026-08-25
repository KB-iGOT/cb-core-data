import uuid

import findspark

findspark.init()

import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.functions import (col, lit, current_date, lit, expr)
from pyspark.sql.functions import udf
from pyspark import StorageLevel
from datetime import datetime, timedelta
import sys
import os
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Reusable imports from userReport structure
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.utils import utils
from jobs.default_config import create_config
from jobs.config import get_environment_config



class NPSUpgradedModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.leaderboard.LearnerLeaderBoardModel"

    def name(self):
        return "NPSUpgradedModel"

    def process_data(self, spark, config):
        users_submitted_rejected_df = self.npsUpgradedTriggerC1DataFrame(spark, config)  # has userid who submitted/rejected in last 15 days
        users_enrolled_completed_df = self.npsUpgradedTriggerC2DataFrame(spark, config) # has userid who enrolled/completed
        users_rated_course_df = self.npsUpgradedTriggerC3DataFrame(spark, config) # has userid who rated


        # Eligible = (C2 ∪ C3) distinct userids
        df = users_enrolled_completed_df.select("userid").unionByName(
            users_rated_course_df.select("userid")
        ).dropDuplicates(["userid"]).na.drop(subset=["userid"])

        # Remove users who already submitted/rejected (C1)
        # Use set subtraction on the single key column to mirror Scala `except`
        filtered_df = df.join(users_submitted_rejected_df,
                                "userid",
                                "left_anti"
                                ).na.drop(subset=["userid"])


        # Check existing feed
        cassandra_df = self.userUpgradedFeedFromCassandraDataFrame(spark, config).select("userid").dropDuplicates(["userid"])

        store_to_cassandra_df = (filtered_df.join(cassandra_df,
                                                    "userid",
                                                    "left_anti")
                                                    )
        
        filtered_store_to_cassandra_df = (
            store_to_cassandra_df
            .filter(
                (col("userid").isNotNull()) &
                (col("userid") != "") &
                (col("userid") != "''")
            )
            .dropDuplicates(["userid"])
        )

        additional_df = (
            filtered_store_to_cassandra_df
            .withColumn("category", lit("NPS2"))
            .withColumn("id", expr("uuid()").cast(StringType()))
            .withColumn("createdby", lit("platform_rating"))
            .withColumn("createdon", current_date())
            .withColumn("action",
                        lit(f"{{\"dataValue\":\"yes\",\"actionData\":{{\"formId\":{config.platformRatingSurveyId}}}}}"))
            .withColumn("expireon", lit(None).cast(DateType()))
            .withColumn("priority", lit(1))
            .withColumn("status", lit("unread"))
            .withColumn("updatedby", lit(None).cast(StringType()))
            .withColumn("updatedon", lit(None).cast(DateType()))
            .withColumn("version", lit("v1"))
        ).persist(StorageLevel.MEMORY_AND_DISK)

        utils.writeToCassandra(additional_df, config.cassandraUserFeedKeyspace, config.cassandraUserFeedTable)
        utils.writeToCassandra(additional_df, "sunbird_notifications", "notification_feed_history")

        additional_df.unpersist()
        print("[SUCCESS] NpsUpgradeModel completed")


    def nps_userids_schema(self):
        return StructType([StructField("userid", StringType(), True)])

    def userUpgradedFeedFromCassandraDataFrame(self, spark, config):
        """Users already having NPS2 feed in user_feed table (Cassandra)."""
        df = utils.read_cassandra_table(spark, config.cassandraUserFeedKeyspace, config.cassandraUserFeedTable)\
            .select(col("userid").alias("userid"))\
            .where(col("category") == "NPS2")
        if df is None:
            return spark.createDataFrame([], self.nps_userids_schema())
        return df.na.drop(subset=["userid"])

    def npsUpgradedTriggerC1DataFrame(self, spark, config):
        """Users who saw the upgraded NPS popup in last 15 days (from Druid)."""
        query = """SELECT userID as userid FROM "nps-upgraded-users-data" WHERE __time >= CURRENT_TIMESTAMP - 
        INTERVAL '15' DAY"""
        df = utils.druidDFOption(query, config.sparkDruidRouterHost, limit=1000000, spark=spark)
        if df is None:
            return spark.createDataFrame([], self.nps_userids_schema())
        return df.na.drop(subset=["userid"])  # ensure clean ids

    def npsUpgradedTriggerC2DataFrame(self, spark, config):
        """Users enrolled/completed at least 1 course in last 15 days (from Cassandra)."""
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        fifteen_days_ago_start = today_start - timedelta(days=15)
        enrolment_df = spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE)\
            .filter((col("firstCompletedOn").between(lit(fifteen_days_ago_start), lit(today_start)))
                    | (col("courseEnrolledTimestamp").between(lit(fifteen_days_ago_start), lit(today_start)))
                    ).select("userid").distinct()
        return enrolment_df

    def npsUpgradedTriggerC3DataFrame(self, spark, config):
        """Users who rated at least one course in last 15 days (rating.createdon is timeuuid)."""

        def _timeuuid_to_millis(u: str) -> int:
            ts_100ns = uuid.UUID(u).time
            return (ts_100ns - 0x01b21dd213814000) // 10_000

        timeuuid_to_millis_udf = udf(_timeuuid_to_millis)

        today_start_ms = int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
        fifteen_days_ago_start_ms = int(
            (datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=15)).timestamp() * 1000)
        ratings_df = spark.read.parquet(ParquetFileConstants.RATING_PARQUET_FILE)\
            .filter(col("createdon").isNotNull())\
            .withColumn("rated_on", timeuuid_to_millis_udf(col("createdon")))\
            .where((col("rated_on") >= lit(fifteen_days_ago_start_ms)) & (col("rated_on") < lit(today_start_ms)))\
            .select("userid")\
            .distinct()

        return ratings_df

def create_spark_session_with_packages(config):
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,org.postgresql:postgresql:42.6.0 pyspark-shell'
    spark = SparkSession.builder \
        .appName("NPS Upgraded Model - Cached") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "16") \
        .config("spark.driver.memory", "40g") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .config("spark.cassandra.connection.host", config.sparkCassandraConnectionHost) \
        .config("spark.cassandra.connection.port", '9042') \
        .config("spark.cassandra.output.batch.size.rows", '10000') \
        .config("spark.cassandra.connection.keepAliveMS", "60000") \
        .config("spark.cassandra.connection.timeoutMS", '30000') \
        .config("spark.cassandra.read.timeoutMS", '30000') \
        .getOrCreate()
    return spark


def main():
    # Create model instance
    start_time = datetime.now()
    try:
        print(f"[START] NPS processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        config_dict = get_environment_config()
        config = create_config(config_dict)
        spark = create_spark_session_with_packages(config)
        model = NPSUpgradedModel()
        model.process_data(spark, config)
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"[END] NPS completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[INFO] Total duration: {duration}")
    except:
        print(f"[ERROR] NPS job failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

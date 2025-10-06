import findspark

findspark.init()

import time
from pyspark.sql import SparkSession
from datetime import datetime
from pathlib import Path
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
from pyspark.sql.functions import (col, lower, when, lit, expr, concat_ws, explode_outer, from_json, to_date,
                                   current_timestamp, date_format, round, coalesce, broadcast, size, map_keys,
                                   map_values)
from zipfile import ZipFile, ZIP_DEFLATED
import pyspark.sql.functions as F
import shutil
import subprocess
import sys
import os

sys.path.append(str(Path(__file__).resolve().parents[2]))

# Reusable imports from userReport structure
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.enrolment import enrolmentDFUtil
from dfutil.content import contentDFUtil
from dfutil.dfexport import dfexportutil
from jobs.config import get_environment_config
from jobs.default_config import create_config


class DataWarehouseModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report..DataWarehouse"

    def name(self):
        return "DataWarehouse"

    @staticmethod
    def get_date():
        """Get current date in required format"""
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def current_date_time():
        """Get current datetime in required format"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def process_data(self, spark, config):
        try:
            start_time = time.time()
            today = self.get_date()
            print("📊 Loading and filtering data...")
            spark = SparkSession.getActiveSession()
            spark.conf.set("spark.sql.adaptive.enabled", "true")
            spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
            warehouse_path = config.warehouseReportDir
            output_path = getattr(config, 'baseCachePath', '/home/analytics/pyspark/data-res/pq_files/cache_pq/')
            postgres_url = f"jdbc:postgresql://{config.dwPostgresHost}/{config.dwPostgresSchema}"
            today_date = datetime.today().strftime('%Y-%m-%d')

            userDetailsDF = spark.read.parquet(f"{warehouse_path}/{config.dwUserTable}") \
                .withColumn("status", col("status").cast("int")) \
                .withColumn("no_of_karma_points", col("no_of_karma_points").cast("int")) \
                .withColumn("marked_as_not_my_user", col("marked_as_not_my_user").cast("boolean")) \
                .withColumn("total_event_learning_hours", col("total_event_learning_hours").cast("double")) \
                .withColumn("total_content_learning_hours", col("total_content_learning_hours").cast("double")) \
                .withColumn("total_learning_hours", col("total_learning_hours").cast("double"))
            self.write_postgres_table(userDetailsDF, postgres_url, "user_detail", config.dwPostgresUsername,
                                      config.dwPostgresCredential)

            contentDF = spark.read.parquet(f"{warehouse_path}/{config.dwCourseTable}") \
                .withColumn("resource_count", col("resource_count").cast("int")) \
                .withColumn("total_certificates_issued", col("total_certificates_issued").cast("int")) \
                .withColumn("content_rating", col("content_rating").cast("float")) \
                .dropDuplicates(["content_id"])
            self.write_postgres_table(contentDF, postgres_url, "content", config.dwPostgresUsername,
                                      config.dwPostgresCredential)
            assessment = spark.read.parquet(f"{warehouse_path}/{config.dwAssessmentTable}") \
                .withColumn("score_achieved", col("score_achieved").cast("float")) \
                .withColumn("overall_score", col("overall_score").cast("float")) \
                .withColumn("cut_off_percentage", col("cut_off_percentage").cast("float")) \
                .withColumn("total_question", col("total_question").cast("int")) \
                .withColumn("number_of_incorrect_responses", col("number_of_incorrect_responses").cast("int")) \
                .withColumn("number_of_retakes", col("number_of_retakes").cast("int")) \
                .filter(col("content_id").isNotNull())
            self.write_postgres_table(assessment, postgres_url, "assessment_detail", config.dwPostgresUsername,
                                      config.dwPostgresCredential)
            bp_enrolments = spark.read.parquet(f"{warehouse_path}/{config.dwBPEnrollmentsTable}") \
                .withColumn("component_progress_percentage", col("component_progress_percentage").cast("float")) \
                .withColumn("offline_session_date", to_date(col("offline_session_date"))) \
                .withColumn("component_completed_on", to_date(col("component_completed_on"))) \
                .withColumn("last_accessed_on", to_date(col("last_accessed_on"))) \
                .withColumnRenamed("instructor(s)_name", "instructors_name") \
                .filter(col("content_id").isNotNull()) \
                .filter(col("user_id").isNotNull()) \
                .filter(col("batch_id").isNotNull())

            self.write_postgres_table(bp_enrolments, postgres_url, "bp_enrolments", config.dwPostgresUsername,
                                      config.dwPostgresCredential)
            content_resource = spark.read.parquet(f"{warehouse_path}/{config.dwContentResourceTable}")
            self.write_postgres_table(content_resource, postgres_url, "content_resource",
                                      config.dwPostgresUsername, config.dwPostgresCredential)

            cb_plan = spark.read.parquet(f"{warehouse_path}/{config.dwCBPlanTable}")
            self.write_postgres_table(cb_plan, postgres_url, "cb_plan", config.dwPostgresUsername,
                                      config.dwPostgresCredential)
            enrolments = spark.read.parquet(f"{warehouse_path}/{config.dwEnrollmentsTable}") \
                .withColumn("content_progress_percentage", col("content_progress_percentage").cast("float")) \
                .withColumn("user_rating", col("user_rating").cast("float")) \
                .withColumn("resource_count_consumed", col("resource_count_consumed").cast("int")) \
                .withColumn("live_cbp_plan_mandate", col("live_cbp_plan_mandate").cast("boolean")) \
                .filter(col("content_id").isNotNull())
            self.write_postgres_table(enrolments, postgres_url, "user_enrolments", config.dwPostgresUsername,
                                      config.dwPostgresCredential)
            org_hierarchy = spark.read.parquet(f"{output_path}/orgHierarchy") \
                .withColumn("mdo_created_on", to_date(col("mdo_created_on")).cast("string"))
            org_hierarchy.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(
                f"{config.warehouseReportDir}/{config.dwOrgTable}")
            self.write_postgres_table(org_hierarchy, postgres_url, "org_hierarchy", config.dwPostgresUsername,
                                      config.dwPostgresCredential)
            kcm_content = spark.read.parquet(f"{warehouse_path}/{config.dwKcmContentTable}") \
                .select("course_id", "competency_area_id", "competency_theme_id", "competency_sub_theme_id",
                        "data_last_generated_on")
            self.write_postgres_table(kcm_content, postgres_url, "kcm_content_mapping", config.dwPostgresUsername,
                                      config.dwPostgresCredential)
            kcm_dict = spark.read.parquet(f"{warehouse_path}/{config.dwKcmDictionaryTable}")
            self.write_postgres_table(kcm_dict, postgres_url, "kcm_dictionary", config.dwPostgresUsername,
                                      config.dwPostgresCredential)
            events = spark.read.parquet(f"{output_path}/eventDetails") \
                .select("event_id", "event_name", "event_provider_mdo_id", "event_start_datetime",
                        "duration", "event_status", "event_type", "presenters", "video_link", "recording_link",
                        "event_tag")
            self.write_postgres_table(events, postgres_url, "events", config.dwPostgresUsername,
                                      config.dwPostgresCredential)
            events.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(
                f"{config.warehouseReportDir}/event_details")
            
            eventEnrolmentsDF = spark.read.parquet(f"{output_path}/eventEnrolmentDetails")
            karmaPointsData = spark.read.parquet(f"{output_path}/userKarmaPoints")\
              .select(F.col("userid").alias("user_id"),\
               F.col("context_id").alias("event_id"),\
               F.col("points"))\
              .withColumn("points",\
               F.when(F.col("points").cast("int").isNotNull(), F.col("points").cast("int")).otherwise(F.lit(0)))\
              .groupBy("user_id", "event_id")\
              .agg(F.sum("points").alias("karma_points"))
            eventsEnrolmentDataDFWithKarmaPoints = eventEnrolmentsDF.join(karmaPointsData, ["user_id", "event_id"],
                                                                          "left")
            self.write_postgres_table(eventsEnrolmentDataDFWithKarmaPoints, postgres_url, "events_enrolment",
                                      config.dwPostgresUsername, config.dwPostgresCredential)
            eventsEnrolmentDataDFWithKarmaPoints.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(
                f"{config.warehouseReportDir}/event_enrolment_details")
            
            print("✅ Processing completed successfully!")


        except Exception as e:
            print(f"❌ Error: {str(e)}")
            raise

    def write_postgres_table(self, df, url: str, table: str, username: str, password: str, mode: str = "overwrite"):
        df.write \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", table) \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .mode(mode) \
            .save()


def create_spark_session_with_packages(config):
    # Set environment variables for PySpark to find packages
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,org.postgresql:postgresql:42.6.0 pyspark-shell'

    spark = SparkSession.builder \
        .appName('DataWarehousetModel') \
        .master("local[*]") \
        .config("spark.executor.memory", '42g') \
        .config("spark.driver.memory", '18g') \
        .config("spark.executor.memoryFraction", '0.7') \
        .config("spark.storage.memoryFraction", '0.2') \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", 'snappy') \
        .config("spark.sql.legacy.json.allowEmptyString.enabled", "true") \
        .config("spark.sql.caseSensitive", "true") \
        .config("spark.cassandra.connection.host", config.sparkCassandraConnectionHost) \
        .config("spark.cassandra.connection.port", '9042') \
        .config("spark.cassandra.output.batch.size.rows", '10000') \
        .config("spark.cassandra.connection.keepAliveMS", "60000") \
        .config("spark.cassandra.connection.timeoutMS", '30000') \
        .config("spark.cassandra.read.timeoutMS", '30000') \
        .config("es.nodes", config.sparkElasticsearchConnectionHost) \
        .config("es.port", config.sparkElasticsearchConnectionPort) \
        .config("es.index.auto.create", "false") \
        .config("es.nodes.wan.only", "true") \
        .config("es.nodes.discovery", "false") \
        .getOrCreate()

    return spark


def main():
    # Initialize Spark Session with optimized settings for caching
    config_dict = get_environment_config()
    config = create_config(config_dict)

    # Initialize Spark Session with optimizations from config
    spark = create_spark_session_with_packages(config)
    start_time = datetime.now()
    print(f"[START] DataWarehouse processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    model = DataWarehouseModel()
    model.process_data(spark, config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] DataWarehouse completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")


if __name__ == "__main__":
    main()

import findspark
findspark.init()

from datetime import datetime
from pathlib import Path
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, broadcast, coalesce, date_format, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,BooleanType,FloatType,TimestampType
from pyspark import StorageLevel
import traceback
import sys
sys.path.append(str(Path(__file__).resolve().parents[2]))
from dfutil.utils import utils
from dfutil.dfexport import dfexportutil
from dfutil.utils import profiling
from jobs.config import get_environment_config
from jobs.default_config import create_config
from constants.ParquetFileConstants import ParquetFileConstants

JOB_NAME = "userActivity"

class UserActivityModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.userActivity"
    
    def name(self):
        return "userActivity"
    
    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

    def processData(self,spark, config):
        today = self.get_date()
        with profiling.phase(JOB_NAME, "read", "organizationDF", spark=spark):
            organizationDF = spark.read.parquet(ParquetFileConstants.ORG_COMPUTED_PARQUET_FILE)
        with profiling.phase(JOB_NAME, "read", "userDF", spark=spark):
            userDF = spark.read.parquet(ParquetFileConstants.USER_COMPUTED_PARQUET_FILE)
        with profiling.phase(JOB_NAME, "read", "userOrgDF", spark=spark):
            userOrgDF = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
        with profiling.phase(JOB_NAME, "read", "orgHierarchyDF", spark=spark):
            orgHierarchyDF = spark.read.parquet(ParquetFileConstants.ORG_HIERARCHY_SELECT_PARQUET_FILE)
        with profiling.phase(JOB_NAME, "read", "contentDF", spark=spark):
            contentDF = spark.read.parquet(ParquetFileConstants.CONTENT_WAREHOUSE_COMPUTED_PARQUET_FILE).withColumnRenamed("content_id", "_id").withColumnRenamed("batch_id", "c_batch_id").drop("data_last_generated_on")
        with profiling.phase(JOB_NAME, "read", "enrollmentDF", spark=spark):
            enrollmentDF = spark.read.parquet(f"{config.warehouseReportDir}/{config.dwEnrollmentsTable}")
        with profiling.phase(JOB_NAME, "read", "eventDF", spark=spark):
            eventDF = spark.read.parquet(ParquetFileConstants.EVENT_PARQUET_FILE).withColumnRenamed("event_id", "ed_event_id")
        with profiling.phase(JOB_NAME, "read", "eventEnrolmentsDF", spark=spark):
            eventEnrolmentsDF = spark.read.parquet(ParquetFileConstants.EVENT_ENROLMENT_PARQUET_FILE)

        eventEnrolmentWithDetails = eventEnrolmentsDF.join(broadcast(eventDF), eventEnrolmentsDF["event_id"] == eventDF["ed_event_id"], how="left")

        userEventEnrolmentsDF = eventEnrolmentWithDetails.join(broadcast(userOrgDF),eventEnrolmentWithDetails["user_id"] == userOrgDF["userID"], how="left") \
            .withColumn("certificate_generated", when((col("certificate_id").isNotNull()) | (col("certificate_id")!=""),lit(True)).otherwise(lit(False))) \
            .withColumn("batch_id",lit("NA")) \
            .withColumn("enrolled_on",col("enrolled_on_datetime").cast("timestamp")) \
            .withColumn("resource_count_consumed",lit(1)) \
            .withColumn("user_rating",lit(0)) \
            .withColumn("live_cbp_plan_mandate",lit(False)) \
            .withColumn("data_last_generated_on",lit(date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss a"
            )).cast("timestamp")) \
            .withColumn("number_of_certificate",when((col("certificate_id").isNotNull()) | (col("certificate_id")!=""),lit(1)).otherwise(lit(0))) \
            .select(
                col("user_id"),
                col("batch_id"),
                col("event_id").alias("content_id"),
        col("event_type").alias("content_type"),
        col("enrolled_on"),
        col("completion_percentage").alias("content_progress_percentage"),
        col("resource_count_consumed"),
        col("status").alias("user_consumption_status"),
        col("completed_on_datetime").alias("first_completed_on"),
        col("completed_on_datetime").alias("first_certificate_generated_on"),
        col("completed_on_datetime").alias("last_completed_on"),
        col("completed_on_datetime").alias("last_certificate_generated_on"),
        col("completed_on_datetime").alias("content_last_accessed_on"),
        col("certificate_generated"),
        col("number_of_certificate"),
        col("user_rating"),
        col("certificate_id"),
        col("live_cbp_plan_mandate"),
        col("data_last_generated_on")
        ) \
            .dropDuplicates(["user_id", "content_id", "batch_id"])

        contentEnrolmentWithDetails = enrollmentDF.join(broadcast(contentDF), enrollmentDF["content_id"] == contentDF["_id"], how="left") \
            .withColumn("user_rating_cast", col("user_rating").cast("int")) \
            .withColumn("certificate_generated_cast", col("certificate_generated").cast("boolean")) \
        .select(
            col("user_id"),
            col("batch_id"),
            col("content_id"),
            col("content_type"),
            col("enrolled_on"),
            col("content_progress_percentage"),
            col("resource_count_consumed"),
            col("user_consumption_status"),
            col("first_completed_on"),
            col("first_certificate_generated_on"),
            col("last_completed_on"),
            col("last_certificate_generated_on"),
            col("content_last_accessed_on"),
            col("certificate_generated_cast").alias("certificate_generated"),
            col("number_of_certificate"),
            col("user_rating_cast").alias("user_rating"),
            col("certificate_id"),
            col("live_cbp_plan_mandate"),
            col("data_last_generated_on")
        ) \
            .dropDuplicates(["user_id", "batch_id", "content_id"])

        warehouseDF = contentEnrolmentWithDetails.union(userEventEnrolmentsDF)
        print("📦 Writing warehouse data...")
        with profiling.phase(JOB_NAME, "write", "warehouseDF", spark=spark):
            warehouseDF.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(f"{config.warehouseReportDir}/{config.dwUserActivityTable}")

        

def main():

    run_id = profiling.get_run_id()
    spark = SparkSession.builder \
        .appName(f'{JOB_NAME}_{run_id}') \
        .master("local[*]") \
        .config("spark.executor.memory", '15g') \
        .config("spark.driver.memory", '15g') \
        .config("spark.executor.memoryFraction", '0.7') \
        .config("spark.storage.memoryFraction", '0.2') \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", 'snappy') \
        .config("spark.sql.legacy.json.allowEmptyString.enabled", "true") \
        .config("spark.sql.caseSensitive", "true") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", f"file://{profiling.event_log_dir()}") \
        .config("spark.eventLog.compress", "true") \
        .getOrCreate()

    config_dict = get_environment_config()
    config = create_config(config_dict)

    print(f"Starting User Activity processing")

    model = UserActivityModel()
    output_path = getattr(config, 'baseCachePath', '/home/analytics/pyspark/data-res/pq_files/cache_pq/')
    start_time = datetime.now()

    print(f"[START] User Activity processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Output path: {output_path}")
    status, error_msg = "ok", None
    try:
        model.processData(spark,config)
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"[END] User Activity completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[INFO] Total duration: {duration}")
    except Exception as e:
        status, error_msg = "error", str(e)
        print(f"Error processing data: {e}")
        print(traceback.format_exc())
        end_time = datetime.now()
        duration = end_time - start_time
    finally:
        profiling.write_summary(JOB_NAME, duration.total_seconds(), status=status, error_msg=error_msg)
        spark.stop()

if __name__ == "__main__":
    main()
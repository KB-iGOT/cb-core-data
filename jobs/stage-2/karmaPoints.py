import findspark
findspark.init()

import uuid
import sys
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (StringType, LongType)

# ----- project imports -----
sys.path.append(str(Path(__file__).resolve().parents[2]))
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.utils import utils
from jobs.default_config import create_config
from jobs.config import get_environment_config

# UUID v1 epoch offset (100ns ticks between 1582-10-15 and 1970-01-01)
UUID_EPOCH_OFFSET = 0x01b21dd213814000


class KarmaPointsModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.KarmaPointsModel"

    def name(self):
        return "KarmaPointsModel"

    @staticmethod
    def timeuuid_to_millis(u: str) -> int:
        if u is None:
            return None
        try:
            ts_100ns = uuid.UUID(u).time
            return (ts_100ns - UUID_EPOCH_OFFSET) // 10_000  # -> milliseconds since Unix epoch
        except Exception:
            return None

    def process_data(self, spark, config):
        try:
            # ---------------- time window (previous month) ----------------
            ist = ZoneInfo("Asia/Kolkata")
            now_ist = datetime.now(ist)
            month_start = (now_ist.replace(day=1) - timedelta(days=1)).replace(day=1)  # first day of previous month
            month_end = now_ist.replace(day=1)  # first day of current month

            # epoch seconds/millis (if needed elsewhere)
            month_start_ms = int(month_start.timestamp() * 1000)
            month_end_ms = int(month_end.timestamp() * 1000)
            month_start_s = month_start_ms // 1000
            month_end_s = month_end_ms // 1000

            # timestamp bounds for filtering timestamp columns
            month_start_ts = F.to_timestamp(F.from_unixtime(F.lit(month_start_s)))
            month_end_ts = F.to_timestamp(F.from_unixtime(F.lit(month_end_s)))

            # ---------------- ratings -> karma (operation_type = RATING) ----------------
            timeuuid_to_millis_udf = F.udf(KarmaPointsModel.timeuuid_to_millis, LongType())

            course_rating_df = (
                spark.read.parquet(ParquetFileConstants.RATING_PARQUET_FILE)
                .withColumnRenamed("activityid", "courseID")
                .withColumnRenamed("userid", "userID")
                .withColumnRenamed("rating", "userRating")
                .withColumnRenamed("activitytype", "cbpType")
                # UUID v1 (ms) -> seconds -> timestamp (one shot)
                .withColumn(
                    "credit_date",
                    F.to_timestamp(
                        F.from_unixtime(
                            (timeuuid_to_millis_udf(F.col("createdOn")) / F.lit(1000))
                        )
                    )
                )
                # IMPORTANT: compare timestamp to timestamp bounds (no lit(...) around bounds)
                .where((F.col("credit_date") >= month_start_ts) & (F.col("credit_date") < month_end_ts))
            )

            categories = [
                "Course", "Program", "Blended Program", "CuratedCollections",
                "Standalone Assessment", "Curated Program"
            ]

            cbp_details = (
                spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE)
                .filter(F.col("category").isin(categories))
                .where(F.col("courseStatus").isin("Live", "Retired"))
                .select("courseID", "courseName", "category")
            )
            course_details = cbp_details.where(F.col("category") == F.lit("Course"))

            karma_from_rating_df = (
                course_rating_df
                .join(cbp_details, ["courseID"], "left")
                .withColumn("operation_type", F.lit("RATING"))
                .withColumn("COURSENAME", F.col("courseName"))
                .withColumn("addinfo", F.to_json(F.struct(F.col("COURSENAME"))))
                .withColumn("points", F.lit(2).cast("int"))
                .select(
                    F.col("courseID").alias("context_id"),
                    F.col("userID").alias("userid"),
                    F.col("category").alias("context_type"),
                    F.col("credit_date"),                 # TIMESTAMP
                    F.col("operation_type"),
                    F.col("addinfo"),
                    F.col("points"),
                )
            )

            # -------- course completions (first 4 per user) -> karma (COURSE_COMPLETION) --------
            course_completion_src = (
                spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE)
                # Here courseCompletedTimestamp must be TIMESTAMP (you were filtering with timestamp bounds already)
                .where(
                    (F.col("dbCompletionStatus") == F.lit(2)) &
                    (F.col("courseCompletedTimestamp") >= month_start_ts) &
                    (F.col("courseCompletedTimestamp") < month_end_ts)
                )
                .join(course_details, ["courseID"], "inner")
            )

            w_first4 = Window.partitionBy("userID").orderBy(F.col("courseCompletedTimestamp").asc())
            first_completion_df = (
                course_completion_src
                .withColumn("rowNum", F.row_number().over(w_first4))
                .where(F.col("rowNum") <= 4)
                .drop("rowNum")
            )

            courses_with_assessment_df = (
                spark.read.parquet(ParquetFileConstants.USER_ASSESSMENT_PARQUET_FILE)
                .where((F.col("assessUserStatus") == F.lit("SUBMITTED")) & F.col("assessChildID").isNotNull())
                .select("courseID").distinct()
                .withColumn("hasAssessment", F.lit(True))
            )

            karma_from_completion_df = (
                first_completion_df
                .join(courses_with_assessment_df, ["courseID"], "left")
                .na.fill(value=False, subset=["hasAssessment"])
                .withColumn("operation_type", F.lit("COURSE_COMPLETION"))
                .withColumn("COURSE_COMPLETION", F.lit(True))
                .withColumn("COURSENAME", F.col("courseName"))
                .withColumn("ACBP", F.lit(False))
                .withColumn("ASSESSMENT", F.col("hasAssessment"))
                .withColumn("addinfo", F.to_json(F.struct("COURSE_COMPLETION", "COURSENAME", "ACBP", "ASSESSMENT")))
                .withColumn("points", F.when(F.col("hasAssessment"), F.lit(10)).otherwise(F.lit(5)))
                .select(
                    F.col("courseID").alias("context_id"),
                    F.col("userID").alias("userid"),
                    F.col("category").alias("context_type"),
                    F.col("courseCompletedTimestamp").alias("credit_date"),   # TIMESTAMP
                    F.col("operation_type"),
                    F.col("addinfo"),
                    F.col("points"),
                )
            )

            # ---------------- union: both credit_date are TIMESTAMP already ----------------
            all_karma_points_df = (
                karma_from_rating_df.unionByName(karma_from_completion_df)
                # (no /1000 on timestamps, no post-union casts needed except optional type hygiene)
                .withColumn("context_id", F.col("context_id").cast(StringType()))
                .withColumn("userid", F.col("userid").cast(StringType()))
                .withColumn("context_type", F.col("context_type").cast(StringType()))
                .withColumn("operation_type", F.col("operation_type").cast(StringType()))
                .withColumn("addinfo", F.col("addinfo").cast(StringType()))
                .withColumn("points", F.col("points").cast("int"))
            )

            # -------- lookup --------
            lookup_df = (
                all_karma_points_df
                .select("userid", "context_type", "context_id", "operation_type", "credit_date")
                .withColumn("user_karma_points_key",
                            F.concat_ws("|", F.col("userid"), F.col("context_type"), F.col("context_id")))
                .drop("userid", "context_type", "context_id")
            )

            # -------- summary --------
            existing_summary_df = (
                spark.read.parquet(ParquetFileConstants.USER_KARMA_POINTS_SUMMARY_PARQUET_FILE)
                .select(F.col("userid"), F.col("total_points").alias("existing_total_points"))
            )

            summary_df = (
                all_karma_points_df
                .groupBy("userid").agg(F.sum(F.col("points")).alias("points"))
                .join(existing_summary_df, ["userid"], "full")
                .na.fill(0, subset=["existing_total_points", "points"])
                .withColumn("total_points", F.expr("existing_total_points + points"))
                .select("userid", "total_points")
            )

            utils.writeToCassandra(all_karma_points_df, config.cassandraUserKeyspace, config.cassandraKarmaPointsTable)
            utils.writeToCassandra(lookup_df, config.cassandraUserKeyspace, config.cassandraKarmaPointsLookupTable)
            utils.writeToCassandra(summary_df, config.cassandraUserKeyspace, config.cassandraKarmaPointsSummaryTable)
            print("[SUCCESS] KarmaPointsModel completed")

        except Exception as e:
            print(f"Error occurred during Karma points processing: {str(e)}")
            raise

def create_spark_session_with_packages(config):
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,org.postgresql:postgresql:42.6.0 pyspark-shell'

    spark = (SparkSession.builder
        .appName("Karma points Model - Cached")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.executor.memory", "18g")
        .config("spark.driver.memory", "18g")
        .config("spark.executor.memoryFraction", "0.7")
        .config("spark.storage.memoryFraction", "0.2")
        .config("spark.storage.unrollFraction", "0.1")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.cassandra.connection.host", config.sparkCassandraConnectionHost)
        .config("spark.cassandra.connection.port", '9042')
        .config("spark.cassandra.output.batch.size.rows", '10000')
        .config("spark.cassandra.connection.keepAliveMS", "60000")
        .config("spark.cassandra.connection.timeoutMS", '30000')
        .config("spark.cassandra.read.timeoutMS", '30000')
        .getOrCreate())
    return spark


def main():
    start_time = datetime.now()
    print(f"[START] Karma Points processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    config_dict = get_environment_config()
    config = create_config(config_dict)
    spark = create_spark_session_with_packages(config)
    model = KarmaPointsModel()
    model.process_data(spark, config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] Karma points completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()

if __name__ == "__main__":
    main()

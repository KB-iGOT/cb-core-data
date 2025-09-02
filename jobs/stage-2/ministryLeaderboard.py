import findspark

findspark.init()
import os
import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, ArrayType
from pyspark.sql.window import Window
from pyspark.sql.functions import (col, row_number, struct, lit, to_json, count, current_timestamp,
                                   current_date, date_format, broadcast, desc, unix_timestamp, when, sum, collect_list,
                                   max, lit, concat_ws, from_unixtime, format_string, expr, coalesce, dense_rank,
                                   add_months, last_day, date_trunc, last_day, date_add, date_format, lit, concat)
from datetime import datetime
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))

# Reusable imports from userReport structure
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.assessment import assessmentDFUtil
from dfutil.content import contentDFUtil
from dfutil.dfexport import dfexportutil
from dfutil.utils.utils import druidDFOption
from jobs.default_config import create_config
from jobs.config import get_environment_config


class MinistryLeaderBoardModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.leaderboard.MinistryLeaderBoardModel"

    def name(self):
        return "MinistryLeaderBoardModel"

    def process_data(self, spark, config):
        try:
            month_start = date_format(date_trunc("MONTH", add_months(current_date(), -1)), "yyyy-MM-dd HH:mm:ss")
            month_end = concat(date_format(last_day(add_months(current_date(), -1)), "yyyy-MM-dd"), lit(" 23:59:59"))

            month_num = date_format(date_add(last_day(add_months(current_date(), -1)), 1), "M")
            year_num  = date_format(add_months(current_date(), -1), "yyyy")
            #month_start, month_end, month_num, year_num = self._prev_month_window_ist()

            # --- Load core user/org datasets ---
            userOrgDF = spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
            orgHierarchyCompleteDF = spark.read.parquet(ParquetFileConstants.ORG_COMPLETE_HIERARCHY_PARQUET_FILE)
            distinctMdoIDsDF = userOrgDF.select("userOrgID").distinct()
            print(f"The number of distinct MDO ids is: {distinctMdoIDsDF.count()}")

            joinedDF = broadcast(orgHierarchyCompleteDF).join(
                distinctMdoIDsDF, orgHierarchyCompleteDF["sborgid"] == distinctMdoIDsDF["userOrgID"], "inner",)
            print(f"The number of distinct orgs in orgHierarchy is: {joinedDF.count()}")

            # --- Karma points for previous month ---
            karmaPointsDataDF = spark.read.parquet(ParquetFileConstants.USER_KARMA_POINTS_PARQUET_FILE)\
                .filter((col("credit_date") >= lit(month_start)) & (col("credit_date") <= lit(month_end)))\
                .groupBy(col("userid"))\
                .agg(sum(col("points")).alias("total_points"), max(col("credit_date")).alias("last_credit_date"),)\
                .cache()

            def process_orgs_l3(df, user_org_df):
                organisation_df = df.dropDuplicates()
                return (
                    user_org_df.join(
                        organisation_df,
                        user_org_df["userOrgID"] == organisation_df["organisationID"],
                        "inner",
                    )
                    .select(
                        col("userID"),
                        col("organisationID").alias("userParentID"),
                        col("professionalDetails.designation").alias("designation"),
                        col("userProfileImgUrl"),
                        col("fullName"),
                        col("userOrgName"),
                    )
                    .distinct()
                )

            def process_department_l2(df, user_org_df, orgHierarchyCompleteDF):
                organisation_df = (
                    df.join(
                        orgHierarchyCompleteDF,
                        df["departmentMapID"] == orgHierarchyCompleteDF["l2mapid"],
                        "inner",
                    )
                    .select(df["departmentID"], col("sborgid").alias("organisationID"))
                    .dropDuplicates()
                )
                return (
                    user_org_df.join(
                        organisation_df,
                        (user_org_df["userOrgID"] == organisation_df["departmentID"]) |
                        (user_org_df["userOrgID"] == organisation_df["organisationID"]),
                        "inner",
                    )
                    .select(
                        col("userID"),
                        col("departmentID").alias("userParentID"),
                        col("professionalDetails.designation").alias("designation"),
                        col("userProfileImgUrl"),
                        col("fullName"),
                        col("userOrgName"),
                    )
                    .distinct()
                )

            def process_ministry_l1(df, user_org_df, orgHierarchyCompleteDF):
                department_and_map_ids_df = (
                    df.join(
                        orgHierarchyCompleteDF,
                        df["ministryMapID"] == orgHierarchyCompleteDF["l1mapid"],
                        "left",
                    )
                    .select(
                        df["ministryID"],
                        col("sborgid").alias("departmentID"),
                        col("mapid").alias("departmentMapID"),
                    )
                )
                organisation_df = (
                    department_and_map_ids_df.join(
                        orgHierarchyCompleteDF,
                        department_and_map_ids_df["departmentMapID"] == orgHierarchyCompleteDF["l2mapid"],
                        "left",
                    )
                    .select(
                        department_and_map_ids_df["ministryID"],
                        department_and_map_ids_df["departmentID"],
                        col("sborgid").alias("organisationID"),
                    )
                    .dropDuplicates()
                )
                return (
                    user_org_df.join(
                        organisation_df,
                        (user_org_df["userOrgID"] == organisation_df["ministryID"]) |
                        (user_org_df["userOrgID"] == organisation_df["departmentID"]) |
                        (user_org_df["userOrgID"] == organisation_df["organisationID"]),
                        "inner",
                    )
                    .select(
                        col("userID"),
                        col("ministryID").alias("userParentID"),
                        col("professionalDetails.designation").alias("designation"),
                        col("userProfileImgUrl"),
                        col("fullName"),
                        col("userOrgName"),
                    )
                    .distinct()
                )

            # --- Build L1/L2/L3 frames (ministry/department/org) ---
            ministry_l1_df = joinedDF.filter(
                (col("sborgtype") == "ministry") | (col("sborgtype") == "state")
            ).select(col("sborgid").alias("ministryID"), col("mapid").alias("ministryMapID"))
            ministry_org_df = process_ministry_l1(ministry_l1_df, userOrgDF, orgHierarchyCompleteDF)

            department_l2_df = joinedDF.filter(col("sborgtype") == "department").select(
                col("sborgid").alias("departmentID"), col("mapid").alias("departmentMapID")
            )
            dept_org_df = process_department_l2(department_l2_df, userOrgDF, orgHierarchyCompleteDF)

            orgs_l3_df = joinedDF.filter(
                (col("sborgtype") == "mdo") & (col("sborgsubtype") != "department")
            ).select(col("sborgid").alias("organisationID"))
            orgs_df = process_orgs_l3(orgs_l3_df, userOrgDF)

            user_org_data = ministry_org_df.union(dept_org_df).union(orgs_df)

            # --- Join with karma points (inner) and prepare leaderboard ---
            user_leaderboard_df = (
                user_org_data.join(
                    karmaPointsDataDF,
                    user_org_data["userID"] == karmaPointsDataDF["userid"],
                    "inner",
                )
                .filter(col("userParentID") != "")
                .select(
                    user_org_data["userID"].alias("userid"),
                    user_org_data["userParentID"].alias("org_id"),
                    user_org_data["fullName"].alias("fullname"),
                    user_org_data["userProfileImgUrl"].alias("profile_image"),
                    user_org_data["userOrgName"].alias("org_name"),
                    user_org_data["designation"],
                    karmaPointsDataDF["total_points"],
                    karmaPointsDataDF["last_credit_date"],
                )
                .withColumn("month", month_num.cast("int") )
                .withColumn("year", year_num.cast("int"))
            )

            # --- Rankings within org ---
            window_spec_rank = Window.partitionBy("org_id").orderBy(desc("total_points"))
            user_leaderboard_df = user_leaderboard_df.withColumn(
                "rank", dense_rank().over(window_spec_rank)
            )

            window_spec_row = Window.partitionBy("org_id").orderBy(
                col("rank"), col("last_credit_date").desc()
            )
            user_leaderboard_df = user_leaderboard_df.withColumn(
                "row_num", row_number().over(window_spec_row)
            )

            # --- Write to Cassandra ---
           # self.writeToCassandra(user_leaderboard_df, config.cassandraUserKeyspace,
            #                      config.cassandraMDOLearnerLeaderboardTable,)
            self.writeToCassandra(user_leaderboard_df, config.cassandraUserKeyspace, "mdo_learner_leaderboard_pyspark_test")
            print("[SUCCESS] MinistryLearnerLeaderboardModel completed")

        except Exception as e:
            print(f"Error occurred during MinistryLearnerLeaderboard processing: {str(e)}")
            raise

    def writeToCassandra(self, df, keyspace: str, table: str, mode: str = "append"):
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", keyspace) \
            .option("table", table) \
            .mode(mode) \
            .save()


def create_spark_session_with_packages(config):
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,org.postgresql:postgresql:42.6.0 pyspark-shell'
    spark = SparkSession.builder \
        .appName("Ministry leaderboard Model - Cached") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "18g") \
        .config("spark.driver.memory", "18g") \
        .config("spark.executor.memoryFraction", "0.7") \
        .config("spark.storage.memoryFraction", "0.2") \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
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
    print(f"[START] Ministry leaderboard processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    config_dict = get_environment_config()
    config = create_config(config_dict)
    spark = create_spark_session_with_packages(config)
    model = MinistryLeaderBoardModel()
    model.process_data(spark, config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] Ministry leaderboard completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()


if __name__ == "__main__":
    main()
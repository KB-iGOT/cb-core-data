import findspark
findspark.init()
import sys
import redis
import json
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from datetime import datetime
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))
from dfutil.content import contentDFUtil
from constants.ParquetFileConstants import ParquetFileConstants
from jobs.config import get_environment_config
from jobs.default_config import create_config
from dfutil.utils import profiling

JOB_NAME = "userDataToRedis"


class UserDataToRedisModel:
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.UserDataToRedisModel"
        
    def name(self):
        return "UserDataToRedisModel"
    
    @staticmethod
    def get_date():
        """Get current date in required format"""
        return datetime.now().strftime("%Y-%m-%d")
    
    @staticmethod
    def current_date_time():
        """Get current datetime in required format"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def process_data(self, spark,config):
        """
        Process KCM data
        
        Args:
            spark: SparkSession
        """
        try:
            userOrgDF_path = ParquetFileConstants.USER_SELECT_PARQUET_FILE
            with profiling.phase(JOB_NAME, "read", "userOrgDF", spark=spark) as m:
                userOrgDF = spark.read.parquet(userOrgDF_path).select(
                    F.col("userID"),
                    F.col("firstName"),
                    F.col("userProfileImgUrl"),
                    F.col("userProfileStatus"),
                    F.col("professionalDetails.designation").alias("designation"),
                    F.col("employmentDetails.departmentName").alias("departmentName")
                )
                m["materialize"] = userOrgDF
                m["input_mb"] = profiling.dir_size_mb(userOrgDF_path)

            # Repartition the larger DataFrame to improve parallelism
            repartitioned_user_data = userOrgDF.repartition(500)
            
            redis_host = config.redisHost
            redis_port = config.redisPort
            
            def process_partition(partition_iter):
                    """Process each partition and write to Redis"""
                    # Create a new Redis connection for each partition
                    redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
                    pipeline = redis_client.pipeline()
                    command_count = 0
                    batch_size = 25000

                    for row in partition_iter:
                        user_id = row["userID"]
                        first_name = row["firstName"]
                        user_profile_img_url = row["userProfileImgUrl"]
                        designation = row["designation"]
                        user_profile_status = row["userProfileStatus"]
                        department_name = row["departmentName"]

                        # Construct Redis key and JSON value
                        redis_key = f"user:{user_id}"
                        redis_value = json.dumps({
                            "user_id": user_id,
                            "first_name": first_name,
                            "user_profile_img_url": user_profile_img_url,
                            "userProfileStatus": user_profile_status,
                            "designation": designation,
                            "department": department_name
                        })

                        # Queue the command in the pipeline
                        pipeline.set(redis_key, redis_value)
                        command_count += 1

                        # Execute pipeline commands after reaching batch size
                        if command_count >= batch_size:
                            pipeline.execute()
                            command_count = 0

                    # Execute any remaining commands
                    if command_count > 0:
                        pipeline.execute()

                    # Close Redis connection
                    redis_client.close()

                # Section to add User Details into Redis

                # Apply the function to each partition
            with profiling.phase(JOB_NAME, "redis_write", "userOrgDF", spark=spark):
                repartitioned_user_data.foreachPartition(process_partition)
        except Exception as e:
            print(f"Error occurred during UserDataToRedisModel processing: {str(e)}")
            sys.exit(1)
    
    
    
def main():
    run_id = profiling.get_run_id()
    spark =SparkSession.builder \
        .appName(f'{JOB_NAME}_{run_id}') \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "15g") \
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

    # Create model instance
    start_time = datetime.now()
    print(f"[START] UserDataToRedisModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    config_dict = get_environment_config()
    config = create_config(config_dict)
    model = UserDataToRedisModel()
    status, error_msg = "ok", None
    try:
        model.process_data(spark,config)
    except SystemExit as e:
        status, error_msg = "error", f"SystemExit: {e.code}"
        raise
    except Exception as e:
        status, error_msg = "error", str(e)
        raise
    finally:
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"[END] UserDataToRedisModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[INFO] Total duration: {duration}")
        profiling.write_summary(JOB_NAME, duration.total_seconds(), status=status, error_msg=error_msg)
    spark.stop()

if __name__ == "__main__":
    main()
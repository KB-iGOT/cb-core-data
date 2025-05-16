from pyspark.sql import SparkSession
from datetime import datetime, date, timedelta
from pyspark.sql.functions import col, current_date, expr, lit
from pyspark.sql.types import StringType
import uuid
import uuid
import os
from datetime import datetime

def end_of_week(today):
    """
    Calculate the end of the week (Sunday)
    
    Args:
        today: date object representing today's date
    
    Returns:
        date object representing the end of the current week (Sunday)
    """
    # Python's weekday() returns 0 for Monday, 6 for Sunday
    # We need to calculate days to add to reach Sunday
    days_to_add = 6 - today.weekday()
    return today + timedelta(days=days_to_add)

def end_of_day(date_obj):
    """
    Get the end of the day (23:59:59.999999)
    
    Args:
        date_obj: date object
    
    Returns:
        datetime object representing the end of the day
    """
    # Create datetime for start of next day
    start_of_next_day = datetime.combine(date_obj + timedelta(days=1), datetime.min.time())
    # Subtract one microsecond to get end of current day
    # (Python's datetime has microsecond precision, not nanosecond)
    return start_of_next_day - timedelta(microseconds=1)


def read_parquet_file(spark, file_path):
    """
    Read data from a Parquet file
    
    Args:
        spark: SparkSession
        file_path: Path to the Parquet file
    
    Returns:
        DataFrame with the Parquet data
    """
    # Read the Parquet file
    df = spark.read.parquet(file_path)
    return df

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("ParquetReader") \
        .getOrCreate()
    
    try:
        # Path to your Parquet file
        parquet_path = "/Users/rajeevsathish/KB/projects/cb-core-data/output/weeklyClaps_combined_data.parquet"

         # Read the Parquet file
        userDF = read_parquet_file(spark, parquet_path)
        
        # Today's date
        today_date = date.today()
        
        # Calculate expireOn date (end of week)
        expire_on_date = end_of_week(today_date)
        #print(f"Expire on date {expire_on_date}")

        # Convert expireOn date to epoch milliseconds
        expire_on_datetime = end_of_day(expire_on_date)
        
        # Convert to UTC timestamp in milliseconds
        expire_on_epoch_ms = int(expire_on_datetime.timestamp() * 1000)
        #print(f"Expire on epochms {expire_on_epoch_ms}") 
        # Filter the DataFrame
        filtered_df = userDF.filter(
            col("claps_updated_this_week") & 
            (expr("date_format(last_claps_updated_on, 'yyyy-MM-dd')") == current_date())
        ).select("userid")
        # Add required columns for feed data
        result_df = filtered_df.withColumn("expireon", lit(expire_on_epoch_ms)) \
            .withColumn("category", lit("InAppReview")) \
            .withColumn("id", expr("uuid()").cast(StringType())) \
            .withColumn("createdby", lit("weekly_claps")) \
            .withColumn("createdon", current_date()) \
            .withColumn("action", lit("{}")) \
            .withColumn("priority", lit(1)) \
            .withColumn("status", lit("unread")) \
            .withColumn("updatedby", lit(None).cast(StringType())) \
            .withColumn("updatedon", lit(None).cast("date")) \
            .withColumn("version", lit("v1"))
        #TODO NEED TO ADD THE CASSANDRA DB
        # Write the dataframe to cassandra user_feed table
        # result_df.write \
        #     .format("org.apache.spark.sql.cassandra") \
        #     .options(keyspace='sunbird_notifications', 
        #             table='notification_feed') \
        #     .mode("append") \
        #     .save()
    except Exception as e:
        print(f"Error processing Parquet file: {str(e)}")
    finally:
        # Stop the Spark session
        spark.stop()

if __name__ == "__main__":
    main()
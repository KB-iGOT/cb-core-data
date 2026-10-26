import findspark
findspark.init()

from pathlib import Path
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import current_timestamp,  lit, col, max
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime
import time
import sys
import os
import requests
import json
import psycopg2
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Reusable imports from userReport structure
from jobs.default_config import create_config
from jobs.config import KAFKA_CONFIG, get_environment_config

class PeerValidationNotificationSender:
    def __init__(self,spark: SparkSession, config):
        self.spark = spark
        self.config = config
        self.class_name = "org.ekstep.analytics.dashboard.report.PeerValidationNotificationSender"

    def name(self):
        return "PeerValidationNotificationSender"

    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

    def write_postgres_table(self, df, table: str, mode: str = "overwrite"):
            postgres_url = f"jdbc:postgresql://{self.config.dwPostgresHost}/{self.config.dwPostgresSchema}"
            df.write \
                .format("jdbc") \
                .option("url", postgres_url) \
                .option("dbtable", table) \
                .option("user", self.config.dwPostgresUsername) \
                .option("password", self.config.dwPostgresCredential) \
                .option("driver", "org.postgresql.Driver") \
                .mode(mode) \
                .save()
            
    def read_postgres_table(self, table: str) -> "DataFrame":
        """Read data from PostgreSQL table"""
        postgres_url = f"jdbc:postgresql://{self.config.dwPostgresHost}/{self.config.dwPostgresSchema}"

        return self.spark.read \
            .format("jdbc") \
            .option("url", postgres_url) \
            .option("dbtable", table) \
            .option("user", self.config.dwPostgresUsername,) \
            .option("password", self.config.dwPostgresCredential) \
            .option("driver", "org.postgresql.Driver") \
            .load()

    def get_pg_connection(self):
        host, port = self.config.dwPostgresHost.split(":")
        return psycopg2.connect(
            host=host,
            port=port,
            database=self.config.dwPostgresSchema,
            user=self.config.dwPostgresUsername,
            password=self.config.dwPostgresCredential
        )

    # --- FIX: this now takes a plain Python list of dicts for ONE batch,
    # not a Spark DataFrame for the whole run. The old version relied on the
    # caller collecting every processed row (across the entire pending queue)
    # into a single Spark DataFrame before calling this — that unbounded
    # accumulation is what caused the driver OOM. Called once per batch now,
    # so memory use is bounded by batch_size, not by total pending rows.
    def update_notification_status_batch(self, conn, batch_results):
        """Update status and error_message columns for one batch of notifications."""
        if not batch_results:
            return
        cursor = conn.cursor()
        try:
            for r in batch_results:
                cursor.execute(
                    f"""
                        UPDATE {self.config.dwpeerValidationNotificationQueue}
                        SET status = %s,
                            error_message = %s,
                            updated_at = NOW()
                        WHERE notification_id = %s
                    """,
                    (r["status"], r["error_message"], r["notification_id"])
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Could not update notification status batch: {str(e)}")
            raise
        finally:
            cursor.close()

    # --- FIX: replaces the old read -> left_anti join -> union -> overwrite pattern.
    # That pattern re-read dw_peer_validation_form_state from scratch on every
    # Spark action (.count(), .show(), the write itself), because the DataFrame
    # was never actually cached. Each of those reads could race against the
    # overwrite's own TRUNCATE, so the "existing minus processed" join sometimes
    # saw an empty/partial table and the final write wiped out other forms'
    # state instead of only updating the ones just processed.
    #
    # This does a plain row-level UPSERT instead: no full-table read, no
    # overwrite, no race condition. Only the forms in latestProcessedDF are
    # touched; every other row in the table is left alone by construction.
    #
    # Uses ON CONFLICT now that dw_peer_validation_form_state has a unique
    # constraint on form_id. Single atomic upsert per row; only the forms in
    # latestProcessedDF are touched, everything else is left alone.
    def update_form_state(self, latestProcessedDF):
        """Upsert last_processed_date per form_id without touching other rows."""
        rows = latestProcessedDF.collect()
        if not rows:
            return

        conn = self.get_pg_connection()
        cursor = conn.cursor()
        try:
            for row in rows:
                cursor.execute(f"""
                    INSERT INTO {self.config.dwpeerValidationFormStateTable}
                        (form_id, last_processed_date, data_generated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (form_id) DO UPDATE
                    SET last_processed_date = EXCLUDED.last_processed_date,
                        data_generated_at = NOW()
                """, (row.form_id, row.last_processed_date))
            conn.commit()
            print(f"[INFO] Updated form state for {len(rows)} forms")
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Could not update form state: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()

    def send_notification(self):
        try:
            pendingNotificationDF = self.read_postgres_table(self.config.dwpeerValidationNotificationQueue) \
            .filter(col("status") == "PENDING")

            if pendingNotificationDF.count() == 0:
                print("[INFO] No pending notifications.")
                return
            
            api_enabled = self.config.apiBasedNotificationEnabled
            api_url = self.config.notificationAPIURL
            batch_size = self.config.notificationBatchSize

            # --- FIX: no more unbounded `results = []` accumulating every row
            # in the whole pending queue. We now update Postgres per batch as
            # we go, and only keep small running aggregates in the driver:
            #   - form_last_trigger_end: max first_trigger_end per form, SENT only
            #   - form_sent_count: count of SENT rows per form
            # Both are tiny (one entry per form_id), regardless of how many
            # notifications are processed, so driver memory stays bounded.
            form_last_trigger_end = {}   # form_id -> max first_trigger_end among SENT rows
            form_sent_count = {}         # form_id -> count of SENT rows
            total_sent = 0

            session = requests.Session()
            conn = self.get_pg_connection()
            batch = []

            def flush_batch(batch_rows):
                """Send a batch of notifications, update Postgres, and update running aggregates."""
                nonlocal total_sent
                if not batch_rows:
                    return

                payload_list = []
                for row in batch_rows:
                    try:
                        payload_dict = json.loads(row.payload)
                    except Exception:
                        payload_dict = {}
                    payload_list.append(payload_dict)

                # Send batch API call
                try:
                    if api_enabled:
                        response = session.post(
                            api_url,
                            json={"request": payload_list},
                            timeout=60
                        )
                        status_code = response.status_code
                        resp_text = response.text
                    else:
                        status_code = 200
                        resp_text = "Saved locally, API disabled."
                except Exception as e:
                    status_code = 500
                    resp_text = str(e)

                status = "SENT" if status_code == 200 else "FAILED"

                # Build the small per-batch update list (bounded by batch_size)
                batch_results = []
                for row in batch_rows:
                    batch_results.append({
                        "notification_id": row.notification_id,
                        "status": status,
                        "error_message": resp_text,
                    })
                    if status == "SENT":
                        form_id = row.form_id
                        current_max = form_last_trigger_end.get(form_id)
                        if current_max is None or (row.first_trigger_end is not None and row.first_trigger_end > current_max):
                            form_last_trigger_end[form_id] = row.first_trigger_end
                        form_sent_count[form_id] = form_sent_count.get(form_id, 0) + 1
                        total_sent += 1

                # Push this batch's status updates to Postgres immediately —
                # nothing from this batch is kept in memory afterward.
                self.update_notification_status_batch(conn, batch_results)

            try:
                # toLocalIterator — streams one partition at a time, not all at once
                for row in pendingNotificationDF.toLocalIterator():
                    batch.append(row)
                    if len(batch) >= batch_size:
                        flush_batch(batch)
                        batch = []
                if batch:
                    flush_batch(batch)  # remaining rows
            finally:
                session.close()
                conn.close()

            if total_sent > 0:
                # Build the tiny per-form state DataFrame directly from the
                # driver-side dict — no giant createDataFrame from raw results.
                state_rows = [
                    Row(form_id=form_id, last_processed_date=last_trigger_end)
                    for form_id, last_trigger_end in form_last_trigger_end.items()
                ]
                latestProcessedDF = self.spark.createDataFrame(state_rows)

                # --- FIX: single safe upsert call replaces the old
                # read -> left_anti join -> union -> overwrite block.
                self.update_form_state(latestProcessedDF)

                kafka_rows = [
                    Row(
                        eventType="PEER_SURVEY_NOTIFICATION_SENT",
                        formId=form_id,
                        incrementBy=count,
                        timestamp=int(time.time() * 1000)
                    )
                    for form_id, count in form_sent_count.items()
                ]
                kafkaDF = self.spark.createDataFrame(kafka_rows)
                from dfutil.utils.utils import dispatch_df_to_kafka
                dispatch_df_to_kafka(kafkaDF, self.config.peerValidationKafkaTopic, broker_list=self.config.kpBrokerList)

                print(f"[INFO] {total_sent} notifications sent successfully")

        except Exception as e:
            print(f"❌ Error: {str(e)}")
            raise


def main():
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,org.postgresql:postgresql:42.6.0 pyspark-shell'
    # Initialize Spark Session with optimized settings for caching
    spark = SparkSession.builder \
        .appName("Peer Validation Notification Sender Model") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "18g") \
        .config("spark.driver.memory", "18g") \
        .config("spark.driver.maxResultSize", "3g") \
        .config("spark.executor.memoryFraction", "0.7") \
        .config("spark.storage.memoryFraction", "0.2") \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    # Create model instance
    start_time = datetime.now()
    print(f"[START] Peer Validation Notification Sender processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    config_dict = get_environment_config()
    config = create_config(config_dict)
    model = PeerValidationNotificationSender(spark, config)
    model.send_notification()
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] Peer Validation Notification Sender completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()

if __name__ == "__main__":
    main()

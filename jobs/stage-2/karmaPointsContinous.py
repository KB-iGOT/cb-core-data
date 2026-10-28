"""
Karma Points -- CONTINUOUS event builder
========================================

Run on a schedule (e.g. daily / weekly). Builds Kafka event records for
activity that qualified in the last LOOKBACK_DAYS days and leaves them in
`events_df` for you to push to Kafka.

KPIs covered (the ones that need continuous awarding):
    2.4 ENGAGEMENT_STREAK   -- a 4-consecutive-week block whose 4th week was
                               met inside the lookback window
    2.6 COURSE_TIME_SPENT   -- course completed inside the lookback window
                               AND >= 50% of content duration spent

2.6 has NO cap: every completion from yesterday with >= 50% of the content
duration spent gets an event. Time spent is looked up in Druid (last 90 days)
for yesterday's completions only.

Runs daily: LOOKBACK_DAYS = 1 covers yesterday only. If a run is missed, set
LOOKBACK_DAYS to the number of days to catch up for that one run (the window
always ends yesterday). A larger value than the run interval emits the same
event on more than one run, so the consumer must be idempotent.

NOTE: 2.6 fetches time spent straight from Druid on every run, so there is no
separate CSV refresh. An audit copy is written to TIME_SPENT_AUDIT_DIR.
At the end the events are pushed to Kafka via dispatch_events;
each day / event type has its own run_path, so re-running the same day does not
re-send batches that already went out.
"""

import re
import sys
from pathlib import Path
import findspark
findspark.init()

sys.path.append(str(Path(__file__).resolve().parents[2]))   # finds `jobs` / `dfutil`
sys.path.append(str(Path(__file__).resolve().parent))       # finds karma_kafka_dispatch.py

import requests

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from jobs.default_config import create_config
from jobs.config import get_environment_config
from karma_kafka_dispatch import dispatch_events, STREAK_SCHEMA, TIME_SPENT_SCHEMA

spark = (
    SparkSession.builder
    .appName("Karma Continuous Events")
    .config("spark.driver.memory", "90g")
    .config("spark.executor.memory", "120g")
    .config("spark.sql.shuffle.partitions", "400")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
BASE_PATH = "/home/analytics/pyspark/warehouse"
STREAK_START_DATE = "2026-10-19"   # Monday = week 1 of the first streak; earlier completions ignored
LOOKBACK_DAYS = 1            # daily run -> yesterday only
TIME_SPENT_NONAPAR_CAP = None  # 2.6 -- no cap: every qualifying completion gets an event
# False: look up time spent ONLY for the completions in the run window (yesterday).
#        The cap then applies within that window only -- earlier days of the month
#        are not looked at, so the 4-per-month limit is NOT enforced across days.
# True : also look up the month's earlier completions so the cap counts the whole month
#        (many more Druid lookups).
MONTHLY_CAP_ACROSS_DAYS = False

# Druid (time spent is fetched live inside this script -- no CSV step)
DRUID_SQL_URL = "http://10.175.5.33:8888/druid/v2/sql"   # <-- fill in
DRUID_AUTH = None                 # e.g. ("user", "password") if your broker needs it
DRUID_TIME_FLOOR_DAYS = 90        # Druid keeps only 90 days of data -> never query beyond that
DRUID_CHUNK_PAIRS = 400           # (user, content) pairs per Druid query
TIME_SPENT_AUDIT_DIR = "/tmp/timeSpentDetails"   # overwritten every run (audit copy)

# Daily run: only YESTERDAY's activity (a full, finished day), so an event is
# never sent twice (today is partial and would be picked up again tomorrow).
window_start = F.date_sub(F.current_date(), LOOKBACK_DAYS)
window_end = F.date_sub(F.current_date(), 1)

# ---------------------------------------------------------------------
# SOURCES
# ---------------------------------------------------------------------
userEnrolmentDF = spark.read.parquet(f"{BASE_PATH}/user_enrolments")
contentDF       = spark.read.parquet(f"{BASE_PATH}/content")


# ---------------------------------------------------------------------
# HELPERS (same as the retro script)
# ---------------------------------------------------------------------
def build_events(df, event_type, edata_fields, version=1, keep_nulls=False):
    """df must have a user_id column. edata_fields = list of Columns, each
    aliased to the exact JSON key name expected inside data.edata."""
    json_opts = {"ignoreNullFields": "false"} if keep_nulls else {}
    return df.select(
        F.lit(event_type).alias("event_type"),
        F.col("user_id").alias("user_id"),
        F.to_json(
            F.struct(
                F.lit(event_type).alias("eventType"),
                F.struct(F.struct(*edata_fields).alias("edata")).alias("data"),
                F.lit(version).alias("version"),
            ),
            json_opts,
        ).alias("value"),
    )


def duration_to_minutes(col_name):
    """HH:MM:SS string -> minutes (same unit as userTimeSpentDF.total_time_spent)."""
    parts = F.split(F.col(col_name), ":")
    seconds = (
            F.coalesce(parts.getItem(0).cast("int"), F.lit(0)) * 3600
            + F.coalesce(parts.getItem(1).cast("int"), F.lit(0)) * 60
            + F.coalesce(parts.getItem(2).cast("int"), F.lit(0))
    )
    return F.round(seconds / 60.0, 2)


def keep_first(df, key_cols, order_cols):
    """Deterministic de-dup: one row per key, the first by order_cols.
    (dropDuplicates keeps an arbitrary row, which would make the month a
    completion falls in -- and so the monthly cap -- change between runs.)"""
    w = Window.partitionBy(*key_cols).orderBy(*[F.col(c).asc() for c in order_cols])
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def cap_per_month(df, date_col, cap, tiebreak_cols, exempt_apar=False):
    """Keep only the first `cap` rows per user per calendar month (earliest
    `date_col` first, ties broken by tiebreak_cols).
    exempt_apar=True: rows with is_apar = true are never capped, and the
    cap is counted over non-APAR rows only.  cap=None: no cap."""
    if cap is None:
        return df
    month = F.date_format(F.to_date(F.col(date_col)), "yyyy-MM")
    parts = ["user_id", month] + (["is_apar"] if exempt_apar else [])
    w = Window.partitionBy(*parts).orderBy(F.col(date_col).asc(), *[F.col(c).asc() for c in tiebreak_cols])
    ranked = df.withColumn("_rn", F.row_number().over(w))
    keep = F.col("_rn") <= cap
    if exempt_apar:
        keep = keep | F.col("is_apar")
    return ranked.filter(keep).drop("_rn")


def build_streaks(completions_df):
    """One row per complete 4-consecutive-week block, per user.

    completions_df needs: user_id, first_certificate_generated_on.
    Output: user_id, start_date, end_date, qualified_on
      start_date   = Monday of week 1 of the block
      end_date     = Sunday of week 4 of the block
      qualified_on = first completion date in week 4 (the day the streak was
                     actually met) -- only used for retro/continuous cut-offs
    A run of N consecutive weeks yields floor(N/4) non-overlapping blocks.
    Blocks are anchored at the start of each run over the user's FULL
    history, so they match what the retro script produced.
    """
    weekly = (
        completions_df
        .withColumn("completion_date", F.to_date("first_certificate_generated_on"))
        .filter(F.col("completion_date").isNotNull())
        .withColumn("week_start", F.to_date(F.date_trunc("week", F.col("completion_date"))))
        # continuous week index (1970-01-05 is a Monday) -- safe across years
        .withColumn(
            "abs_week",
            F.floor(F.datediff(F.col("week_start"), F.lit("1970-01-05").cast("date")) / 7).cast("long"),
        )
        .groupBy("user_id", "abs_week")
        .agg(F.min("completion_date").alias("first_completion_date"))
    )

    w_seq = Window.partitionBy("user_id").orderBy("abs_week")
    runs = (
        weekly
        .withColumn("grp", F.col("abs_week") - F.row_number().over(w_seq))
        .groupBy("user_id", "grp")
        .agg(F.min("abs_week").alias("run_start_abs"), F.count("*").alias("run_len"))
        .filter(F.col("run_len") >= 4)  # also keeps sequence() below ascending
    )

    blocks = (
        runs
        .withColumn(
            "k",
            F.explode(F.sequence(F.lit(0), F.floor(F.col("run_len") / 4).cast("int") - 1)),
        )
        .withColumn("block_start_abs", F.col("run_start_abs") + F.col("k") * 4)
        .withColumn("block_end_abs", F.col("block_start_abs") + 3)
    )

    return (
        blocks
        .join(
            weekly.select(
                "user_id",
                F.col("abs_week").alias("block_end_abs"),
                F.col("first_completion_date").alias("qualified_on"),
            ),
            on=["user_id", "block_end_abs"],
        )
        # week index -> dates (1970-01-05 is a Monday)
        .withColumn("start_date", F.expr("date_add(to_date('1970-01-05'), cast(7 * block_start_abs as int))"))
        .withColumn("end_date", F.expr("date_add(to_date('1970-01-05'), cast(7 * block_end_abs + 6 as int))"))
        .select("user_id", "start_date", "end_date", "qualified_on")
    )


contentDF = contentDF.withColumn("content_duration_minutes", duration_to_minutes("content_duration"))

# completed enrolments (certificate_id present & length > 5) -- FULL history
enrolments_completed = (
    userEnrolmentDF
    .filter(F.col("certificate_id").isNotNull() & (F.length(F.col("certificate_id")) > 5))
    .join(contentDF.select("content_id", "content_duration_minutes"), on="content_id", how="left")
    .withColumn("is_apar", F.coalesce(F.col("live_cbp_plan_mandate"), F.lit(False)))
)

# ---------------------------------------------------------------------
# 2.4  ENGAGEMENT_STREAK  -- streaks met (4th week) inside the window
# ---------------------------------------------------------------------
new_streaks = build_streaks(
    enrolments_completed.filter(F.to_date("first_certificate_generated_on") >= F.lit(STREAK_START_DATE).cast("date"))
).filter(
    (F.col("qualified_on") >= window_start) & (F.col("qualified_on") <= window_end)
)

e_2_4 = build_events(
    new_streaks,
    "ENGAGEMENT_STREAK",
    [
        F.col("user_id").alias("userId"),
        F.date_format("start_date", "dd-MM-yyyy").alias("startDate"),
        F.date_format("end_date", "dd-MM-yyyy").alias("endDate"),
    ],
)

# ---------------------------------------------------------------------
# 2.6  COURSE_TIME_SPENT  -- completed AND >= 50% time spent.
#      Cap is counted over the FULL month first, THEN the lookback window
#      is applied (see module docstring).
# ---------------------------------------------------------------------
completed_unique = keep_first(
    enrolments_completed, ["user_id", "content_id"], ["first_certificate_generated_on", "batch_id"]
)

# --- Fetch time spent from Druid ONLY for the (user, content) pairs that matter.
# Pairs = completions inside the run window (yesterday) -- or the whole month so
# far when MONTHLY_CAP_ACROSS_DAYS is True (the cap ranks across the month). Pairs
# with no content duration are dropped first -- they can never pass the 50% test.
pairs_from = F.trunc(window_start, "month") if MONTHLY_CAP_ACROSS_DAYS else window_start
pairs_df = (
    completed_unique
    .filter(F.col("content_duration_minutes") > 0)
    .filter(
        (F.to_date("first_certificate_generated_on") >= pairs_from)
        & (F.to_date("first_certificate_generated_on") <= window_end)
    )
    .select("user_id", "content_id").distinct()
)
pairs = sorted((r["user_id"], r["content_id"]) for r in pairs_df.collect())
print("pairs to look up in Druid:", len(pairs))

_SAFE = re.compile(r"^[A-Za-z0-9_\-]+$")


def _in_list(values):
    return ", ".join("'" + v + "'" for v in sorted(set(values)) if v and _SAFE.match(v))


def fetch_time_spent(pairs):
    """One Druid query per chunk of pairs (sorted by user, so each chunk touches
    few distinct users/contents). Returns rows (user_id, content_id, seconds)."""
    out = []
    for i in range(0, len(pairs), DRUID_CHUNK_PAIRS):
        chunk = pairs[i:i + DRUID_CHUNK_PAIRS]
        sql = (
            "SELECT actor_id AS user_id, object_rollup_l1 AS content_id, "
            "SUM(edata_duration) AS total_time_spent_sec "
            'FROM "telemetry-events-syncts" '
            "WHERE eid = 'END' AND edata_type = 'Player' "
            f"AND object_rollup_l1 IN ({_in_list(c for _, c in chunk)}) "
            f"AND actor_id IN ({_in_list(u for u, _ in chunk)}) "
            f"AND __time >= CURRENT_TIMESTAMP - INTERVAL '{DRUID_TIME_FLOOR_DAYS}' DAY "
            "GROUP BY 1, 2"
        )
        resp = requests.post(DRUID_SQL_URL, json={"query": sql}, auth=DRUID_AUTH, timeout=600)
        resp.raise_for_status()
        out.extend((r["user_id"], r["content_id"], float(r["total_time_spent_sec"] or 0)) for r in resp.json())
    return out


_rows = fetch_time_spent(pairs)
# Druid returns seconds; downstream compares in MINUTES (content_duration_minutes).
userTimeSpentDF = (
    spark.createDataFrame(_rows, "user_id string, content_id string, sec double")
    .withColumn("total_time_spent", F.col("sec") / 60.0)
    .drop("sec")
    .dropDuplicates(["user_id", "content_id"])   # a pair can come back from more than one chunk query
)
# audit copy, overwritten each run (the pipeline itself uses the DataFrame above)
userTimeSpentDF.coalesce(1).write.mode("overwrite").option("header", True).csv(TIME_SPENT_AUDIT_DIR)

time_spent_ok = (
    completed_unique
    .select("user_id", "content_id", "batch_id", "is_apar",
            "content_duration_minutes", "first_certificate_generated_on")
    .join(userTimeSpentDF.select("user_id", "content_id", "total_time_spent"),
          on=["user_id", "content_id"], how="inner")
    .filter(F.col("content_duration_minutes") > 0)
    .filter(F.col("total_time_spent") >= 0.5 * F.col("content_duration_minutes"))
    .dropDuplicates(["user_id", "content_id"])
)

time_spent_capped = cap_per_month(
    time_spent_ok, "first_certificate_generated_on", TIME_SPENT_NONAPAR_CAP,
    ["content_id"], exempt_apar=True,
)

recent_time_spent = time_spent_capped.filter(
    (F.to_date("first_certificate_generated_on") >= window_start)
    & (F.to_date("first_certificate_generated_on") <= window_end)
)

e_2_6 = build_events(
    recent_time_spent,
    "COURSE_TIME_SPENT",
    [
        F.col("user_id").alias("userId"),
        F.col("content_id").alias("courseId"),
        F.col("batch_id").alias("batchId"),
    ],
)

# ---------------------------------------------------------------------
# ALL EVENTS -> events_df (push this to Kafka)
# ---------------------------------------------------------------------
events_df = e_2_4.unionByName(e_2_6)

events_df.groupBy("event_type").count().orderBy("event_type").show(truncate=False)
for et in ["ENGAGEMENT_STREAK", "COURSE_TIME_SPENT"]:
    events_df.filter(F.col("event_type") == et).select("value").show(2, truncate=False)

# ---------------------------------------------------------------------
# INSIGHTS (print only -- nothing is written or pushed)
# ---------------------------------------------------------------------
def _cnt(df):
    r = df.agg(F.count("*").alias("n"), F.countDistinct("user_id").alias("u")).first()
    return r["n"], r["u"]


_day = spark.range(1).select(window_end.alias("d")).first()["d"]
_in_window = (F.to_date("first_certificate_generated_on") >= window_start) & (
        F.to_date("first_certificate_generated_on") <= window_end
)

comp_y = completed_unique.filter(_in_window)
with_dur = comp_y.filter(F.col("content_duration_minutes") > 0)
ts_found = with_dur.join(
    userTimeSpentDF.select("user_id", "content_id", "total_time_spent"),
    on=["user_id", "content_id"], how="inner",
)
met_50 = ts_found.filter(F.col("total_time_spent") >= 0.5 * F.col("content_duration_minutes"))

n_comp, u_comp = _cnt(comp_y)
n_dur, _ = _cnt(with_dur)
n_ts, _ = _cnt(ts_found)
n_met, u_met = _cnt(met_50)
n_awd, u_awd = _cnt(recent_time_spent)
n_awd_apar, _ = _cnt(recent_time_spent.filter(F.col("is_apar")))
n_str, u_str = _cnt(new_streaks)
_, u_streak_pool = _cnt(
    enrolments_completed.filter(F.to_date("first_certificate_generated_on") >= F.lit(STREAK_START_DATE).cast("date"))
)

print("=" * 64)
print(f"INSIGHTS for completions on {_day}")
print("=" * 64)
print(f"Completions (unique user+course)        : {n_comp:>9}   users: {u_comp}")
print(f"  with a content duration               : {n_dur:>9}")
print(f"  with a time-spent record in Druid     : {n_ts:>9}")
print(f"  spent >= 50% of the duration          : {n_met:>9}   users: {u_met}")
print(f"COURSE_TIME_SPENT events (points)       : {n_awd:>9}   users: {u_awd}")
print(f"    of which APAR / non-APAR            : {n_awd_apar} / {n_awd - n_awd_apar}")
print(f"  met 50% but held back by monthly cap  : {n_met - n_awd:>9}")
print(f"ENGAGEMENT_STREAK events due            : {n_str:>9}   users: {u_str}")
print(f"Users with a completion since {STREAK_START_DATE}: {u_streak_pool}")
print("=" * 64)


# ---------------------------------------------------------------------
# KAFKA PUSH -- same dispatch_events call as the retro script
# (it adds the timestamp to every message itself)
# ---------------------------------------------------------------------
config = create_config(get_environment_config())
TOPIC = "prod.karma.points.unified.v2.event"

if n_awd > 0:
    dispatch_events(spark, events_df.filter(F.col("event_type") == "COURSE_TIME_SPENT"),
                    TOPIC, config.kpBrokerList,
                    run_path=f"{BASE_PATH}/karma_dispatch/continuous_{_day}/COURSE_TIME_SPENT",
                    schema=TIME_SPENT_SCHEMA, num_batches=1)

if n_str > 0:
    dispatch_events(spark, events_df.filter(F.col("event_type") == "ENGAGEMENT_STREAK"),
                    TOPIC, config.kpBrokerList,
                    run_path=f"{BASE_PATH}/karma_dispatch/continuous_{_day}/ENGAGEMENT_STREAK",
                    schema=STREAK_SCHEMA, num_batches=1)

# Bottlenecks & Tech Debt Notes

Living doc for the architect discussion on pipeline speed (currently 7-9
hours end to end). This seeds — it does not replace — the data-driven
findings from an actual profiled test run; see [`README.md`](README.md) for
how to run that and [`aggregate_profiling.py`](aggregate_profiling.py)'s
generated `report.md` for the numbers. Everything below is what's already
visible from reading the code, before a single profiled run has happened.

## Structural / architectural

- **No shared SparkSession factory.** 39 independent `SparkSession.builder`
  call sites across `jobs/stage-0`, `jobs/stage-1/prejoinData.py`, and every
  `jobs/stage-2/*.py` file, each with its own hand-tuned (and inconsistent)
  memory/partition/package config. Example:
  `jobs/stage-1/prejoinData.py:33` hardcodes `spark.driver.memory=180g`;
  `jobs/stage-2/dataWarehouse.py` (pre-instrumentation) used
  `spark.executor.memory=180g` + `spark.driver.memory=30g` under
  `master("local[*]")` in the same repo. There's no single place to tune
  memory/partitioning globally, and no guarantee two jobs are even
  comparable when their configs disagree this much.

- **Every job runs Spark in `local[N]` mode on a single host** — there are no
  separate executors, so the entire 30-40GB parquet working set, every
  multi-way join, and every warehouse-build aggregation is bounded by one
  machine's cores/RAM/disk IO with no horizontal scale-out available. This is
  probably the single biggest lever if the goal is to cut total wall time
  rather than just trim inefficiency within a job — worth raising explicitly
  with the architect as a "do we re-platform onto a real cluster" question,
  separate from anything the phase-level profiling will surface.

- **Monolithic `process_data` methods.** Every stage-0/stage-2 job (and most
  of stage-1's `dfutil` functions) is one long method mixing read, transform,
  write, DB-write, and upload inline — `jobs/stage-0/dataExhaust.py`'s
  `process_data` is ~565 lines handling ~25 datasets in one method;
  `jobs/stage-2/dashboardSync.py` is 1548 lines total. No unit tests are
  visible anywhere in `jobs/` or `dfutil/`. Practical consequence: a job can't
  be partially re-run — if dataset #20 of 25 fails, the whole job (and
  whatever it already wrote) has to be re-run from the top, which inflates
  the cost of any transient failure (a flaky Cassandra read, a Postgres
  timeout) far beyond the actual work lost.

## Duplicated / inconsistent code

- **`write_postgres_table` is duplicated per-class across 8 stage-2 files**,
  with inconsistent signatures:
  `jobs/stage-2/dataWarehouse.py:188`, `l2Assessments.py:38`,
  `ministryLeaderboard.py:314`, `nationalLearningWeek.py:753`,
  `weeklyClaps.py:200` (all five: `(self, df, url, table, username,
  password, mode="overwrite")`) vs. `gamificationNotificationProducer.py:44`,
  `peerValidationNotificationSender.py:34`,
  `peerValidationEligibleUsers.py:46` (all three: `(self, df, table,
  mode="overwrite")`, no explicit `url`). A consolidation into one shared
  `dfutil` helper is a plausible follow-up — it would also make it trivial to
  turn on JDBC batch-size/isolation-level tuning in one place instead of 8.
  All 8 default to `mode="overwrite"` — worth checking with data owners
  whether every one of those ~13+ warehouse tables truly needs a full
  overwrite every run vs. an incremental/upsert, since full overwrites of
  large tables are a plausible, easily-overlooked time sink.

- **No shared parquet read/write helper in stage-2.** Every job calls
  `spark.read.parquet(...)`/`.write.parquet(...)` directly with its own
  path-building, rather than going through a common helper the way
  `dfutil/user/userDFUtil.py:exportDFToParquet` already does for stage-1.
  Makes it hard to enforce consistent partitioning/compression choices
  pipeline-wide, or to add caching/retry behavior in one place later.

- **Timing/logging was ad hoc before this instrumentation pass** — 66
  independent `time.time()`/`print()` call sites across 15 files, no shared
  helper, and only 4 files used Python's `logging` module at all
  (`org_hierarchy.py`, `dataExhaust.py`, `surveyStatusReport.py`,
  `surveyQuestionReport.py`). Not a performance bottleneck itself, but it's
  why no one could answer "which phase of which job is slow" before now.

## Operational

- **No CLI args anywhere** (`jobs/main.py` and every stage script run via a
  hardcoded `.main()` call, no `argparse`) — there's no way to run a single
  job's sub-portion, dry-run against a data sample, or override config at
  invocation time without editing code. This will also shape how granular
  any profiling-driven fix can be tested in isolation before a full 7-9 hour
  run.
- **No Airflow DAG file lives in this repo** — orchestration/scheduling is
  external to `cb-core-data`; this repo only contains `jobs/main.py`'s fixed
  stage-1 → stage-2A/B/C/D sequence and the individual job scripts. Any
  fix that changes job boundaries or introduces parallelism between
  independent stage-2 jobs will need a corresponding change wherever that
  external DAG lives, not just here.

## To fill in after a profiled test run

Once `aggregate_profiling.py`'s `report.md` exists for a representative
test-environment run, append here:
- The actual top N slowest phases run-wide, and for each: was it
  data-volume-bound (large read, no Spark-side red flags), spill-bound (high
  `spilled_mb`), shuffle-bound (high `shuffle_read_mb`/`shuffle_write_mb`,
  suggesting a missing broadcast join or key skew), or GC-bound (high
  `gc_time_s` relative to duration)?
- Which jobs' JVM RSS high-water mark is closest to the configured
  driver/executor memory limit — those are the ones most likely to be one
  data-growth-quarter away from an OOM, not just "slow."
- Whether the read phases for the 13+ warehouse tables in `dataWarehouse.py`
  (now individually timed) are dominated by a handful of genuinely large
  tables, or spread evenly — changes whether the fix is "shard this one job"
  vs. "the whole read pattern needs rethinking."

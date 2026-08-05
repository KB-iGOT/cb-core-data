# Pipeline Profiling: Run & Evaluation Guide

This is the how-to for the phase-level performance instrumentation added across
`jobs/stage-0`, `jobs/stage-1`, `jobs/stage-2`, and the shared `dfutil/utils/*`
helpers. It exists so anyone — not just whoever wired it up — can run a
profiled pipeline pass and read the results.

See also [`BOTTLENECKS.md`](BOTTLENECKS.md) for the running list of suspected
pain points and tech debt, which this profiling data is meant to confirm or
correct.

## What this is for

Airflow already tells you when each job starts and finishes. It doesn't tell
you *why* a job is slow — whether it's reading 30-40GB of parquet, a heavy
join, a slow Postgres/GCS/Redis write, or Spark itself spilling to disk. This
instrumentation breaks each job into named phases (`read` / `process` /
`write` / `db_write` / `upload` / `redis_write`), times each one, samples
memory, and (optionally) correlates against Spark's own event log so you can
see real shuffle/spill/GC numbers behind a slow phase — not just wall time.

## Why a "read" phase can show ~0 duration, and how to fix that for a run

Spark transformations (`spark.read.parquet(...).withColumn(...)`) don't execute
anything by themselves — they only extend a lazy logical plan. Nothing runs
until an *action* (`.write.save()`, `.count()`, `.collect()`, etc.) forces
Spark to actually read/compute. That means a `with profiling.phase(...,
"read", ...):` block that only contains transformations times how long Python
took to build a DataFrame object (usually low milliseconds), **not** how long
the real read took — the real cost silently lands inside whichever later
phase happens to contain the first action (often a `write`/`db_write` phase
several steps later). Wrapping code in `profiling.phase()` alone does not fix
this; it's a property of how Spark executes, not a bug in the instrumentation
itself. If a report shows every `read` phase at ~0s and all the time on the
`write`/`db_write` phases, this is why.

**The fix: `metrics["materialize"]`.** A `read` or `process` phase can
register its resulting DataFrame:
```python
with profiling.phase(JOB_NAME, "read", "userDetailsDF", spark=spark) as m:
    userDetailsDF = spark.read.parquet(path).withColumn(...)
    m["materialize"] = userDetailsDF
    m["input_mb"] = profiling.dir_size_mb(path)  # free filesystem stat, no action needed
```
When the env var `PROFILING_FORCE_MATERIALIZE=true` is set for a run,
`profiling.phase()` calls `.cache()` + `.count()` on whatever DataFrame is
registered as `"materialize"` before the phase ends — forcing Spark to
actually execute that phase's plan right there, so `duration_s` and
`rss_jvm_delta_mb` reflect real execution instead of ~0. Because `.cache()`
mutates the DataFrame object in place, the same variable used later in a
`write`/`db_write` phase is then served from memory instead of re-reading the
source, so that later phase's duration becomes real write-only time — giving
a genuine read/process/write time split instead of one number that silently
bundles all three.

```bash
# One-off profiling run with a true read/process/write split:
PROFILING_FORCE_MATERIALIZE=true python3 -m jobs.stage-2.dataWarehouse
# or the whole pipeline:
PROFILING_FORCE_MATERIALIZE=true python3 -m jobs.main
```

This is **off by default** and resolved the same way as
`PROFILING_EVENT_LOG_COMPRESS` (see `profiling.force_materialize()`'s
docstring) — a plain env var wouldn't reach every separate Airflow-task
process in a run, so it rendezvous through a shared marker file the same way
`get_run_id()`/`event_log_compress()` do. With it off (the default for every
routine production run), a registered `"materialize"` DataFrame is simply
ignored — zero behavior change, zero added cost.

**This has a real cost — use it deliberately, not as a permanent setting.**
Forcing materialization adds an extra pass over that phase's data (the
`.count()` itself), and `.cache()` changes the JVM memory profile you're also
trying to measure (a DataFrame that wouldn't normally be cached now holds
storage memory for the rest of the job). Turn this on for a profiling run
when you need the true split; leave it off for routine runs.

Every `read`/`process` phase across `jobs/stage-0`, `jobs/stage-1`, and
`jobs/stage-2` that resolves to one clean DataFrame from a single source has
been wired up with `materialize` (and `input_mb` wherever the source is a
concrete filesystem path, via the free `profiling.dir_size_mb()` stat below).
A handful of phases were deliberately left as plain wall-clock timers because
there was no single DataFrame to force — a `.groupBy`/`.join` spanning
multiple ambiguous inputs, a non-Spark HTTP/API call, or a write that fans out
across many per-partition output paths with no single path to stat. Those are
still timed the same way this instrumentation always worked (Python wall
time only) — just without the forced-materialization split.

## The other half of the same bug: real work with no phase at all

`materialize` only fixes phases that already exist. A separate, larger gap
showed up once someone ran a job with `PROFILING_FORCE_MATERIALIZE=true` and
looked at real timestamps: `courseBasedAssessmentReport.py` had ~1,678 of its
2,041 total seconds (**82%**) sitting in three stretches of real joins,
`groupBy`s, and window functions between its `read` phases and its final
`write` phase — code that was never wrapped in *any* `profiling.phase()` call,
materialize or not. That work was 100% invisible: no duration, no RSS sample,
nothing. It's the same root cause (Spark's laziness) showing up a different
way — this time the gap isn't a mistimed phase, it's a missing one.

**The fix** is the same primitive, used slightly differently: insert a new,
otherwise-empty `"process"` phase right after the gap's final DataFrame
variable is built, registering only that one variable:
```python
finalDF = someUpstreamDF.join(otherDF, ...).groupBy(...).agg(...)
# finalDF's lineage covers everything since the last materialized phase -
# the join and groupBy above - which doesn't execute until forced.
with profiling.phase(JOB_NAME, "process", "finalDF", spark=spark) as m:
    m["materialize"] = finalDF
```
This works because forcing a `.count()` on `finalDF` pulls its **entire**
upstream lineage into this one measured phase — every join/groupBy/transform
back to whatever was last actually materialized — without touching any of
the surrounding business logic. Every job in this repo has been swept for
this pattern; each new phase's comment explains what lineage it covers. A few
stretches were deliberately left unwrapped where instrumenting them wouldn't
mean anything: dead/unreachable code (confirmed via grep for callers before
skipping), pure-Python/HTTP/psycopg2 work with no Spark DataFrame involved,
and writes that fan out across many per-partition output paths via a thread
pool with no single result to register.

**Reading a report after this fix**: if a job's `write`/`db_write` phase
duration used to look large, check whether a new `"process"` phase upstream
of it now accounts for most of that time instead — that's the fix working,
not a regression. If total job time still doesn't add up to the sum of its
phases even after this, that's a sign of a genuine remaining gap worth
investigating the same way this one was found (compare consecutive phases'
`start_ts`/`end_ts` for unexplained jumps).

## How to run it

1. **Kick off a run.** Either run the full pipeline:
   ```bash
   python3 -m jobs.main
   ```
   or a single job standalone, e.g.:
   ```bash
   python3 -m jobs.stage-2.dataWarehouse
   ```
   or however Airflow invokes each job (one task per job, each its own OS
   process — this is how it runs in production). You don't need to set
   `PROFILING_RUN_ID` yourself in any of these cases: `profiling.get_run_id()`
   checks that env var first (so `jobs/main.py`'s single-process runs still
   work exactly as before), and otherwise rendezvous with every other
   process through a shared marker file at `profiling_output/.current_run_id`.
   The first job of a run creates it; every other job — even one launched by
   a completely separate Airflow task/process minutes or hours later — reads
   it and reuses the same run id, as long as it's less than
   `RUN_ID_MARKER_MAX_AGE_HOURS` (20h by default) old. A new pipeline run
   (the next night's DAG run) naturally gets a fresh run id once that marker
   goes stale. You can still force a specific run id explicitly by setting
   `PROFILING_RUN_ID` yourself before running a job, which always takes
   priority over the marker file.

2. **Where output lands.**
   - `profiling_output/{run_id}/{job_name}.jsonl` — one file per job, one JSON
     line per phase plus one trailing `job_summary` line.
   - `profiling_output/.current_run_id` — the shared marker file jobs use to
     agree on the current run id across separate processes (see step 1).
     Not something you need to read directly.
   - `spark-events/{run_id}/` — raw Spark event logs (one file per Spark
     application in that run), used for the shuffle/spill/GC correlation.
   - `profiling_output/{run_id}/report.md` and `aggregate.csv` — generated by
     the aggregation script (step 4 below).

   Both `profiling_output/` and `spark-events/` are gitignored — treat them as
   disposable scratch output, not something to commit.

3. **Field glossary** (JSONL records in `profiling_output/{run_id}/*.jsonl`):

   | Field | Meaning |
   |---|---|
   | `record_type` | `"phase"` for one instrumented block, `"job_summary"` for the one-per-job total. |
   | `run_id` | Shared id correlating this record with a specific pipeline run. |
   | `job_name` | The job (or shared-helper caller) this record belongs to, e.g. `"dataWarehouse"`, `"stage1_prejoinData"`, `"stage1_dfutil"` (dfutil warehouse-build sub-phases), `"shared_utils"` (default when a caller didn't pass its own `job_name` into a shared helper). |
   | `stage_name` | Human-readable label for what this phase did, usually the DataFrame/table variable name being read or written. |
   | `phase_type` | One of `read`, `process`, `write`, `db_write`, `upload`, `redis_write`. `read`/`write` are parquet or similar file I/O; `db_write` is a Postgres/JDBC write; `upload` is GCS/blob storage; `redis_write` is a Redis bulk update; `process` is a transform/join with no I/O of its own (used sparingly — most transform time is implicit as "whatever's left" between a phase's read and write). |
   | `start_ts` / `end_ts` | Unix epoch seconds. |
   | `duration_s` | Wall-clock time for the phase. |
   | `rss_python_mb` / `rss_python_end_mb` | RSS of the Python driver process at phase start/end. In local-mode Spark this is just the thin py4j client — usually flat and NOT a meaningful "processing" memory signal, kept for completeness only. |
   | `rss_jvm_mb` / `rss_jvm_end_mb` / `rss_jvm_delta_mb` | RSS of the JVM child process(es) at phase start/end, and the delta. **This is the number that matters** — in local[N] Spark mode, the JVM child is where all Spark compute and caching actually happens, so this is the real "did this phase balloon memory" signal. |
   | `input_mb` / `output_mb` | Data volume read/written by this phase, in MB, captured live (filesystem stat or file-size sum, not a Spark action) by whichever call site opted in — see "Live I/O metrics coverage" below. `null` for phases that don't capture it. |
   | `record_count` | Row/key/file count for this phase, captured live where free to obtain (e.g. Redis keys pushed, files uploaded, or a forced `.count()` — see below). `null` where not captured. |
   | `throughput_mbps` | `(input_mb + output_mb) / duration_s`, computed automatically whenever either size is set. `null` otherwise. |
   | `forced_materialization` | `true` if this phase registered a `metrics["materialize"]` DataFrame **and** `PROFILING_FORCE_MATERIALIZE` was on for this run, so `duration_s`/`rss_jvm_*` reflect real forced execution rather than lazy plan-building. `false` otherwise — see "Why a 'read' phase can show ~0 duration" below. |
   | `status` | `"ok"` or `"error"` — instrumentation never swallows exceptions, so an `"error"` phase record means the wrapped code actually raised. |
   | `error_msg` | Set when `status="error"`. |
   | `pid` | Python process id, for disambiguating concurrent runs. |

   `job_summary` records additionally have `total_duration_s` and
   `completed_at` instead of the phase-specific fields.

   **Live I/O metrics coverage.** `profiling.phase(...)` optionally yields a
   `metrics` dict the wrapped code can fill in (`with profiling.phase(...) as m:`)
   when it already knows its own I/O volume for free. Wired up today:
   - `upload` via `sync_reports()` (`dfutil/utils/utils.py`) — `output_mb` is
     the on-disk size of the files uploaded, `record_count` is the file count.
   - `redis_write` via `Redis.bulk_update()` / `bulk_update_in_batches()`
     (`dfutil/utils/redis.py`) — `record_count` is the number of keys pushed.
   - `write` via `DataExhaustModel.write_parquet()`
     (`jobs/stage-0/dataExhaust.py`) — `output_mb` is the on-disk size of the
     parquet output directory after the write completes.
   - `write` via `dfexportutil.write_csv_per_mdo_id_duckdb()` /
     `write_csv_combined()` / `write_csv_per_mdo_id()` — `output_mb` (and
     `record_count` where cheaply available) is the on-disk size of the CSV
     output once the write completes, whether the caller passes its own
     `job_name=` (per-mdo-id variant) or wraps the call in its own
     `profiling.phase(..., "write", ...) as m:` and passes `metrics=m`
     (the other two).
   - Almost every `read` phase across `jobs/stage-0/1/2` that resolves to a
     single concrete filesystem path also sets `input_mb` directly via
     `profiling.dir_size_mb(path)` — a plain filesystem stat, not a Spark
     action, so it's populated unconditionally (not gated behind
     `PROFILING_FORCE_MATERIALIZE`).

   `db_write` (Postgres/JDBC) still has no free row/byte count — Spark's
   `OutputMetrics` aren't populated for JDBC writes, and there's no on-disk
   path to stat. Its row count *is* available indirectly: if the DataFrame
   being written was read via a `read`/`process` phase with `materialize`
   set, running with `PROFILING_FORCE_MATERIALIZE=true` populates that
   upstream phase's `record_count` from the forced `.count()` — the
   `db_write` phase writes the same (unchanged) row count. For everything
   still `null` in the JSONL, use the Spark-event-log-correlated `read_mb`/
   `write_mb`/`records_read`/`records_written` columns in the aggregated
   "Top N slowest phases" table instead (see below) — best-effort, but covers
   every phase type since it comes from Spark's own task metrics, not from
   instrumenting call sites.

4. **Run the aggregator** once a run has finished (or after any single job
   you want a quick look at):
   ```bash
   python3 jobs/analysis/aggregate_profiling.py --run-id 20260720_143000
   # or, to use the most recent run automatically:
   python3 jobs/analysis/aggregate_profiling.py
   ```
   If you ever end up with several separate `run_id` directories that should
   really be one logical run — e.g. data collected before the shared-marker
   fix above landed, or several standalone job invocations you want combined
   — merge them into one report instead of re-running anything:
   ```bash
   # every run_id directory starting with a given date:
   python3 jobs/analysis/aggregate_profiling.py --date 20260721
   # or an explicit list:
   python3 jobs/analysis/aggregate_profiling.py --run-ids 20260721_181628,20260721_182329
   ```
   Merged output lands at `profiling_output/merged_{date-or-run-id}/` instead
   of a single run's directory, so it never collides with real run data.

   This produces `profiling_output/{run_id}/report.md` (human-readable,
   Markdown tables) and `aggregate.csv` (the same per-phase data, flat, for
   pivoting in a spreadsheet).

## How to read `report.md`

- **Per-job totals** — each job's overall wall time from its `job_summary`
  record, sorted slowest-first. This is the same granularity Airflow already
  gives you; it's the baseline the rest of the report explains.
- **Phase breakdown per job** — for each job, every phase sorted by duration,
  with `pct_of_job` showing what fraction of that job's *measured* phase time
  (not necessarily its full wall time, if some code wasn't wrapped) it ate.
  A phase at 70%+ of a job's time is the one to target first. Also shows
  `input_mb`/`output_mb`/`record_count`/`throughput_mbps` where the phase
  captured them live (see the field glossary above for which phase types do)
  — `-` means not captured for that phase, not "zero."
- **Top N slowest phases (run-wide)** — the single worst phases across the
  *entire* run, regardless of which job they're in. This is usually the most
  useful table to open a pain-point discussion with. Two families of I/O
  columns can appear, from two different sources — read the note under the
  table to tell them apart:
  - `input_mb`/`output_mb`/`record_count`/`throughput_mbps` — live, exact,
    captured by the job itself (same values as the per-job breakdown table).
  - `read_mb`/`write_mb`/`records_read`/`records_written`/`shuffle_read_mb`/
    `shuffle_write_mb`/`spilled_mb`/`gc_time_s`/`matched_tasks` — best-effort,
    correlated post-hoc from the Spark event log by matching task timestamps
    to the phase's wall-clock window. Covers phases the live metrics don't
    (most `read`s, remaining inline `write`s, `db_write`), but only when
    `spark.eventLog.compress=false` for that run (see below) and Spark
    actually ran tasks in that window (a phase that's pure Python/driver work
    with no Spark job underneath will show `-`).
  Use these to tell "slow because of data volume" apart from "slow because of
  a Spark-side inefficiency":
  - High `spilled_mb` → the phase's data didn't fit in executor memory and
    spilled to disk — classic signal for "needs more partitions or less
    driver memory pressure," not more raw compute.
  - High `shuffle_read_mb`/`shuffle_write_mb` relative to input size → a wide
    join or repartition is moving more data across the network/disk than
    expected — check for a missing broadcast join or a skewed key.
  - High `gc_time_s` relative to `duration_s` → JVM memory pressure, not I/O
    or genuine compute — a tuning target (executor memory / cached DataFrame
    lifetime), not necessarily a data-volume problem.
  - Low `throughput_mbps` with high `duration_s` and high `input_mb`/
    `output_mb`/`read_mb`/`write_mb` → genuinely data-volume-bound, not a
    tuning target. Low `throughput_mbps` with *low* volume → the phase is
    slow for a reason other than I/O (e.g. a wide transform, or the sink
    itself is slow per-row, like Postgres/Redis round trips).
  - If a row shows `-` in the correlated columns, either no Spark event log
    was captured for that run, or the event log was compressed and skipped
    (see limitations below) — treat the row as wall-time-only (or
    live-metrics-only, if those are populated).
- **Per-job JVM RSS high-water mark** — the peak JVM RSS seen across each
  job's phases. Useful for spotting which jobs are memory-bound candidates
  for splitting or for driver-memory tuning, independent of how long they
  took.

## Known limitations (state these when sharing results)

- **`PROFILING_FORCE_MATERIALIZE` changes the run it's measuring.** With it
  on, every phase that registers a `materialize` DataFrame pays for an extra
  `.count()` pass over its data, and `.cache()` holds that DataFrame in JVM
  storage memory for the rest of the job — both real costs that don't exist
  in a normal (flag-off) run. Treat a forced-materialize run's absolute
  numbers as "real work, plus known instrumentation overhead," not as an
  exact stand-in for production wall time — the *relative* read vs. process
  vs. write split is the useful signal, not the absolute totals.
- **RSS is two-point, not continuous.** Sampled at phase enter/exit only, not
  on a background timer. A transient mid-phase memory spike that fully
  subsides before the phase ends will not show up in `rss_jvm_delta_mb`.
- **Memory signal is local-mode-only.** All of this assumes Spark runs in
  `local[N]` mode (single JVM per job, no separate executors) — which is how
  every job in this repo runs today. If that ever changes (e.g. a move to a
  real cluster), JVM RSS sampled this way would only capture the driver, not
  executors, and this instrumentation would need rethinking.
- **Spark event log correlation is best-effort, and off by default.** Every
  job's `SparkSession` sets `spark.eventLog.compress` from
  `profiling.event_log_compress()`, which defaults to `"true"` (Spark's own
  default) so normal runs are unaffected. With compression on, event log
  files are compressed with whichever codec Spark was configured to use, and
  `aggregate_profiling.py` will skip files it can't parse as plain JSON lines
  (logged as a `WARNING`) rather than guessing a codec — you'll see
  `input_mb`/`output_mb`/etc. from live metrics (where captured) but `-` in
  the correlated `read_mb`/`write_mb`/`shuffle_*`/`spilled_mb`/`gc_time_s`
  columns. To get the correlation, set the env var before running a job (or
  the whole pipeline):
  ```bash
  PROFILING_EVENT_LOG_COMPRESS=false python3 -m jobs.main
  # or a single job:
  PROFILING_EVENT_LOG_COMPRESS=false python3 -m jobs.stage-2.dataWarehouse
  ```
  Uncompressed event logs are larger on disk (`spark-events/{run_id}/` is
  gitignored scratch space — clean it up after you're done reading the
  report). This only affects Spark's own bookkeeping file; it doesn't change
  what the job reads/writes/computes.
- **Coverage isn't 100% of every line of code.** Trivial one-line transforms
  between a phase's read and write were deliberately left un-instrumented
  (see the per-file notes each instrumentation pass reported) to avoid an
  unreviewable diff — "processing" time for those is implicitly whatever's
  left between the read and write phase durations, not a directly measured
  number.
- **Shared run-id marker file has a narrow race window.** Two independent
  processes (e.g. two Airflow tasks) starting within the same instant, right
  as the previous run's marker goes stale, could each mint a slightly
  different run id before one wins via the `fcntl` lock — in practice this
  needs two jobs launching at essentially the same moment right at a
  20-hour-old boundary, which is unlikely for a once-nightly DAG. If it ever
  happens, `aggregate_profiling.py --date YYYYMMDD` (or `--run-ids`) merges
  the split run ids back into one report.

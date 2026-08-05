"""
Phase-level performance instrumentation for pipeline jobs.

Every job runs Spark in local mode (single JVM per job, no separate executor
process), so a Java child of the current Python process IS where all Spark
compute/memory lives. RSS is therefore reported for both the Python driver
process and its JVM child so the two are never conflated - the JVM number is
the one that reflects real Spark work.

See jobs/analysis/README.md for the field-by-field glossary and how to read
the resulting profiling_output/*.jsonl files and the aggregate report.
"""

import json
import os
import time
import traceback
from contextlib import contextmanager
from pathlib import Path

import psutil

try:
    import fcntl
except ImportError:
    fcntl = None

PHASE_TYPES = frozenset({"read", "process", "write", "db_write", "upload", "redis_write"})

BASE_DIR = Path(__file__).resolve().parents[2]
PROFILING_OUTPUT_DIR = BASE_DIR / "profiling_output"
SPARK_EVENT_LOG_DIR = BASE_DIR / "spark-events"

_RUN_ID_ENV_VAR = "PROFILING_RUN_ID"
_RUN_ID_MARKER_PATH = PROFILING_OUTPUT_DIR / ".current_run_id"
_RUN_ID_LOCK_PATH = PROFILING_OUTPUT_DIR / ".current_run_id.lock"
RUN_ID_MARKER_MAX_AGE_HOURS = 20


def _read_marker(marker_path: Path, max_age_hours: float):
    try:
        age_hours = (time.time() - marker_path.stat().st_mtime) / 3600
        if age_hours > max_age_hours:
            return None
        value = marker_path.read_text().strip()
        return value or None
    except (FileNotFoundError, OSError):
        return None


def _write_marker(marker_path: Path, value: str) -> None:
    tmp_path = marker_path.with_suffix(".tmp")
    tmp_path.write_text(value)
    os.replace(tmp_path, marker_path)


def _rendezvous(marker_path: Path, lock_path: Path, max_age_hours: float, mint_value) -> str:
    """
    Generic rendezvous for agreeing on one value across independent OS
    processes (e.g. one Airflow task per job) that don't inherit each other's
    environment variables but do share this filesystem. Reuses the marker
    file's value if it's fresh (within max_age_hours), otherwise calls
    mint_value() and becomes the marker for subsequent processes. A flock on
    a sentinel file (best-effort - degrades to last-writer-wins if fcntl is
    unavailable) avoids two processes starting within the same instant each
    minting a different value.
    """
    PROFILING_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if fcntl is None:
        value = _read_marker(marker_path, max_age_hours)
        if value:
            return value
        value = mint_value()
        _write_marker(marker_path, value)
        return value

    with open(lock_path, "w") as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
        except OSError:
            pass
        try:
            value = _read_marker(marker_path, max_age_hours)
            if value:
                return value
            value = mint_value()
            _write_marker(marker_path, value)
            return value
        finally:
            try:
                fcntl.flock(lock_file, fcntl.LOCK_UN)
            except OSError:
                pass


def _resolve_shared_run_id() -> str:
    return _rendezvous(
        _RUN_ID_MARKER_PATH, _RUN_ID_LOCK_PATH, RUN_ID_MARKER_MAX_AGE_HOURS,
        mint_value=lambda: time.strftime("%Y%m%d_%H%M%S"),
    )


def get_run_id() -> str:
    """
    Returns the shared run id for this pipeline run. Checks PROFILING_RUN_ID
    first (an explicit override, or set once by jobs/main.py when a whole
    pipeline run happens inside a single process). Otherwise rendezvous with
    other independent processes (e.g. separate Airflow tasks, one per job)
    via a shared marker file under profiling_output/ - see
    _resolve_shared_run_id().
    """
    run_id = os.environ.get(_RUN_ID_ENV_VAR)
    if run_id:
        return run_id
    run_id = _resolve_shared_run_id()
    os.environ[_RUN_ID_ENV_VAR] = run_id
    return run_id


def _run_output_dir() -> Path:
    run_dir = PROFILING_OUTPUT_DIR / get_run_id()
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def event_log_dir() -> str:
    """
    Resolves (and creates) the Spark event log directory for this run.
    Spark does not create spark.eventLog.dir itself, so callers must create
    it before building the SparkSession.
    """
    run_dir = SPARK_EVENT_LOG_DIR / get_run_id()
    run_dir.mkdir(parents=True, exist_ok=True)
    return str(run_dir)


_EVENT_LOG_COMPRESS_ENV_VAR = "PROFILING_EVENT_LOG_COMPRESS"
_EVENT_LOG_COMPRESS_MARKER_PATH = PROFILING_OUTPUT_DIR / ".current_event_log_compress"
_EVENT_LOG_COMPRESS_LOCK_PATH = PROFILING_OUTPUT_DIR / ".current_event_log_compress.lock"


def event_log_compress() -> str:
    """
    Value for the SparkSession's spark.eventLog.compress config, agreed once
    per pipeline run and shared across every job the same way get_run_id()
    is - each Airflow task is its own OS process and does NOT inherit a
    PROFILING_EVENT_LOG_COMPRESS set in another task's shell, so an env var
    alone would only affect whichever single job happened to have it set,
    leaving the rest of the run's event logs compressed (and unusable for
    correlation) while that one job's looked uncompressed - a broken, hard
    to notice, half-and-half state.

    Resolution order:
      1. PROFILING_EVENT_LOG_COMPRESS env var, if set on THIS process -
         becomes the marker for the rest of the run, so setting it on just
         one job (e.g. the first Airflow task, or a single-process
         `python3 -m jobs.main` run) is enough for every other job in the
         same run_id to pick up the same value.
      2. The shared marker file under profiling_output/, if another job in
         this run already resolved one (same RUN_ID_MARKER_MAX_AGE_HOURS
         freshness window as the run id marker - see get_run_id()).
      3. Default "true" (Spark's own default), which also becomes the
         marker so the whole run stays consistent even without a marker of
         its own to start from.

    Set PROFILING_EVENT_LOG_COMPRESS=false before running a job to get
    plain-JSON event logs that aggregate_profiling.py can actually parse for
    shuffle/spill/GC/read/write correlation - see jobs/analysis/README.md.
    """
    env_value = os.environ.get(_EVENT_LOG_COMPRESS_ENV_VAR)
    if env_value is not None:
        value = env_value.strip().lower()
        PROFILING_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        _write_marker(_EVENT_LOG_COMPRESS_MARKER_PATH, value)
        return value

    return _rendezvous(
        _EVENT_LOG_COMPRESS_MARKER_PATH, _EVENT_LOG_COMPRESS_LOCK_PATH, RUN_ID_MARKER_MAX_AGE_HOURS,
        mint_value=lambda: "true",
    )


_FORCE_MATERIALIZE_ENV_VAR = "PROFILING_FORCE_MATERIALIZE"
_FORCE_MATERIALIZE_MARKER_PATH = PROFILING_OUTPUT_DIR / ".current_force_materialize"
_FORCE_MATERIALIZE_LOCK_PATH = PROFILING_OUTPUT_DIR / ".current_force_materialize.lock"


def force_materialize() -> bool:
    """
    Whether profiling.phase() should force real Spark execution (cache +
    count) on the DataFrame a caller registers via metrics["materialize"] =
    df, instead of only timing however long Python takes to build the lazy
    plan. Spark transformations (including .read.parquet(...).withColumn(...))
    don't execute anything by themselves - they just extend a logical plan -
    so without this, a "read" phase's duration/RSS reflect plan-building
    only, and the real read+transform cost lands wherever the next action
    (often a later "write"/"db_write" phase) happens to fire. Forcing
    materialization here makes phase boundaries match real Spark execution,
    at the cost of one extra pass over that phase's data (the .count()
    itself) - see jobs/analysis/README.md. Off by default so a routine
    production run's performance is unaffected; turn on deliberately for a
    profiling run that needs a true read/process/write time split.

    Resolution mirrors event_log_compress() (see its docstring for why a
    plain env var isn't enough across independent per-job Airflow
    processes): PROFILING_FORCE_MATERIALIZE env var on this process wins and
    becomes the marker for the rest of the run; otherwise reuse another
    job's marker from this run, if any; default "false".
    """
    env_value = os.environ.get(_FORCE_MATERIALIZE_ENV_VAR)
    if env_value is not None:
        value = env_value.strip().lower()
        PROFILING_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        _write_marker(_FORCE_MATERIALIZE_MARKER_PATH, value)
        return value == "true"

    value = _rendezvous(
        _FORCE_MATERIALIZE_MARKER_PATH, _FORCE_MATERIALIZE_LOCK_PATH, RUN_ID_MARKER_MAX_AGE_HOURS,
        mint_value=lambda: "false",
    )
    return value == "true"


def _jvm_rss_mb() -> float:
    """
    Sums RSS across java child processes of the current process. In local[N]
    Spark mode there is one JVM child holding the driver+executor memory; this
    is the meaningful "processing" memory signal, unlike the thin Python-side
    py4j client process.
    """
    try:
        proc = psutil.Process()
    except psutil.Error:
        return 0.0

    total_bytes = 0
    for child in proc.children(recursive=True):
        try:
            name = (child.name() or "").lower()
            if "java" in name:
                total_bytes += child.memory_info().rss
        except psutil.Error:
            continue
    return round(total_bytes / (1024 * 1024), 2)


def _python_rss_mb() -> float:
    try:
        return round(psutil.Process().memory_info().rss / (1024 * 1024), 2)
    except psutil.Error:
        return 0.0


def dir_size_mb(path: str) -> float:
    """
    Recursively sums file sizes under `path` (or the size of `path` itself if
    it's a single file). Used to report on-disk read/write volume for phases
    where the caller knows a concrete path (e.g. write_parquet output) -
    filesystem stat only, no Spark action triggered.
    """
    p = Path(path)
    try:
        if p.is_file():
            return round(p.stat().st_size / (1024 * 1024), 2)
        total_bytes = sum(f.stat().st_size for f in p.rglob("*") if f.is_file())
        return round(total_bytes / (1024 * 1024), 2)
    except OSError:
        return None


def _append_record(job_name: str, record: dict) -> None:
    out_path = _run_output_dir() / f"{job_name}.jsonl"
    with open(out_path, "a") as f:
        f.write(json.dumps(record) + "\n")


@contextmanager
def phase(job_name: str, phase_type: str, stage_name: str, spark=None):
    """
    Context manager wrapping one logical phase (read/process/write/db_write/
    upload/redis_write) of a job. Records wall time and driver+JVM RSS at
    enter/exit. Never swallows exceptions - records status="error" and
    re-raises, matching this repo's existing error-handling pattern.

    Usage:
        with profiling.phase("dataWarehouse", "read", "userEnrolmentDF"):
            df = spark.read.parquet(path)

    Optionally yields a `metrics` dict the wrapped code can fill in when it
    already knows its own I/O volume for free (no extra Spark action):
        with profiling.phase(JOB_NAME, "write", "userDetailsDF", spark=spark) as m:
            write_parquet(df, path, metrics=m)
    Recognized keys: "input_mb", "output_mb", "record_count" - any left unset
    are recorded as null. See jobs/analysis/README.md for which phase types
    populate these today vs. rely on best-effort Spark event log correlation
    in aggregate_profiling.py instead.

    A "read" or "process" phase can additionally opt into a true wall-time
    split instead of just timing how long Python takes to build a lazy plan:
        with profiling.phase(JOB_NAME, "read", "userDetailsDF", spark=spark) as m:
            userDetailsDF = spark.read.parquet(path).withColumn(...)
            m["materialize"] = userDetailsDF
            m["input_mb"] = dir_size_mb(path)
    When force_materialize() is on, phase() calls .cache() + .count() on
    whatever DataFrame is registered as "materialize" before the phase ends
    - forcing Spark to actually execute this phase's plan now, so
    duration_s/rss_jvm_delta_mb reflect real execution instead of ~0. Because
    .cache() mutates the DataFrame in place, the same object used later in a
    "write"/"db_write" phase is served from memory instead of re-reading the
    source, so that later phase's duration becomes real write-only time. Off
    by default (see force_materialize()'s docstring) - with it off, a
    registered "materialize" DataFrame is simply ignored, so existing call
    sites that add this hook see no behavior change until the run opts in.
    "record_count" is set from the forced count if the caller didn't already
    set one. Errors during the forced count are recorded as a failure of
    *this* phase (correctly - if materialization fails, this phase failed).
    """
    if phase_type not in PHASE_TYPES:
        raise ValueError(f"Unknown phase_type '{phase_type}', expected one of {sorted(PHASE_TYPES)}")

    start_ts = time.time()
    rss_python_start = _python_rss_mb()
    rss_jvm_start = _jvm_rss_mb()
    status = "ok"
    error_msg = None
    metrics = {}
    forced_materialization = False

    try:
        yield metrics
        materialize_df = metrics.pop("materialize", None)
        if materialize_df is not None and force_materialize():
            materialize_df.cache()
            forced_count = materialize_df.count()
            metrics.setdefault("record_count", forced_count)
            forced_materialization = True
    except Exception as e:
        status = "error"
        error_msg = f"{type(e).__name__}: {e}"
        raise
    finally:
        end_ts = time.time()
        rss_python_end = _python_rss_mb()
        rss_jvm_end = _jvm_rss_mb()
        duration_s = round(end_ts - start_ts, 3)

        input_mb = metrics.get("input_mb")
        output_mb = metrics.get("output_mb")
        io_mb = (input_mb or 0) + (output_mb or 0)
        throughput_mbps = round(io_mb / duration_s, 2) if (input_mb is not None or output_mb is not None) and duration_s > 0 else None

        record = {
            "record_type": "phase",
            "run_id": get_run_id(),
            "job_name": job_name,
            "stage_name": stage_name,
            "phase_type": phase_type,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "duration_s": duration_s,
            "rss_python_mb": rss_python_start,
            "rss_python_end_mb": rss_python_end,
            "rss_jvm_mb": rss_jvm_start,
            "rss_jvm_end_mb": rss_jvm_end,
            "rss_jvm_delta_mb": round(rss_jvm_end - rss_jvm_start, 2),
            "input_mb": input_mb,
            "output_mb": output_mb,
            "record_count": metrics.get("record_count"),
            "throughput_mbps": throughput_mbps,
            "forced_materialization": forced_materialization,
            "status": status,
            "error_msg": error_msg,
            "pid": os.getpid(),
        }
        _append_record(job_name, record)


def write_summary(job_name: str, total_duration_s: float, status: str = "ok", error_msg: str = None) -> None:
    """
    One call per job's main(), recording the job's overall wall time.
    Additive to (not a replacement for) whatever the job already prints.
    """
    record = {
        "record_type": "job_summary",
        "run_id": get_run_id(),
        "job_name": job_name,
        "total_duration_s": round(total_duration_s, 3),
        "status": status,
        "error_msg": error_msg,
        "pid": os.getpid(),
        "completed_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    _append_record(job_name, record)

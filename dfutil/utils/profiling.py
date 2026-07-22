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


def _read_marker():
    try:
        age_hours = (time.time() - _RUN_ID_MARKER_PATH.stat().st_mtime) / 3600
        if age_hours > RUN_ID_MARKER_MAX_AGE_HOURS:
            return None
        run_id = _RUN_ID_MARKER_PATH.read_text().strip()
        return run_id or None
    except (FileNotFoundError, OSError):
        return None


def _write_marker(run_id: str) -> None:
    tmp_path = _RUN_ID_MARKER_PATH.with_suffix(".tmp")
    tmp_path.write_text(run_id)
    os.replace(tmp_path, _RUN_ID_MARKER_PATH)


def _resolve_shared_run_id() -> str:
    """
    Rendezvous point for sharing one run id across independent OS processes
    (e.g. one Airflow task per job) that don't inherit each other's
    environment variables but do share this filesystem. Reuses the marker
    file's run id if it's fresh (within RUN_ID_MARKER_MAX_AGE_HOURS, which
    comfortably spans one nightly pipeline run but expires before the next),
    otherwise mints a new run id and becomes the marker for subsequent
    processes. A flock on a sentinel file (best-effort - degrades to
    last-writer-wins if fcntl is unavailable) avoids two processes starting
    within the same instant each minting a different run id.
    """
    PROFILING_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if fcntl is None:
        run_id = _read_marker()
        if run_id:
            return run_id
        run_id = time.strftime("%Y%m%d_%H%M%S")
        _write_marker(run_id)
        return run_id

    with open(_RUN_ID_LOCK_PATH, "w") as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
        except OSError:
            pass
        try:
            run_id = _read_marker()
            if run_id:
                return run_id
            run_id = time.strftime("%Y%m%d_%H%M%S")
            _write_marker(run_id)
            return run_id
        finally:
            try:
                fcntl.flock(lock_file, fcntl.LOCK_UN)
            except OSError:
                pass


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
    """
    if phase_type not in PHASE_TYPES:
        raise ValueError(f"Unknown phase_type '{phase_type}', expected one of {sorted(PHASE_TYPES)}")

    start_ts = time.time()
    rss_python_start = _python_rss_mb()
    rss_jvm_start = _jvm_rss_mb()
    status = "ok"
    error_msg = None

    try:
        yield
    except Exception as e:
        status = "error"
        error_msg = f"{type(e).__name__}: {e}"
        raise
    finally:
        end_ts = time.time()
        rss_python_end = _python_rss_mb()
        rss_jvm_end = _jvm_rss_mb()

        record = {
            "record_type": "phase",
            "run_id": get_run_id(),
            "job_name": job_name,
            "stage_name": stage_name,
            "phase_type": phase_type,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "duration_s": round(end_ts - start_ts, 3),
            "rss_python_mb": rss_python_start,
            "rss_python_end_mb": rss_python_end,
            "rss_jvm_mb": rss_jvm_start,
            "rss_jvm_end_mb": rss_jvm_end,
            "rss_jvm_delta_mb": round(rss_jvm_end - rss_jvm_start, 2),
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

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

PHASE_TYPES = frozenset({"read", "process", "write", "db_write", "upload", "redis_write"})

BASE_DIR = Path(__file__).resolve().parents[2]
PROFILING_OUTPUT_DIR = BASE_DIR / "profiling_output"
SPARK_EVENT_LOG_DIR = BASE_DIR / "spark-events"

_RUN_ID_ENV_VAR = "PROFILING_RUN_ID"


def get_run_id() -> str:
    """
    Returns the shared run id for this pipeline run (set once by jobs/main.py
    via PROFILING_RUN_ID so every job/SparkSession in the same run correlates).
    Falls back to a per-process timestamp for standalone job runs.
    """
    run_id = os.environ.get(_RUN_ID_ENV_VAR)
    if run_id:
        return run_id
    run_id = time.strftime("%Y%m%d_%H%M%S")
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

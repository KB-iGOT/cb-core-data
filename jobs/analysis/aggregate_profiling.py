"""
Aggregates one or more pipeline runs' profiling_output/{run_id}/*.jsonl
phase/summary records, correlated (best-effort) against
spark-events/{run_id}/* Spark event logs, into a ranked Markdown report +
raw CSV for an architect discussion.

Usage:
    python3 jobs/analysis/aggregate_profiling.py --run-id 20260720_143000
    python3 jobs/analysis/aggregate_profiling.py   # uses the most recent run

    # Merge several runs into one report (e.g. runs scattered across
    # separate run_id directories before the shared-marker-file fix, or
    # several standalone job invocations you want combined):
    python3 jobs/analysis/aggregate_profiling.py --run-ids 20260721_181628,20260721_182329
    python3 jobs/analysis/aggregate_profiling.py --date 20260721   # every run_id starting with that date

See jobs/analysis/README.md for how to run a profiled pipeline and how to
read the generated report.md / aggregate.csv.
"""

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
PROFILING_DIR = BASE_DIR / "profiling_output"
EVENT_LOG_DIR = BASE_DIR / "spark-events"

TOP_N_SLOWEST = 25


def latest_run_id():
    runs = sorted(p.name for p in PROFILING_DIR.iterdir() if p.is_dir() and not p.name.startswith(".") and not p.name.startswith("merged_")) if PROFILING_DIR.exists() else []
    if not runs:
        sys.exit(f"No runs found under {PROFILING_DIR}")
    return runs[-1]


def discover_run_ids_for_date(date_str):
    if not PROFILING_DIR.exists():
        sys.exit(f"No runs found under {PROFILING_DIR}")
    runs = sorted(
        p.name for p in PROFILING_DIR.iterdir()
        if p.is_dir() and p.name.startswith(date_str)
    )
    if not runs:
        sys.exit(f"No run_id directories starting with '{date_str}' found under {PROFILING_DIR}")
    return runs


def resolve_run_ids(args):
    provided = [name for name, val in (("--run-id", args.run_id), ("--run-ids", args.run_ids), ("--date", args.date)) if val]
    if len(provided) > 1:
        sys.exit(f"Pass only one of --run-id / --run-ids / --date, not {provided}")

    if args.run_id:
        return [args.run_id]
    if args.run_ids:
        run_ids = [r.strip() for r in args.run_ids.split(",") if r.strip()]
        if not run_ids:
            sys.exit("--run-ids was given but contained no run ids")
        return run_ids
    if args.date:
        return discover_run_ids_for_date(args.date)
    return [latest_run_id()]


def merged_output_dir(run_ids, date):
    if len(run_ids) == 1:
        return PROFILING_DIR / run_ids[0]
    label = date if date else f"{run_ids[0]}_plus{len(run_ids) - 1}"
    return PROFILING_DIR / f"merged_{label}"


def _load_records_for_run(run_id):
    run_dir = PROFILING_DIR / run_id
    if not run_dir.exists():
        print(f"WARNING: no profiling output found for run_id={run_id} at {run_dir}, skipping")
        return [], []

    phases, summaries = [], []
    for jsonl_file in sorted(run_dir.glob("*.jsonl")):
        with open(jsonl_file) as f:
            for line_no, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    print(f"WARNING: skipping unparseable line {line_no} in {jsonl_file.name}")
                    continue
                if record.get("record_type") == "phase":
                    phases.append(record)
                elif record.get("record_type") == "job_summary":
                    summaries.append(record)
    return phases, summaries


def load_records(run_ids):
    all_phases, all_summaries = [], []
    for run_id in run_ids:
        phases, summaries = _load_records_for_run(run_id)
        all_phases.extend(phases)
        all_summaries.extend(summaries)
    if not all_phases and not all_summaries:
        sys.exit(f"No profiling output found for any of {run_ids}")
    return all_phases, all_summaries


def _load_spark_task_events_for_run(run_id):
    """
    Best-effort parse of Spark event logs for SparkListenerTaskEnd events.
    Event logs are one-JSON-object-per-line when uncompressed. If
    spark.eventLog.compress was left enabled, files are compressed with
    Spark's configured codec and this parser skips them with a warning
    rather than guessing a codec - rerun with spark.eventLog.compress=false
    (or decompress manually) if shuffle/spill/GC correlation is needed.
    """
    run_dir = EVENT_LOG_DIR / run_id
    if not run_dir.exists():
        print(f"INFO: no Spark event logs found at {run_dir}, skipping shuffle/spill/GC correlation for this run")
        return []

    task_events = []
    skipped = []
    for event_file in sorted(run_dir.iterdir()):
        if event_file.is_dir():
            continue
        try:
            with open(event_file, "r", errors="strict") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        skipped.append(event_file.name)
                        break
                    if event.get("Event") == "SparkListenerTaskEnd":
                        task_events.append(event)
        except (UnicodeDecodeError, OSError):
            skipped.append(event_file.name)

    if skipped:
        preview = skipped[:5]
        suffix = "..." if len(skipped) > 5 else ""
        print(f"WARNING: skipped {len(skipped)} unreadable/compressed event log file(s): {preview}{suffix}")
    return task_events


def load_spark_task_events(run_ids):
    all_events = []
    for run_id in run_ids:
        all_events.extend(_load_spark_task_events_for_run(run_id))
    return all_events


def extract_task_metrics(event):
    tm = event.get("Task Metrics", {}) or {}
    task_info = event.get("Task Info", {}) or {}
    shuffle_read_m = tm.get("Shuffle Read Metrics", {}) or {}
    shuffle_write_m = tm.get("Shuffle Write Metrics", {}) or {}
    shuffle_read = shuffle_read_m.get("Remote Bytes Read", 0) + shuffle_read_m.get("Local Bytes Read", 0)
    shuffle_write = shuffle_write_m.get("Shuffle Bytes Written", 0)
    spill = tm.get("Memory Bytes Spilled", 0) + tm.get("Disk Bytes Spilled", 0)
    gc_time = tm.get("JVM GC Time", 0)
    return {
        "start_ms": task_info.get("Launch Time"),
        "end_ms": task_info.get("Finish Time"),
        "shuffle_read_bytes": shuffle_read,
        "shuffle_write_bytes": shuffle_write,
        "spilled_bytes": spill,
        "gc_time_ms": gc_time,
    }


def correlate_phase_with_tasks(phase_row, task_metrics):
    if not task_metrics:
        return {}
    start_ms = phase_row["start_ts"] * 1000
    end_ms = phase_row["end_ts"] * 1000
    matched = [t for t in task_metrics if t["start_ms"] is not None and start_ms <= t["start_ms"] <= end_ms]
    if not matched:
        return {}
    return {
        "shuffle_read_mb": round(sum(t["shuffle_read_bytes"] for t in matched) / (1024 * 1024), 1),
        "shuffle_write_mb": round(sum(t["shuffle_write_bytes"] for t in matched) / (1024 * 1024), 1),
        "spilled_mb": round(sum(t["spilled_bytes"] for t in matched) / (1024 * 1024), 1),
        "gc_time_s": round(sum(t["gc_time_ms"] for t in matched) / 1000, 1),
        "matched_tasks": len(matched),
    }


def build_report(run_ids, phases, summaries, task_metrics):
    phases_df = pd.DataFrame(phases)
    summaries_df = pd.DataFrame(summaries)

    if len(run_ids) == 1:
        title = f"# Profiling Report - run_id {run_ids[0]}"
    else:
        title = f"# Profiling Report - merged from {len(run_ids)} runs: {', '.join(run_ids)}"
    lines = [title, ""]

    if summaries_df.empty and phases_df.empty:
        lines.append("No profiling records found for this run.")
        return "\n".join(lines), phases_df

    lines.append("## Per-job totals")
    lines.append("")
    if not summaries_df.empty:
        job_totals = summaries_df.sort_values("total_duration_s", ascending=False)[
            ["job_name", "total_duration_s", "status"]
        ]
        lines.append(job_totals.to_markdown(index=False))
    else:
        lines.append("_No job_summary records found._")
    lines.append("")

    lines.append("## Phase breakdown per job (% of that job's own phase time)")
    lines.append("")
    if not phases_df.empty:
        job_phase_totals = phases_df.groupby("job_name")["duration_s"].sum().rename("job_phase_total_s")
        phases_df = phases_df.merge(job_phase_totals, on="job_name")
        phases_df["pct_of_job"] = (phases_df["duration_s"] / phases_df["job_phase_total_s"] * 100).round(1)
        for job_name, group in phases_df.groupby("job_name"):
            lines.append(f"### {job_name}")
            table = group.sort_values("duration_s", ascending=False)[
                ["stage_name", "phase_type", "duration_s", "pct_of_job", "rss_jvm_delta_mb", "status"]
            ]
            lines.append(table.to_markdown(index=False))
            lines.append("")
    else:
        lines.append("_No phase records found._")
    lines.append("")

    lines.append(f"## Top {TOP_N_SLOWEST} slowest phases (run-wide)")
    lines.append("")
    if not phases_df.empty:
        slowest = phases_df.sort_values("duration_s", ascending=False).head(TOP_N_SLOWEST).reset_index(drop=True)
        correlations = [correlate_phase_with_tasks(row, task_metrics) for _, row in slowest.iterrows()]
        corr_df = pd.DataFrame(correlations).fillna("-")
        display_cols = ["job_name", "stage_name", "phase_type", "duration_s", "rss_jvm_delta_mb"]
        combined = pd.concat([slowest[display_cols], corr_df], axis=1)
        lines.append(combined.to_markdown(index=False))
        if not task_metrics:
            lines.append("")
            lines.append(
                "_No Spark event log task metrics available for correlation "
                "(see warnings above) - showing wall time and RSS only._"
            )
    else:
        lines.append("_No phase records found._")
    lines.append("")

    lines.append("## Per-job JVM RSS high-water mark")
    lines.append("")
    if not phases_df.empty:
        hwm = phases_df.groupby("job_name")[["rss_jvm_mb", "rss_jvm_end_mb"]].max()
        hwm["rss_jvm_high_water_mb"] = hwm.max(axis=1)
        hwm = hwm[["rss_jvm_high_water_mb"]].sort_values("rss_jvm_high_water_mb", ascending=False).reset_index()
        lines.append(hwm.to_markdown(index=False))
    lines.append("")

    return "\n".join(lines), phases_df


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id", default=None, help="Single run id to aggregate (default: most recent under profiling_output/)")
    parser.add_argument("--run-ids", default=None, help="Comma-separated run ids to merge into one report")
    parser.add_argument("--date", default=None, help="YYYYMMDD - merge every run_id directory starting with this date prefix")
    args = parser.parse_args()

    run_ids = resolve_run_ids(args)
    print(f"Aggregating profiling data for {len(run_ids)} run(s): {run_ids}")

    phases, summaries = load_records(run_ids)
    print(f"Loaded {len(phases)} phase records and {len(summaries)} job summaries")

    task_events = load_spark_task_events(run_ids)
    task_metrics = [extract_task_metrics(e) for e in task_events]
    print(f"Loaded {len(task_metrics)} Spark task-end events for correlation")

    report_md, phases_df = build_report(run_ids, phases, summaries, task_metrics)

    out_dir = merged_output_dir(run_ids, args.date)
    out_dir.mkdir(parents=True, exist_ok=True)

    report_path = out_dir / "report.md"
    report_path.write_text(report_md)
    print(f"Wrote {report_path}")

    if not phases_df.empty:
        csv_path = out_dir / "aggregate.csv"
        phases_df.to_csv(csv_path, index=False)
        print(f"Wrote {csv_path}")


if __name__ == "__main__":
    main()

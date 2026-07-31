"""
Rolls up one or more pipeline runs' profiling_output/{run_id}/*.jsonl
records into a single high-level Markdown report: total read / ETL / write
time and data volume per job, plus a per-job breakdown of what was actually
read and written. This is the "don't make me add it up myself" view - for
the full phase-by-phase detail (% of job, per-stage memory delta, Spark
event log correlation, etc.) see aggregate_profiling.py's report.md, which
this script leaves untouched.

"ETL" here is not a separately instrumented phase_type (profiling.phase only
knows read/process/write/db_write/upload/redis_write - see
dfutil/utils/profiling.py). It's computed as whatever's left of a job's
total wall time after its read and write phases: total_duration_s - read_s -
write_s. That remainder also covers any "process" phases a job wraps
explicitly, matching the convention already documented in
jobs/analysis/README.md ("most transform time is implicit as 'whatever's
left' between a phase's read and write").

The read/write breakdown groups phases into what they read/wrote:
  - db_write / redis_write / upload phase_type -> "DB write" / "Cache
    (Redis) write" / "Upload" directly, since the job already tagged them
    that way at the call site.
  - Everything else (phase_type "read" or "write") is Parquet unless the
    stage_name mentions "csv" - matching this codebase's actual convention
    of naming CSV stages explicitly (e.g. "csv_per_mdoid",
    "enrolmentReportDF_csv", "convert_parquet_to_csv" - see
    dfutil/dfexport/dfexportutil.py). profiling.phase doesn't track
    source/sink format directly, so this is a best-effort label based on
    naming, not a hard guarantee. A category with zero phases for a given
    job is simply omitted - there's nothing to sum to zero and print.

Usage:
    python3 jobs/analysis/summarize_profiling.py --run-id 20260720_143000
    python3 jobs/analysis/summarize_profiling.py   # most recent run
    python3 jobs/analysis/summarize_profiling.py --run-ids id1,id2
    python3 jobs/analysis/summarize_profiling.py --date 20260721
"""

import argparse

import pandas as pd

from aggregate_profiling import load_records, merged_output_dir, resolve_run_ids

WRITE_PHASE_TYPES = {"write", "db_write", "redis_write", "upload"}

WRITE_CATEGORY_BY_PHASE_TYPE = {
    "db_write": "DB write",
    "redis_write": "Cache (Redis) write",
    "upload": "Upload",
}


def format_hms(seconds):
    if seconds is None or pd.isna(seconds):
        return "-"
    total = int(round(seconds))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def classify_read(row):
    return "CSV read" if "csv" in str(row["stage_name"]).lower() else "Parquet read"


def classify_write(row):
    category = WRITE_CATEGORY_BY_PHASE_TYPE.get(row["phase_type"])
    if category:
        return category
    return "CSV write" if "csv" in str(row["stage_name"]).lower() else "Parquet write"


def summarize(phases, summaries):
    phases_df = pd.DataFrame(phases)
    summaries_df = pd.DataFrame(summaries)

    if phases_df.empty and summaries_df.empty:
        return pd.DataFrame(), phases_df

    # input_mb/output_mb are opt-in (most phases leave them null), so a
    # column that's null for every row loads as object dtype rather than
    # float64 - and object dtype breaks .round()/.sum() downstream in
    # categorize()/render_io_breakdown(). Coerce once, here, so every
    # consumer of phases_df gets a real numeric column.
    for col in ("input_mb", "output_mb", "rss_jvm_delta_mb"):
        if col in phases_df.columns:
            phases_df[col] = pd.to_numeric(phases_df[col], errors="coerce")

    # A job_name can have more than one job_summary line - e.g. an Airflow
    # retry re-running the same job under the same run_id, or several run_ids
    # merged together (--run-ids/--date) - so aggregate rather than index on
    # job_name directly (which would leave duplicates as a Series, not a
    # scalar, and blow up any .get(job_name) lookup below).
    total_by_job = (
        summaries_df.groupby("job_name")["total_duration_s"].sum()
        if not summaries_df.empty else pd.Series(dtype=float)
    )
    status_by_job = (
        summaries_df.groupby("job_name")["status"].agg(lambda s: "error" if (s == "error").any() else "ok")
        if not summaries_df.empty else pd.Series(dtype=object)
    )

    job_names = sorted(set(phases_df.get("job_name", pd.Series(dtype=object))) | set(total_by_job.index))

    rows = []
    for job_name in job_names:
        job_phases = phases_df[phases_df["job_name"] == job_name] if not phases_df.empty else phases_df

        read_s = job_phases.loc[job_phases["phase_type"] == "read", "duration_s"].sum() if not job_phases.empty else 0.0
        write_s = job_phases.loc[job_phases["phase_type"].isin(WRITE_PHASE_TYPES), "duration_s"].sum() if not job_phases.empty else 0.0
        read_data_mb = job_phases.loc[job_phases["phase_type"] == "read", "input_mb"].sum(min_count=1) if not job_phases.empty else None
        write_data_mb = job_phases.loc[job_phases["phase_type"].isin(WRITE_PHASE_TYPES), "output_mb"].sum(min_count=1) if not job_phases.empty else None
        rss_jvm_delta_mb = job_phases["rss_jvm_delta_mb"].sum(min_count=1) if not job_phases.empty else None

        total_s = total_by_job.get(job_name)
        if total_s is None or pd.isna(total_s):
            total_s = job_phases["duration_s"].sum() if not job_phases.empty else None
            etl_s = None
        else:
            etl_s = max(total_s - read_s - write_s, 0.0)

        rows.append({
            "job_name": job_name,
            "total_duration_s": total_s,
            "read_s": read_s,
            "etl_s": etl_s,
            "write_s": write_s,
            "read_data_mb": round(read_data_mb, 1) if pd.notna(read_data_mb) else None,
            "write_data_mb": round(write_data_mb, 1) if pd.notna(write_data_mb) else None,
            "rss_jvm_delta_mb": round(rss_jvm_delta_mb, 1) if pd.notna(rss_jvm_delta_mb) else None,
            "status": status_by_job.get(job_name, "-"),
        })

    summary_df = pd.DataFrame(rows).sort_values("total_duration_s", ascending=False, na_position="last")
    return summary_df.reset_index(drop=True), phases_df


def render_top_table(summary_df):
    display_df = summary_df.copy()
    for col in ("total_duration_s", "read_s", "etl_s", "write_s"):
        display_df[col] = display_df[col].apply(format_hms)
    display_df = display_df.rename(columns={
        "total_duration_s": "total", "read_s": "read", "etl_s": "etl", "write_s": "write",
    })
    return display_df.fillna("-").to_markdown(index=False)


def categorize(phases_df, classify_fn, data_col):
    """
    Labels each row of phases_df (already filtered to one side - read or
    write) with its category via classify_fn, normalizing data_col to a
    plain "data_mb" name so read (input_mb) and write (output_mb) frames
    can be concatenated/compared directly.
    """
    if phases_df.empty:
        return phases_df.assign(category=pd.Series(dtype=object), data_mb=pd.Series(dtype=float))
    phases_df = phases_df.copy()
    if data_col not in phases_df.columns:
        phases_df[data_col] = None
    phases_df["category"] = phases_df.apply(classify_fn, axis=1)
    return phases_df.rename(columns={data_col: "data_mb"})


def category_stats(categorized_df):
    """count / total duration / total data size per category."""
    if categorized_df.empty:
        return pd.DataFrame(columns=["category", "count", "duration_s", "data_mb"])
    return (
        categorized_df.groupby("category")
        .agg(count=("stage_name", "size"), duration_s=("duration_s", "sum"), data_mb=("data_mb", lambda s: s.sum(min_count=1)))
        .reset_index()
    )


def render_category_stats_table(stats_df):
    if stats_df.empty:
        return "_None._"
    display_df = stats_df.sort_values("duration_s", ascending=False).copy()
    display_df["duration_s"] = display_df["duration_s"].apply(format_hms)
    display_df["data_mb"] = display_df["data_mb"].round(1)
    display_df = display_df.rename(columns={"duration_s": "total_duration"})
    return display_df.fillna("-").to_markdown(index=False)


def render_io_breakdown(categorized_df):
    """
    categorized_df already filtered to one job and one side (read or
    write), with a "category"/"data_mb" column from categorize(). Emits a
    header line with count+total duration+total size per category, then a
    per-stage table - skipping categories with no phases at all, per "if
    it's 0 ignore that item".
    """
    if categorized_df.empty:
        return []

    lines = []
    for category in sorted(categorized_df["category"].unique()):
        cat_df = categorized_df[categorized_df["category"] == category]
        total_data_mb = cat_df["data_mb"].sum(min_count=1)
        size_note = f", {round(total_data_mb, 1)} MB total" if pd.notna(total_data_mb) else ""
        lines.append(f"_{category} - {len(cat_df)}, {format_hms(cat_df['duration_s'].sum())} total{size_note}_")
        lines.append("")
        table = cat_df.sort_values("duration_s", ascending=False)[
            ["stage_name", "duration_s", "data_mb", "rss_jvm_delta_mb", "status"]
        ].copy()
        table["duration_s"] = table["duration_s"].apply(format_hms)
        table["data_mb"] = table["data_mb"].round(1)
        table["rss_jvm_delta_mb"] = table["rss_jvm_delta_mb"].round(1)
        table = table.rename(columns={"duration_s": "duration"})
        lines.append(table.fillna("-").to_markdown(index=False))
        lines.append("")
    return lines


def build_report(run_ids, summary_df, phases_df):
    if len(run_ids) == 1:
        title = f"# Profiling Summary - run_id {run_ids[0]}"
    else:
        title = f"# Profiling Summary - merged from {len(run_ids)} runs: {', '.join(run_ids)}"
    lines = [title, ""]

    if summary_df.empty:
        lines.append("No profiling records found for this run.")
        return "\n".join(lines)

    lines.append("## Per-job totals")
    lines.append("")
    lines.append(
        "`etl` is not separately instrumented - it's `total - read - write` (includes any explicit "
        "`process` phases plus untimed transform code). `read_data_mb`/`write_data_mb` are only "
        "populated for phase types that report `input_mb`/`output_mb` live; `rss_jvm_delta_mb` (JVM "
        "RSS change, summed across all phases) is shown alongside as a size signal that's always "
        "available."
    )
    lines.append("")
    lines.append(render_top_table(summary_df))
    lines.append("")

    lines.append("## Read/write summary (all jobs combined)")
    lines.append("")
    lines.append(
        "Count, total time, and total data size per category, summed across every job in this run - "
        "e.g. how many CSV files were written and their combined size, how many Parquet reads happened "
        "in total, etc. See the category note below for how categories are assigned."
    )
    lines.append("")
    all_reads = categorize(phases_df[phases_df["phase_type"] == "read"] if not phases_df.empty else phases_df, classify_read, "input_mb")
    all_writes = categorize(phases_df[phases_df["phase_type"].isin(WRITE_PHASE_TYPES)] if not phases_df.empty else phases_df, classify_write, "output_mb")
    overall_stats = pd.concat([category_stats(all_reads), category_stats(all_writes)], ignore_index=True)
    lines.append(render_category_stats_table(overall_stats))
    lines.append("")

    lines.append("## Per-job read/write breakdown")
    lines.append("")
    lines.append(
        "Reads/writes are grouped by what they read or wrote. `db_write`/`redis_write`/`upload` "
        "phases are labelled directly from their instrumented phase_type; everything else is "
        "Parquet unless the stage name mentions \"csv\" (this codebase's convention for naming CSV "
        "stages) - a best-effort label, not a hard guarantee, since profiling.phase doesn't track "
        "source/sink format itself. Categories with no phases for a job aren't shown."
    )
    lines.append("")
    for job_name in summary_df["job_name"]:
        job_phases = phases_df[phases_df["job_name"] == job_name] if not phases_df.empty else phases_df
        if job_phases.empty:
            continue
        lines.append(f"### {job_name}")
        lines.append("")

        read_phases = job_phases[job_phases["phase_type"] == "read"]
        lines.append(f"**Reads - {len(read_phases)} total, {format_hms(read_phases['duration_s'].sum())}**")
        lines.append("")
        lines.extend(render_io_breakdown(categorize(read_phases, classify_read, "input_mb")))

        write_phases = job_phases[job_phases["phase_type"].isin(WRITE_PHASE_TYPES)]
        lines.append(f"**Writes - {len(write_phases)} total, {format_hms(write_phases['duration_s'].sum())}**")
        lines.append("")
        lines.extend(render_io_breakdown(categorize(write_phases, classify_write, "output_mb")))

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id", default=None, help="Single run id to summarize (default: most recent under profiling_output/)")
    parser.add_argument("--run-ids", default=None, help="Comma-separated run ids to merge into one summary")
    parser.add_argument("--date", default=None, help="YYYYMMDD - merge every run_id directory starting with this date prefix")
    args = parser.parse_args()

    run_ids = resolve_run_ids(args)
    print(f"Summarizing profiling data for {len(run_ids)} run(s): {run_ids}")

    phases, summaries = load_records(run_ids)
    print(f"Loaded {len(phases)} phase records and {len(summaries)} job summaries")

    summary_df, phases_df = summarize(phases, summaries)
    report_md = build_report(run_ids, summary_df, phases_df)

    out_dir = merged_output_dir(run_ids, args.date)
    out_dir.mkdir(parents=True, exist_ok=True)

    summary_path = out_dir / "summary.md"
    summary_path.write_text(report_md)
    print(f"Wrote {summary_path}")


if __name__ == "__main__":
    main()

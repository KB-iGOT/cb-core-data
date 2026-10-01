"""
polars_csv_export.py — standalone job, NO Spark/findspark imports.

Each report's Spark job writes a PLAIN (non-partitioned) parquet file and
then stops. This script runs AFTER that job has fully exited, reads the
plain parquet, and does the per-org split + CSV write using Polars'
single-pass partition_by() (one vectorized split, not a per-group re-scan
loop). Running as a genuinely separate process means the full machine's
memory and all cores are available here, with zero Spark JVM reservation
competing for resources.

HOW TO ADD A NEW REPORT:
  1. In that report's Spark job, replace its CSV-writing step with a plain
     parquet write (no partitionBy, no coalesce(1)) to a STABLE path (not
     auto-cleaned by that job).
  2. Add ONE entry to get_reports() below with that path, the output dir,
     partition column, and csv filename. Do not create a new file per report.

Usage:
    python3 polars_csv_export.py                  # run every registered report
    python3 polars_csv_export.py --only cba        # run only reports whose name contains "cba"
    python3 polars_csv_export.py --no-cleanup      # keep the plain parquet after export (debugging)
"""
import sys
import time
import shutil
import argparse
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

sys.path.append(str(Path(__file__).resolve().parents[2]))
# NOTE: these config loaders are plain Python (no pyspark import) - safe to
# reuse here so paths stay in sync with what each Spark job writes, instead
# of hardcoding/duplicating path strings in two places.
from jobs.default_config import create_config
from jobs.config import get_environment_config


def write_csv_per_mdo_id_polars(parquet_path: str, output_dir: str, partition_col: str,
                                csv_filename: str, max_workers: int, cleanup: bool = True,
                                clean_output_dir: bool = True):
    """
    Reads a plain (non-partitioned) parquet file and splits it into one CSV
    per partition_col value, using a single-pass partition_by() rather than
    a per-group filter+collect loop (which would re-scan the source file
    once per group and risk the same memory spike documented in this
    project's optimization notes).

    clean_output_dir: set False when multiple calls write into the SAME
    output_dir (e.g. CBA's govt + non-govt calls both land in one final
    folder) - cleaning must happen ONCE for that shared folder, before any
    of the calls that write into it, not on every individual call (which
    would wipe out the previous call's just-written CSVs). main() handles
    this by cleaning each distinct output_dir once up front and passing
    clean_output_dir=False here.
    """
    try:
        import polars as pl
    except ImportError:
        print("Polars not installed - run: pip install polars --break-system-packages")
        return None

    out_path = Path(output_dir)
    if clean_output_dir and out_path.exists():
        print(f"   🧹 Removing existing output directory: {output_dir}")
        shutil.rmtree(out_path)
    out_path.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    # Glob pattern, not the bare directory - Spark writes hidden .crc
    # checksum files alongside the .parquet part files; Polars chokes on
    # those if pointed at the directory directly.
    df = pl.read_parquet(f"{parquet_path}/*.parquet")
    t_read = time.time() - t0
    print(f"   Read: {t_read:.2f}s ({df.height:,} rows)")

    t1 = time.time()
    partitions = df.partition_by(partition_col, as_dict=True)
    t_partition = time.time() - t1
    print(f"   partition_by (single pass): {t_partition:.2f}s, {len(partitions)} groups")

    def write_org(item):
        org_value, part_df = item
        # partition_by's dict key is a tuple even for a single grouping column
        safe_org = str(org_value[0] if isinstance(org_value, tuple) else org_value).replace("/", "_")
        folder = out_path / f"{partition_col}={safe_org}"
        folder.mkdir(parents=True, exist_ok=True)
        part_df.write_csv(str(folder / csv_filename))

    t2 = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        list(executor.map(write_org, partitions.items()))
    t_write = time.time() - t2
    print(f"   Parallel CSV write ({max_workers} workers): {t_write:.2f}s")

    elapsed = time.time() - t0
    print(f"✅ Export took {elapsed:.2f}s total ({elapsed/60:.2f} min) -> {output_dir}")

    if cleanup:
        try:
            shutil.rmtree(parquet_path)
            print(f"🧹 Cleaned up: {parquet_path}")
        except Exception as e:
            print(f"⚠️  Could not clean up {parquet_path}: {e}")

    return elapsed


def get_reports(today: str, config):
    """
    ============================================================
    REPORT REGISTRY — add one entry per report here.
    ============================================================
    Keep each entry's "parquet_path" in exact sync with the path that
    report's own Spark job writes to (search that job for the matching
    comment block). Nothing else in this file needs to change when adding
    a new report.
    """
    reports = []

    # ---- Course Based Assessment Report (CBA) ----
    cba_out_path = f"{config.localReportDir}/{config.cbaReportPath}/{today}"

    reports.append({
        "name": "CBA - Govt",
        "parquet_path": f"{config.localReportDir}/temp/cba-report-plain/{today}/govt",
        "output_dir": cba_out_path,
        "partition_col": "mdoid",
        "csv_filename": config.cbaReport,
        "max_workers": 16,
    })
    reports.append({
        "name": "CBA - Non-Govt",
        "parquet_path": f"{config.localReportDir}/temp/cba-report-plain/{today}/non_govt",
        "output_dir": cba_out_path,
        "partition_col": "mdoid",
        "csv_filename": config.cbaReport,
        "max_workers": 16,
    })

    # ---- ACBP: CBP Enrolment Report ----
    reports.append({
        "name": "ACBP - CBP Enrolment Report",
        "parquet_path": f"{config.localReportDir}/temp/cbp-enrolment-report-plain/{today}",
        "output_dir": f"{config.localReportDir}/{config.acbpMdoEnrolmentReportPath}/{today}",
        "partition_col": "mdoid",
        "csv_filename": config.cbpEnrolmentReport,
        "max_workers": 16,
    })

    # ---- ACBP: CBP User Summary Report ----
    reports.append({
        "name": "ACBP - CBP Summary Report",
        "parquet_path": f"{config.localReportDir}/temp/cbp-summary-report-plain/{today}",
        "output_dir": f"{config.localReportDir}/{config.acbpMdoSummaryReportPath}/{today}",
        "partition_col": "mdoid",
        "csv_filename": config.cbpSummaryReport,
        "max_workers": 16,
    })

    # ---- Course Report ----
    reports.append({
        "name": "Course Report",
        "parquet_path": f"{config.localReportDir}/temp/course-report-plain/{today}",
        "output_dir": f"{config.localReportDir}/{config.courseReportPath}/{today}",
        "partition_col": "mdoid",
        "csv_filename": config.courseReport,
        "max_workers": 16,
    })

    # ---- Next report goes here — copy the block above and adjust paths ----
    # reports.append({
    #     "name": "...",
    #     "parquet_path": f"{config.localReportDir}/temp/.../{today}",
    #     "output_dir": f"{config.localReportDir}/{config....Path}/{today}",
    #     "partition_col": "mdoid",
    #     "csv_filename": config....,
    #     "max_workers": 16,
    # })

    return reports


def main():
    parser = argparse.ArgumentParser(description="Polars per-org CSV export — runs every registered report by default")
    parser.add_argument("--only", help="Run only reports whose name contains this text (case-insensitive)")
    parser.add_argument("--no-cleanup", action="store_true", help="Keep plain parquet files after export (debugging)")
    args = parser.parse_args()

    today = datetime.now().strftime("%Y-%m-%d")
    config_dict = get_environment_config()
    config = create_config(config_dict)

    reports = get_reports(today, config)
    if args.only:
        reports = [r for r in reports if args.only.lower() in r["name"].lower()]
        if not reports:
            print(f"No registered report matches --only '{args.only}'")
            return

    print(f"[START] Polars CSV export — {len(reports)} report(s) queued")
    overall_start = time.time()

    # Clean each DISTINCT output_dir exactly once, before any writes into it.
    # Reports that share an output_dir (e.g. CBA govt + non-govt both land in
    # the same final folder, split into mdoid=X vs mdoid=X_non_govt
    # subfolders within it) must not have a later call's cleanup wipe out an
    # earlier call's just-written CSVs - so this cleans the shared folder
    # once, up front, regardless of how many report entries write into it.
    seen_output_dirs = set()
    for report in reports:
        output_dir = report["output_dir"]
        if output_dir in seen_output_dirs:
            continue
        out_path = Path(output_dir)
        if out_path.exists():
            print(f"🧹 Cleaning output directory (once): {output_dir}")
            shutil.rmtree(out_path)
        out_path.mkdir(parents=True, exist_ok=True)
        seen_output_dirs.add(output_dir)

    for report in reports:
        parquet_path = Path(report["parquet_path"])
        if not parquet_path.exists():
            print(f"\n⏭️  Skipping {report['name']} — no parquet found at {report['parquet_path']} "
                  f"(the upstream Spark job may not have run, or found no rows for this subset)")
            continue

        print(f"\n➡️  {report['name']}: {report['parquet_path']} -> {report['output_dir']}")
        write_csv_per_mdo_id_polars(
            report["parquet_path"],
            report["output_dir"],
            report["partition_col"],
            report["csv_filename"],
            report["max_workers"],
            cleanup=not args.no_cleanup,
            clean_output_dir=False,  # already cleaned once above, per unique output_dir
        )

    print(f"\n[END] All reports processed in {time.time() - overall_start:.2f}s")


if __name__ == "__main__":
    main()

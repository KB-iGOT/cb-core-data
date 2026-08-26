import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, when, sum, bround, broadcast, coalesce, lit,
    current_timestamp, date_format, from_unixtime, concat_ws
)
import os
import math
import time
import duckdb
import shutil
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(str(Path(__file__).resolve().parents[2]))

from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.enrolment.acbp import acbpDFUtil
from dfutil.enrolment import enrolmentDFUtil
from dfutil.content import contentDFUtil


def _get_duckdb_pragma_settings(max_workers: int, mem_reserve_fraction: float = 0.6):
    """
    Divide system resources across concurrent DuckDB workers so multiple
    workers don't each request the full machine's resources simultaneously.
    """
    total_cpus = os.cpu_count() or 8
    total_mem_gb = 64  # conservative fallback
    try:
        with open('/proc/meminfo') as f:
            for line in f:
                if line.startswith('MemTotal'):
                    total_mem_gb = int(line.split()[1]) / (1024 * 1024)  # kB -> GB
                    break
    except Exception:
        pass

    threads = max(2, total_cpus // max_workers)
    mem_gb = max(4, int((total_mem_gb * mem_reserve_fraction) // max_workers))
    return threads, mem_gb


def write_csv_per_mdo_id(df, output_dir, groupByAttr, isIndividualWrite=False, threshold=100000, csv_filename="report.csv"):
    """
    Optimized hybrid write strategy:
    - Small/medium groups: Direct CSV write via Spark partitionBy
    - Large groups: Filter first, then write to parquet for DuckDB processing
    """
    if isIndividualWrite == False:
        print("📊 Step 1: Analyzing group sizes...")
        df.cache().count()
        group_counts = df.groupBy(groupByAttr).count()
        group_counts.cache()

        small_ids = [row[groupByAttr] for row in group_counts.filter(col("count") <= threshold).collect()]
        large_ids = [row[groupByAttr] for row in group_counts.filter(col("count") > threshold).collect()]

        print(f"📈 Small groups (≤{threshold} rows, fast write): {len(small_ids)}")
        print(f"📊 Large groups (>{threshold} rows, DuckDB write): {len(large_ids)}")

        if small_ids:
            print("🚀 Writing small groups directly via Spark...")
            small_df = df.filter(col(groupByAttr).isin(small_ids))
            small_df.repartition(groupByAttr).write.mode("overwrite").partitionBy(groupByAttr) \
                .option("header", True).csv(output_dir + "_spark_temp")
            convert_spark_partitions_to_folders(output_dir + "_spark_temp", output_dir, groupByAttr, csv_filename)
            print(f"✅ Completed writing {len(small_ids)} small groups")

        if large_ids:
            print("🦆 Processing large groups via DuckDB...")
            large_df = df.filter(col(groupByAttr).isin(large_ids))
            parquet_tmp_path = output_dir + '_tmp_large_groups'
            write_csv_per_mdo_id_duckdb(large_df, output_dir, groupByAttr, parquet_tmp_path, large_ids, csv_filename=csv_filename)
            print(f"✅ Completed writing {len(large_ids)} large groups")

        group_counts.unpersist()
    else:
        print("📦 Writing as partitioned parquet...")
        df.repartition(groupByAttr).write.mode("overwrite").partitionBy(groupByAttr) \
            .option("header", True).option("compression", "snappy").parquet(output_dir)


def convert_spark_partitions_to_folders(spark_output_dir: str, final_output_dir: str, partition_column: str, csv_filename: str):
    spark_path = Path(spark_output_dir)
    final_path = Path(final_output_dir)
    final_path.mkdir(parents=True, exist_ok=True)

    partition_dirs = [d for d in spark_path.iterdir()
                     if d.is_dir() and d.name.startswith(f"{partition_column}=")]

    for partition_dir in partition_dirs:
        partition_value = partition_dir.name.split("=", 1)[1]
        safe_partition_value = str(partition_value).replace("/", "_")
        target_folder = final_path / f"{partition_column}={safe_partition_value}"
        target_folder.mkdir(parents=True, exist_ok=True)
        csv_files = list(partition_dir.glob("*.csv"))
        if csv_files:
            if len(csv_files) == 1:
                shutil.copy2(csv_files[0], target_folder / csv_filename)
            else:
                merge_csv_files(csv_files, target_folder / csv_filename)

    try:
        shutil.rmtree(spark_output_dir)
        print(f"✅ Cleaned up temporary Spark output: {spark_output_dir}")
    except Exception as e:
        print(f"⚠️ Could not clean up {spark_output_dir}: {e}")


def merge_csv_files(csv_files: list, output_file: Path):
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        first_file = True
        for csv_file in csv_files:
            with open(csv_file, 'r', encoding='utf-8') as infile:
                lines = infile.readlines()
                if first_file:
                    outfile.writelines(lines)
                    first_file = False
                else:
                    outfile.writelines(lines[1:])


def write_csv_per_mdo_id_duckdb(df, output_dir: str, group_by_attr: str, parquet_tmp_path: str = None,
                               large_ids=None, max_workers: int = 16, keep_parquets: bool = False,
                               csv_filename: str = "report.csv"):
    """
    Writes CSVs per group_by_attr using partitioned parquet files and batched
    parallel conversion (one DuckDB connection reused per worker batch, NOT
    one connection per org - still produces exactly one folder+CSV per org).
    """
    _t0 = time.time()

    if parquet_tmp_path is None:
        parquet_tmp_path = output_dir + "_temp_partitioned_parquets"

    print(f"📦 Step 1: Writing partitioned parquets...")
    print(f"    - Parquet path: {parquet_tmp_path}")

    if large_ids is not None and len(large_ids) > 0:
        print(f"    - Filtering to {len(large_ids)} specific groups")
        df = df.filter(col(group_by_attr).isin(large_ids))

    df.repartition(group_by_attr).write.mode("overwrite").partitionBy(group_by_attr) \
        .option("compression", "snappy").parquet(parquet_tmp_path)

    _t1 = time.time()
    print(f"⏱️  Step 1 (parquet write) took {_t1 - _t0:.1f}s")

    df.unpersist(blocking=True)
    print("🧹 Freed DataFrame from cache after parquet write")

    result = convert_partitioned_parquets_to_csv(
        parquet_input_dir=parquet_tmp_path,
        csv_output_dir=output_dir,
        partition_column=group_by_attr,
        max_workers=max_workers,
        process_subset=large_ids,
        keep_parquets=keep_parquets,
        csv_filename=csv_filename
    )

    _t2 = time.time()
    print(f"⏱️  Step 2 (DuckDB CSV conversion) took {_t2 - _t1:.1f}s")
    print(f"⏱️  write_csv_per_mdo_id_duckdb TOTAL: {_t2 - _t0:.1f}s")
    print("🎉 Done writing all group CSV files using partitioned parquet approach.")

    return {
        'successful_writes': result['successful_conversions'],
        'failed_writes': result['failed_conversions'],
        'total_groups': result['total_partitions'],
        'success_rate': result['success_rate'],
        'detailed_results': result
    }


def write_single_csv_duckdb(df, output_path: str, parquet_tmp_path: str = None, filter_condition: str = None,
                           keep_parquets: bool = False):
    """Unchanged from original - kept for backward compatibility."""
    from pathlib import Path
    import shutil

    if parquet_tmp_path is None:
        output_file = Path(output_path)
        parquet_tmp_path = str(output_file.parent / f"{output_file.stem}_temp_parquets")

    print(f"📦 Step 1: Writing DataFrame to partitioned parquet...")
    if filter_condition:
        df = df.filter(filter_condition)

    df.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(parquet_tmp_path)
    df.unpersist(blocking=True)

    con = duckdb.connect()
    try:
        threads, mem_gb = _get_duckdb_pragma_settings(1)
        con.execute(f"PRAGMA memory_limit='{mem_gb}GB';")
        con.execute(f"PRAGMA threads={threads};")
        con.execute("PRAGMA temp_directory='/tmp/duckdb_spill';")
        con.execute("INSTALL parquet; LOAD parquet;")
        con.execute(f"CREATE TABLE source_df AS SELECT * FROM parquet_scan('{parquet_tmp_path}/**/*.parquet');")
        total_rows = con.execute("SELECT COUNT(*) FROM source_df").fetchone()[0]

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            COPY (SELECT * FROM source_df) TO '{output_path}' (FORMAT CSV, HEADER, DELIMITER ',');
        """)
        success = output_file.exists()
    except Exception as e:
        print(f"❌ Error during CSV writing: {e}")
        success = False
        total_rows = 0
    finally:
        con.close()

    if not keep_parquets:
        try:
            shutil.rmtree(parquet_tmp_path)
        except Exception as e:
            print(f"⚠️  Could not clean up {parquet_tmp_path}: {e}")

    return {'success': success, 'output_path': output_path, 'rows_written': total_rows,
            'total_rows': total_rows, 'filter_applied': filter_condition is not None}


def write_single_parquet(df, final_path: str):
    """Unchanged legacy method."""
    temp_dir = f"{final_path}_tmp_{uuid.uuid4().hex}"
    df.coalesce(1).write.mode("overwrite").parquet(temp_dir)
    part_file = None
    for file in os.listdir(temp_dir):
        if file.endswith(".parquet"):
            part_file = os.path.join(temp_dir, file)
            break
    if not part_file:
        raise Exception("No parquet part file found in temp directory")
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    shutil.move(part_file, final_path)
    shutil.rmtree(temp_dir, ignore_errors=True)
    print(f"✅ Parquet written to: {final_path}")


def _convert_partition_batch(partition_dirs_batch, csv_output_dir, partition_column, csv_filename, threads, mem_gb):
    """
    Processes a batch of org partitions using ONE reused DuckDB connection.
    Still produces exactly one folder + one CSV per org - only the connection
    setup cost (previously paid per-org) is shared across the batch.
    """
    con = duckdb.connect()
    con.execute(f"PRAGMA memory_limit='{mem_gb}GB';")
    con.execute(f"PRAGMA threads={threads};")
    con.execute("PRAGMA temp_directory='/tmp/duckdb_spill';")
    con.execute("INSTALL parquet; LOAD parquet;")

    batch_results = []
    for partition_dir in partition_dirs_batch:
        partition_value = "unknown"
        csv_output_path = None
        partition_folder = None
        try:
            dir_name = Path(partition_dir).name
            partition_value = dir_name.split("=", 1)[1] if "=" in dir_name else "unknown"
            safe_partition_value = str(partition_value).replace("/", "_")

            partition_folder = Path(csv_output_dir) / f"{partition_column}={safe_partition_value}"
            partition_folder.mkdir(parents=True, exist_ok=True)
            csv_output_path = partition_folder / csv_filename

            parquet_pattern = f"{partition_dir}/*.parquet"
            row_count = con.execute(f"SELECT COUNT(*) FROM parquet_scan('{parquet_pattern}')").fetchone()[0]
            con.execute(f"""
                COPY (SELECT * FROM parquet_scan('{parquet_pattern}'))
                TO '{csv_output_path}' (FORMAT CSV, HEADER, DELIMITER ',');
            """)

            if csv_output_path.exists():
                file_size_mb = csv_output_path.stat().st_size / (1024 * 1024)
                batch_results.append({
                    'success': True, 'partition_value': partition_value,
                    'csv_path': str(csv_output_path), 'folder_path': str(partition_folder),
                    'rows_written': row_count, 'file_size_mb': round(file_size_mb, 2), 'error': None
                })
            else:
                batch_results.append({
                    'success': False, 'partition_value': partition_value,
                    'csv_path': str(csv_output_path), 'folder_path': str(partition_folder),
                    'rows_written': 0, 'file_size_mb': 0, 'error': 'CSV file was not created'
                })
        except Exception as e:
            batch_results.append({
                'success': False, 'partition_value': partition_value,
                'csv_path': str(csv_output_path) if csv_output_path else 'unknown',
                'folder_path': str(partition_folder) if partition_folder else 'unknown',
                'rows_written': 0, 'file_size_mb': 0, 'error': str(e)
            })

    con.close()
    return batch_results


def convert_partitioned_parquets_to_csv(parquet_input_dir: str, csv_output_dir: str,
                                      partition_column: str, max_workers: int = 16,
                                      process_subset: list = None, keep_parquets: bool = False,
                                      csv_filename: str = "report.csv"):
    """
    Convert partitioned parquet files to individual CSV files inside folders.
    Creates structure: csv_output_dir/partition_column=value/csv_filename

    Uses BATCHED DuckDB connections (one connection reused per worker across
    many orgs) instead of one connection per org, to cut connection-setup
    overhead when there are thousands of small partitions.
    """
    print(f"🦆 Converting partitioned parquets to CSV folders...")
    print(f"    - Source: {parquet_input_dir}")
    print(f"    - Target: {csv_output_dir}")
    print(f"    - Max workers: {max_workers}")
    print(f"    - CSV filename: {csv_filename}")

    parquet_path = Path(parquet_input_dir)
    if not parquet_path.exists():
        print(f"❌ Parquet directory does not exist: {parquet_input_dir}")
        return {'success': False, 'error': 'Parquet directory not found'}

    partition_dirs = [d for d in parquet_path.iterdir()
                     if d.is_dir() and d.name.startswith(f"{partition_column}=")]

    if process_subset:
        subset_set = set(str(v) for v in process_subset)
        partition_dirs = [
            d for d in partition_dirs
            if (d.name.split("=", 1)[1] if "=" in d.name else "unknown") in subset_set
        ]
        print(f"    - Processing subset: {len(partition_dirs)} partitions")

    total_partitions = len(partition_dirs)
    print(f"    - Found {total_partitions} partitions to convert")

    if total_partitions == 0:
        print("❌ No partition directories found")
        return {'success': False, 'error': 'No partitions found'}

    # Conversion tracking - MUST be initialized before the if/else below
    results = []
    successful_conversions = 0
    failed_conversions = 0
    total_rows = 0
    total_size_mb = 0

    if max_workers == 1:
        print("🔄 Processing partitions sequentially...")
        threads, mem_gb = _get_duckdb_pragma_settings(1)
        batch_result = _convert_partition_batch(
            [str(p) for p in partition_dirs], csv_output_dir, partition_column, csv_filename, threads, mem_gb
        )
        for i, result in enumerate(batch_result, 1):
            results.append(result)
            if result['success']:
                successful_conversions += 1
                total_rows += result['rows_written']
                total_size_mb += result['file_size_mb']
                print(f"✅ {i}/{total_partitions}: {result['partition_value']} ({result['rows_written']:,} rows)")
            else:
                failed_conversions += 1
                print(f"❌ {i}/{total_partitions}: {result['partition_value']} - {result['error']}")
    else:
        threads_per_worker, mem_gb_per_worker = _get_duckdb_pragma_settings(max_workers)
        batch_size = math.ceil(total_partitions / max_workers)
        batches = [partition_dirs[i:i + batch_size] for i in range(0, total_partitions, batch_size)]

        print(f"🚀 Processing {total_partitions} partitions in {len(batches)} batches "
              f"(~{batch_size} orgs/batch, {threads_per_worker} threads & {mem_gb_per_worker}GB per worker)")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_convert_partition_batch, [str(p) for p in batch],
                               csv_output_dir, partition_column, csv_filename,
                               threads_per_worker, mem_gb_per_worker)
                for batch in batches
            ]

            completed_partitions = 0
            for future in as_completed(futures):
                try:
                    batch_result = future.result()
                    for result in batch_result:
                        results.append(result)
                        completed_partitions += 1
                        if result['success']:
                            successful_conversions += 1
                            total_rows += result['rows_written']
                            total_size_mb += result['file_size_mb']
                        else:
                            failed_conversions += 1
                            print(f"❌ {result['partition_value']} - {result['error']}")
                except Exception as e:
                    print(f"❌ Batch failed entirely: {e}")

            print(f"✅ Completed {completed_partitions}/{total_partitions} partitions "
                  f"({successful_conversions} success, {failed_conversions} failed)")

    if not keep_parquets:
        print(f"\n🧹 Cleaning up partitioned parquet files...")
        try:
            shutil.rmtree(parquet_input_dir)
            print(f"✅ Cleaned up: {parquet_input_dir}")
        except Exception as e:
            print(f"⚠️ Could not clean up {parquet_input_dir}: {e}")
    else:
        print(f"\n💾 Keeping partitioned parquet files at: {parquet_input_dir}")

    success_rate = (successful_conversions / total_partitions * 100) if total_partitions > 0 else 0

    print(f"\n📈 Conversion Summary:")
    print(f"   📊 Total partitions: {total_partitions}")
    print(f"   ✅ Successful: {successful_conversions}")
    print(f"   ❌ Failed: {failed_conversions}")
    print(f"   🎯 Success rate: {success_rate:.1f}%")
    print(f"   📄 Total rows: {total_rows:,}")
    print(f"   💾 Total size: {total_size_mb:.2f} MB")
    print(f"   📂 CSV output structure: {csv_output_dir}/partition=value/{csv_filename}")

    return {
        'success': successful_conversions > 0,
        'total_partitions': total_partitions,
        'successful_conversions': successful_conversions,
        'failed_conversions': failed_conversions,
        'success_rate': success_rate,
        'total_rows': total_rows,
        'total_size_mb': total_size_mb,
        'csv_output_dir': csv_output_dir,
        'results': results
    }
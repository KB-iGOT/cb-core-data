import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, when, sum, bround, broadcast, coalesce, lit,
    current_timestamp, date_format, from_unixtime, concat_ws
)
import os
import duckdb

# Add parent directory to sys.path for importing project-specific modules
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Import reusable utilities from project
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.enrolment.acbp import acbpDFUtil
from dfutil.enrolment import enrolmentDFUtil
from dfutil.content import contentDFUtil


def write_csv_per_mdo_id(df, output_dir, groupByAttr, isIndividualWrite=False, threshold=100000):
    """
    Optimized hybrid write strategy: 
    - Small/medium groups: Direct CSV write via Spark partitionBy
    - Large groups: Filter first, then write to parquet for DuckDB processing
    
    Args:
        df (DataFrame): Source DataFrame
        output_dir (str): Output directory path
        groupByAttr (str): Column to group by
        isIndividualWrite (bool): If True, write as parquet instead of CSV
        threshold (int): Max row count per group to consider for fast write
    """
    
    if isIndividualWrite == False:
        print("📊 Step 1: Analyzing group sizes...")
        df.cache().count()  # Cache the DataFrame to avoid recomputation
        # Step 1: Get group counts
        group_counts = df.groupBy(groupByAttr).count()
        group_counts.cache()  # Cache since we'll use it multiple times
        
        # Step 2: Collect IDs for fast + fallback paths
        small_ids = [row[groupByAttr] for row in group_counts.filter(col("count") <= threshold).collect()]
        large_ids = [row[groupByAttr] for row in group_counts.filter(col("count") > threshold).collect()]
        
        print(f"📈 Small groups (≤{threshold} rows, fast write): {len(small_ids)}")
        print(f"📊 Large groups (>{threshold} rows, DuckDB write): {len(large_ids)}")
        
        # Step 3: Fast write for small/medium groups - FILTER FIRST
        if small_ids:
            print("🚀 Writing small groups directly via Spark...")
            small_df = df.filter(col(groupByAttr).isin(small_ids))
            
            small_df \
                .repartition(groupByAttr) \
                .write \
                .mode("overwrite") \
                .partitionBy(groupByAttr) \
                .option("header", True) \
                .csv(output_dir)
            
            print(f"✅ Completed writing {len(small_ids)} small groups")

        # Step 4: DuckDB write for large groups - FILTER FIRST, then write to parquet
        if large_ids:
            print("🦆 Processing large groups via DuckDB...")
            # df.cache().count()  # Ensure the DataFrame is cached for performance
            # OPTIMIZATION: Filter large IDs first, then write only filtered data to parquet
            large_df = df.filter(col(groupByAttr).isin(large_ids))
            
            parquet_tmp_path = output_dir + '_tmp_large_groups'
            write_csv_per_mdo_id_duckdb(large_df, output_dir, groupByAttr, parquet_tmp_path, large_ids)
            
            print(f"✅ Completed writing {len(large_ids)} large groups")
        
        # Cleanup
        group_counts.unpersist()
        
    else:
        # Parquet write mode
        print("📦 Writing as partitioned parquet...")
        df.repartition(groupByAttr) \
            .write \
            .mode("overwrite") \
            .partitionBy(groupByAttr) \
            .option("header", True) \
            .option("compression", "snappy") \
            .parquet(output_dir)

def write_csv_per_mdo_id_duckdb(df, output_dir: str, group_by_attr: str, parquet_tmp_path: str, large_ids=None):
    """
    Optimized: Writes CSVs per group_by_attr using pre-filtered DataFrame.
    Only processes large groups that were already filtered by Spark.

    Args:
        df (DataFrame): Pre-filtered Spark DataFrame containing only large groups
        output_dir (str): Output directory to write CSVs
        group_by_attr (str): Column to group by (e.g., "mdo_id")
        parquet_tmp_path (str): Temporary Parquet output path
        large_ids (list): List of large group IDs (for validation/logging)
    """
    print(f"📦 Step 1: Writing filtered large groups to Parquet...")
    # print(f"    - Processing {len(large_ids)} large groups")
    print(f"    - Parquet path: {parquet_tmp_path}")
    
    # Write only the pre-filtered large groups to parquet
    df \
        .write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(parquet_tmp_path)
    df.unpersist(blocking=True) 

    print("🦆 Step 2: Loading into DuckDB...")
    con = duckdb.connect()
    
    # DuckDB optimizations
    con.execute("PRAGMA memory_limit='40GB';")
    con.execute("PRAGMA threads=14;")
    con.execute("PRAGMA enable_progress_bar=false;")
    con.execute("PRAGMA preserve_insertion_order=false;")
    con.execute("PRAGMA temp_directory='/tmp/duckdb_spill';")
    con.execute("INSTALL parquet; LOAD parquet;")
    
    # Load the parquet data
    con.execute(f"CREATE TABLE result_df AS SELECT * FROM parquet_scan('{parquet_tmp_path}/**/*.parquet');")

    print("🔍 Step 3: Verifying large group IDs...")
    # Use the provided large_ids directly (since we already filtered)
    # group_ids = [(val,) for val in large_ids]
    
    # Optional: Verify that our filtering worked correctly
    actual_groups = con.execute(f"SELECT DISTINCT {group_by_attr} FROM result_df").fetchall()
    # print(f"    - Expected groups: {len(group_ids)}")
    print(f"    - Actual groups in parquet: {len(actual_groups)}")

    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    print("📤 Step 4: Writing individual CSV files for large groups...")
    
    # Tracking variables
    successful_writes = 0
    failed_writes = 0
    total_groups = len(actual_groups)
    
    for i, (group_val,) in enumerate(actual_groups, 1):
        try:
            safe_val = str(group_val).replace("/", "_") if group_val is not None else "null"
            output_path = Path(output_dir) / f"{group_by_attr}={safe_val}.csv"

            con.execute(f"""
                COPY (
                    SELECT * FROM result_df
                    WHERE {group_by_attr} = ?
                ) TO '{output_path}' (FORMAT CSV, HEADER, DELIMITER ',');
            """, [group_val])

            successful_writes += 1
            
            # Progress update every 10 files or for the last file
            if i % 10 == 0 or i == total_groups:
                print(f"📊 Progress: {i}/{total_groups} large groups written ({(i/total_groups)*100:.1f}%)")
            
        except Exception as e:
            failed_writes += 1
            print(f"❌ Failed to write group {group_val}: {e}")

    # Final summary
    print(f"\n📈 Large Group CSV Writing Summary:")
    print(f"   ✅ Successfully written: {successful_writes}")
    print(f"   ❌ Failed: {failed_writes}")
    print(f"   📊 Total processed: {total_groups}")
    print(f"   🎯 Success rate: {(successful_writes/total_groups)*100:.1f}%")

    con.close()
    
    # Optional: Cleanup temporary parquet files
    print("\n🧹 Cleaning up temporary parquet files...")
    try:
        import shutil
        shutil.rmtree(parquet_tmp_path)
        print(f"✅ Cleaned up: {parquet_tmp_path}")
    except Exception as e:
        print(f"⚠️  Could not clean up {parquet_tmp_path}: {e}")
    
    print("🎉 Done writing all large group CSV files.")
    
    # Return tracking info for further use if needed
    return {
        'successful_writes': successful_writes,
        'failed_writes': failed_writes,
        'total_groups': total_groups,
        'success_rate': (successful_writes/total_groups)*100 if total_groups > 0 else 0
    }

def write_single_csv_duckdb(df, output_path: str, parquet_tmp_path: str, filter_condition: str = None):
    """
    Optimized: Writes a single CSV file using DuckDB from Spark DataFrame.
    
    Args:
        df (DataFrame): Spark DataFrame to write
        output_path (str): Full path for the output CSV file (including filename)
        parquet_tmp_path (str): Temporary Parquet output path
        filter_condition (str, optional): SQL WHERE condition to filter data (e.g., "mdo_id > 1000")
    
    Returns:
        dict: Summary of the operation with row counts and success status
    """
    import duckdb
    from pathlib import Path
    import shutil
    
    print(f"📦 Step 1: Writing DataFrame to Parquet...")
    print(f"    - Parquet path: {parquet_tmp_path}")
    
    # Write DataFrame to parquet with optimization
    df \
        .write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(parquet_tmp_path)
    df.unpersist(blocking=True) 
    
    print("🦆 Step 2: Loading into DuckDB...")
    con = duckdb.connect()
    
    try:
        # DuckDB optimizations
        con.execute("PRAGMA memory_limit='40GB';")
        con.execute("PRAGMA threads=14;")
        con.execute("PRAGMA temp_directory='/tmp/duckdb_spill';")
        con.execute("INSTALL parquet; LOAD parquet;")
        
        # Load the parquet data
        con.execute(f"CREATE TABLE source_df AS SELECT * FROM parquet_scan('{parquet_tmp_path}/**/*.parquet');")
        
        # Get row count for verification
        total_rows = con.execute("SELECT COUNT(*) FROM source_df").fetchone()[0]
        print(f"    - Total rows loaded: {total_rows:,}")
        
        # Ensure output directory exists
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        print(f"📤 Step 3: Writing CSV to {output_path}...")
        
        # Build the query based on filter condition
        if filter_condition:
            query = f"""
                COPY (
                    SELECT * FROM source_df
                    WHERE {filter_condition}
                ) TO '{output_path}' (FORMAT CSV, HEADER, DELIMITER ',');
            """
            print(f"    - Applying filter: {filter_condition}")
            
            # Get filtered row count
            filtered_rows = con.execute(f"SELECT COUNT(*) FROM source_df WHERE {filter_condition}").fetchone()[0]
            print(f"    - Filtered rows: {filtered_rows:,}")
            
        else:
            query = f"""
                COPY (
                    SELECT * FROM source_df
                ) TO '{output_path}' (FORMAT CSV, HEADER, DELIMITER ',');
            """
            filtered_rows = total_rows
        
        # Execute the CSV write
        con.execute(query)
        
        # Verify the output file was created
        if output_file.exists():
            file_size = output_file.stat().st_size / (1024 * 1024)  # Size in MB
            print(f"✅ CSV successfully written!")
            print(f"    - File size: {file_size:.2f} MB")
            print(f"    - Rows written: {filtered_rows:,}")
            success = True
        else:
            print(f"❌ CSV file was not created!")
            success = False
            
    except Exception as e:
        print(f"❌ Error during CSV writing: {e}")
        success = False
        filtered_rows = 0
        
    finally:
        con.close()
    
    # Cleanup temporary parquet files
    print("\n🧹 Cleaning up temporary parquet files...")
    try:
        shutil.rmtree(parquet_tmp_path)
        print(f"✅ Cleaned up: {parquet_tmp_path}")
    except Exception as e:
        print(f"⚠️  Could not clean up {parquet_tmp_path}: {e}")
    
    # Summary
    print(f"\n📈 CSV Writing Summary:")
    print(f"   📁 Output file: {output_path}")
    print(f"   📊 Rows written: {filtered_rows:,}")
    print(f"   ✅ Success: {success}")
    
    if success:
        print("🎉 Done writing CSV file!")
    
    return {
        'success': success,
        'output_path': output_path,
        'rows_written': filtered_rows,
        'total_rows': total_rows if 'total_rows' in locals() else 0,
        'filter_applied': filter_condition is not None
    }

def write_csv_combined(df, single_csv_path: str, partitioned_output_dir: str, 
                      partition_column: str, parquet_tmp_path: str, 
                      filter_condition: str = None, threshold: int = 100000):
    """
    Combined method to write both single CSV and partitioned CSVs efficiently.
    Writes parquet only once and loads DuckDB only once.
    
    Args:
        df (DataFrame): Spark DataFrame to write
        single_csv_path (str): Full path for the single CSV file (including filename)
        partitioned_output_dir (str): Output directory for partitioned CSV files
        partition_column (str): Column to partition by (e.g., "mdoid", "mdo_id")
        parquet_tmp_path (str): Temporary Parquet output path
        filter_condition (str, optional): SQL WHERE condition to filter data
        threshold (int): Max row count per partition for optimal processing
    
    Returns:
        dict: Summary of both operations
    """
    import duckdb
    from pathlib import Path
    import shutil
    
    print(f"📦 Step 1: Writing DataFrame to Parquet...")
    print(f"    - Parquet path: {parquet_tmp_path}")
    
    # Write DataFrame to parquet with optimization - ONLY ONCE
    df \
        .write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(parquet_tmp_path)
    
    # Free DataFrame memory immediately after parquet write
    df.unpersist(blocking=True)
    print("🧹 Freed DataFrame from cache after parquet write")
    
    print("🦆 Step 2: Loading into DuckDB...")
    con = duckdb.connect()
    
    # Summary tracking
    summary = {
        'single_csv': {'success': False, 'rows_written': 0, 'path': single_csv_path},
        'partitioned_csv': {'successful_writes': 0, 'failed_writes': 0, 'total_partitions': 0},
        'total_rows': 0,
        'filter_applied': filter_condition is not None
    }
    
    try:
        # DuckDB optimizations
        con.execute("PRAGMA memory_limit='40GB';")
        con.execute("PRAGMA threads=14;")
        con.execute("PRAGMA temp_directory='/tmp/duckdb_spill';")
        con.execute("INSTALL parquet; LOAD parquet;")
        
        # Load the parquet data - ONLY ONCE
        con.execute(f"CREATE TABLE source_df AS SELECT * FROM parquet_scan('{parquet_tmp_path}/**/*.parquet');")
        
        # Get total row count
        total_rows = con.execute("SELECT COUNT(*) FROM source_df").fetchone()[0]
        summary['total_rows'] = total_rows
        print(f"    - Total rows loaded: {total_rows:,}")
        
        # Apply filter if specified
        if filter_condition:
            con.execute(f"CREATE TABLE filtered_df AS SELECT * FROM source_df WHERE {filter_condition};")
            filtered_rows = con.execute("SELECT COUNT(*) FROM filtered_df").fetchone()[0]
            working_table = "filtered_df"
            print(f"    - Applying filter: {filter_condition}")
            print(f"    - Filtered rows: {filtered_rows:,}")
        else:
            working_table = "source_df"
            filtered_rows = total_rows
        
        # ===== STEP 3: Write Single CSV =====
        print(f"📤 Step 3: Writing single CSV to {single_csv_path}...")
        
        # Ensure output directory exists
        single_csv_file = Path(single_csv_path)
        single_csv_file.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            query = f"""
                COPY (
                    SELECT * FROM {working_table}
                ) TO '{single_csv_path}' (FORMAT CSV, HEADER, DELIMITER ',');
            """
            
            con.execute(query)
            
            # Verify single CSV was created
            if single_csv_file.exists():
                file_size = single_csv_file.stat().st_size / (1024 * 1024)  # Size in MB
                print(f"✅ Single CSV successfully written!")
                print(f"    - File size: {file_size:.2f} MB")
                print(f"    - Rows written: {filtered_rows:,}")
                summary['single_csv']['success'] = True
                summary['single_csv']['rows_written'] = filtered_rows
            else:
                print(f"❌ Single CSV file was not created!")
                
        except Exception as e:
            print(f"❌ Error writing single CSV: {e}")
        
        # ===== STEP 4: Write Partitioned CSVs =====
        print(f"📤 Step 4: Writing partitioned CSVs to {partitioned_output_dir}...")
        
        # Get unique partition values
        partition_values = con.execute(f"SELECT DISTINCT {partition_column} FROM {working_table}").fetchall()
        total_partitions = len(partition_values)
        summary['partitioned_csv']['total_partitions'] = total_partitions
        
        print(f"    - Found {total_partitions} unique partitions")
        
        # Ensure partitioned output directory exists
        Path(partitioned_output_dir).mkdir(parents=True, exist_ok=True)
        
        # Track progress
        successful_writes = 0
        failed_writes = 0
        
        # Write individual partition files
        for i, (partition_val,) in enumerate(partition_values, 1):
            try:
                safe_val = str(partition_val).replace("/", "_") if partition_val is not None else "null"
                output_path = Path(partitioned_output_dir) / f"{partition_column}={safe_val}.csv"
                
                con.execute(f"""
                    COPY (
                        SELECT * FROM {working_table}
                        WHERE {partition_column} = ?
                    ) TO '{output_path}' (FORMAT CSV, HEADER, DELIMITER ',');
                """, [partition_val])
                
                successful_writes += 1
                
                # Progress update every 10 files or for the last file
                if i % 10 == 0 or i == total_partitions:
                    print(f"📊 Progress: {i}/{total_partitions} partitions written ({(i/total_partitions)*100:.1f}%)")
                
            except Exception as e:
                failed_writes += 1
                print(f"❌ Failed to write partition {partition_val}: {e}")
        
        # Update summary
        summary['partitioned_csv']['successful_writes'] = successful_writes
        summary['partitioned_csv']['failed_writes'] = failed_writes
        
        # Final partition summary
        print(f"\n📈 Partitioned CSV Writing Summary:")
        print(f"   ✅ Successfully written: {successful_writes}")
        print(f"   ❌ Failed: {failed_writes}")
        print(f"   📊 Total partitions: {total_partitions}")
        if total_partitions > 0:
            print(f"   🎯 Success rate: {(successful_writes/total_partitions)*100:.1f}%")
        
    except Exception as e:
        print(f"❌ Error during DuckDB processing: {e}")
        
    finally:
        con.close()
    
    # Cleanup temporary parquet files
    print("\n🧹 Cleaning up temporary parquet files...")
    try:
        shutil.rmtree(parquet_tmp_path)
        print(f"✅ Cleaned up: {parquet_tmp_path}")
    except Exception as e:
        print(f"⚠️  Could not clean up {parquet_tmp_path}: {e}")
    
    # Overall summary
    print(f"\n🎉 Combined CSV Writing Complete!")
    print(f"   📁 Single CSV: {single_csv_path}")
    print(f"   📂 Partitioned CSVs: {partitioned_output_dir}")
    print(f"   📊 Total rows processed: {summary['total_rows']:,}")
    
    return summary

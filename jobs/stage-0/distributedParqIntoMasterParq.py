import sys
from pathlib import Path
import os
import time

# ==============================
# 1. Configuration and Constants
# ==============================

# Ensure the parent directory is in sys.path for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckutil import duckutil  # Assuming duckutil is in the parent directory

# Set BASE_DATA_DIR two folders above the script location
BASE_DATA_DIR = Path(__file__).resolve().parents[2] / 'data-res/pq_files/cache_pq'
LARGE_DUCK_MEMORY_LIMIT = '14GB'  # Adjusted for efficient memory usage
SMALL_DUCK_MEMORY_LIMIT= "4GB"    # Smaller Threshold for Efficient Storage
BATCH_SIZE_SMALL = 10             # Larger for smaller folders
BATCH_SIZE_LARGE = 15             # Smaller for larger folders

# Lists of data folders categorized by size
LARGE_DATA_FOLDERS = ["enrolment", "user"]
SMALL_DATA_FOLDERS = [
    "acbp", "batch","esContent", "externalCourseEnrolments",
    "hierarchy", "kcmV6", "learnerLeaderBoard", "nlwContentCertificateGeneratedCount",
    "nlwContentLearningHours", "orgCompleteHierarchy", "orgHierarchy",
    "rating", "ratingSummary", "role", "userKarmaPoints",
    "userKarmaPointsSummary","org","externalContent","eventDetails","eventEnrolmentDetails"
]

# ==============================
# 2. Data Processing Function
# ==============================

def process_directory_data_into_db(folder_type: str, batch_size: int, memoryLimit):

    # Initialize DuckDB connection with optimized memory settings
    conn = duckutil.initialize_duckdb(memoryLimit)

    # Define the directory containing Parquet files
    mdo_dir = Path(BASE_DATA_DIR) / folder_type
    parquet_files = list(mdo_dir.glob("*.parquet"))

    # Check for the existence of Parquet files
    if not parquet_files:
        print(f"No Parquet files in {mdo_dir}, skipping.")
        conn.close()
        return

    # Create output directory for combined Parquet file
    BASE_OUTPUT_DIR = Path(__file__).resolve().parents[2] / 'output'
    output_dir = Path(BASE_OUTPUT_DIR)
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / f"{folder_type}_combined_data.parquet"

    # Remove existing output file (if any)
    if output_file.exists():
        os.remove(output_file)

    # Start processing time tracking
    start_time = time.perf_counter()

     # Initialize the combined table using the first Parquet file
    duckutil.executeQuery(conn,f"CREATE TABLE IF NOT EXISTS {folder_type}_combined AS SELECT * FROM read_parquet('{parquet_files[0]}');")

    # Append data from the remaining Parquet files in batches
    for file in parquet_files[1:]:
        try:
            duckutil.executeQuery(conn,f"INSERT INTO {folder_type}_combined SELECT * FROM read_parquet('{file}');")
        except Exception as e:
            print(f"Error processing file {file}: {e}")

    # Save the final combined table as a compressed Parquet file
    duckutil.executeQuery(conn,f"COPY {folder_type}_combined TO '{output_file}' (FORMAT PARQUET, COMPRESSION ZSTD);")
    
    # Cleanup: Drop the temporary combined table to free memory
    # duckutil.executeQuery(conn,f"DROP TABLE {folder_type}_combined")
    duckutil.close_duck_db(conn)

    # Display processing duration
    duration = time.perf_counter() - start_time
    print(f"Processed {folder_type} in {duration:.2f} seconds")


    
# ==============================
# Main Function
# ==============================
def main():
    # Process small data folders
    for folder in SMALL_DATA_FOLDERS:
        process_directory_data_into_db(folder, BATCH_SIZE_LARGE,SMALL_DUCK_MEMORY_LIMIT)

    # Process large data folders
    for folder in LARGE_DATA_FOLDERS:
        #print("Hello")
        process_directory_data_into_db(folder, BATCH_SIZE_SMALL,LARGE_DUCK_MEMORY_LIMIT)

if __name__ == "__main__":
    main()

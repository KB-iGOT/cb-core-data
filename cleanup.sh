
# ===== 1. LOGGING SETUP =====
log_file="/home/analytics/pyspark/cleanup.log"
exec &>> "$log_file"
echo "Running script at $(date)"
# 🎯 PURPOSE: All output (stdout & stderr) goes to log file for monitoring

# ===== 2. REPORT TYPES DEFINITION =====
data_reports=(
    'course-report'
    'user-report' 
    'user-enrollment-report'
    'cba-report'
    'org-hierarchy-report'
    'blended-program-report-cbp'  
    'cbp-report-mdo-enrolment' 
    'user-assessment-report-cbp'
    'blended-program-report-mdo' 
    'cbp-report'  
    'cbp-report-mdo-summary '  
    'kcm-report'
)
# 🎯 PURPOSE: Defines 6 types of analytics reports to process

# ===== 3. DIRECTORY SETUP =====
source_dir="/home/analytics/pyspark/reports/standalone-reports"     # Where reports are generated
# destination_dir="/home/analytics/pyspark/datadownload/data"              # Where reports are copied for download
# 🎯 PURPOSE: Source = production reports, Destination = download staging area

# ===== 4. CLEANUP OLD DATA FROM SOURCE =====
echo "Removing $source_dir old data"
cd $source_dir
for i in $(ls -A .)
do
    echo "Removing the content from $source_dir/$i"
    find $source_dir/$i/* -mtime +1 -exec rm -rvf {} \;  # Remove files older than 1 day
done
# 🎯 PURPOSE: Clean up old report files from production directory (keep only 1 day)

# ===== 5. DATE CALCULATIONS =====
today=$(date "+%Y-%m-%d")                               # 2024-08-21
yesterday=$(date -d "yesterday" "+%Y-%m-%d")             # 2024-08-20  
day_before_yesterday=$(date -d "$yesterday - 1 day" "+%d%b")  # 19Aug
# 🎯 PURPOSE: Calculate dates for file naming and cleanup operations

# ===== 6. PREPARE DESTINATION DIRECTORIES =====
# echo "Removing the directories under - $destination_dir"
# rm -rf "$destination_dir"                               # Delete entire download staging area

# echo "Creating the subdirectories directories - $destination_dir"
# for dir in "${data_reports[@]}"; do
#     mkdir -p "$destination_dir/$dir/$today-warehouse"   # Create fresh directories for today's reports
# done
# 🎯 PURPOSE: Clean slate for today's report downloads

# ===== 7. AUDIT SOURCE DIRECTORIES =====
# for dir in "${data_reports[@]}"; do
#     echo "Running ls -latr of $source_dir/$dir"
#     ls -latr "$source_dir/$dir/"*                       # List contents for debugging
# done
# 🎯 PURPOSE: Log what's available in source before copying

# # ===== 8. COPY TODAY'S REPORTS =====
# echo "Copying the full reports to $destination_dir"
# for dir in "${data_reports[@]}"; do
#     cp -r "$source_dir/$dir/"$today*warehouse "$destination_dir/$dir"  # Copy today's warehouse reports
# done
# chmod -R 777 "$destination_dir"                         # Make everything accessible
# # 🎯 PURPOSE: Stage today's reports for download

# ===== 9. CREATE BACKUP ZIP =====
# echo "Creating the zip"
# cd /home/analytics/pyspark/datadownload
# zip -r "/home/analytics/pyspark/report_backup/$(date "+%d%b").zip" data/     # Create zip named like "21Aug.zip"
# chmod 777 "/home/analytics/pyspark/report_backup/$(date "+%d%b").zip"
# rm "/home/analytics/pyspark/report_backup/$day_before_yesterday.zip"          # Remove 2-day-old backup
# # 🎯 PURPOSE: Create compressed backup and maintain 2-day retention

# ===== 10. CLEANUP YESTERDAY'S REPORTS FROM SOURCE =====
echo "Removing yesterdays's report from $source_dir"
# for dir in "${data_reports[@]}"; do
    # rm -rf "$source_dir/$dir/$yesterday-full"
    # rm -rf "$source_dir/$dir/$yesterday-warehouse" 
    rm -rf "$source_dir/merged"
    rm -rf "/home/analytics/pyspark/warehouse/fullReport"
# done
# 🎯 PURPOSE: Remove yesterday's reports from production directory

# # ===== 11. LOG CLEANUP =====
# echo "Removing the logs from /home/analytics/pyspark/logs/data-products and keeping the logs for 3 days"
# cd /home/analytics/pyspark/logs/data-products/
# find . -atime +2 -exec rm {} \;                         # Remove log files not accessed for 2+ days
# # 🎯 PURPOSE: Prevent log directory from filling up disk space
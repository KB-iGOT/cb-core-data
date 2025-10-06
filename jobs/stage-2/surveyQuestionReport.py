import json
import os
import glob
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
from pathlib import Path
from datetime import datetime
import sys


from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import (
    col, from_json, lit, udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType
)
# from pyspark.sql import SaveMode
from pyspark import StorageLevel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


sys.path.append(str(Path(__file__).resolve().parents[2]))

from jobs.default_config import create_config
from jobs.config import get_environment_config
from dfutil.utils.utils import druidDFOption
from dfutil.dfexport import dfexportutil


class SurveyQuestionReportModel:
    """PySpark implementation of SurveyQuestionReportModel for generating survey question reports."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.class_name = "SurveyQuestionReportModel"
    
    def name(self) -> str:
        """Return model name."""
        return "SurveyQuestionReportModel"
    
    def get_date(self) -> str:
        """Get current date in YYYY-MM-DD format."""
        return datetime.now().strftime('%Y-%m-%d')
    
    def get_report_config(self, filter_name: str) -> str:
        """
        Get report configuration from MongoDB.
        
        Args:
            filter_name: Filter to identify specific report configuration
            
        Returns:
            JSON string containing report configuration
        """
        try:
            logger.info("Querying MongoDB database to get report configurations")
            
            # MongoDB connection details
            mongo_uri = f"mongodb://{self.config.mlSparkMongoConnectionHost}:27017"
            database = self.config.mlMongoDatabase
            collection = self.config.reportConfigCollection
            
            # Read from MongoDB using Spark MongoDB connector
            mongo_df = (self.spark.read
                       .format("mongo")
                       .option("uri", mongo_uri)
                       .option("database", database)
                       .option("collection", collection)
                       .load())
            
            # Filter by report name
            filtered_df = mongo_df.filter(col("report") == filter_name)
            
            if filtered_df.count() == 0:
                raise ValueError(f"No configuration found for report: {filter_name}")
            
            # Get configuration string
            config_string = filtered_df.select("config").collect()[0]["config"]
            logger.info(f"Report config for {filter_name}:\n{config_string}")
            
            return config_string
            
        except Exception as e:
            logger.error(f"Error retrieving report config: {str(e)}")
            raise
    
    def get_solution_ids_as_df(self, solution_ids: str) -> DataFrame:
        """
        Convert solution IDs string to DataFrame.
        
        Args:
            solution_ids: Comma-separated solution IDs
            
        Returns:
            DataFrame with solutionId and solutionName columns
        """
        # Parse solution IDs (assuming format: "id1:name1,id2:name2")
        solution_data = []
        for item in solution_ids.split(','):
            if ':' in item:
                solution_id, solution_name = item.strip().split(':', 1)
                solution_data.append(Row(solutionId=solution_id, solutionName=solution_name))
            else:
                solution_data.append(Row(solutionId=item.strip(), solutionName=item.strip()))
        
        return self.spark.createDataFrame(solution_data)
    
    def load_all_unique_solution_ids(self, datasource: str) -> DataFrame:
        """
        Load all unique solution IDs from Druid datasource.
        
        Args:
            datasource: Druid datasource name
            
        Returns:
            DataFrame with unique solution IDs and names
        """
        query = f'SELECT DISTINCT solutionId, solutionName FROM "{datasource}"'
        return druidDFOption(query, self.config.sparkDruidRouterHost)
    
    def get_solutions_end_date(self, solution_ids_df: DataFrame) -> DataFrame:
        """
        Get solution end dates from MongoDB.
        
        Args:
            solution_ids_df: DataFrame containing solution IDs
            
        Returns:
            DataFrame with solution IDs, names, and end dates
        """
        try:
            # Get solution IDs as list
            solution_ids = [row['solutionId'] for row in solution_ids_df.collect()]
            
            # MongoDB query for solutions
            mongo_uri = f"mongodb://{self.config.ml_spark_mongo_connection_host}:27017"
            database = self.config.ml_mongo_database
            collection = "solutions"  # Assuming solutions collection name
            
            solutions_df = (self.spark.read
                          .format("mongo")
                          .option("uri", mongo_uri)
                          .option("database", database)
                          .option("collection", collection)
                          .load())
            
            # Filter by solution IDs
            filtered_solutions = solutions_df.filter(col("solutionId").isin(solution_ids))
            
            return filtered_solutions.select("solutionId", "solutionName", "endDate")
            
        except Exception as e:
            logger.error(f"Error retrieving solutions end date: {str(e)}")
            raise
    
    def is_solution_within_report_date(self, end_date_str: str) -> bool:
        """
        Check if solution end date is within reporting period.
        
        Args:
            end_date_str: End date string in YYYY-MM-DD format
            
        Returns:
            True if solution is within reporting period
        """
        try:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
            today = datetime.now().date()
            grace_period = int(self.config.grace_period)
            updated_date = today - timedelta(days=grace_period)
            
            return (end_date == today or 
                   end_date > today or 
                   end_date > updated_date or 
                   end_date == updated_date)
            
        except Exception as e:
            logger.error(f"Error checking solution date: {str(e)}")
            return False
    
    def process_profile_data(self, original_df: DataFrame, 
                           profile_schema: StructType, 
                           required_csv_columns: List[str]) -> DataFrame:
        """
        Process user profile data from JSON.
        
        Args:
            original_df: Original DataFrame with userProfile column
            profile_schema: Schema for parsing JSON profile
            required_csv_columns: Required columns for CSV output
            
        Returns:
            DataFrame with processed profile data
        """
        has_user_profile = "userProfile" in original_df.columns
        
        if has_user_profile:
            # Parse JSON profile
            parsed_df = original_df.withColumn(
                "parsedProfile", 
                from_json(col("userProfile"), profile_schema)
            )
            return parsed_df.select(*required_csv_columns)
        else:
            # Add empty parsed profile
            empty_parsed_df = original_df.withColumn(
                "parsedProfile", 
                lit(None).cast(StringType())
            )
            return empty_parsed_df.select(*required_csv_columns)
    
    def validate_columns(self, df: DataFrame, expected_columns: List[str]) -> bool:
        """
        Validate if DataFrame has all expected columns.
        
        Args:
            df: DataFrame to validate
            expected_columns: List of expected column names
            
        Returns:
            True if all columns exist
        """
        df_columns = set(df.columns)
        expected_columns_set = set(expected_columns)
        return expected_columns_set.issubset(df_columns)
    
    def combine_csv_files(self, input_path: str, output_path: str):
        """
        Combine multiple CSV part files into single CSV.
        
        Args:
            input_path: Directory containing part files
            output_path: Output CSV file path
        """
        try:
            # Find all CSV part files
            part_files = glob.glob(os.path.join(input_path, "part-*.csv"))
            part_files.sort()
            
            is_first_file = True
            
            with open(output_path, 'w', encoding='utf-8') as output_file:
                for part_file in part_files:
                    with open(part_file, 'r', encoding='utf-8') as input_file:
                        lines = input_file.readlines()
                        
                        for idx, line in enumerate(lines):
                            # Skip header for subsequent files
                            if is_first_file or idx > 0:
                                output_file.write(line)
                        
                        is_first_file = False
            
            # Delete part files
            for part_file in part_files:
                os.remove(part_file)
                
            logger.info(f"Combined CSV files into: {output_path}")
            
        except Exception as e:
            logger.error(f"Error combining CSV files: {str(e)}")
            raise
    
    def get_solution_id_data(self, columns: str, datasource: str, 
                           solution_id: str, solution_name: str, batch_size: int):
        """
        Get and process data for a specific solution ID in batches.
        
        Args:
            columns: Comma-separated list of columns to query
            datasource: Druid datasource name
            solution_id: Solution ID to process
            solution_name: Solution name for file naming
            batch_size: Batch size for processing
        """
        try:
            # Get all survey submission IDs for the solution
            submission_query = f'''SELECT DISTINCT(surveySubmissionId) FROM "{datasource}" WHERE solutionId='{solution_id}' '''
            
            submission_ids_df = druidDFOption(
                submission_query, 
                self.config.sparkDruidRouterHost, 
                limit=1000000
            )
            
            if submission_ids_df is None:
                submission_ids_df = self.spark.createDataFrame([], StringType())
            
            submission_ids = [row['surveySubmissionId'] for row in submission_ids_df.collect()]
            logger.info(f"Total {len(submission_ids)} Survey Submissions for solutionId: {solution_id}")
            
            # Process in batches
            batch_count = 0
            for i in range(0, len(submission_ids), batch_size):
                batch_count += 1
                batch_submission_ids = submission_ids[i:i + batch_size]
                
                # Create batch query
                ids_list = "','".join(batch_submission_ids)
                batch_query = f'''SELECT {columns} FROM "{datasource}" WHERE solutionId='{solution_id}' AND surveySubmissionId IN ('{ids_list}')'''
                
                # Execute batch query
                batch_df = druidDFOption(
                    batch_query, 
                    self.config.sparkDruidRouterHost, 
                    limit=1000000
                )
                
                if batch_df is None:
                    batch_df = self.spark.createDataFrame([], StringType())
                
                # Cache DataFrame
                batch_df.persist(StorageLevel.DISK_ONLY)
                
                # Process evidences column if exists
                if "evidences" in batch_df.columns:
                    base_url = self.config.baseUrlForEvidences
                    
                    # UDF to add base URL to evidences
                    def add_base_url(evidences):
                        if evidences and evidences.strip():
                            urls = evidences.split(", ")
                            return ",".join([f"{base_url}{url}" for url in urls])
                        return evidences
                    
                    add_base_url_udf = udf(add_base_url, StringType())
                    batch_df = batch_df.withColumn("evidences", add_base_url_udf(col("evidences")))
                
                # Process profile data (this would need the schema and columns from config)
                final_solution_df = self.process_profile_data(batch_df, user_profile_schema, required_csv_columns)
                
                # For now, use batch_df as is
                # final_solution_df = batch_df
                
                # Validate columns and generate report
                expected_columns = getattr(self.config, 'sortingColumns', '').split(',')
                expected_columns = [col.strip() for col in expected_columns if col.strip()]
                
                if self.validate_columns(final_solution_df, expected_columns):
                    sorted_df = final_solution_df.select(*expected_columns)
                    
                    today = self.get_date()
                    report_path = f"{self.config.mlReportPath}/{today}/SurveyQuestionsReport"
                    dfexportutil.write_single_csv_duckdb(sorted_df, report_path,  f"{self.config.localReportDir}/temp/SurveyQuestionsReport/{today}",)
                    
                    logger.info(f"Batch: {batch_count}, Successfully generated survey question csv report for solutionId: {solution_id}")
                else:
                    logger.error(f"Error occurred while matching DataFrame columns with config sort columns for solutionId: {solution_id}")
                
                # Unpersist DataFrame
                batch_df.unpersist()
            
            # Clean solution name for file naming
            clean_solution_name = solution_name
            clean_solution_name = ''.join(c for c in clean_solution_name if c.isalnum() or c.isspace())
            clean_solution_name = ' '.join(clean_solution_name.split())
            
            logger.info(f"Total {batch_count} batches processed for solutionId: {solution_id}")
            
            # Combine CSV files
            today = self.get_date()
            report_dir = os.path.join(self.config.localReportDir, 
                                    self.config.mlReportPath, today, "SurveyQuestionsReport")
            output_file = os.path.join(report_dir, f"{clean_solution_name}-{solution_id}.csv")
            
            self.combine_csv_files(report_dir, output_file)
            
        except Exception as e:
            logger.error(f"Error processing solution ID data: {str(e)}")
            raise
    
    def generate_survey_question_report(self, solution_id: str, solution_name: str, 
                                      columns_to_query: str):
        """
        Generate survey question report for a specific solution.
        
        Args:
            solution_id: Solution ID to process
            solution_name: Solution name
            columns_to_query: Columns to query from Druid
        """
        datasource = "sl-survey"
        batch_size = int(self.config.survey_question_report_batch_size)
        
        self.get_solution_id_data(columns_to_query, datasource, solution_id, solution_name, batch_size)
        logger.info(f"Successfully Generated Survey Question CSV In Batches And Combined Into A Single File For SolutionId: {solution_id}")
    
    def zip_and_sync_reports(self, local_path: str, remote_path: str):
        """
        Zip reports and sync to blob storage.
        
        Args:
            local_path: Local directory path
            remote_path: Remote blob storage path
        """
        try:
            # This would typically integrate with cloud storage APIs
            logger.info(f"Zipping and syncing reports from {local_path} to {remote_path}")
            # Implementation would depend on specific cloud provider (Azure, AWS, GCP)
            
        except Exception as e:
            logger.error(f"Error zipping and syncing reports: {str(e)}")
            raise
    
    def process_data(self):
        """
        Main processing method for SurveyQuestionReportModel.
        
        Args:
            timestamp: Processing timestamp
        """
        try:
            today = self.get_date()
            logger.info("Starting SurveyQuestionReportModel processing")
            
            # Get report configuration
            survey_question_report_config = self.get_report_config("surveyQuestionReport")
            
            # Parse JSON configuration
            config_map = json.loads(survey_question_report_config)
            report_columns_map = config_map["reportColumns"]
            user_profile_columns_map = config_map["userProfileColumns"]
            sorting_columns = config_map["sortingColumns"]
            
            # Prepare columns and schema
            columns_to_query = ",".join(report_columns_map.keys())
            
            # Create user profile schema
            user_profile_fields = [
                StructField(key, StringType(), True) 
                for key in user_profile_columns_map.keys()
            ]
            user_profile_schema = StructType(user_profile_fields)
            
            # Prepare column mappings for CSV
            report_columns = [col(key).alias(report_columns_map[key]) for key in report_columns_map.keys()]
            user_profile_columns = [
                col(f"parsedProfile.{key}").alias(user_profile_columns_map[key]) 
                for key in user_profile_columns_map.keys()
            ]
            required_csv_columns = [col.alias for col in (report_columns + user_profile_columns)]
            
            report_path = f"{self.config.mlReportPath}/{today}/SurveyQuestionsReport"
            
            # Process solution IDs
            solution_ids = getattr(self.config, 'solutionIDs', None)
            if solution_ids and solution_ids.strip():
                logger.info("Processing report requests for specified solutionId's")
                solution_ids_df = self.get_solution_ids_as_df(solution_ids)
                
                for row in solution_ids_df.collect():
                    solution_id = row['solutionId']
                    solution_name = row['solutionName']
                    logger.info(f"Started processing report request for solutionId: {solution_id}")
                    self.generate_survey_question_report(solution_id, solution_name, columns_to_query)
            
            else:
                logger.info("Processing report requests for all solutionId's")
                logger.info("Querying Druid to get all unique solutionId's")
                solution_ids_df = self.load_all_unique_solution_ids("sl-survey")
                
                if getattr(self.config, 'includeExpiredSolutionIDs', False):
                    logger.info("Generating report for all expired solutionId's also")
                    for row in solution_ids_df.collect():
                        solution_id = row['solutionId']
                        solution_name = row['solutionName']
                        logger.info(f"Started processing report request for solutionId: {solution_id}")
                        self.generate_survey_question_report(solution_id, solution_name, columns_to_query)
                
                else:
                    logger.info("Query MongoDB to get solution end-date for all unique solutionId's")
                    solutions_end_date_df = self.get_solutions_end_date(solution_ids_df)
                    
                    for row in solutions_end_date_df.collect():
                        solution_id = row['solutionId']
                        solution_name = row['solutionName']
                        end_date = row['endDate']
                        
                        if end_date:
                            end_date_str = end_date.strftime('%Y-%m-%d') if hasattr(end_date, 'strftime') else str(end_date)
                            logger.info(f"Started processing report request for solutionId: {solution_id}")
                            
                            if self.is_solution_within_report_date(end_date_str):
                                logger.info(f"Solution with Id {solution_id} will end on {end_date_str}")
                                self.generate_survey_question_report(solution_id, solution_name, columns_to_query)
                            else:
                                logger.info(f"Solution with Id {solution_id} has ended on {end_date_str}, not generating report")
                        else:
                            logger.info(f"End Date for solutionId: {solution_id} is NULL, skipping report generation")
            
            # Zip and sync reports
            logger.info("Zipping the CSV content folder and syncing to blob storage")
            local_report_path = os.path.join(self.config.local_report_dir, report_path)
            self.zip_and_sync_reports(local_report_path, report_path)
            logger.info("Successfully zipped folder and synced to blob storage")
            
        except Exception as e:
            logger.error(f"Error occurred during SurveyQuestionReportModel processing: {str(e)}")
            raise


def main():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1'

    spark = SparkSession.builder \
        .appName("Survey Question Report") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "15g") \
        .config("spark.driver.memory", "15g") \
        .config("spark.executor.memoryFraction", "0.7") \
        .config("spark.storage.memoryFraction", "0.2") \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    config_dict = get_environment_config()
    config = create_config(config_dict)
    start_time = datetime.now()
    print(f"[START] SurveyQuestionReportModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    model = SurveyQuestionReportModel(spark,config)
    model.process_data()
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] SurveyQuestionReportModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()

if __name__ == "__main__":
   main()
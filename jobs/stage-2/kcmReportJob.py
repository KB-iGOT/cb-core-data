import sys
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import MapType, StringType, StructType, StructField
from datetime import datetime
import sys


# ==============================
# 1. Configuration and Constants
# ==============================

# Ensure the parent directory is in sys.path for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckutil import duckutil,datautil,storageutil  # Assuming duckutil is in the parent directory
from dfutil.content import contentDF  # Assuming duckutil is in the parent directory
from constants.QueryConstants import QueryConstants
from ParquetFileConstants import ParquetFileConstants

class KCMModel:
    """
    Python implementation of KCM (Knowledge and Competency Management) Model
    """
    
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.kcm.KCMModel"
        
    def name(self):
        return "KCMModel"
    
    @staticmethod
    def get_date():
        """Get current date in required format"""
        return datetime.now().strftime("%Y-%m-%d")
    
    @staticmethod
    def current_date_time():
        """Get current datetime in required format"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def process_data(self, spark):
        """
        Process KCM data
        
        Args:
            timestamp: Long timestamp
            spark: SparkSession
            sc: SparkContext
            fc: FrameworkContext
            conf: DashboardConfig
        """
        try:
            duckdb_conn = duckutil.initialize_duckdb()
            today = self.get_date()
            report_path_content_competency_mapping = f"standalone-reports/kcm-report/{today}/ContentCompetencyMapping"
            report_path_competency_hierarchy = f"standalone-reports/kcm-report/{today}/CompetencyHierarchy"
            file_name = "ContentCompetencyMapping"

            # Content - Competency Mapping data
            categories = ["Course", "Program", "Blended Program", "CuratedCollections", "Standalone Assessment", "Curated Program"]
            cbp_details= contentDF.all_course_program_es_data_frame(spark,categories).where("courseStatus IN ('Live', 'Retired')") \
                .select("courseID", "competencyAreaRefId", "competencyThemeRefId", "competencySubThemeRefId", "courseName")
            
            #explode area, theme and sub theme separately
            area_exploded = cbp_details.select(
                F.col("courseID"), 
                F.expr("posexplode_outer(competencyAreaRefId) as (pos, competency_area_id)")
            ).repartition(F.col("courseID"))
            
            
            theme_exploded = cbp_details.select(
                F.col("courseID"), 
                F.expr("posexplode_outer(competencyThemeRefId) as (pos, competency_theme_id)")
            ).repartition(F.col("courseID"))
            
            sub_theme_exploded = cbp_details.select(
                F.col("courseID"), 
                F.expr("posexplode_outer(competencySubThemeRefId) as (pos, competency_sub_theme_id)")
            ).repartition(F.col("courseID"))
            
            # # Joining area, theme and subtheme based on position
            competency_joined_df = area_exploded.join(theme_exploded, ["courseID", "pos"]) \
                .join(sub_theme_exploded, ["courseID", "pos"])
            
          
            # # joining with cbp_details for getting courses with no competencies mapped to it
            competency_content_mapping_df = cbp_details \
                .join(competency_joined_df, ["courseID"], "left") \
                .dropDuplicates(["courseID", "competency_area_id", "competency_theme_id", "competency_sub_theme_id"]) \
            
            content_mapping_df = competency_content_mapping_df \
                .withColumn("data_last_generated_on", F.lit(self.current_date_time())) \
                .select(
                    F.col("courseID").alias("course_id"), 
                    F.col("competency_area_id"), 
                    F.col("competency_theme_id"), 
                    F.col("competency_sub_theme_id"), 
                    F.col("data_last_generated_on")
                )

            # content_mapping_df.show()
            # # # changes for creating avro file for warehouse
            # # self.warehouse_cache_write(content_mapping_df.distinct().coalesce(1), conf.dw_kcm_content_table)
            # # self.warehouse_pq_cache_write(content_mapping_df.distinct().coalesce(1), conf.dw_kcm_content_table)

            # # Load KCM v6 data
            # 1. Read the data
            kcmv6 = spark.read.parquet(ParquetFileConstants.KCMV6_PARQUET_FILE)

            # 2. Define the schema
            hierarchy_schema = """
            STRUCT<
                categories: ARRAY<
                    STRUCT<
                        terms: ARRAY<
                            STRUCT<
                                refId: STRING,
                                name: STRING,
                                description: STRING,
                                associations: ARRAY<
                                    STRUCT<
                                        refId: STRING,
                                        name: STRING,
                                        description: STRING
                                    >
                                >
                            >
                        >
                    >
                >
            >
            """

            # 3. Parse the JSON column (ONLY ONCE)
            kcmv6 = kcmv6.withColumn(
                "hierarchy_parsed", 
                F.from_json(F.col("hierarchy"), hierarchy_schema)
            )

            # 4. Process area data (using hierarchy_parsed)
            kcm_area = kcmv6.withColumn(
                "competencyAreaData", 
                F.col("hierarchy_parsed.categories")[0]
            ).withColumn(
                "termsExploded", 
                F.explode(F.col("competencyAreaData.terms"))
            ).withColumn(
                "associatedTheme", 
                F.explode(F.col("termsExploded.associations"))
            ).select(
                F.col("termsExploded.refId").alias("areaID"),
                F.col("termsExploded.name").alias("areaName"),
                F.col("termsExploded.description").alias("areaDescription"),
                F.col("associatedTheme.refId").alias("themeID"),
                F.col("associatedTheme.name").alias("themeName")
            )

            # 5. Process theme data (using hierarchy_parsed)
            kcm_theme = kcmv6.withColumn(
                "competencyThemeData", 
                F.col("hierarchy_parsed.categories")[1]
            ).withColumn(
                "termsExploded", 
                F.explode(F.col("competencyThemeData.terms"))
            ).withColumn(
                "associatedSubTheme", 
                F.explode(F.col("termsExploded.associations"))
            ).select(
                F.col("termsExploded.refId").alias("themeID"),
                F.col("termsExploded.name").alias("themeName"),
                F.col("termsExploded.description").alias("themeDescription"),
                F.col("associatedSubTheme.refId").alias("subThemeID"),
                F.col("associatedSubTheme.name").alias("subThemeName"),
                F.col("associatedSubTheme.description").alias("subThemeDescription")
            )

            # 6. Join and finalize
            competency_details_df = kcm_area.join(
                kcm_theme, 
                ["themeID", "themeName"], 
                "outer"
            ).select(
                F.col("areaID").alias("competency_area_id"),
                F.col("areaName").alias("competency_area"),
                F.col("areaDescription").alias("competency_area_description"),
                F.col("themeID").alias("competency_theme_id"),
                F.col("themeName").alias("competency_theme"),
                F.col("themeDescription").alias("competency_theme_description"),
                F.col("subThemeID").alias("competency_sub_theme_id"),
                F.col("subThemeName").alias("competency_sub_theme"),
                F.col("subThemeDescription").alias("competency_sub_theme_description")
            ).withColumn(
                "competency_theme_type", 
                F.lit("Null")
            ).withColumn(
                "data_last_generated_on", 
                F.lit(self.current_date_time())
            )

           

            # # Write to warehouse cache
            # # self.warehouse_cache_write(competency_details_df.distinct().coalesce(1), conf.dw_kcm_dictionary_table)
            # # self.warehouse_pq_cache_write(competency_details_df.distinct().coalesce(1), conf.dw_kcm_dictionary_table)

            # # Competency reporting
            competency_reporting = competency_content_mapping_df \
                .join(competency_details_df, ["competency_area_id", "competency_theme_id", "competency_sub_theme_id"]) \
                .withColumn("competency_theme_type", F.lit("Null")) \
                .select(
                    F.col("courseID").alias("content_id"),
                    F.col("courseName").alias("content_name"),
                    F.col("competency_area"),
                    F.col("competency_area_description"),
                    F.col("competency_theme"),
                    F.col("competency_theme_description"),
                    F.col("competency_theme_type"),
                    F.col("competency_sub_theme"),
                    F.col("competency_sub_theme_description")
                ) \
                .orderBy("content_id") \
                .distinct()

            competency_reporting.show()
            # # Generate report
            # self.generate_report(competency_reporting, report_path_content_competency_mapping, file_name=file_name)

            storageutil.generate_report(competency_reporting, report_path_content_competency_mapping, file_name=file_name)

            # Report sync if enabled
            # if conf.report_sync_enable:
            #     self.sync_reports(f"{conf.local_report_dir}/{report_path_content_competency_mapping}", 
            #                        report_path_content_competency_mapping)
                
        except Exception as e:
            print(f"Error occurred during KCMModel processing: {str(e)}")
            sys.exit(1)
    
    @staticmethod
    def get_kcm_schema():
        """Returns the schema for KCM data"""
        # KCM v6 schema translation from Scala
        from pyspark.sql.types import StructType, StructField, StringType, ArrayType
        
        return StructType([
            StructField("categories", ArrayType(
                StructType([
                    StructField("code", StringType(), False),
                    StructField("terms", ArrayType(
                        StructType([
                            StructField("name", StringType(), False),
                            StructField("description", StringType(), False),
                            StructField("refId", StringType(), False),
                            StructField("category", StringType(), False),
                            StructField("associations", ArrayType(
                                StructType([
                                    StructField("name", StringType(), False),
                                    StructField("refType", StringType(), False),
                                    StructField("description", StringType(), False),
                                    StructField("refId", StringType(), False),
                                    StructField("category", StringType(), False)
                                ]), 
                                False
                            ))
                        ]), 
                        False
                    ))
                ]),
                False
            ))
        ])
    
   
# Example usage:
if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder.appName("KCM Model").getOrCreate()
    
    # Create model instance
    model = KCMModel()
    model.process_data( spark=spark)
    
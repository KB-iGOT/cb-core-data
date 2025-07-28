from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user.userDFUtil import exportDFToParquet
from dfutil.content import contentDFUtil
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode_outer,from_json, lit,when, format_string, expr,lower,avg,count
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from pyspark.sql import DataFrame, SparkSession
from constants.ParquetFileConstants import ParquetFileConstants
from typing import List
from util import schemas

def esContentDataFrame(
    spark: SparkSession
) -> DataFrame:
    return spark.read.parquet(ParquetFileConstants.ESCONTENT_PARQUET_FILE)

def preComputeAllCourseProgramESDataFrame(spark: SparkSession) -> DataFrame:
  
    contentDF = esContentDataFrame(spark) \
        .withColumn("courseOrgID", explode_outer(col("createdFor"))) \
        .withColumn("contentLanguage", explode_outer(col("language"))) \
        .withColumn("competencyAreaRefId", 
                   F.when(F.col("competencies_v6").isNotNull(), 
                         F.col("competencies_v6")["competencyAreaRefId"])
                   .otherwise(F.array())) \
        .withColumn("competencyThemeRefId", 
                   F.when(F.col("competencies_v6").isNotNull(), 
                         F.col("competencies_v6")["competencyThemeRefId"])
                   .otherwise(F.array())) \
        .withColumn("competencySubThemeRefId", 
                   F.when(F.col("competencies_v6").isNotNull(), 
                         F.col("competencies_v6")["competencySubThemeRefId"])
                   .otherwise(F.array())) \
        .select(
            col("identifier").alias("courseID"),
            col("primaryCategory").alias("category"),
            col("name").alias("courseName"),
            col("status").alias("courseStatus"),
            col("reviewStatus").alias("courseReviewStatus"),
            col("channel").alias("courseChannel"),
            col("lastPublishedOn").alias("courseLastPublishedOn"),
            col("duration").cast(FloatType()).alias("courseDuration"),
            col("leafNodesCount").alias("courseResourceCount"),
            col("lastStatusChangedOn").alias("lastStatusChangedOn"),
            col("programDirectorName"),
            col("courseOrgID"),
            F.col("competencyAreaRefId"),
            F.col("competencyThemeRefId"),
            F.col("competencySubThemeRefId"),
            col("contentLanguage"),
            col("courseCategory")
        ) \
        .dropDuplicates(["courseID", "category"]) \
        .fillna(0.0, subset=["courseDuration"]) \
        .fillna(0, subset=["courseResourceCount"])
        
    exportDFToParquet(contentDF,ParquetFileConstants.ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE)

def preComputeRatingAndSummaryDataFrame(spark):
    ratingSummaryDF = spark.read.parquet(ParquetFileConstants.RATING_SUMMARY_PARQUET_FILE) \
        .where(expr("total_number_of_ratings > 0")) \
        .withColumn("ratingAverage", expr("sum_of_total_ratings / total_number_of_ratings")) \
        .select(
            col("activityid").alias("courseID"),
            col("activitytype").alias("categoryLower"),
            col("sum_of_total_ratings").alias("ratingSum"),
            col("total_number_of_ratings").alias("ratingCount"),
            col("ratingAverage"),
            col("totalcount1stars").alias("count1Star"),
            col("totalcount2stars").alias("count2Star"),
            col("totalcount3stars").alias("count3Star"),
            col("totalcount4stars").alias("count4Star"),
            col("totalcount5stars").alias("count5Star")
        ) \
        .withColumn("categoryLower", lower(col("categoryLower"))) \
        .dropDuplicates(["courseID", "categoryLower"])
    
    exportDFToParquet(ratingSummaryDF,ParquetFileConstants.RATING_SUMMARY_COMPUTED_PARQUET_FILE)

    ratingRawDF = spark.read.parquet(ParquetFileConstants.RATING_PARQUET_FILE)
    ratingDF = ratingRawDF \
        .select(
            col("activityid").alias("courseID"),
            col("userid").alias("userID"), 
            col("rating").alias("userRating"),
            col("activitytype").alias("cbpType"),
            col("createdon").alias("createdOn")
        )
    exportDFToParquet(ratingDF,ParquetFileConstants.RATING_COMPUTED_PARQUET_FILE)


    contentRating = ratingDF.filter(col("userRating").isNotNull()) \
    .groupBy("courseID") \
    .agg(
        count("userRating").alias("totalRatings"),
        avg("userRating").alias("rating")
    ) \
    .select("courseID", "totalRatings", "rating")

    exportDFToParquet(contentRating,ParquetFileConstants.CONTENT_RATING_COMPUTED_PARQUET_FILE)

def preComputeContentDataFrame(spark: SparkSession):
    contentRatingDF=spark.read.parquet(ParquetFileConstants.CONTENT_RATING_COMPUTED_PARQUET_FILE)    
    contentWithRatingDF=spark.read.parquet(ParquetFileConstants.ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE).join(
        contentRatingDF,"courseID","left").drop(contentRatingDF["courseID"])
    
    orgDF=spark.read.parquet(ParquetFileConstants.ORG_COMPUTED_PARQUET_FILE).select(
      col("orgID").alias("courseOrgID"),
      col("orgName").alias("courseOrgName"),
      col("orgStatus").alias("courseOrgStatus")
    )

    contentWithOrgRatingDF=contentWithRatingDF.join(orgDF, contentWithRatingDF["courseOrgID"] == orgDF["courseOrgID"], "left") \
        .drop(orgDF["courseOrgID"]) \
        
    exportDFToParquet(contentWithOrgRatingDF,ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE)


def preComputeExternalContentDataFrame(spark) -> DataFrame:
        df = (
            spark.read.parquet(ParquetFileConstants.EXTERNAL_CONTENT_PARQUET_FILE)
            .withColumn("parsed_data", from_json(col("cios_data"), schemas.cios_data_schema))
            .withColumnRenamed("courseid", "content_id")
            .select(
                col("content_id"),
                col("parsed_data.content.name").alias("courseName"),
                col("parsed_data.content.duration").alias("courseDuration"),
                col("parsed_data.content.lastUpdatedOn").alias("courseLastPublishedOn"),
                col("parsed_data.content.contentPartner.id").alias("courseOrgID"),
                col("parsed_data.content.contentPartner.contentPartnerName").alias("courseOrgName"),
                lit("External Content").alias("category"),
                lit("LIVE").alias("courseStatus")
            )
        )
        exportDFToParquet(df,ParquetFileConstants.EXTERNAL_CONTENT_COMPUTED_PARQUET_FILE)

def precomputeContentHierarchyDataFrame(spark: SparkSession) -> DataFrame:
    contentHierarchydf = spark.read.parquet(ParquetFileConstants.HIERARCHY_PARQUET_FILE).select(col("identifier"), col("hierarchy"))
    exportDFToParquet(contentHierarchydf, ParquetFileConstants.CONTENT_HIERARCHY_SELECT_PARQUET_FILE)
    
@staticmethod
def duration_format(df, in_col, out_col=None):
    out_col_name = out_col if out_col is not None else in_col
    return df.withColumn(out_col_name,
        when(col(in_col).isNull(), lit(""))
        .otherwise(
            format_string("%02d:%02d:%02d",
                expr(f"{in_col} / 3600").cast("int"),
                expr(f"{in_col} % 3600 / 60").cast("int"),
                expr(f"{in_col} % 60").cast("int")
            )
        )
    )

def allCourseProgramDetailsWithCompetenciesJsonDataFrame(
    allCourseProgramESDF: DataFrame, 
    hierarchyDF: DataFrame, 
    orgDF: DataFrame, 
) -> DataFrame:
    # Add hierarchy column with competencies
    df = addHierarchyColumn(
        allCourseProgramESDF, 
        hierarchyDF, 
        "courseID", 
        "data", 
        competencies=True
    ).withColumn("competenciesJson", col("data.competencies_v3"))
    
    # Add course organization details and clean up
    courseOrgDetailsDF = addCourseOrgDetails(df, orgDF) \
        .fillna(0.0, subset=["courseDuration"]) \
        .fillna(0, subset=["courseResourceCount"]) \
        .drop("data")
    
    return courseOrgDetailsDF

def addHierarchyColumn(
    df: DataFrame, 
    hierarchyDF: DataFrame, 
    idCol: str, 
    asCol: str,
    children: bool = False, 
    competencies: bool = False, 
    l2Children: bool = False,
) -> DataFrame:
    # Get the hierarchy schema based on the flags
    hierarchySchema = schemas.make_hierarchy_schema(children, competencies, l2Children)
    
    result_df = df.join(
        hierarchyDF, 
        df[idCol] == hierarchyDF["identifier"], 
        "left"
    ) \
    .fillna("{}", subset=["hierarchy"]) \
    .withColumn(asCol, from_json(col("hierarchy"), hierarchySchema)) \
    .drop("hierarchy")
    
    return result_df

def addCourseOrgDetails(
    courseDF: DataFrame, 
    orgDF: DataFrame
) -> DataFrame:
    # Prepare organization DataFrame with renamed columns
    joinOrgDF = orgDF.select(
        col("orgID").alias("courseOrgID"),
        col("orgName").alias("courseOrgName"),
        col("orgStatus").alias("courseOrgStatus")
    )
    df = courseDF.join(joinOrgDF, ["courseOrgID"], "left")
    
    return df
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user.userDFUtil import exportDFToParquet
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode_outer,from_json, lit,when, format_string, expr
from pyspark.sql.functions import count, avg, col
from pyspark.sql.types import FloatType
from typing import List
from util import schemas

def esContentDataFrame(
    primary_categories: List[str], 
    spark: SparkSession
) -> DataFrame:
    return spark.read.parquet(ParquetFileConstants.ESCONTENT_PARQUET_FILE).filter(col("primaryCategory").isin(primary_categories))


def preComputeAllCourseProgramESDataFrame(spark: SparkSession,
    primary_categories: List[str]= ["Course", "Program", "Blended Program", "CuratedCollections", "Curated Program"],
) -> DataFrame:
  
    contentDF = esContentDataFrame(primary_categories, spark) \
        .withColumn("courseOrgID", explode_outer(col("createdFor"))) \
        .withColumn("contentLanguage", explode_outer(col("language"))) \
        .withColumn("competency", explode_outer(col("competencies_v6")))  \
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
            col("courseOrgID"),
            col("competency.competencyAreaRefId"),
            col("competency.competencyThemeRefId"),
            col("competency.competencySubThemeRefId"),
            col("contentLanguage"),
            col("courseCategory")
        ) \
        .dropDuplicates(["courseID", "category"]) \
        .fillna(0.0, subset=["courseDuration"]) \
        .fillna(0, subset=["courseResourceCount"])
    
    exportDFToParquet(contentDF,ParquetFileConstants.ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE)

def preComputeContentDataFrame(spark: SparkSession):
    ratingRawDF = spark.read.parquet(ParquetFileConstants.RATING_PARQUET_FILE)
    ratingDF = ratingRawDF \
        .select(
            col("activityid").alias("courseID"),
            col("userid").alias("userID"), 
            col("rating").alias("userRating"),
            col("activitytype").alias("cbpType"),
            col("createdon").alias("createdOn")
        )
    exportDFToParquet(ratingDF,ParquetFileConstants.RATING_SELECT_PARQUET_FILE)


    contentRating = ratingDF.filter(col("userRating").isNotNull()) \
    .groupBy("courseID") \
    .agg(
        count("userRating").alias("totalRatings"),
        avg("userRating").alias("rating")
    ) \
    .select("courseID", "totalRatings", "rating")

    contentWithRatingDF=spark.read.parquet(ParquetFileConstants.ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE).join(
        contentRating,"courseID","left").drop(contentRating["courseID"])
    
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
            .select(
                col("content_id").alias("courseID"),
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
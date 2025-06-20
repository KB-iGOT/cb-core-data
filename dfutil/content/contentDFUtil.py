from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user.userDFUtil import exportDFToParquet
from dfutil.content import contentDFUtil
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode_outer,from_json, lit,when, format_string, expr,lower,avg,count
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from constants.ParquetFileConstants import ParquetFileConstants
from typing import List
from util import schemas

def esContentDataFrame(
    primary_categories: List[str], 
    spark: SparkSession
) -> DataFrame:
    return spark.read.parquet(ParquetFileConstants.ESCONTENT_PARQUET_FILE).filter(col("primaryCategory").isin(primary_categories))


def AllCourseProgramESDataFrame(spark: SparkSession,
    primary_categories: List[str] = ["Course", "Program", "Blended Program", "CuratedCollections", "Curated Program"]
) -> DataFrame:
    
    return esContentDataFrame(primary_categories, spark) \
        .withColumn("courseOrgID", F.explode_outer(F.col("createdFor"))) \
        .withColumn("contentLanguage", F.explode_outer(F.col("language"))) \
        .withColumn("competencyAreaRefId", 
                   F.when(F.col("competencies_v6").isNotNull(), 
                         F.col("competencies_v6")["competencyAreaRefId"]).otherwise(F.lit(None))) \
        .withColumn("competencyThemeRefId", 
                   F.when(F.col("competencies_v6").isNotNull(), 
                         F.col("competencies_v6")["competencyThemeRefId"]).otherwise(F.lit(None))) \
        .withColumn("competencySubThemeRefId", 
                   F.when(F.col("competencies_v6").isNotNull(), 
                         F.col("competencies_v6")["competencySubThemeRefId"]).otherwise(F.lit(None))) \
        .select(
            F.col("identifier").alias("courseID"),
            F.col("primaryCategory").alias("category"),
            F.col("name").alias("courseName"),
            F.col("status").alias("courseStatus"),
            F.col("reviewStatus").alias("courseReviewStatus"),
            F.col("channel").alias("courseChannel"),
            F.col("lastPublishedOn").alias("courseLastPublishedOn"),
            F.col("duration").cast("float").alias("courseDuration"),
            F.col("leafNodesCount").alias("courseResourceCount"),
            F.col("lastStatusChangedOn").alias("lastStatusChangedOn"),
            F.col("courseOrgID"),
            F.col("competencyAreaRefId"),
            F.col("competencyThemeRefId"),
            F.col("competencySubThemeRefId"),
            F.col("contentLanguage"),
            F.col("courseCategory")
        ) \
        .dropDuplicates(["courseID", "category"]) \
        .na.fill({"courseDuration": 0.0, "courseResourceCount": 0})

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
    contentWithRatingDF=contentDFUtil.AllCourseProgramESDataFrame(spark).join(
        contentRatingDF,"courseID","left").drop(contentRatingDF["courseID"])
    contentWithRatingDF.printSchema()
    contentWithRatingDF.show(5,truncate=False)
    
    orgDF=spark.read.parquet(ParquetFileConstants.ORG_COMPUTED_PARQUET_FILE).select(
      col("orgID").alias("courseOrgID"),
      col("orgName").alias("courseOrgName"),
      col("orgStatus").alias("courseOrgStatus")
    )

    contentWithOrgRatingDF=contentWithRatingDF.join(orgDF, contentWithRatingDF["courseOrgID"] == orgDF["courseOrgID"], "left") \
        .drop(orgDF["courseOrgID"]) \
        
    contentWithOrgRatingDF.printSchema()
    
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
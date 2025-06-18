from typing import List
from dfutil.content import contentDFUtil
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, explode_outer, from_json, 
    unix_timestamp, expr,
    expr
)
from pyspark.sql.types import FloatType
from util import schemas



def assessment_es_dataframe(spark: SparkSession, primary_categories: List[str]= ["Standalone Assessment"]) -> DataFrame:
    assessmentdf = contentDFUtil.esContentDataFrame(primary_categories, spark) \
        .withColumn("assessOrgID", explode_outer(col("createdFor"))) \
        .select(
            col("identifier").alias("assessID"),
            col("primaryCategory").alias("assessCategory"),
            col("name").alias("assessName"),
            col("status").alias("assessStatus"),
            col("reviewStatus").alias("assessReviewStatus"),
            col("channel").alias("assessChannel"),
            col("duration").cast(FloatType()).alias("assessDuration"),
            col("leafNodesCount").alias("assessChildCount"),
            col("lastPublishedOn").alias("assessLastPublishedOn"),
            col("assessOrgID")
        ) \
        .dropDuplicates(["assessID", "assessCategory"]) \
        .na.fill({"assessDuration": 0.0, "assessChildCount": 0})
    return assessmentdf

def transform_assessment_data(assess_with_hierarchy_data: DataFrame, org_df: DataFrame) -> DataFrame:
    """
    Joins assessment data with org data, flattens nested 'data' fields,
    and converts timestamp strings to long.

    :param assess_with_hierarchy_data: DataFrame containing 'data' struct and 'assessOrgID'
    :param org_df: DataFrame containing org metadata with orgID, orgName, orgStatus
    :return: Transformed DataFrame with flattened and timestamp-converted fields
    """

    # Join org details
    df = assess_with_hierarchy_data.join(
        org_df.select(
            col("orgID").alias("assessOrgID"),
            col("orgName").alias("assessOrgName"),
            col("orgStatus").alias("assessOrgStatus")
        ),
        on="assessOrgID",
        how="left"
    )
    
    # # Flatten fields from 'data'
    df = df \
        .withColumn("children", col("data.children")) \
        .withColumn("assessPublishType", col("data.publish_type")) \
        .withColumn("assessIsExternal", col("data.isExternal")) \
        .withColumn("assessContentType", col("data.contentType")) \
        .withColumn("assessObjectType", col("data.objectType")) \
        .withColumn("assessUserConsent", col("data.userConsent")) \
        .withColumn("assessVisibility", col("data.visibility")) \
        .withColumn("assessCreatedOn", col("data.createdOn")) \
        .withColumn("assessLastUpdatedOn", col("data.lastUpdatedOn")) \
        .withColumn("assessLastSubmittedOn", col("data.lastSubmittedOn")) \
        .drop("data")

    # Convert date strings to Unix timestamps (long)
    timestamp_cols = [
        "assessCreatedOn", "assessLastUpdatedOn",
        "assessLastPublishedOn", "assessLastSubmittedOn"
    ]
    for col_name in timestamp_cols:
        df = df.withColumn(col_name, unix_timestamp(col(col_name), "yyyy-MM-dd'T'HH:mm:ss").cast("long"))
    return df

def add_hierarchy_column(
    df: DataFrame,
    hierarchy_df: DataFrame,
    id_col: str,
    as_col: str,
    spark: SparkSession,
    children: bool = False,
    competencies: bool = False,
    l2_children: bool = False
) -> DataFrame:
    """
    Adds a hierarchy struct column to the input DataFrame by joining with the hierarchy DataFrame.
    """
    # Assume this returns a StructType schema dynamically
    hierarchy_schema = schemas.make_hierarchy_schema(children, competencies, l2_children)
    
    result_df = df.join(
        hierarchy_df,
        df[id_col] == hierarchy_df["identifier"],
        how="left"
    ).fillna("{}", subset=["hierarchy"]) \
     .withColumn(as_col, from_json(col("hierarchy"), hierarchy_schema)) \
    .drop("hierarchy")
    return result_df

def assessment_children_dataframe(assess_with_hierarchy_df: DataFrame) -> DataFrame:
    """
    Extracts and flattens children data from the given assessment DataFrame.

    :param assess_with_hierarchy_df: DataFrame with a 'children' array column
    :return: Flattened DataFrame with one row per child
    """
    
    df = assess_with_hierarchy_df.select(
        col("assessID"), explode_outer(col("children")).alias("ch")
    ).select(
        col("assessID"),
        col("ch.identifier").alias("assessChildID"),
        col("ch.name").alias("assessChildName"),
        col("ch.duration").cast(FloatType()).alias("assessChildDuration"),
        col("ch.primaryCategory").alias("assessChildPrimaryCategory"),
        col("ch.contentType").alias("assessChildContentType"),
        col("ch.objectType").alias("assessChildObjectType"),
        col("ch.showTimer").alias("assessChildShowTimer"),
        col("ch.allowSkip").alias("assessChildAllowSkip")
    )
    return df
def user_assessment_children_dataframe(user_assessment_df: DataFrame, assess_children_df: DataFrame) -> DataFrame:
    """
    Joins user assessment data with assessment children data on 'assessChildID'.

    :param user_assessment_df: DataFrame containing user assessment records.
    :param assess_children_df: DataFrame containing flattened assessment child data.
    :return: Joined DataFrame.
    """
    df = user_assessment_df.join(assess_children_df, on="assessChildID", how="inner")
    return df

def user_assessment_children_details_dataframe(
    user_assess_children_df: DataFrame,
    assess_with_details_df: DataFrame,
    all_course_programdetails_with_rating_df: DataFrame,
    user_org_df: DataFrame
) -> DataFrame:
    """
    Joins user assessment data with assessment details, course details (excluding ratings), and user org info.

    :return: Enriched DataFrame with assessment, course, and org details.
    """
    # Drop rating count columns from course data
    course_df = all_course_programdetails_with_rating_df.drop(
        "count1Star", "count2Star", "count3Star", "count4Star", "count5Star"
    )

    # Perform joins
    df = user_assess_children_df \
        .join(assess_with_details_df, on="assessID", how="left") \
        .join(course_df, on="courseID", how="left") \
        .join(user_org_df, on="userID", how="left")

    return df

def all_course_program_details_with_competencies_json_dataframe(
    all_course_program_es_df: DataFrame,
    hierarchy_df: DataFrame,
    org_df: DataFrame,
    spark: SparkSession
) -> DataFrame:
    """
    Enriches course data with hierarchy (including competencies), extracts competencies JSON,
    joins with organization details, and fills nulls.

    :param all_course_program_es_df: DataFrame from ES with course program info.
    :param hierarchy_df: DataFrame with hierarchy JSON data.
    :param org_df: DataFrame with organization info.
    :return: Enriched course DataFrame.
    """
    # Step 1: Add hierarchy column with competencies
    df_with_hierarchy = add_hierarchy_column(
        all_course_program_es_df,
        hierarchy_df,
        id_col="courseID",
        as_col="data",
        spark=spark,
        competencies=True
    ).withColumn("competenciesJson", col("data.competencies_v3"))
    # Step 2: Join with org details
    course_org_details_df = add_course_org_details(df_with_hierarchy, org_df) \
        .na.fill(0.0, subset=["courseDuration"]) \
        .na.fill(0, subset=["courseResourceCount"]) \
        .drop("data")

    return course_org_details_df

def all_course_program_details_with_rating_df(
    all_course_program_details_df: DataFrame,
    course_rating_df: DataFrame
) -> DataFrame:
    """
    Joins course program details with rating information on courseID and lowercased category.
    """
    df = all_course_program_details_df.withColumn("categoryLower", expr("LOWER(category)")) \
        .join(course_rating_df, ["courseID", "categoryLower"], "left")
    return df

def add_course_org_details(course_df: DataFrame, org_df: DataFrame) -> DataFrame:
    join_org_df = org_df.select(
        col("orgID").alias("courseOrgID"),
        col("orgName").alias("courseOrgName"),
        col("orgStatus").alias("courseOrgStatus")
    )

    df = course_df.join(join_org_df, on="courseOrgID", how="left")
    return df
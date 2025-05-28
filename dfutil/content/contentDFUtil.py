
import sys
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame,functions as F
from pyspark.sql.functions import col, from_json, lit, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from typing import List
from functools import reduce

# ==============================
# 1. Configuration and Constants
# ==============================

# Ensure the parent directory is in sys.path for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckutil import duckutil, datautil  # Assuming duckutil is in the parent directory
from constants.ParquetFileConstants import ParquetFileConstants
from constants.QueryConstants import QueryConstants


def content_es_dataframe(spark_session, primary_categories: list, prefix: str = "course") -> DataFrame:

    # Fetch the ElasticSearch data directly as DuckDB Parquet Path
    content_parquet_path = ParquetFileConstants.CONTENT_MASTER  # Replace with the actual path
    contentForPrimaryCategoryDF = spark_session.read.parquet(content_parquet_path) \
        .filter(F.col("primaryCategory").isin(primary_categories))
    print("Content For Primary")
    print(contentForPrimaryCategoryDF.count())
    # Process DataFrame
    processed_df = (
        contentForPrimaryCategoryDF
        .withColumn(f"{prefix}OrgID", F.explode_outer(F.col("createdFor")))
        .select(
            F.col("identifier").alias(f"{prefix}ID"),
            F.col("primaryCategory").alias(f"{prefix}Category"),
            F.col("name").alias(f"{prefix}Name"),
            F.col("status").alias(f"{prefix}Status"),
            F.col("reviewStatus").alias(f"{prefix}ReviewStatus"),
            F.col("channel").alias(f"{prefix}Channel"),
            F.col("lastPublishedOn").alias(f"{prefix}LastPublishedOn"),
            F.col("duration").cast("float").alias(f"{prefix}Duration"),
            F.col("leafNodesCount").alias(f"{prefix}ResourceCount"),
            F.col("lastStatusChangedOn").alias(f"{prefix}LastStatusChangedOn"),
            F.col("programDirectorName").alias(f"{prefix}ProgramDirectorName"),
            F.col(f"{prefix}OrgID")
        )
        .dropDuplicates([f"{prefix}ID", f"{prefix}Category"])
        .na.fill({f"{prefix}Duration": 0.0, f"{prefix}ResourceCount": 0})
    )

    return processed_df

def all_course_program_es_data_frame(spark_session, primary_categories: list) -> DataFrame:
    # Directly load the Parquet file using Spark (replace with the actual path)
    content_parquet_path = ParquetFileConstants.CONTENT_MASTER  # Replace with the actual path
    content_df = spark_session.read.parquet(content_parquet_path) \
        .filter(F.col("primaryCategory").isin(primary_categories)).withColumn("competencyAreaRefId", F.col("competencies_v6")["competencyAreaRefId"]) \
            .withColumn("competencyThemeRefId", F.col("competencies_v6")["competencyThemeRefId"]) \
            .withColumn("competencySubThemeRefId", F.col("competencies_v6")["competencySubThemeRefId"])


    # Process DataFrame without Pandas
    processed_df = (
        content_df
        .withColumn("courseOrgID", F.explode_outer(F.col("createdFor")))
        .withColumn("contentLanguage", F.explode_outer(F.col("language")))
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
        )
        .dropDuplicates(["courseID", "category"])
        .na.fill({"courseDuration": 0.0, "courseResourceCount": 0})
    )
    return processed_df

def assessment_es_dataframe(spark_session: SparkSession, primary_categories: list) -> DataFrame:
    """
    Creates an assessment DataFrame from Elasticsearch content with specified primary categories.

    Args:
        spark_session (SparkSession): The active Spark session.
        primary_categories (list): List of primary categories for filtering.

    Returns:
        DataFrame: Processed DataFrame with assessment data.
    """
    # Load the ElasticSearch Content DataFrame
    content_parquet_path = ParquetFileConstants.ESCONTENT_PARQUET_FILE  # Replace with the actual path
    content_df = spark_session.read.parquet(content_parquet_path) \
        .filter(F.col("primaryCategory").isin(primary_categories))
    
    # Process the DataFrame for assessments
    processed_df = (
        content_df
        .withColumn("assessOrgID", F.explode_outer(F.col("createdFor")))
        .select(
            F.col("identifier").alias("assessID"),
            F.col("primaryCategory").alias("assessCategory"),
            F.col("name").alias("assessName"),
            F.col("status").alias("assessStatus"),
            F.col("reviewStatus").alias("assessReviewStatus"),
            F.col("channel").alias("assessChannel"),
            F.col("duration").cast("float").alias("assessDuration"),
            F.col("leafNodesCount").alias("assessChildCount"),
            F.col("lastPublishedOn").alias("assessLastPublishedOn"),
            F.col("assessOrgID")
        )
        .dropDuplicates(["assessID", "assessCategory"])
        .na.fill({"assessDuration": 0.0, "assessChildCount": 0})
    )

    return processed_df

def add_assess_org_details(assessment_df: DataFrame, org_df: DataFrame) -> DataFrame:
    # Selecting and renaming columns from orgDF
    assess_org_df = org_df.select(
        F.col("orgID").alias("assessOrgID"),
        F.col("orgName").alias("assessOrgName"),
        F.col("orgStatus").alias("assessOrgStatus")
    )

    # Performing LEFT JOIN with assessmentDF
    result_df = assessment_df.join(assess_org_df, on="assessOrgID", how="left")
    
    return result_df

def assess_with_hierarchy_dataframe(spark: SparkSession, assessment_df: DataFrame, hierarchy_df: DataFrame, org_df: DataFrame) -> DataFrame:
    """
    Merges assessment, hierarchy, and organization data into a single DataFrame.
    """
    # Step 1: Add Hierarchy Data (Left Join)
    merged_df = (
        assessment_df
        .join(hierarchy_df, assessment_df["assessID"] == hierarchy_df["id"], "left")
        .join(
            org_df.select(
                F.col("orgID").alias("assessOrgID"),
                F.col("orgName").alias("assessOrgName"),
                F.col("orgStatus").alias("assessOrgStatus")
            ),
            on="assessOrgID",
            how="left"
        )
    )
    
    # Step 2: Extract Hierarchy Columns (Flattening)
    merged_df = merged_df \
        .withColumn("children", F.col("hierarchy_data.children")) \
        .withColumn("assessPublishType", F.col("hierarchy_data.publish_type")) \
        .withColumn("assessIsExternal", F.col("hierarchy_data.isExternal")) \
        .withColumn("assessContentType", F.col("hierarchy_data.contentType")) \
        .withColumn("assessObjectType", F.col("hierarchy_data.objectType")) \
        .withColumn("assessUserConsent", F.col("hierarchy_data.userConsent")) \
        .withColumn("assessVisibility", F.col("hierarchy_data.visibility")) \
        .withColumn("assessCreatedOn", F.col("hierarchy_data.createdOn")) \
        .withColumn("assessLastUpdatedOn", F.col("hierarchy_data.lastUpdatedOn")) \
        .withColumn("assessLastSubmittedOn", F.col("hierarchy_data.lastSubmittedOn")) \
        .drop("hierarchy_data")

    # Step 3: Convert Timestamp Columns to UNIX (Long)
    timestamp_columns = [
        "assessCreatedOn", 
        "assessLastUpdatedOn", 
        "assessLastPublishedOn", 
        "assessLastSubmittedOn"
    ]
    
    for col_name in timestamp_columns:
        merged_df = merged_df.withColumn(
            col_name, 
            F.unix_timestamp(F.col(col_name), "yyyy-MM-dd'T'HH:mm:ss").cast("long")
        )
    
    return merged_df

def add_course_org_details(course_df: DataFrame, org_df: DataFrame) -> DataFrame:
    # Select and rename columns for joining
    join_org_df = org_df.select(
        F.col("orgID").alias("courseOrgID"),
        F.col("orgName").alias("courseOrgName"),
        F.col("orgStatus").alias("courseOrgStatus")
    )
    
    # Perform the left join on 'courseOrgID'
    enriched_course_df = course_df.join(join_org_df, on="courseOrgID", how="left")
    
    return enriched_course_df

def add_hierarchy_and_org_details(df: DataFrame,hierarchy_df: DataFrame,org_df: DataFrame,id_col: str,
    as_col: str,org_id_col: str,org_name_col: str,org_status_col: str,children: bool = False,
    competencies: bool = False,l2_children: bool = False
) -> DataFrame:
   
    # Step 1: Define Hierarchy Schema
    hierarchy_schema = StructType([
        StructField("identifier", StringType(), True),
        StructField("children", ArrayType(StringType()), True) if children else None,
        StructField("competencies", ArrayType(StringType()), True) if competencies else None,
        StructField("l2Children", ArrayType(StringType()), True) if l2_children else None
    ]).filter(lambda x: x is not None)

    # Step 2: Add Hierarchy Details
    df_with_hierarchy = (
        df.join(hierarchy_df, df[id_col] == hierarchy_df["identifier"], "left")
          .na.fill("{}", subset=["hierarchy"])
          .withColumn(as_col, F.from_json(F.col("hierarchy"), hierarchy_schema))
          .drop("hierarchy")
    )

    # Step 3: Add Organization Details
    org_df_selected = org_df.select(
        F.col("orgID").alias(org_id_col),
        F.col("orgName").alias(org_name_col),
        F.col("orgStatus").alias(org_status_col)
    )

    final_df = df_with_hierarchy.join(org_df_selected, org_id_col, "left")
    
    return final_df

def all_course_program_details_with_competencies(
    all_course_program_df: DataFrame,
    hierarchy_df: DataFrame,
    org_df: DataFrame
) -> DataFrame:
   
    # Define the hierarchy schema with competencies
    hierarchy_schema = StructType([
        StructField("competencies_v3", ArrayType(StringType()), True)
    ])

    # Step 1: Add Hierarchy with Competencies
    df_with_hierarchy = (
        all_course_program_df.join(
            hierarchy_df,
            all_course_program_df["courseID"] == hierarchy_df["identifier"],
            "left"
        )
        .na.fill("{}", subset=["hierarchy"])
        .withColumn("data", F.from_json(F.col("hierarchy"), hierarchy_schema))
        .withColumn("competenciesJson", F.col("data.competencies_v3"))
        .drop("hierarchy")
    )

    # Step 2: Add Organization Details
    course_org_df = org_df.select(
        F.col("orgID").alias("courseOrgID"),
        F.col("orgName").alias("courseOrgName"),
        F.col("orgStatus").alias("courseOrgStatus")
    )

    df_with_org = (
        df_with_hierarchy.join(course_org_df, "courseOrgID", "left")
        .na.fill({"courseDuration": 0.0, "courseResourceCount": 0})
        .drop("data")
    )

    return df_with_org

def all_course_program_details_dataframe(all_course_program_details_with_comp_df: DataFrame) -> DataFrame:
    return all_course_program_details_with_comp_df.drop("competenciesJson")

def validate_primary_categories(primary_categories: list):
    allowed_categories = {
        "Course", "Program", "Blended Program", 
        "CuratedCollections", "Standalone Assessment",
        "Moderated Course", "Curated Program"
    }
    not_allowed = set(primary_categories) - allowed_categories

    if not_allowed:
        raise Exception(f"Category not allowed: {', '.join(not_allowed)}")

def content_with_org_details_dataframe(org_df: DataFrame, primary_categories: list, spark) -> DataFrame:
    # Validate Primary Categories
    validate_primary_categories(primary_categories)

    # Fetch course/program details (Replace with your actual method)
    all_course_program_details_df = all_course_program_details_dataframe(primary_categories, spark)

    # Prepare Organization Details
    course_org_df = org_df.select(
        F.col("orgID").alias("courseOrgID"),
        F.col("orgName").alias("courseOrgName"),
        F.col("orgStatus").alias("courseOrgStatus")
    )

    # Combine Course/Program Details with Organization Details
    combined_df = all_course_program_details_df.join(course_org_df, "courseOrgID", "left")

    return combined_df

def validate(actual: int, expected: int, message: str):
    """
    Simple validation function to compare two values.
    """
    if actual != expected:
        raise Exception(f"Validation failed: {message}. Actual: {actual}, Expected: {expected}")


def all_course_program_competency_dataframe(all_course_program_details_with_comp_df: DataFrame, spark: SparkSession) -> DataFrame:
    # Define the schema for the competencies JSON
    course_competencies_schema = ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("competencyType", StringType(), True),
        StructField("selectedLevelLevel", StringType(), True)
    ]))
    
    # Filter for rows with competencies JSON and extract the JSON data
    df = (all_course_program_details_with_comp_df
          .filter(F.col("competenciesJson").isNotNull())
          .withColumn("competencies", F.from_json(F.col("competenciesJson"), course_competencies_schema))
          .select(
              "courseID", "category", "courseName", "courseStatus",
              "courseReviewStatus", "courseOrgID", "courseOrgName", "courseOrgStatus",
              "courseDuration", "courseResourceCount",
              F.explode_outer("competencies").alias("competency")
          )
          .filter(F.col("competency").isNotNull())
          .withColumn("competencyLevel", F.trim(F.col("competency.selectedLevelLevel")))
          .withColumn(
              "competencyLevel",
              F.when(F.col("competencyLevel").rlike("[0-9]+"),
                     F.regexp_extract(F.col("competencyLevel"), "[0-9]+", 0).cast(IntegerType()))
              .otherwise(1)
          )
          .select(
              "courseID", "category", "courseName", "courseStatus",
              "courseReviewStatus", "courseOrgID", "courseOrgName", "courseOrgStatus",
              "courseDuration", "courseResourceCount",
              F.col("competency.id").alias("competencyID"),
              F.col("competency.name").alias("competencyName"),
              F.col("competency.competencyType").alias("competencyType"),
              "competencyLevel"
          )
         )
    
    return df

def all_course_program_details_with_rating_dataframe(spark, allCourseProgramDetailsWithCompDF):
    # Define the Schema for competencies JSON
    competency_schema = ArrayType(StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("competencyType", StringType(), True),
        StructField("selectedLevelLevel", StringType(), True)
    ]))
    
    # Parse the JSON and expand competencies
    df = allCourseProgramDetailsWithCompDF \
        .filter(F.col("competenciesJson").isNotNull()) \
        .withColumn("competencies", F.from_json(F.col("competenciesJson"), competency_schema)) \
        .withColumn("competency", F.explode_outer(F.col("competencies"))) \
        .filter(F.col("competency").isNotNull()) \
        .withColumn("competencyLevel", 
                    F.when(F.col("competency.selectedLevelLevel").rlike(r'\d+'), 
                           F.regexp_extract(F.col("competency.selectedLevelLevel"), r'(\d+)', 0).cast(IntegerType()))
                    .otherwise(1)
                   ) \
        .select(
            "courseID", "category", "courseName", "courseStatus",
            "courseReviewStatus", "courseOrgID", "courseOrgName", "courseOrgStatus",
            "courseDuration", "courseResourceCount",
            F.col("competency.id").alias("competencyID"),
            F.col("competency.name").alias("competencyName"),
            F.col("competency.competencyType").alias("competencyType"),
            "competencyLevel"
        )

    print(f"Course Program Competency DataFrame loaded with {df.count()} rows.")
    return df


def content_dataframes(
        org_df: DataFrame, 
        primary_categories: list = None, 
        run_validation: bool = True, 
        spark: SparkSession = None
) -> tuple:
    
    if primary_categories is None:
        primary_categories = [
            "Course", "Program", "Blended Program", 
            "Curated Program", "Moderated Course", 
            "Standalone Assessment", "CuratedCollections"
        ]

    # Validate primary categories
    validate_primary_categories(primary_categories)

    # Load Hierarchy DataFrame (replace with actual function)
    hierarchy_df = datautil.content_hierarchy_dataframe(spark)
    
    # Load Course/Program Details (replace with actual function)
    all_course_program_es_df = all_course_program_details_dataframe(primary_categories, spark)
    
    # Add Competency Data (replace with actual function)
    all_course_program_details_with_comp_df = all_course_program_details_with_competencies(
        all_course_program_es_df, hierarchy_df, org_df, spark
    )
    
    # Drop Competency JSON Column
    all_course_program_details_df = all_course_program_details_with_comp_df.drop("competenciesJson")
    
    # Load and Attach Course Ratings (replace with actual function)
    course_rating_df = datautil.course_rating_summary_dataframe(spark).todf()
    all_course_program_details_with_rating_df = all_course_program_details_with_rating_dataframe(
        all_course_program_details_df, course_rating_df, spark
    )

    # Validation Process
    if run_validation:
        # Validate row count consistency
        validate(
            all_course_program_es_df.count(),
            all_course_program_details_with_rating_df.count(),
            "ES course count should equal final DF with rating count"
        )

        # Validate ratings count
        pc_lower_str = ",".join([f"'{c.lower()}'" for c in primary_categories])
        validate(
            course_rating_df.filter(F.expr(f"categoryLower IN ({pc_lower_str}) AND ratingSum > 0")).count(),
            all_course_program_details_with_rating_df.filter(F.expr(f"LOWER(category) IN ({pc_lower_str}) AND ratingSum > 0")).count(),
            "number of ratings in cassandra table for courses and programs with ratingSum > 0 should equal those in final dataframe"
        )

        # Rating value validation for sanity check
        for i in range(1, 6):
            validate(
                course_rating_df.filter(F.expr(f"categoryLower IN ({pc_lower_str}) AND ratingAverage <= {i}")).count(),
                all_course_program_details_with_rating_df.filter(F.expr(f"LOWER(category) IN ({pc_lower_str}) AND ratingAverage <= {i}")).count(),
                f"Rating data row count for courses and programs should equal final DF for ratingAverage <= {i}"
            )

    return (
        hierarchy_df, 
        all_course_program_details_with_comp_df, 
        all_course_program_details_df, 
        all_course_program_details_with_rating_df
    )



def withUserCourseCompletionStatusColumn(df: DataFrame) -> DataFrame:
    """Calculate completion percentage with boundary checks"""
    df = df.withColumn(
        "completionPercentage",
        F.expr("""
            CASE 
                WHEN courseResourceCount = 0 OR courseProgress = 0 OR dbCompletionStatus = 0 THEN 0.0 
                WHEN dbCompletionStatus = 2 THEN 100.0 
                ELSE 100.0 * courseProgress / courseResourceCount 
            END
        """)
    )
    df = df.withColumn(
        "completionPercentage",
        F.expr("""
            CASE 
                WHEN completionPercentage > 100.0 THEN 100.0 
                WHEN completionPercentage < 0.0 THEN 0.0  
                ELSE completionPercentage 
            END
        """)
    )
    return df

def withCompletionPercentageColumn(df: DataFrame) -> DataFrame:
    """
    completionPercentage   completionStatus    IDI status
    NULL                   not-enrolled        not-started
    0.0                    enrolled            not-started
    0.0 < % < 10.0         started             enrolled
    10.0 <= % < 100.0      in-progress         in-progress
    100.0                  completed           completed
    """
    return df.withColumn(
        "completionStatus",
        F.expr("""
            CASE 
                WHEN completionPercentage IS NULL THEN 'not-enrolled' 
                WHEN completionPercentage = 0.0 THEN 'enrolled' 
                WHEN completionPercentage < 10.0 THEN 'started' 
                WHEN completionPercentage < 100.0 THEN 'in-progress' 
                ELSE 'completed' 
            END
        """)
    )

def withOldCompletionStatusColumn(df: DataFrame) -> DataFrame:
    """
    dbCompletionStatus     userCourseCompletionStatus
    NULL                   not-enrolled
    0                      not-started
    1                      in-progress
    2                      completed
    """
    return df.withColumn(
        "userCourseCompletionStatus",
        F.expr("""
            CASE 
                WHEN dbCompletionStatus IS NULL THEN 'not-enrolled' 
                WHEN dbCompletionStatus = 0 THEN 'not-started' 
                WHEN dbCompletionStatus = 1 THEN 'in-progress' 
                ELSE 'completed' 
            END
        """)
    )

def allCourseProgramCompletionWithDetailsDataFrame(
    user_course_program_completion_df: DataFrame,
    all_course_program_details_df: DataFrame,
    user_org_df: DataFrame
) -> DataFrame:
    """
    Get course completion data with details attached
    
    Args:
        user_course_program_completion_df: DataFrame(userID, courseID, batchID, courseCompletedTimestamp, 
            courseEnrolledTimestamp, lastContentAccessTimestamp, courseProgress, dbCompletionStatus)
        all_course_program_details_df: DataFrame(courseID, category, courseName, courseStatus,
            courseReviewStatus, courseOrgID, courseOrgName, courseOrgStatus, courseDuration, courseResourceCount)
        user_org_df: DataFrame(userID, firstName, lastName, maskedEmail, userStatus, 
            userOrgID, userOrgName, userOrgStatus)
    
    Returns:
        DataFrame with all joined fields plus calculated completion metrics
    """
    # Get distinct non-empty categories
    category_list = all_course_program_details_df.select("category") \
        .distinct() \
        .filter(F.col("category").isNotNull() & (F.col("category") != "")) \
        .rdd.flatMap(lambda x: x).collect()
    
    # Join dataframes
    df = user_course_program_completion_df.join(
        all_course_program_details_df, 
        on=["courseID"], 
        how="left"
    ).filter(
        F.col("category").isin(category_list)
    ).join(
        user_org_df, 
        on=["userID"], 
        how="left"
    )
    
    # Add calculated columns
    completion_percentage_df = withCompletionPercentageColumn(df)
    old_completions_df = withOldCompletionStatusColumn(completion_percentage_df)
    final_df = withUserCourseCompletionStatusColumn(old_completions_df)
    
    return final_df

def calculate_course_progress(user_course_program_completion_df: DataFrame) -> DataFrame:
    """Calculate course progress metrics"""
    df = withCompletionPercentageColumn(user_course_program_completion_df)
    df = withUserCourseCompletionStatusColumn(df)
    return df



def acbpDetailsDF(spark: SparkSession) -> DataFrame:
    """
    Get ACBP (Adaptive Competency Based Plan) details DataFrame
    
    Args:
        spark: SparkSession instance
        conf: Dashboard configuration
        cache: Cache object with load method
        schema: Schema object containing cbplan_draft_data_schema
        
    Returns:
        DataFrame: Combined ACBP details from draft and non-draft data
    """
    
    # Load and select basic ACBP data
     # Define final column order
    final_columns = [
        "acbpID", "userOrgID", "acbpStatus", "acbpCreatedBy", "cbPlanName",
        "assignmentType", "assignmentTypeInfo", "completionDueDate", 
        "allocatedOn", "acbpCourseIDList"
    ]
    
    # Load base data
    df = spark.read.parquet(ParquetFileConstants.ACBP_PARQUET_FILE) \
        .select(
            col("id").alias("acbpID"),
            col("orgid").alias("userOrgID"),
            col("draftdata"),
            col("status").alias("acbpStatus"),
            col("createdby").alias("acbpCreatedBy"),
            col("name").alias("cbPlanName"),
            col("assignmenttype").alias("assignmentType"),
            col("assignmenttypeinfo").alias("assignmentTypeInfo"),
            col("enddate").alias("completionDueDate"),
            col("publishedat").alias("allocatedOn"),
            col("contentlist").alias("acbpCourseIDList")
        ) \
        .fillna("", ["cbPlanName"])
    
    # Draft data processing
    draft_cbp_data = df \
        .filter((col("acbpStatus") == "DRAFT") & col("draftdata").isNotNull()) \
        .withColumn("draftData", from_json(col("draftdata"), schemas.cbplan_draft_data_schema)) \
        .withColumn("cbPlanName", col("draftData.name")) \
        .withColumn("assignmentType", col("draftData.assignmentType")) \
        .withColumn("assignmentTypeInfo", col("draftData.assignmentTypeInfo")) \
        .withColumn("completionDueDate", col("draftData.endDate")) \
        .withColumn("allocatedOn", lit("not published")) \
        .withColumn("acbpCourseIDList", col("draftData.contentList")) \
        .select(*final_columns)  # Select only final columns
    
    # Non-draft data
    non_draft_cbp_data = df \
        .filter(col("acbpStatus") != "DRAFT") \
        .select(*final_columns)  # Select only final columns
    
    return non_draft_cbp_data.union(draft_cbp_data)

def exploded_acbp_details(acbp_df: DataFrame, user_data_df: DataFrame, columns: List[str]) -> DataFrame:
    """
    More concise version using reduce (similar to Scala)
    """
    
    # CustomUser
    acbp_custom_user_allotment_df = acbp_df \
        .filter(col("assignmentType") == "CustomUser") \
        .withColumn("userID", explode(col("assignmentTypeInfo"))) \
        .join(user_data_df, ["userID", "userOrgID"], "left")
    
    # Designation  
    acbp_designation_allotment_df = acbp_df \
        .filter(col("assignmentType") == "Designation") \
        .withColumn("designation", explode(col("assignmentTypeInfo"))) \
        .join(user_data_df, ["userOrgID", "designation"], "left")
    
    # All User
    acbp_all_user_allotment_df = acbp_df \
        .filter(col("assignmentType") == "AllUser") \
        .join(user_data_df, ["userOrgID"], "left")
    
    # List of DataFrames
    dataframes = [
        acbp_custom_user_allotment_df,
        acbp_designation_allotment_df, 
        acbp_all_user_allotment_df
    ]
    
    # Select columns and union using reduce (most similar to Scala)
    selected_dfs = [df.select(*columns) for df in dataframes]
    acbp_allotment_df = reduce(lambda a, b: a.union(b), selected_dfs)
    
    return acbp_allotment_df


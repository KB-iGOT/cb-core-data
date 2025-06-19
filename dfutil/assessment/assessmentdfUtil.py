from typing import List
import time
from dfutil.content import contentDFUtil
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, explode_outer, from_json, 
    unix_timestamp, expr
)
from pyspark.sql.types import FloatType
from util import schemas


def print_dataframe_info(df: DataFrame, df_name: str, show_sample: bool = True, sample_rows: int = 5):
    """
    Utility function to print comprehensive DataFrame information.
    """
    print(f"\n{'='*60}")
    print(f"📊 DataFrame Info: {df_name}")
    print(f"{'='*60}")
    
    try:
        # row_count = df.count()
        # print(f"📈 Row Count: {row_count:,}")
        
        print(f"\n🔧 Schema:")
        df.printSchema()
        
        # if show_sample and row_count > 0:
        #     print(f"\n📋 Sample Data (First {min(sample_rows, row_count)} rows):")
        #     df.show(sample_rows, truncate=False)
        
        print(f"📊 Column Count: {len(df.columns)}")
        print(f"📝 Columns: {', '.join(df.columns)}")
        
    except Exception as e:
        print(f"❌ Error getting DataFrame info: {str(e)}")
    
    print(f"{'='*60}\n")


def assessment_es_dataframe(spark: SparkSession, primary_categories: List[str] = ["Standalone Assessment"]) -> DataFrame:
    """
    Creates assessment DataFrame from Elasticsearch data with comprehensive logging.
    """
    print(f"\n🚀 Starting assessment_es_dataframe function")
    print(f"📋 Primary Categories: {primary_categories}")
    start_time = time.time()
    
    try:
        print("📥 Fetching data from Elasticsearch...")
        raw_df = contentDFUtil.esContentDataFrame(primary_categories, spark)
        #print(f"✅ Raw ES data loaded - Row count: {raw_df.count():,}")
        
        print("🔄 Exploding createdFor column...")
        exploded_df = raw_df.withColumn("assessOrgID", explode_outer(col("createdFor")))
        #print(f"✅ After exploding createdFor - Row count: {exploded_df.count():,}")
        
        print("🔧 Selecting and aliasing columns...")
        assessmentdf = exploded_df.select(
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
        )
        
        print("🗑️ Removing duplicates...")
        before_dedup = assessmentdf.count()
        assessmentdf = assessmentdf.dropDuplicates(["assessID", "assessCategory"])
        after_dedup = assessmentdf.count()
        print(f"📊 Deduplication: {before_dedup:,} → {after_dedup:,} rows (removed {before_dedup - after_dedup:,})")
        
        print("🔧 Filling null values...")
        assessmentdf = assessmentdf.na.fill({"assessDuration": 0.0, "assessChildCount": 0})
        
        execution_time = time.time() - start_time
        print(f"⏱️ Function completed in {execution_time:.2f} seconds")
        
        # Print comprehensive DataFrame info
        print_dataframe_info(assessmentdf, "Assessment ES DataFrame")
        
        return assessmentdf
        
    except Exception as e:
        print(f"❌ Error in assessment_es_dataframe: {str(e)}")
        raise


def transform_assessment_data(assess_with_hierarchy_data: DataFrame, org_df: DataFrame) -> DataFrame:
    """
    Joins assessment data with org data, flattens nested 'data' fields,
    and converts timestamp strings to long with comprehensive logging.
    """
    print(f"\n🚀 Starting transform_assessment_data function")
    start_time = time.time()
    
    try:
        print("📊 Input DataFrames info:")
        # print(f"   - Assessment with hierarchy rows: {assess_with_hierarchy_data.count():,}")
        # print(f"   - Organization rows: {org_df.count():,}")
        
        print("🔄 Preparing organization DataFrame for join...")
        org_join_df = org_df.select(
            col("orgID").alias("assessOrgID"),
            col("orgName").alias("assessOrgName"),
            col("orgStatus").alias("assessOrgStatus")
        )
        print(f"✅ Organization DataFrame prepared - Columns: {org_join_df.columns}")
        
        print("🔗 Joining assessment data with organization data...")
        df = assess_with_hierarchy_data.join(org_join_df, on="assessOrgID", how="left")
        # join_result_count = df.count()
        # print(f"✅ Join completed - Result rows: {join_result_count:,}")
        
        print("📋 Flattening nested 'data' fields...")
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
        
        print("✅ Data flattening completed")
        
        print("📅 Converting timestamp columns to Unix timestamps...")
        timestamp_cols = [
            "assessCreatedOn", "assessLastUpdatedOn",
            "assessLastPublishedOn", "assessLastSubmittedOn"
        ]
        
        for i, col_name in enumerate(timestamp_cols, 1):
            print(f"   {i}. Converting {col_name}...")
            df = df.withColumn(
                col_name, 
                unix_timestamp(col(col_name), "yyyy-MM-dd'T'HH:mm:ss").cast("long")
            )
        
        print("✅ All timestamp conversions completed")
        
        execution_time = time.time() - start_time
        print(f"⏱️ Function completed in {execution_time:.2f} seconds")
        
        # Print comprehensive DataFrame info
        print_dataframe_info(df, "Transformed Assessment Data")
        
        return df
        
    except Exception as e:
        print(f"❌ Error in transform_assessment_data: {str(e)}")
        raise


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
    print(f"\n🚀 Starting add_hierarchy_column function")
    print(f"📋 Parameters:")
    print(f"   - ID Column: {id_col}")
    print(f"   - As Column: {as_col}")
    print(f"   - Children: {children}")
    print(f"   - Competencies: {competencies}")
    print(f"   - L2 Children: {l2_children}")
    
    start_time = time.time()
    
    try:
        print("📊 Input DataFrames info:")
        print(f"   - Main DataFrame rows: {df.count():,}")
        print(f"   - Hierarchy DataFrame rows: {hierarchy_df.count():,}")
        
        print("🔧 Creating hierarchy schema...")
        hierarchy_schema = schemas.make_hierarchy_schema(children, competencies, l2_children)
        print(f"✅ Hierarchy schema created: {hierarchy_schema}")
        
        print(f"🔗 Joining DataFrames on {id_col} = identifier...")
        joined_df = df.join(
            hierarchy_df,
            df[id_col] == hierarchy_df["identifier"],
            how="left"
        )
        join_count = joined_df.count()
        print(f"✅ Join completed - Result rows: {join_count:,}")
        
        print("🔧 Processing hierarchy column...")
        result_df = joined_df \
            .fillna("{}", subset=["hierarchy"]) \
            .withColumn(as_col, from_json(col("hierarchy"), hierarchy_schema)) \
            .drop("hierarchy")
        
        print("✅ Hierarchy column processing completed")
        
        execution_time = time.time() - start_time
        print(f"⏱️ Function completed in {execution_time:.2f} seconds")
        
        # Print comprehensive DataFrame info
        print_dataframe_info(result_df, f"DataFrame with Hierarchy Column ({as_col})")
        
        return result_df
        
    except Exception as e:
        print(f"❌ Error in add_hierarchy_column: {str(e)}")
        raise


def assessment_children_dataframe(assess_with_hierarchy_df: DataFrame) -> DataFrame:
    """
    Extracts and flattens children data from the given assessment DataFrame with comprehensive logging.
    """
    print(f"\n🚀 Starting assessment_children_dataframe function")
    start_time = time.time()
    
    try:
        input_count = assess_with_hierarchy_df.count()
        print(f"📊 Input DataFrame rows: {input_count:,}")
        
        print("🔄 Exploding children array...")
        exploded_df = assess_with_hierarchy_df.select(
            col("assessID"), explode_outer(col("children")).alias("ch")
        )
        exploded_count = exploded_df.count()
        print(f"✅ After exploding children - Rows: {exploded_count:,}")
        
        print("🔧 Selecting and casting child columns...")
        df = exploded_df.select(
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
        
        final_count = df.count()
        print(f"✅ Final DataFrame rows: {final_count:,}")
        
        execution_time = time.time() - start_time
        print(f"⏱️ Function completed in {execution_time:.2f} seconds")
        
        # Print comprehensive DataFrame info
        print_dataframe_info(df, "Assessment Children DataFrame")
        
        return df
        
    except Exception as e:
        print(f"❌ Error in assessment_children_dataframe: {str(e)}")
        raise


def user_assessment_children_dataframe(user_assessment_df: DataFrame, assess_children_df: DataFrame) -> DataFrame:
    """
    Joins user assessment data with assessment children data on 'assessChildID' with comprehensive logging.
    """
    print(f"\n🚀 Starting user_assessment_children_dataframe function")
    start_time = time.time()
    
    try:
        print("📊 Input DataFrames info:")
        user_count = user_assessment_df.count()
        children_count = assess_children_df.count()
        print(f"   - User Assessment rows: {user_count:,}")
        print(f"   - Assessment Children rows: {children_count:,}")
        
        print("🔗 Performing inner join on 'assessChildID'...")
        df = user_assessment_df.join(assess_children_df, on="assessChildID", how="inner")
        
        result_count = df.count()
        print(f"✅ Join completed - Result rows: {result_count:,}")
        
        # Calculate join efficiency
        if user_count > 0 and children_count > 0:
            join_efficiency = (result_count / min(user_count, children_count)) * 100
            print(f"📊 Join efficiency: {join_efficiency:.2f}%")
        
        execution_time = time.time() - start_time
        print(f"⏱️ Function completed in {execution_time:.2f} seconds")
        
        # Print comprehensive DataFrame info
        print_dataframe_info(df, "User Assessment Children DataFrame", show_sample=True, sample_rows=3)
        
        return df
        
    except Exception as e:
        print(f"❌ Error in user_assessment_children_dataframe: {str(e)}")
        raise


def user_assessment_children_details_dataframe(
    user_assess_children_df: DataFrame,
    assess_with_details_df: DataFrame,
    all_course_programdetails_with_rating_df: DataFrame,
    user_org_df: DataFrame
) -> DataFrame:
    """
    Joins user assessment data with assessment details, course details, and user org info with comprehensive logging.
    """
    print(f"\n🚀 Starting user_assessment_children_details_dataframe function")
    start_time = time.time()
    
    try:
        print("📊 Input DataFrames info:")
        user_assess_count = user_assess_children_df.count()
        assess_details_count = assess_with_details_df.count()
        course_details_count = all_course_programdetails_with_rating_df.count()
        user_org_count = user_org_df.count()
        
        print(f"   - User Assessment Children rows: {user_assess_count:,}")
        print(f"   - Assessment Details rows: {assess_details_count:,}")
        print(f"   - Course Program Details rows: {course_details_count:,}")
        print(f"   - User Organization rows: {user_org_count:,}")
        
        print("🗑️ Dropping rating count columns from course data...")
        course_df = all_course_programdetails_with_rating_df.drop(
            "count1Star", "count2Star", "count3Star", "count4Star", "count5Star"
        )
        print(f"✅ Course DataFrame cleaned - Remaining columns: {len(course_df.columns)}")
        
        print("🔗 Step 1: Joining with assessment details...")
        df1 = user_assess_children_df.join(assess_with_details_df, on="assessID", how="left")
        step1_count = df1.count()
        print(f"✅ Step 1 completed - Rows: {step1_count:,}")
        
        print("🔗 Step 2: Joining with course details...")
        df2 = df1.join(course_df, on="courseID", how="left")
        step2_count = df2.count()
        print(f"✅ Step 2 completed - Rows: {step2_count:,}")
        
        print("🔗 Step 3: Joining with user organization data...")
        df = df2.join(user_org_df, on="userID", how="left")
        final_count = df.count()
        print(f"✅ Step 3 completed - Final rows: {final_count:,}")
        
        print("📊 Join Summary:")
        print(f"   - Initial → After assess details: {user_assess_count:,} → {step1_count:,}")
        print(f"   - After assess details → After course details: {step1_count:,} → {step2_count:,}")
        print(f"   - After course details → Final: {step2_count:,} → {final_count:,}")
        
        execution_time = time.time() - start_time
        print(f"⏱️ Function completed in {execution_time:.2f} seconds")
        
        # Print comprehensive DataFrame info
        print_dataframe_info(df, "User Assessment Children Details DataFrame", show_sample=True, sample_rows=2)
        
        return df
        
    except Exception as e:
        print(f"❌ Error in user_assessment_children_details_dataframe: {str(e)}")
        raise


def all_course_program_details_with_competencies_json_dataframe(
    all_course_program_es_df: DataFrame,
    hierarchy_df: DataFrame,
    org_df: DataFrame,
    spark: SparkSession
) -> DataFrame:
    """
    Enriches course data with hierarchy, competencies, and organization details with comprehensive logging.
    """
    print(f"\n🚀 Starting all_course_program_details_with_competencies_json_dataframe function")
    start_time = time.time()
    
    try:
        print("📊 Input DataFrames info:")
        course_count = all_course_program_es_df.count()
        hierarchy_count = hierarchy_df.count()
        org_count = org_df.count()
        
        print(f"   - Course Program ES rows: {course_count:,}")
        print(f"   - Hierarchy rows: {hierarchy_count:,}")
        print(f"   - Organization rows: {org_count:,}")
        
        print("🔧 Step 1: Adding hierarchy column with competencies...")
        df_with_hierarchy = add_hierarchy_column(
            all_course_program_es_df,
            hierarchy_df,
            id_col="courseID",
            as_col="data",
            spark=spark,
            competencies=True
        )
        
        print("📋 Extracting competencies JSON...")
        df_with_hierarchy = df_with_hierarchy.withColumn("competenciesJson", col("data.competencies_v3"))
        print("✅ Competencies JSON extracted")
        
        print("🔧 Step 2: Adding course organization details...")
        course_org_details_df = add_course_org_details(df_with_hierarchy, org_df)
        
        print("🔧 Filling null values...")
        final_df = course_org_details_df \
            .na.fill(0.0, subset=["courseDuration"]) \
            .na.fill(0, subset=["courseResourceCount"]) \
            .drop("data")
        
        final_count = final_df.count()
        print(f"✅ Final DataFrame rows: {final_count:,}")
        
        execution_time = time.time() - start_time
        print(f"⏱️ Function completed in {execution_time:.2f} seconds")
        
        # Print comprehensive DataFrame info
        print_dataframe_info(final_df, "Course Program Details with Competencies JSON", show_sample=True, sample_rows=2)
        
        return final_df
        
    except Exception as e:
        print(f"❌ Error in all_course_program_details_with_competencies_json_dataframe: {str(e)}")
        raise


def all_course_program_details_with_rating_df(
    all_course_program_details_df: DataFrame,
    course_rating_df: DataFrame
) -> DataFrame:
    """
    Joins course program details with rating information with comprehensive logging.
    """
    print(f"\n🚀 Starting all_course_program_details_with_rating_df function")
    start_time = time.time()
    
    try:
        print("📊 Input DataFrames info:")
        course_details_count = all_course_program_details_df.count()
        rating_count = course_rating_df.count()
        
        print(f"   - Course Program Details rows: {course_details_count:,}")
        print(f"   - Course Rating rows: {rating_count:,}")
        
        print("🔧 Creating categoryLower column...")
        df_with_category = all_course_program_details_df.withColumn("categoryLower", expr("LOWER(category)"))
        print("✅ categoryLower column created")
        
        print("🔗 Joining with rating data on courseID and categoryLower...")
        df = df_with_category.join(course_rating_df, ["courseID", "categoryLower"], "left")
        
        final_count = df.count()
        print(f"✅ Join completed - Final rows: {final_count:,}")
        
        # Calculate join statistics
        if course_details_count > 0:
            join_ratio = (final_count / course_details_count) * 100
            print(f"📊 Join ratio: {join_ratio:.2f}% (should be ~100% for left join)")
        
        execution_time = time.time() - start_time
        print(f"⏱️ Function completed in {execution_time:.2f} seconds")
        
        # Print comprehensive DataFrame info
        print_dataframe_info(df, "Course Program Details with Rating", show_sample=True, sample_rows=2)
        
        return df
        
    except Exception as e:
        print(f"❌ Error in all_course_program_details_with_rating_df: {str(e)}")
        raise


def add_course_org_details(course_df: DataFrame, org_df: DataFrame) -> DataFrame:
    """
    Adds organization details to course DataFrame with comprehensive logging.
    """
    print(f"\n🚀 Starting add_course_org_details function")
    start_time = time.time()
    
    try:
        print("📊 Input DataFrames info:")
        course_count = course_df.count()
        org_count = org_df.count()
        
        print(f"   - Course DataFrame rows: {course_count:,}")
        print(f"   - Organization DataFrame rows: {org_count:,}")
        
        print("🔧 Preparing organization DataFrame for join...")
        join_org_df = org_df.select(
            col("orgID").alias("courseOrgID"),
            col("orgName").alias("courseOrgName"),
            col("orgStatus").alias("courseOrgStatus")
        )
        print(f"✅ Organization DataFrame prepared - Columns: {join_org_df.columns}")
        
        print("🔗 Joining course data with organization details...")
        df = course_df.join(join_org_df, on="courseOrgID", how="left")
        
        final_count = df.count()
        print(f"✅ Join completed - Final rows: {final_count:,}")
        
        # Verify join integrity
        if course_count == final_count:
            print("✅ Join integrity verified - Row count maintained")
        else:
            print(f"⚠️ Warning: Row count changed from {course_count:,} to {final_count:,}")
        
        execution_time = time.time() - start_time
        print(f"⏱️ Function completed in {execution_time:.2f} seconds")
        
        # Print comprehensive DataFrame info
        print_dataframe_info(df, "Course with Organization Details", show_sample=True, sample_rows=2)
        
        return df
        
    except Exception as e:
        print(f"❌ Error in add_course_org_details: {str(e)}")
        raise


def print_execution_summary():
    """
    Print a summary of the execution.
    """
    print(f"\n{'='*80}")
    print(f"🎉 EXECUTION SUMMARY")
    print(f"{'='*80}")
    print(f"✅ All functions executed successfully!")
    print(f"📊 Check the detailed logs above for performance metrics")
    print(f"🔍 Review DataFrame schemas and row counts for data quality")
    print(f"{'='*80}\n")
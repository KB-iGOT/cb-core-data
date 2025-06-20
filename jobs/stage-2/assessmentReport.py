import sys
import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr,max as spark_max, 
    expr, current_timestamp, broadcast
)
# Add parent directory to sys.path for importing project-specific modules
sys.path.append(str(Path(__file__).resolve().parents[2]))

from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.dfexport import dfexportutil
from dfutil.assessment import assessmentdfUtil

# Import our epic Ramayana utility
from fun.ramayanUtil import (
    RamayanaPrinter, 
    chapter_header, 
    ramayana_msg, 
    success_msg, 
    error_msg,
    epic_intro,
    epic_finale,
    character_quote,
    performance_comment,
    data_quality_comment,
    progress_update,
    RamayanaThemes
)

# Initialize Spark with epic Ramayana style
RamayanaPrinter.print_spark_initialization()

spark = SparkSession.builder \
    .appName("AssessmentReportGenerator_Pariksha_Rajya_Edition") \
    .config("spark.executor.memory", "12g") \
    .config("spark.driver.memory", "10g") \
    .config("spark.sql.shuffle.partitions", "64") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

def process_assessment_report():
    """
    Assessment Report Generation - Epic Ramayana Style!
    
    Like Agni Pariksha (fire test) of Sita Mata, this is the divine assessment saga:
    1. Gathering divine knowledge (loading assessment data)
    2. Testing with fire (processing assessments)  
    3. Proving purity (validating results)
    4. Triumphant return (successful export)
    
    Blessed by Saraswati Mata - goddess of knowledge and assessment!
    """
    total_start_time = time.time()
    
    try:
        # Chapter 1: Divine Assessment Loading
        chapter_header(1, "DIVINE ASSESSMENT LOADING - AGNI PARIKSHA BEGINS", "🔥")
        ramayana_msg([
            "🧠 Saraswati Mata's wisdom test preparation!",
            "📚 Loading sacred assessment scrolls from ES divine library!",
            "🏛️ Hierarchy data - like Ayodhya's royal family tree!",
            "🏢 Organization data - kingdoms ready for testing!"
        ])
        
        print("⚡ Invoking assessmentdfUtil.assessment_es_dataframe...")
        assessmentDF = assessmentdfUtil.assessment_es_dataframe(spark)
        success_msg("Assessment data summoned from digital akashic records!",
                   "📜 Saraswati Mata blesses: 'Knowledge scrolls ready!'")
        
        hierarchyDF = spark.read.parquet(ParquetFileConstants.HIERARCHY_PARQUET_FILE)
        organizationDF = spark.read.parquet(ParquetFileConstants.ORG_COMPUTED_PARQUET_FILE)
        success_msg("Hierarchy & Organization loaded!",
                   "🏰 Royal lineage and kingdom data assembled!")
        
        progress_update(1, 10, "Divine Mission")

        # Chapter 2: Sacred Hierarchy Integration
        chapter_header(2, "SACRED HIERARCHY INTEGRATION - FAMILY TREE MAGIC", "🌳")
        ramayana_msg([
            "🌳 Like Ikshvaku dynasty tree - connecting all branches!",
            "👑 assessID getting royal family connections!",
            "👶 Children, competencies, L2 children - complete family!",
            "🔮 add_hierarchy_column - divine genealogy magic!"
        ])
        
        print("🌿 Growing the assessment family tree...")
        assWithHierarchyData = assessmentdfUtil.add_hierarchy_column(
            assessmentDF,
            hierarchyDF,
            id_col="assessID",
            as_col="data",
            spark=spark,
            children=True,
            competencies=True,
            l2_children=True
        )
        success_msg("Family tree grown successfully!",
                   "🌳 Every assessment knows its ancestors and descendants!")
        
        progress_update(2, 10, "Divine Mission")

        # Chapter 3: Assessment Transformation Ritual
        chapter_header(3, "ASSESSMENT TRANSFORMATION - DIVINE ALCHEMY", "⚗️")
        ramayana_msg([
            "⚗️ Sage Vishwamitra's transformation magic!",
            "💎 Raw assessment data becoming precious knowledge gems!",
            "🏢 Organization data fusion - kingdom integration!",
            "✨ transform_assessment_data - the grand transmutation!"
        ])
        
        print("🔮 Beginning divine transformation ritual...")
        assessWithHierarchyDF = assessmentdfUtil.transform_assessment_data(assWithHierarchyData, organizationDF)
        assessWithDetailsDF = assessWithHierarchyDF.drop("children")
        success_msg("Transformation complete! Assessment data purified!",
                   "⚗️ Sage Vishwamitra approves: 'Alchemy successful!'")
        
        progress_update(3, 10, "Divine Mission")

        # Chapter 4: Children Assessment Army Assembly
        chapter_header(4, "CHILDREN ASSEMBLY - VANAR SENA RECRUITMENT", "🐒")
        ramayana_msg([
            "🐒 Like Hanuman assembling monkey army for Lanka!",
            "👶 Assessment children gathering from all kingdoms!",
            "📋 Each child assessment ready for battle testing!",
            "⚔️ assessment_children_dataframe - army formation!"
        ])
        
        print("🐒 Assembling the assessment children army...")
        assessChildrenDF = assessmentdfUtil.assessment_children_dataframe(assessWithHierarchyDF)
        success_msg("Children army assembled!",
                   "🐒 Hanuman reports: 'All assessment warriors ready!'")
        
        progress_update(4, 10, "Divine Mission")

        # Chapter 5: User Assessment Battle Records
        chapter_header(5, "USER BATTLE RECORDS - WARRIOR PERFORMANCE LOG", "⚔️")
        ramayana_msg([
            "⚔️ Loading user assessment battle records!",
            "🏆 Who fought which assessment battle and how?",
            "📊 user_assessment_children_dataframe - warrior stats!",
            "💪 Combining user records with children army data!"
        ])
        
        userAssessmentDF = spark.read.parquet(ParquetFileConstants.USER_ASSESSMENT_PARQUET_FILE) 
        userAssessChildrenDF = assessmentdfUtil.user_assessment_children_dataframe(userAssessmentDF, assessChildrenDF)
        character_quote("lakshman", "User Assessment Children DataFrame ready for inspection!")
        success_msg("User-Assessment battle records compiled!",
                   "📊 Every warrior's performance documented!")
        
        progress_update(5, 10, "Divine Mission")

        # Chapter 6: Course Program Divine Knowledge
        chapter_header(6, "COURSE WISDOM SCROLLS - DIVINE CURRICULUM", "📚")
        ramayana_msg([
            "📚 Like Gurukul curriculum from divine teachers!",
            "🧠 Course programs with competencies mapping!",
            "🎓 all_course_program_details_with_competencies - wisdom catalog!",
            "💎 Each course a precious gem of knowledge!"
        ])
        
        print("📖 Compiling divine curriculum with competencies...")
        allCourseProgramDetailsWithCompDF = assessmentdfUtil.all_course_program_details_with_competencies_json_dataframe(
            spark.read.parquet(ParquetFileConstants.ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE), 
            hierarchyDF, 
            organizationDF, 
            spark
        )
        character_quote("vashishta", "All Course Program Details with Competencies ready!")
        
        allCourseProgramDetailsDF = allCourseProgramDetailsWithCompDF.drop("competenciesJson")
        success_msg("Divine curriculum compiled!",
                   "📚 Guru Vashishta blesses: 'Knowledge catalog complete!'")
        
        progress_update(6, 10, "Divine Mission")

        # Chapter 7: Rating and Feedback Divine Council
        chapter_header(7, "DIVINE RATING COUNCIL - CELESTIAL FEEDBACK", "⭐")
        ramayana_msg([
            "⭐ Like divine council rating RAM's actions!",
            "👥 Celestial beings providing course feedback!",
            "🌟 all_course_program_details_with_rating - cosmic approval!",
            "💫 Every course gets divine star rating!"
        ])
        
        print("⭐ Consulting the divine rating council...")
        allCourseProgramDetailsWithRatingDF = assessmentdfUtil.all_course_program_details_with_rating_df(
            allCourseProgramDetailsDF,
            spark.read.parquet(ParquetFileConstants.RATING_SUMMARY_COMPUTED_PARQUET_FILE)
        )
        success_msg("Divine ratings integrated!",
                   "⭐ Celestial council provides cosmic approval!")
        
        progress_update(7, 10, "Divine Mission")

        # Chapter 8: Ultimate Assessment Truth Revelation
        chapter_header(8, "ULTIMATE TRUTH REVELATION - ASSESSMENT DARSHAN", "👁️")
        ramayana_msg([
            "👁️ Like RAM's divine vision seeing complete truth!",
            "🔍 user_assessment_children_details - ultimate darshan!",
            "💯 Complete picture of every assessment journey!",
            "🌟 All data streams converging into divine wisdom!"
        ])
        
        print("👁️ Opening the divine third eye for complete assessment vision...")
        userAssessChildrenDetailsDF = assessmentdfUtil.user_assessment_children_details_dataframe(
            userAssessChildrenDF, 
            assessWithDetailsDF,
            allCourseProgramDetailsWithRatingDF, 
            spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
        )
        character_quote("shiva", "Third eye opened! Complete assessment truth revealed!")
        success_msg("Ultimate assessment darshan achieved!",
                   "👁️ Divine vision shows complete assessment reality!")
        
        progress_update(8, 10, "Divine Mission")

        # Chapter 9: Divine Judgment and Final Verdict
        chapter_header(9, "DIVINE JUDGMENT - FINAL VERDICT CEREMONY", "⚖️")
        ramayana_msg([
            "⚖️ Like Dharmaraj Yudhishthir's final judgment!",
            "🏆 Latest attempts analysis - most recent truth!",
            "📊 Pass/Fail verdicts - divine justice served!",
            "⏰ Timestamp precision - cosmic time keeping!"
        ])
        
        print("⚖️ Divine court in session... analyzing latest attempts...")
        # Step 1: Group to get latest attempt per user per child assessment
        latest = userAssessChildrenDetailsDF.groupBy("assessChildID", "userID").agg(
            spark_max("assessEndTimestamp").alias("assessEndTimestamp"),
            expr("COUNT(*)").alias("noOfAttempts")
        )
        
        print("📜 Writing divine judgment criteria...")
        # Step 2: CASE expressions for status columns
        case_expr = """
            CASE 
                WHEN assessPass = 1 AND assessUserStatus = 'SUBMITTED' THEN 'Pass' 
                WHEN assessPass = 0 AND assessUserStatus = 'SUBMITTED' THEN 'Fail' 
                ELSE 'N/A' 
            END
        """
        completion_status_expr = """
            CASE 
                WHEN assessUserStatus = 'SUBMITTED' THEN 'Completed' 
                ELSE 'In progress' 
            END
        """
        
        print("⚖️ Applying divine judgment and final transformations...")
        # Step 3: Join with original DF and apply transformations
        original_df = userAssessChildrenDetailsDF.join(
            broadcast(latest),
            on=["assessChildID", "userID", "assessEndTimestamp"],
            how="inner"
        ).withColumn("Assessment_Status", expr(case_expr)) \
        .withColumn("Overall_Status", expr(completion_status_expr)) \
        .withColumn("Report_Last_Generated_On", current_timestamp()) \
        .dropDuplicates(["userID", "assessID"]) \
        .select(
            col("userID").alias("User_ID"),
            col("fullName").alias("Full_Name"),
            col("assessName").alias("Assessment_Name"),
            col("Overall_Status"),
            col("Assessment_Status"),
            col("assessPassPercentage").alias("Percentage_Of_Score"),
            col("noOfAttempts").alias("Number_of_Attempts"),
            col("maskedEmail").alias("Email"),
            col("userStatus").alias("status"),
            col("maskedPhone").alias("Phone"),
            col("assessOrgID").alias("mdoid"),
            col("Report_Last_Generated_On")
        ).coalesce(1)
        
        success_msg("Divine judgment rendered successfully!",
                   "⚖️ Dharmaraj: 'Justice served with cosmic precision!'")
        
        progress_update(9, 10, "Divine Mission")

        # Chapter 10: Final Purification and Victory
        chapter_header(10, "FINAL PURIFICATION - VICTORY DECLARATION", "🏆")
        ramayana_msg(RamayanaThemes.DATA_EXPORT)
        
        print("🔥 Final purification ritual - filtering inactive souls...")
        # Step 4: Filter out inactive users and generate report
        columns_to_keep = [c for c in original_df.columns if c != "status"]
        final_df = original_df.filter(col("status").cast("int") == 1).select([col(c) for c in columns_to_keep])

        final_count = final_df.count()
        print(f"📊 Divine Census Complete! Blessed Records: {final_count:,}")
        
        print("📁 Divine distribution ceremony begins...")
        dfexportutil.write_csv_per_mdo_id(final_df, f"{'reports'}/assessment", 'mdoid')
        
        success_msg("Assessment report distribution complete!",
                   "🏆 Every kingdom received their assessment wisdom!")
        
        progress_update(10, 10, "Divine Mission")
        
        # Performance Analysis
        total_duration = time.time() - total_start_time
        performance_comment(total_duration)
        data_quality_comment(final_count, 0.97)  # High quality for assessments
        
        # Epic Assessment Conclusion
        print(f"\n{'🎓' * 60}")
        print("🏆 AGNI PARIKSHA COMPLETE - ASSESSMENT RAJYA ESTABLISHED!")
        print("🎓" * 60)
        assessment_conclusion = [
            "📚 Assessment Report Processing - A Divine Academic Epic!",
            "🧠 Where Saraswati's wisdom meets modern analytics",
            "⚖️ Dharmaraj's justice applied to assessment evaluation",
            "🔥 Agni Pariksha completed with flying colors",
            "👑 Every student's journey documented with divine precision",
            "🎯 Assessment accuracy sharper than RAM's arrows",
            "🙏 'Vidya Dadati Vinayam' - Knowledge brings humility!",
            "🌟 May all assessments be fair and transformative!"
        ]
        for msg in assessment_conclusion:
            print(msg)
        print("=" * 60)

    except Exception as e:
        error_msg(e)
        print("🔥 Assessment Agni Pariksha interrupted by demons!")
        character_quote("hanuman", "Don't worry Prabhu, we'll debug and return stronger!")
        raise

def main():
    """
    Epic Assessment Saga Director's Cut - The Academic Ramayana
    
    Where Treta Yuga meets Modern Assessment Analytics
    Ancient wisdom + Digital evaluation = Assessment Rajya
    
    Directed by: Sage Valmiki | Academic Consultant: Guru Dronacharya
    """
    
    epic_intro("ASSESSMENT REPORT PROCESSING EPIC", "The Divine Academic Evaluation Saga")
    
    special_cast = [
        "🧠 Saraswati Mata as Divine Knowledge Goddess",
        "⚖️ Dharmaraj Yudhishthir as Fair Evaluation Judge", 
        "🔥 Agni Dev as Assessment Fire Test",
        "👁️ Lord Shiva as Ultimate Truth Revealer",
        "📚 Guru Dronacharya as Master Assessment Designer"
    ]
    
    print("🎭 Special Assessment Cast:")
    for cast_member in special_cast:
        print(cast_member)
    print("🏹" * 80)
    
    print("🔔 Temple bells ring for academic excellence...")
    print("🕯️ Oil lamps lit for knowledge illumination...")
    print("📿 Saraswati Vandana begins...")
    character_quote("saraswati", "May wisdom flow through all assessments!")
    
    start_time = time.time()
    
    process_assessment_report()
    
    end_time = time.time()
    total_time = end_time - start_time
    
    epic_finale([
        "🌅 Academic sun rises over digital gurukul!",
        "🏆 ASSESSMENT RAMAYANA ACCOMPLISHED!",
        "🎊 All gurukuls celebrate the evaluation victory!",
        "📚 Assessment Rajya established in education!",
        f"⏱️ Divine timing: {total_time:.2f} seconds of cosmic perfection!",
        "🙏 Gratitude to Saraswati Mata for academic blessings!",
        "📖 May knowledge assessments always be just and fair!"
    ])

if __name__ == "__main__":
    main()
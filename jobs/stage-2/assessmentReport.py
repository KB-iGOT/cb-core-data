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

# Initialize Spark with Baahubali epic grandeur
print("👑 BAAHUBALI 2 DATA PROCESSING - THE EPIC CONCLUSION!")
print("⚔️ Spark Session ko Baahubali ki army ki tarah powerful banayenge!")
print("💪 12GB executor memory - Baahubali ki strength jaisi unstoppable!")
print("🧠 10GB driver memory - Kattappa ke wisdom jaisa strategic!")
print("🎯 64 partitions - Mahishmati kingdom ka complete coordination!")
print("🔥 Why Kattappa killed Baahubali? Because bugs needed debugging!")

spark = SparkSession.builder \
    .appName("AssessmentReportGenerator_Baahubali_Epic_Edition") \
    .config("spark.executor.memory", "12g") \
    .config("spark.driver.memory", "10g") \
    .config("spark.sql.shuffle.partitions", "64") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

print("✅ Spark Session ready! Mahishmati army assembled for final battle!")
print("🎵 'Saahore Baahubali' playing - war drums echo across data centers!")

def process_assessment_report():
    """
    Assessment Report Generation - Baahubali 2 Epic Style!
    
    Like the final battle between Baahubali and Bhallaladeva for Mahishmati throne,
    we're fighting the ultimate data processing war for assessment supremacy!
    
    ⚔️ Each step is like an epic battle sequence - from army assembly to victory celebration!
    """
    total_start_time = time.time()
    
    try:
        # Chapter 1: Army Assembly - Assessment Data Loading
        print("\n" + "⚔️" * 70)
        print("🎬 SCENE 1: ARMY ASSEMBLY - MAHISHMATI FORCES GATHER")
        print("⚔️" * 70)
        print("👑 Baahubali declares: 'Yuddh ki taiyari shuru karte hain!'")
        print("🏰 Like assembling Mahishmati's greatest army, we gather assessment data")
        print("📊 Assessment ES data = Our elite warrior battalions")
        print("⚡ Loading with the power of Baahubali's war cry")
        print("🎵 Background score: 'Dandalayya' - The epic war preparation!")
        print("-" * 60)
        
        print("⚡ Summoning the mighty assessment warriors from ES kingdom...")
        assessmentDF = assessmentdfUtil.assessment_es_dataframe(spark)
        print("✅ Elite assessment army assembled! Warriors ready for battle!")
        
        hierarchyDF = spark.read.parquet(ParquetFileConstants.HIERARCHY_PARQUET_FILE)
        organizationDF = spark.read.parquet(ParquetFileConstants.ORG_COMPUTED_PARQUET_FILE)
        print("🏰 Hierarchy reinforcements arrived! Kingdom structure secured!")
        print("🎬 Kattappa's strategy: 'Baahubali, sabko organize kar diya hai!'")

        # Chapter 2: Kingdom Hierarchy - Family Tree Power
        print("\n" + "⚔️" * 70)
        print("🎬 SCENE 2: ROYAL LINEAGE - MAHISHMATI FAMILY TREE")
        print("⚔️" * 70)
        print("👑 Royal bloodline mapping! Assessment hierarchy building!")
        print("🌳 Like Mahishmati dynasty tree, connecting all assessment families")
        print("👶 Assessment children = Prince heirs and royal descendants")
        print("⚔️ add_hierarchy_column = Royal genealogy magic!")
        print("🏰 Every assessment knows its royal heritage!")
        print("-" * 60)
        
        print("🌿 Growing the royal assessment dynasty tree...")
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
        print("✅ Dynasty tree complete! Every assessment royal has found their lineage!")
        print("🎬 Queen Mother approves: 'Vansh parampara safal ho gayi!'")

        # Chapter 3: Kingdom Transformation - Divine Alchemy
        print("\n" + "⚔️" * 70)
        print("🎬 SCENE 3: DIVINE WEAPONS FORGING - TRANSFORMATION RITUAL")
        print("⚔️" * 70)
        print("🗡️ Forging legendary weapons! Assessment data transformation!")
        print("⚗️ Like creating Baahubali's divine sword, transforming raw data")
        print("🔥 Organization fusion = Melting metals in royal foundry")
        print("✨ transform_assessment_data = The ultimate weapon creation!")
        print("💎 Raw assessment ore becoming invincible weapons!")
        print("-" * 60)
        
        print("🔮 Royal blacksmiths starting divine weapon forging...")
        assessWithHierarchyDF = assessmentdfUtil.transform_assessment_data(assWithHierarchyData, organizationDF)
        assessWithDetailsDF = assessWithHierarchyDF.drop("children")
        print("✅ Legendary weapons forged! Assessment arsenal ready for war!")
        print("🎬 Master blacksmith: 'Baahubali, ye weapons invincible hain!'")

        # Chapter 4: War Council - Children Army Formation
        print("\n" + "⚔️" * 70)
        print("🎬 SCENE 4: WAR COUNCIL - YOUNG WARRIORS RECRUITMENT")
        print("⚔️" * 70)
        print("🏹 Young warriors joining the battle! Assessment children assembly!")
        print("👶 Like training royal princes for war, preparing assessment kids")
        print("📋 Each child warrior trained for specific battle skills")
        print("⚔️ assessment_children_dataframe = War academy graduation!")
        print("🎯 Future kings and queens of assessment domain!")
        print("-" * 60)
        
        print("🏹 Training the young assessment warriors in royal academy...")
        assessChildrenDF = assessmentdfUtil.assessment_children_dataframe(assessWithHierarchyDF)
        print("✅ Young warrior battalion ready! Next generation prepared!")
        print("🎬 Training master: 'Ye bachhe future ke Baahubali hain!'")

        # Chapter 5: Battle Records - Warrior Performance
        print("\n" + "⚔️" * 70)
        print("🎬 SCENE 5: BATTLE RECORDS - WARRIOR PERFORMANCE LOG")
        print("⚔️" * 70)
        print("📜 Documenting every warrior's battle history!")
        print("🏆 User assessment battles = Individual warrior achievements")
        print("📊 user_assessment_children_dataframe = Hall of fame records!")
        print("💪 Every battle scar tells a story of courage!")
        print("⚔️ Like Kattappa maintaining royal battle chronicles!")
        print("-" * 60)
        
        userAssessmentDF = spark.read.parquet(ParquetFileConstants.USER_ASSESSMENT_PARQUET_FILE) 
        userAssessChildrenDF = assessmentdfUtil.user_assessment_children_dataframe(userAssessmentDF, assessChildrenDF)
        print("✅ Battle chronicles complete! Every warrior's valor documented!")
        print("🎬 Kattappa notes: 'Har yoddha ka itihaas yaad rakhenge!'")

        # Chapter 6: Royal Academy - Course Wisdom
        print("\n" + "⚔️" * 70)
        print("🎬 SCENE 6: ROYAL ACADEMY - MAHISHMATI WISDOM VAULT")
        print("⚔️" * 70)
        print("📚 Ancient wisdom scrolls! Course curriculum compilation!")
        print("🧠 Like Mahishmati's royal library, storing all knowledge")
        print("🎓 all_course_program_details = Ancient wisdom catalog!")
        print("💎 Each course a precious gem of royal knowledge!")
        print("👑 Education fit for kings and queens!")
        print("-" * 60)
        
        print("📖 Compiling royal curriculum from ancient wisdom scrolls...")
        allCourseProgramDetailsWithCompDF = assessmentdfUtil.all_course_program_details_with_competencies_json_dataframe(
            spark.read.parquet(ParquetFileConstants.ALL_COURSE_PROGRAM_COMPUTED_PARQUET_FILE), 
            hierarchyDF, 
            organizationDF, 
            spark
        )
        
        allCourseProgramDetailsDF = allCourseProgramDetailsWithCompDF.drop("competenciesJson")
        print("✅ Royal academy curriculum ready! Ancient wisdom preserved!")
        print("🎬 Royal scholar: 'Maharaj, gyan ka bhandar taiyar hai!'")

        # Chapter 7: Royal Court - Divine Ratings
        print("\n" + "⚔️" * 70)
        print("🎬 SCENE 7: ROYAL COURT JUDGMENT - DIVINE RATING COUNCIL")
        print("⚔️" * 70)
        print("⭐ Royal court evaluating performance! Divine ratings council!")
        print("👥 Like Mahishmati court judging royal deeds")
        print("🌟 all_course_program_details_with_rating = Royal approval!")
        print("💫 Every course gets royal star certification!")
        print("⚖️ Justice and fairness in all evaluations!")
        print("-" * 60)
        
        print("⭐ Royal court in session... evaluating course excellence...")
        allCourseProgramDetailsWithRatingDF = assessmentdfUtil.all_course_program_details_with_rating_df(
            allCourseProgramDetailsDF,
            spark.read.parquet(ParquetFileConstants.RATING_SUMMARY_COMPUTED_PARQUET_FILE)
        )
        print("✅ Royal court verdict delivered! All courses rated fairly!")
        print("🎬 Chief Justice: 'Nyay aur insaaf ke saath rating complete!'")

        # Chapter 8: Ultimate Vision - Complete Truth
        print("\n" + "⚔️" * 70)
        print("🎬 SCENE 8: BAAHUBALI'S VISION - ULTIMATE TRUTH REVELATION")
        print("⚔️" * 70)
        print("👁️ Baahubali's divine vision seeing complete battlefield!")
        print("🔍 user_assessment_children_details = Ultimate war strategy!")
        print("💯 Complete picture of every warrior's journey!")
        print("🌟 All battle data converging into supreme wisdom!")
        print("⚔️ This is the moment of ultimate strategic clarity!")
        print("-" * 60)
        
        print("👁️ Baahubali opens his divine third eye for complete battlefield vision...")
        userAssessChildrenDetailsDF = assessmentdfUtil.user_assessment_children_details_dataframe(
            userAssessChildrenDF, 
            assessWithDetailsDF,
            allCourseProgramDetailsWithRatingDF, 
            spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
        )
        print("✅ Divine vision complete! Battlefield strategy perfected!")
        print("🎬 Baahubali roars: 'Ab mujhe sab dikh raha hai! Victory confirmed!'")

        # Chapter 9: Final Battle - Divine Judgment
        print("\n" + "⚔️" * 70)
        print("🎬 SCENE 9: FINAL BATTLE - EPIC CLIMAX CONFRONTATION")
        print("⚔️" * 70)
        print("🏆 The ultimate showdown! Final assessment judgment!")
        print("⚖️ Like Baahubali vs Bhallaladeva climax battle")
        print("📊 Pass/Fail verdicts = Victory or defeat in arena!")
        print("⏰ Latest attempts = Final battle performance!")
        print("👑 Justice will prevail! Truth will win!")
        print("-" * 60)
        
        print("⚖️ Epic battle arena ready... final confrontation begins...")
        # Step 1: Group to get latest attempt per user per child assessment
        latest = userAssessChildrenDetailsDF.groupBy("assessChildID", "userID").agg(
            spark_max("assessEndTimestamp").alias("assessEndTimestamp"),
            expr("COUNT(*)").alias("noOfAttempts")
        )
        
        print("📜 Writing epic battle judgment criteria...")
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
        
        print("⚔️ Delivering final battle blows with epic transformations...")
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
        
        print("✅ Epic battle won! Baahubali emerges victorious!")
        print("🎬 Victory roar: 'BAAHUBALI! BAAHUBALI! BAAHUBALI!'")

        # Chapter 10: Victory Celebration - Kingdom Glory
        print("\n" + "⚔️" * 70)
        print("🎬 SCENE 10: VICTORY CELEBRATION - MAHISHMATI GLORY")
        print("⚔️" * 70)
        print("🏆 BAAHUBALI WINS! Kingdom saved! Assessment empire secured!")
        print("🎉 Entire Mahishmati celebrates - CSV export ceremony!")
        print("📁 Royal decrees distributed to all kingdoms - Data sharing!")
        print("🥳 Dhol-nagada, fireworks, victory parade!")
        print("👑 Rightful king crowned! Assessment throne reclaimed!")
        print("-" * 60)
        
        print("🔥 Royal purification ceremony - filtering inactive subjects...")
        # Step 4: Filter out inactive users and generate report
        columns_to_keep = [c for c in original_df.columns if c != "status"]
        final_df = original_df.filter(col("status").cast("int") == 1).select([col(c) for c in columns_to_keep])

        final_count = final_df.count()
        print(f"📊 Royal census complete! Loyal subjects: {final_count:,}")
        
        print("📁 Royal decree distribution begins across all kingdoms...")
        dfexportutil.write_csv_per_mdo_id(final_df, f"{'reports'}/assessment", 'mdoid')
        print("✅ Victory celebration complete! Every kingdom received royal orders!")
        print("🎬 Baahubali's proclamation: 'Assessment rajya mein peace aur prosperity!'")

        # Performance Analysis
        total_duration = time.time() - total_start_time
        print(f"\n⚔️ EPIC BATTLE STATISTICS:")
        print(f"⏱️ Total war duration: {total_duration:.2f} seconds ({total_duration/60:.1f} minutes)")
        
        if total_duration < 60:
            print("⚡ Lightning victory! Baahubali ki speed jaisi unstoppable!")
        elif total_duration < 300:
            print("⚔️ Epic battle! Strategic warfare with perfect timing!")
        else:
            print("🏰 Siege warfare! Patient strategy leads to ultimate victory!")

        # Epic Conclusion
        print("\n" + "🏆" * 70)
        print("🎬 EPIC CONCLUSION: BAAHUBALI 2 - THE ASSESSMENT CONCLUSION")
        print("🏆" * 70)
        print("🥳 Why Kattappa killed Baahubali? To debug the ultimate assessment!")
        print("👑 Baahubali becomes the eternal king of assessment analytics!")
        print("⚔️ Evil defeated, justice restored, data processing perfected!")
        print("🎵 'Jai Jai Kara' playing - Victory song across three realms!")
        print("🏆 From impossible assessment challenge to legendary triumph!")
        print("📊 Assessment reports as magnificent as Mahishmati palace!")
        print("🙏 Divine blessing of data processing immortality achieved!")
        print("=" * 70)

    except Exception as e:
        print(f"\n💥 BHALLALADEVA STRIKES! EVIL ATTACKS!")
        print("⚔️" * 70)
        print("😈 Bhallaladeva's evil scheme to destroy assessment kingdom!")
        print(f"🎭 Villain's curse: {str(e)}")
        print("💪 But Baahubali never surrenders! 'Evil ko haar manna padega!'")
        print("⚔️ Calling reinforcements - Backup army assembling!")
        print("☕ Strategic retreat for war council and planning!")
        print("🎬 Kattappa's support: 'Baahubali, main hamesha tumhare saath hun!'")
        print("🙏 'Saahore Baahubali' - Divine strength for comeback!")
        print("⚔️" * 70)
        raise

def main():
    """
    Baahubali 2 Epic Director's Cut - The Assessment Conclusion
    
    From kingdom preparation to ultimate victory
    Epic Battle + Assessment Processing = Legendary Entertainment
    """
    
    print("\n" + "⚔️" * 80)
    print("🎬 BAAHUBALI 2 PRODUCTIONS PRESENTS")
    print("📊 ASSESSMENT REPORT GENERATION")
    print("🏆 THE EPIC ASSESSMENT KINGDOM SAGA")
    print("⚔️" * 80)
    print("🎭 Royal Cast:")
    print("👑 Apache Spark as Baahubali (The Righteous King)")
    print("⚔️ Data Transformations as Kattappa (The Loyal General)")
    print("🏰 Exception Handling as Royal Guards (The Protectors)")
    print("👸 Clean Schema as Devasena (The Pure Queen)")
    print("⚡ Query Optimizer as Royal Arrows (The Precision Weapons)")
    print("🎵 Music Director: M.M. Keeravani | Story: Assessment Kingdom vs Evil Bugs")
    print("⚔️" * 80)
    
    print("\n⚔️ War drums echo: 'Mahishmati assessment battle begins!'")
    print("🎺 Royal trumpets announce the epic data processing war!")
    print("🎬 Narrator: 'Yeh hai hamari kahani... assessment processing ka Baahubali!'")
    
    start_time = time.time()
    
    process_assessment_report()
    
    end_time = time.time()
    total_time = end_time - start_time
    
    print("\n🏆 WAR WON! BAAHUBALI 2 ASSESSMENT VICTORY ACHIEVED!")
    print("🥳 Mahishmati kingdom celebrates the impossible assessment triumph!")
    print("⚔️ From data processing war to legendary assessment victory!")
    print("🎊 Victory celebration continues across all three realms!")
    print(f"⏱️ Epic war duration: {total_time:.2f} seconds of pure grandeur!")
    print("🙏 'Baahubali! Baahubali!' - The legend of assessment processing!")
    print("🎬 THE END - An Epic Assessment Saga for Eternity!")

if __name__ == "__main__":
    main()
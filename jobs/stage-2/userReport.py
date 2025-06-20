import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, when, sum, bround, broadcast, coalesce, lit,
    current_timestamp, date_format, from_unixtime, concat_ws
)
import os

# Add parent directory to sys.path for importing project-specific modules
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Import reusable utilities from project
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.enrolment.acbp import acbpDFUtil
from dfutil.enrolment import enrolmentDFUtil
from dfutil.content import contentDFUtil
from dfutil.dfexport import dfexportutil

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
    RamayanaThemes
)

# Initialize Spark with epic style
RamayanaPrinter.print_spark_initialization()

spark = SparkSession.builder \
    .appName("UserReportGenerator_RamRajya_Edition") \
    .config("spark.executor.memory", "12g") \
    .config("spark.driver.memory", "10g") \
    .config("spark.sql.shuffle.partitions", "64") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

def processUserReport():
    """
    User Report Generation - Epic Ramayana Style!
    Clean version using ramayanUtil for all humor functions.
    """
    import time

    try:
        start_time = time.time()

        # Chapter 1: User Master Data
        chapter_header(1, "THE ROYAL CENSUS - USER MASTER DATA")
        ramayana_msg(RamayanaThemes.DATA_LOADING)
        
        user_master_df = spark.read.parquet(ParquetFileConstants.USER_COMPUTED_PARQUET_FILE)
        user_count = user_master_df.count()
        success_msg("User Master loaded! RAM ji khush!", "🏰 Ayodhya ki population ready!")
        data_quality_comment(user_count)

        # Chapter 2: Enrolment Data  
        chapter_header(2, "THE GURUKUL RECORDS - ENROLMENT DATA")
        ramayana_msg([
            "🎓 Vishwamitra ji ke gurukul ka attendance register!",
            "📝 'Kaun kaun se course mein enrolled hai?' - checking records",
            "🔍 Lakshman says: 'Bhaiya, schema complex lag raha hai!'",
            "😅 RAM: 'Koi baat nahi, decode kar denge!'"
        ])
        
        user_enrolment_df = spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE)
        print("🔍 Schema examination - Jatayu ki tarah sharp observation!")
        user_enrolment_df.printSchema()
        success_msg("Enrolment data decoded! Gurukul records clear!", 
                   "📖 Vishwamitra ji approve: 'Students ki list tayar!'")

        # Chapter 3: Content Duration
        chapter_header(3, "THE KNOWLEDGE TREASURY - CONTENT DURATION")
        ramayana_msg([
            "📖 Sage Vashishta's library - course duration wisdom!",
            "⚡ Filter operation like RAM's divine arrow - precise target!",
            "🎯 'Course' category mein se gems nikalne hain!",
            "💎 courseID transform - alchemy jaisi magic!"
        ])
        
        content_duration_df = (
            spark.read.parquet(ParquetFileConstants.CONTENT_COMPUTED_PARQUET_FILE)
            .filter(col("category") == "Course")
            .select(
                col("courseID").alias("content_id"),
                col("courseDuration").cast("double"),
                col("category")
            )
        )
        content_count = content_duration_df.count()
        success_msg(f"Knowledge treasury opened! Course gems: {content_count}",
                   "🧙‍♂️ Sage Vashishta blesses: 'Wisdom successfully extracted!'")

        # Chapter 4: Status Classification
        chapter_header(4, "THE DIVINE CLASSIFICATION - STATUS SORTING")
        ramayana_msg([
            "⚖️ Dharmaraj Yudhishthir style - justice for all statuses!",
            "🔮 Crystal ball reveals: 'Kaun kaha pahuncha hai learning mein?'",
            "👻 Null values = Manthara ki shakti - confusion create karti hai!",
            "⚡ When conditions = RAM ka dhanush - powerful transformation!"
        ])
        
        user_enrolment_df = user_enrolment_df.withColumn(
            "user_consumption_status",
            when(col("dbCompletionStatus").isNull(), "not-enrolled")
            .when(col("dbCompletionStatus") == 0, "not-started")
            .when(col("dbCompletionStatus") == 1, "in-progress")
            .otherwise("completed")
        )
        
        success_msg("Divine justice served! Status classification complete!",
                   "👑 RAM ji approves: 'Sabka saath, sabka vikas, sabka status!'")

        # Chapter 5: Data Joining
        chapter_header(5, "THE SACRED UNION - DATA JOINING CEREMONY")
        ramayana_msg(RamayanaThemes.DATA_JOINING)
        
        user_enrolment_master_df = userDFUtil.appendContentDurationCompletionForEachUser(
            spark, user_master_df, user_enrolment_df, content_duration_df
        )
        success_msg("Vivah sampann! Data marriage successful!",
                   "🍬 Laddu distribution - all systems celebrating!")

        # Chapter 6: Event Metrics
        chapter_header(6, "THE DIVINE WEAPONS - EVENT METRICS POWER")
        ramayana_msg([
            "🏹 RAM ka brahmastra - appendEventDuration function!",
            "💥 Event learning metrics = Hanuman ka gada power!",
            "🌊 Data flowing like Ganga - pure and powerful!",
            "⚡ 'Asambhav ko sambhav banana hai!' - impossible made possible!"
        ])
        
        user_complete_data = userDFUtil.appendEventDurationCompletionForEachUser(
            spark, user_enrolment_master_df
        )
        success_msg("Brahmastra successful! Event metrics embedded!",
                   "💪 Hanuman reports: 'Mission accomplished, Prabhu!'")

        # Chapter 7: Derived Columns
        chapter_header(7, "THE ROYAL MAKEOVER - DERIVED COLUMNS BEAUTY")
        ramayana_msg(RamayanaThemes.DATA_TRANSFORMATION)
        
        user_complete_data = user_complete_data \
            .withColumn("Tag", concat_ws(", ", col("additionalProperties.tag"))) \
            .withColumn("Total_Learning_Hours",
                        coalesce(col("total_event_learning_hours_with_certificates"), lit(0)) +
                        coalesce(col("total_content_duration"), lit(0))) \
            .withColumn("weekly_claps_day_before_yesterday",
                        when(col("weekly_claps_day_before_yesterday").isNull() |
                             (col("weekly_claps_day_before_yesterday") == ""),
                             lit(0)).otherwise(col("weekly_claps_day_before_yesterday")))

        success_msg("Makeover complete! Data looking like Sita Mata!",
                   "✨ Ready for the royal court presentation!")

        # Chapter 8: Final Selection
        chapter_header(8, "THE ROYAL COURT - FINAL COLUMN SELECTION")
        ramayana_msg([
            "⚖️ RAM Rajya court - final judgment and selection!",
            "📜 Royal decree: 'Ye columns chosen hain for export!'",
            "⏰ Time captured like Kaal Chakra - eternal timestamp!",
            "🎯 Each select statement = royal command precision!"
        ])
        
        dateTimeFormat = "yyyy-MM-dd HH:mm:ss"
        currentDateTime = current_timestamp()

        user_complete_df = user_complete_data \
            .withColumn("marked_as_not_my_user", when(col("userProfileStatus") == "NOT-MY-USER", lit(True)).otherwise(lit(False))) \
            .withColumn("data_last_generated_on", currentDateTime) \
            .withColumn("is_verified_karmayogi", when(col("userProfileStatus") == "VERIFIED", lit(True)).otherwise(lit(False))) \
            .select(
                col("userID").alias("user_id"),
                col("userOrgID").alias("mdo_id"),
                col("userStatus").alias("status"),
                coalesce(col("total_points"), lit(0)).alias("no_of_karma_points"),
                col("fullName").alias("full_name"),
                col("professionalDetails.designation").alias("designation"),
                col("personalDetails.primaryEmail").alias("email"),
                col("personalDetails.mobile").alias("phone_number"),
                col("professionalDetails.group").alias("groups"),
                col("Tag").alias("tag"),
                col("userProfileStatus").alias("profile_status"),
                date_format(from_unixtime(col("userCreatedTimestamp")), dateTimeFormat).alias("user_registration_date"),
                col("role").alias("roles"),
                col("personalDetails.gender").alias("gender"),
                col("personalDetails.category").alias("category"),
                col("marked_as_not_my_user"),
                col("is_verified_karmayogi"),
                col("userCreatedBy").alias("created_by_id"),
                col("additionalProperties.externalSystem").alias("external_system"),
                col("additionalProperties.externalSystemId").alias("external_system_id"),
                col("weekly_claps_day_before_yesterday"),
                coalesce(col("total_event_learning_hours_with_certificates"), lit(0)).alias("total_event_learning_hours"),
                coalesce(col("total_content_duration"), lit(0)).alias("total_content_learning_hours"),
                coalesce(col("Total_Learning_Hours"), lit(0)).alias("total_learning_hours"),
                col("employmentDetails.employeeCode").alias("employee_id"),
                col("data_last_generated_on")
            )

        character_quote("ram", "Royal court decision final! Perfect column selection!")
        print("🔍 Schema Darshan - Divine revelation of structure!")
        user_complete_df.printSchema()

        # Chapter 9: Victory Celebration
        chapter_header(9, "VICTORY CELEBRATION - CSV EXPORT FESTIVAL")
        ramayana_msg(RamayanaThemes.DATA_EXPORT)
        
        dfexportutil.write_csv_per_mdo_id(user_complete_df, ParquetFileConstants.USER_REPORT_CSV, 'mdo_id')
        success_msg("Distribution complete! Every kingdom got their data!",
                   "🙏 Hanuman reports: 'Sab jagah pahunch gaya, Prabhu!'")

        # Performance analysis
        total_duration = time.time() - start_time
        final_count = user_complete_df.count()
        
        performance_comment(total_duration)
        data_quality_comment(final_count, 0.95)  # Assuming high quality

        # Epic Conclusion
        RamayanaPrinter.print_epic_conclusion()

    except Exception as e:
        error_msg(e)
        raise

def main():
    """Epic Saga Director's Cut - Clean Version with ramayanUtil"""
    
    epic_intro("USER REPORT PROCESSING EPIC", "Clean Architecture with Divine Humor")
    
    print("🔔 Temple bells ring... 🕯️ Aarti begins... 🙏 Bhajan starts...")
    character_quote("ram", "Data processing ki shururat karte hain!")
    
    processUserReport()
    
    epic_finale([
        "🌅 Suryoday! Victory sun rises over digital Ayodhya!",
        "🏆 MISSION RAMAYANA ACCOMPLISHED!",
        "🎊 All of Ayodhya celebrates the data victory!"])
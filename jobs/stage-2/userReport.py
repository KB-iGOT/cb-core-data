import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, when, sum, bround, broadcast, coalesce, lit,
    current_timestamp, date_format, from_unixtime, concat_ws
)
import os
import time

# Add parent directory to sys.path for importing project-specific modules
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Import reusable utilities from project
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.enrolment.acbp import acbpDFUtil
from dfutil.enrolment import enrolmentDFUtil
from dfutil.content import contentDFUtil
from dfutil.dfexport import dfexportutil

# Initialize Spark with Queen's independent spirit
print("👑 QUEEN DATA PROCESSING - SOLO JOURNEY BEGINS!")
print("✈️ Spark Session ko Rani ki European trip ki tarah adventurous banayenge!")
print("💪 12GB executor memory - Rani ki newfound confidence jaisi strong!")
print("🧠 10GB driver memory - Paris mein discovery ki tarah intelligent!")
print("🎯 64 partitions - Solo travel ki tarah independent!")

spark = SparkSession.builder \
    .appName("UserReportGenerator_Queen_Solo_Journey_Edition") \
    .config("spark.executor.memory", "12g") \
    .config("spark.driver.memory", "10g") \
    .config("spark.sql.shuffle.partitions", "64") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

print("✅ Spark Session ready! Solo journey ke liye perfectly prepared!")
print("🎵 'London Thumakda' playing - adventure spirit activated!")

def processUserReport():
    """
    User Report Generation - Queen Movie Style!
    
    Like Rani's solo Europe trip where she discovers her true self,
    we're taking our data on a solo journey of transformation and discovery!
    
    👑 Each step is like a city on Rani's trip - from shy beginning to confident finale!
    """

    try:
        start_time = time.time()

        # Chapter 1: Departure Preparation - User Master Data
        print("\n" + "👑" * 70)
        print("🎬 SCENE 1: DEPARTURE PREPARATION - PACKING FOR PARIS")
        print("👑" * 70)
        print("✈️ Rani says: 'Main akeli ja rahi hun, scared hun but excited bhi!'")
        print("🧳 Just like Rani packing for her solo trip, we prepare user data")
        print("📊 User Master Data = Our travel essentials and passport")
        print("⚡ Loading user data with Rani's nervous but determined energy")
        print("🎵 Background music: 'Ranga Re' - Colorful start to journey!")
        print("-" * 60)
        
        user_master_df = spark.read.parquet(ParquetFileConstants.USER_COMPUTED_PARQUET_FILE)
        user_count = user_master_df.count()
        print(f"✅ Suitcase packed! Travel companions ready: {user_count:,}")
        
        if user_count > 50000:
            print("👑 Wow! Pura Europe tour group! 'Itne saare log, main manage kar sakti hun!'")
        elif user_count > 10000:
            print("😊 Good group size! Rani confident - 'Main kar sakti hun!'")
        else:
            print("💕 Small group trip! 'Intimate journey, better connections!'")
        
        print("🎬 Rani's determination: 'Chalo, adventure shuru karte hain!'")

        # Chapter 2: Flight Journey - Enrolment Data
        print("\n" + "👑" * 70)
        print("🎬 SCENE 2: FLIGHT TO PARIS - NERVOUS EXCITEMENT")
        print("👑" * 70)
        print("✈️ Flight mein Rani nervous! Enrolment data exploration!")
        print("🎯 Air hostess explains: 'Safety instructions complex hain!'")
        print("📚 Rani: 'Slowly slowly samjhungi, koi jaldi nahi!'")
        print("🌍 Enrolment patterns = Learning about new cultures")
        print("⚾ Schema examination = Understanding foreign customs")
        print("-" * 60)
        
        user_enrolment_df = spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE)
        print("🔍 Flight attendant explaining safety procedures... Schema analysis!")
        print("📋 'Dekho dekho, emergency exits kahan hain?' - Navigation time!")
        user_enrolment_df.printSchema()
        print("✅ Flight smooth! Rani getting comfortable with the journey!")
        print("🎬 Rani's realization: 'Yeh toh easy hai, main kar sakti hun!'")

        # Chapter 3: Paris Hotel Check-in - Content Duration
        print("\n" + "👑" * 70)
        print("🎬 SCENE 3: PARIS HOTEL CHECK-IN - SETTLING IN")
        print("👑" * 70)
        print("🏨 Hotel check-in kar rahe hain - Content Duration data!")
        print("⚾ Hotel facilities ka tour le rahe hain!")
        print("🎯 Course duration = Tourist attractions ka time planning")
        print("💎 Filter operation = Choosing best Paris experiences")
        print("🗼 Perfect location selection for memorable trip!")
        print("-" * 60)
        
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
        print(f"🏨 Hotel room ready! Paris attractions planned: {content_count:,}")
        print("🎬 Hotel manager: 'Mademoiselle, everything is perfect for you!'")

        # Chapter 4: Language Barrier - Status Classification
        print("\n" + "👑" * 70)
        print("🎬 SCENE 4: LANGUAGE LESSONS - COMMUNICATION BREAKTHROUGH")
        print("👑" * 70)
        print("🗣️ French language seekh rahi hai! Status classification!")
        print("🧠 Rani: 'English thoda-thoda aata hai, French seekhungi!'")
        print("⚾ User status classification = Language learning levels")
        print("🎯 Not-enrolled = Can't speak, In-progress = Learning phrases")
        print("🏆 Completed = Fluent conversations with locals!")
        print("-" * 60)
        
        user_enrolment_df = user_enrolment_df.withColumn(
            "user_consumption_status",
            when(col("dbCompletionStatus").isNull(), "not-enrolled")
            .when(col("dbCompletionStatus") == 0, "not-started")
            .when(col("dbCompletionStatus") == 1, "in-progress")
            .otherwise("completed")
        )
        
        print("✅ Language barrier broken! Communication successful!")
        print("🎬 Rani's confidence: 'Bonjour! Main French bol sakti hun!'")

        # Chapter 5: Making Friends - Data Joining
        print("\n" + "👑" * 70)
        print("🎬 SCENE 5: MAKING NEW FRIENDS - JOINING THE GROUP")
        print("👑" * 70)
        print("🤝 Naye friends bana rahi hai - Data joining time!")
        print("⚾ User data aur Content duration ka friendship!")
        print("🏆 Like Rani bonding with Taka and other travelers")
        print("💪 appendContentDuration = Building international friendships!")
        print("🎵 Playing: 'Hungama Ho Gaya' - The joy of new connections!")
        print("-" * 60)
        
        user_enrolment_master_df = userDFUtil.appendContentDurationCompletionForEachUser(
            spark, user_master_df, user_enrolment_df, content_duration_df
        )
        print("✅ Friendship goals achieved! International bonding complete!")
        print("🎬 Taka says: 'Rani, you are amazing! True friendship found!'")

        # Chapter 6: Amsterdam Adventure - Event Metrics
        print("\n" + "👑" * 70)
        print("🎬 SCENE 6: AMSTERDAM WILD NIGHT - BREAKING BOUNDARIES")
        print("👑" * 70)
        print("🎉 Amsterdam mein party! Event metrics building!")
        print("⚾ appendEventDuration = Dancing and discovering new self")
        print("💪 Every experience adding to confidence metrics!")
        print("🎯 Like Rani trying things she never imagined!")
        print("🏆 Breaking out of comfort zone with each adventure!")
        print("-" * 60)
        
        user_complete_data = userDFUtil.appendEventDurationCompletionForEachUser(
            spark, user_enrolment_master_df
        )
        print("✅ Wild night complete! Confidence level maximum!")
        print("🎬 Rani dancing: 'Yeh main hun! This is the real me!'")

        # Chapter 7: Shopping & Makeover - Derived Columns
        print("\n" + "👑" * 70)
        print("🎬 SCENE 7: SHOPPING SPREE - COMPLETE MAKEOVER")
        print("👑" * 70)
        print("👗 Shopping kar rahi hai - Data beautification!")
        print("✨ Derived columns = New clothes and complete makeover")
        print("🏏 Total_Learning_Hours = Total life experiences gained")
        print("⭐ Weekly claps = Compliments and new confidence")
        print("🎯 Getting ready for the final presentation of new self!")
        print("-" * 60)
        
        user_complete_data = user_complete_data \
            .withColumn("Tag", concat_ws(", ", col("additionalProperties.tag"))) \
            .withColumn("Total_Learning_Hours",
                        coalesce(col("total_event_learning_hours_with_certificates"), lit(0)) +
                        coalesce(col("total_content_duration"), lit(0))) \
            .withColumn("weekly_claps_day_before_yesterday",
                        when(col("weekly_claps_day_before_yesterday").isNull() |
                             (col("weekly_claps_day_before_yesterday") == ""),
                             lit(0)).otherwise(col("weekly_claps_day_before_yesterday")))

        print("✅ Makeover complete! Looking absolutely stunning!")
        print("🎬 Friend's compliment: 'Rani, you look like a different person!'")

        # Chapter 8: Return Journey - Final Selection
        print("\n" + "👑" * 70)
        print("🎬 SCENE 8: RETURN JOURNEY - NEW CONFIDENT RANI")
        print("👑" * 70)
        print("🏆 Europe trip complete! Final confident version ready!")
        print("📋 New Rani deciding what to take back home")
        print("⏰ Time to return - Final column selection for life!")
        print("🎯 Only the best experiences make it to the final self!")
        print("👑 This is it - the transformed, confident woman!")
        print("-" * 60)
        
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

        print("👑 New Rani ready! Confident and beautiful selection!")
        print("🔍 Life review - Final transformation announcement!")
        user_complete_df.printSchema()
        print("📋 New confident Rani approves her life choices!")

        # Chapter 9: Homecoming - CSV Export
        print("\n" + "👑" * 70)
        print("🎬 SCENE 9: HOMECOMING - SHARING THE NEW ME")
        print("👑" * 70)
        print("🏆 GHAR WAPAS! Transformation complete victory!")
        print("🎉 Sabko apna naya confident self dikhana hai - CSV export time!")
        print("📁 Life experiences sharing with everyone - Data distribution!")
        print("🥳 'Main udna chahti hun' - Confidence celebration!")
        print("👑 Old shy Rani is gone, new Queen has arrived!")
        print("-" * 60)
        
        dfexportutil.write_csv_per_mdo_id(user_complete_df, ParquetFileConstants.USER_REPORT_CSV, 'mdo_id')
        final_count = user_complete_df.count()
        print(f"✅ Homecoming celebration complete! Experiences shared: {final_count:,} records!")
        print("🎬 Rani's declaration: 'Main strong hun! Data processing mein bhi Queen hun!'")

        # Performance Analysis
        total_duration = time.time() - start_time
        print(f"\n👑 JOURNEY STATISTICS:")
        print(f"⏱️ Total Europe trip duration: {total_duration:.2f} seconds ({total_duration/60:.1f} minutes)")
        
        if total_duration < 60:
            print("⚡ Quick transformation! Paris ki magic jaisi speed!")
        elif total_duration < 300:
            print("👑 Perfect journey! Steady confidence building!")
        else:
            print("🧘‍♀️ Deep soul-searching! But transformation is beautiful!")

        # Epic Conclusion
        print("\n" + "🏆" * 70)
        print("🎬 EPIC CONCLUSION: QUEEN'S TRANSFORMATION COMPLETE")
        print("🏆" * 70)
        print("🥳 From shy housewife to confident Queen!")
        print("👑 Rani becomes the hero of her own data story!")
        print("💃 Dancing with joy - Perfect self-discovery!")
        print("🎵 'Badra Bahaar' playing - New season of life!")
        print("🏆 From data processing challenge to personal victory!")
        print("📊 User reports as beautiful as Rani's new confidence!")
        print("🙏 Self-love ka ashirwad hai - Divine self-acceptance!")
        print("=" * 70)

    except Exception as e:
        print(f"\n💥 OH NO! TRAVEL DISASTER!")
        print("👑" * 70)
        print("😰 Lost passport ya flight cancel! Travel nightmare!")
        print(f"🎭 Error message: {str(e)}")
        print("💪 But Rani never gives up! 'Main handle kar sakti hun!'")
        print("👑 Problem-solving time - Independent woman mode!")
        print("☕ Cafe mein chai peeke solution dhundhenge!")
        print("🎬 Friend's support: 'Rani, tum strong ho, yeh kar sakti ho!'")
        print("🙏 'Main udna chahti hun' - Even problems can't stop dreams!")
        print("👑" * 70)
        raise

def main():
    """
    Queen Movie Director's Cut - The Solo Journey Epic
    
    From shy beginning to confident finale
    Solo Travel + Data Processing = Perfect Self-Discovery
    """
    
    print("\n" + "👑" * 80)
    print("🎬 QUEEN PRODUCTIONS PRESENTS")
    print("📊 USER REPORT GENERATION")
    print("🏆 THE SOLO JOURNEY DATA PROCESSING EPIC")
    print("👑" * 80)
    print("🎭 Cast:")
    print("👑 Apache Spark as Rani (The Independent Traveler)")
    print("✈️ Data Transformations as Travel Experiences (The Teachers)")
    print("🤝 Exception Handling as Taka (The Supportive Friend)")
    print("💃 Clean Schema as Confident New Self (The Beautiful Result)")
    print("⚡ Query Optimizer as Adventure Spirit (The Bold Explorer)")
    print("🎵 Music Director: Amit Trivedi | Story: Data Self-Discovery Journey")
    print("👑" * 80)
    
    print("\n👑 Airport announcement: 'Flight to Data Processing, boarding now!'")
    print("🎺 Adventure music starts... passport ready!")
    print("🎬 Narrator: 'Yeh hai hamari kahani... data processing ki Queen!'")
    
    processUserReport()
    
    print("\n🏆 JOURNEY COMPLETE! QUEEN TRANSFORMATION ACHIEVED!")
    print("🥳 Solo traveler becomes confident data processor!")
    print("👑 From shy data handling to bold analytics!")
    print("💃 Victory dance in every data center!")
    print("🙏 'Main udna chahti hun' - Dreams do come true!")
    print("🎬 THE END - A Solo Journey Epic for the Ages!")

if __name__ == "__main__":
    main()
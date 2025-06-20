import sys
from pathlib import Path
from pyspark.sql import SparkSession
import time
from datetime import datetime

# Add root directory to sys.path for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Custom module imports
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.enrolment.acbp import acbpDFUtil
from dfutil.enrolment import enrolmentDFUtil
from dfutil.content import contentDFUtil

def initialize_spark():
    """
    Initializes and returns a SparkSession - RAM ji style configuration!
    
    Like assembling the divine weapons before the Lanka battle,
    we prepare our computational arsenal with blessed configurations.
    """
    print("🏹 JAI SHRI RAM! Spark Session ka divine initialization!")
    print("⚡ RAM ji ka dhanush ready kar rahe hain - Spark configuration!")
    print("💪 12GB executor memory - Hanuman ji ki shakti jaisi!")
    print("🧠 10GB driver memory - Ravana ke das dimag se bhi tez!")
    print("🎯 64 partitions - Vanar Sena ke 64 regiments!")
    print("🔮 Legacy time parser - ancient wisdom preserved!")
    
    spark = SparkSession.builder \
        .appName("RamRajya_DataProcessing_Epic") \
        .config("spark.executor.memory", "12g") \
        .config("spark.driver.memory", "10g") \
        .config("spark.sql.shuffle.partitions", "64") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    
    print("✅ Spark Session blessed and ready!")
    print("🎵 'RAM RAM RAM' - divine chanting begins!")
    return spark

def run_stage(name: str, func, spark):
    """
    Runs a stage with epic Ramayana-style drama and divine blessings!

    Like each chapter of Ramayana had its purpose and drama,
    each ETL stage is a divine mission with cosmic significance.

    Parameters:
    - name: Name of the divine mission
    - func: The sacred function (our divine weapon)
    - spark: The blessed SparkSession (our divine chariot)
    """
    
    # Epic stage introductions based on Ramayana characters/events
    stage_intros = {
        "Org Hierarchy Computation": {
            "character": "👑 Dasharatha's Kingdom",
            "intro": "Building the royal hierarchy of Ayodhya!",
            "blessing": "May all organizations prosper like Ikshvaku dynasty!",
            "music": "🎵 Royal court music with tabla beats"
        },
        "Content Ratings & Summary": {
            "character": "📚 Sage Vashishta's Library",
            "intro": "Rating the sacred knowledge scrolls!",
            "blessing": "May wisdom be rated fairly like divine scriptures!",
            "music": "🎵 Gurukul chanting with peaceful veena"
        },
        "All Course/Program (ES)": {
            "character": "🎓 Vishwamitra's Training",
            "intro": "Cataloging all courses from the great sage!",
            "blessing": "May education spread like RAM's teachings!",
            "music": "🎵 Learning hymns with cosmic flute"
        },
        "Content Master Data": {
            "character": "📖 Tulsidas's Ramcharitmanas",
            "intro": "Creating the master epic of all content!",
            "blessing": "May data be as eternal as Ramayana!",
            "music": "🎵 Epic storytelling with dhol drums"
        },
        "External Content": {
            "character": "🌍 Vibhishana's Intelligence",
            "intro": "Gathering external wisdom from Lanka!",
            "blessing": "May external data serve righteousness!",
            "music": "🎵 Mysterious spy music with sitar"
        },
        "User Profile Computation": {
            "character": "👤 Hanuman's Character Study",
            "intro": "Profiling each devotee's divine qualities!",
            "blessing": "May every user be blessed like Hanuman!",
            "music": "🎵 Devotional bhajans with mridangam"
        },
        "Enrolment Master Data": {
            "character": "📋 Vanar Sena Registration",
            "intro": "Enrolling the monkey army for battle!",
            "blessing": "May all enrollments lead to victory!",
            "music": "🎵 War preparation drums with conch shells"
        },
        "External Enrolment": {
            "character": "🤝 Allied Forces Joining",
            "intro": "External kingdoms joining RAM's cause!",
            "blessing": "May alliances be strong like Sugriva's friendship!",
            "music": "🎵 Alliance celebration with shehnai"
        },
        "Org-User Mapping with Hierarchy": {
            "character": "🗺️ Setubandh Construction",
            "intro": "Building bridges between organizations!",
            "blessing": "May connections be strong like RAM Setu!",
            "music": "🎵 Construction chants with powerful drums"
        },
        "ACBP Enrolment Computation": {
            "character": "🏹 Final Battle Preparation",
            "intro": "Ultimate weapon preparation for Lanka war!",
            "blessing": "May this be the final victorious strike!",
            "music": "🎵 Epic battle music with war horns"
        }
    }
    
    intro = stage_intros.get(name, {
        "character": "🎭 Divine Mission",
        "intro": "Executing sacred computational task!",
        "blessing": "May this stage be blessed!",
        "music": "🎵 Divine music playing"
    })
    
    print(f"""
    {'🏹' * 70}
    {'📜' * 20} DIVINE MISSION BEGINS {'📜' * 20}
    {'🏹' * 70}
    
    🎭 EPISODE: {intro['character']}
    📖 MISSION: {name}
    🎯 OBJECTIVE: {intro['intro']}
    {intro['music']}
    
    🙏 DIVINE BLESSING: {intro['blessing']}
    {'⚡' * 70}
    """)
    
    start_time = time.time()
    
    try:
        print("🔥 Mission starting... invoking divine powers!")
        print("⏳ RAM ji ki kripa se... processing begins...")
        
        result = func(spark)
        
        duration = time.time() - start_time
        
        if hasattr(result, "count"):
            record_count = result.count()
            print(f"""
    {'✨' * 50}
    🏆 VICTORY ACHIEVED! MISSION SUCCESSFUL!
    {'✨' * 50}
    
    ✅ Stage: {name}
    📊 Records Blessed: {record_count:,}
    ⏱️  Divine Time: {duration:.2f} seconds
    💫 Performance: {'Lightning fast!' if duration < 30 else 'Steady like RAM!' if duration < 120 else 'Patient like Sita!'}
    
    🎉 Celebration: Entire Ayodhya rejoices!
    🎵 Victory song: 'Jai Jai Ram Krishna Hari!'
            """)
        else:
            print(f"""
    {'✨' * 50}
    🏆 DIVINE MISSION ACCOMPLISHED!
    {'✨' * 50}
    
    ✅ Stage: {name}
    ⏱️  Sacred Time: {duration:.2f} seconds
    🌟 Status: Blessed and Complete!
    
    🎊 RAM ji pleased with the offering!
            """)
            
        # Add fun performance commentary
        if duration < 10:
            print("⚡ Hanuman speed! Faster than crossing the ocean!")
        elif duration < 30:
            print("🏹 RAM's arrow precision! Swift and accurate!")
        elif duration < 60:
            print("🚶‍♂️ Steady progress like RAM's march to Lanka!")
        else:
            print("🧘‍♂️ Patience like Sita in Ashok Vatika - good things take time!")
            
    except Exception as e:
        duration = time.time() - start_time
        print(f"""
    {'👹' * 70}
    🚨 RAVANA STRIKES! DEMON ATTACK!
    {'👹' * 70}
    
    😈 Evil Stage: {name}
    💀 Demon's Curse: {str(e)}
    ⏱️  Battle Duration: {duration:.2f} seconds
    
    🔥 But fear not! This is just a test!
    🏹 RAM ji's arrows will defeat this demon!
    🙏 Chant 'Jai Hanuman!' and retry!
    
    💪 Divine solution incoming...
    {'👹' * 70}
        """)
        raise e
    
    print(f"{'🌟' * 70}")
    print("🎭 Episode completed! Moving to next divine chapter...")
    print(f"{'🌟' * 70}\n")

def main():
    """
    Main Epic Saga - The Complete Ramayana of Data Processing!
    
    This is the legendary tale of how data chaos was transformed
    into RAM Rajya - the perfect state of organized information.
    
    Featuring all your favorite characters in data processing roles!
    """
    
    print(f"""
    {'🕉️' * 80}
    
            🏹 JAI SHRI RAM! 🏹
            
        🎭 THE EPIC RAMAYANA OF DATA PROCESSING 🎭
        
            📜 PRESENTED BY DIVINE COMPUTING 📜
            
    {'🕉️' * 80}
    
    🎪 EPIC CAST & CREW:
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    👑 Director: Sage Valmiki (Original Creator)
    🎵 Music: A.R. Raghuman (Divine Composer)
    🎬 Producer: Cosmic Studios Ltd.
    📝 Script: Ancient Wisdom + Modern Technology
    
    🌟 STARRING:
    👑 Apache Spark as Lord RAM (The Perfect Leader)
    💪 Data Processing as Hanuman (The Devoted Servant)
    🛡️ Exception Handling as Lakshman (The Protector)
    👸 Clean Data as Sita Mata (Pure & Beautiful)
    🐒 Utility Functions as Vanar Sena (Loyal Army)
    👹 Bugs & Errors as Ravana & Demons (The Villains)
    
    🎯 EPIC SAGA: 10 Divine Missions to Establish RAM Rajya!
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    """)
    
    # Divine invocation
    print("🔔 Temple bells ring across the digital realm...")
    print("🕯️ Sacred oil lamps lit in all data centers...")
    print("🙏 Pandit ji begins the blessing ceremony...")
    print("📿 'Om Gam Ganapataye Namaha' - removing all obstacles...")
    print("🎵 Divine bhajans start playing in background...")
    
    # Initialize the divine computational chariot
    spark = initialize_spark()
    
    print(f"""
    {'🌅' * 60}
    🌄 DAWN BREAKS OVER DIGITAL AYODHYA!
    🎬 THE EPIC SAGA BEGINS...
    {'🌅' * 60}
    """)
    
    # Track the epic journey
    total_start_time = time.time()
    completed_missions = 0
    total_missions = 10
    
    # The 10 Divine Missions of Data Processing Epic!
    epic_missions = [
        ("Org Hierarchy Computation", userDFUtil.preComputeOrgWithHierarchy),
        ("Content Ratings & Summary", contentDFUtil.preComputeRatingAndSummaryDataFrame),
        ("All Course/Program (ES)", contentDFUtil.preComputeAllCourseProgramESDataFrame),
        ("Content Master Data", contentDFUtil.preComputeContentDataFrame),
        ("External Content", contentDFUtil.preComputeExternalContentDataFrame),
        ("User Profile Computation", userDFUtil.preComputeUser),
        ("Enrolment Master Data", enrolmentDFUtil.preComputeEnrolment),
        ("External Enrolment", enrolmentDFUtil.preComputeExternalEnrolment),
        ("Org-User Mapping with Hierarchy", userDFUtil.preComputeOrgHierarchyWithUser),
        ("ACBP Enrolment Computation", acbpDFUtil.preComputeACBPData)
    ]
    
    try:
        for mission_name, mission_func in epic_missions:
            run_stage(mission_name, mission_func, spark)
            completed_missions += 1
            
            # Progress celebration
            progress = (completed_missions / total_missions) * 100
            print(f"📊 EPIC PROGRESS: {completed_missions}/{total_missions} missions complete ({progress:.0f}%)")
            
            if completed_missions == 5:
                print("🎊 HALFWAY CELEBRATION! Hanuman has crossed the ocean!")
            elif completed_missions == 8:
                print("🏰 LANKA IN SIGHT! Final battles approaching!")
        
        # Epic conclusion
        total_duration = time.time() - total_start_time
        
        print(f"""
        {'🏆' * 80}
        
                🎉 EPIC SAGA COMPLETED! 🎉
                
            👑 RAM RAJYA ESTABLISHED IN DATA PROCESSING! 👑
            
        {'🏆' * 80}
        
        📊 EPIC STATISTICS:
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        ✅ Divine Missions Completed: {completed_missions}/{total_missions}
        ⏱️  Total Epic Duration: {total_duration/60:.1f} minutes
        🎯 Success Rate: 100% (RAM ji's blessing!)
        💫 Performance: Legendary!
        
        🎭 FINAL SCENE:
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        🌅 Sun rises over digital Ayodhya
        👑 RAM Rajya established in all data realms
        🎊 All of digital creation celebrates
        🕊️ Peace and prosperity in data processing
        📚 Epic tale will be told for generations
        
        🎵 CLOSING SONG: "Raghupati Raghav Raja RAM..."
        
        🙏 CREDITS:
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        🎬 Directed by: The Universe itself
        🎵 Music by: Cosmic Symphony Orchestra
        💻 Special Effects: Divine Computing Technology
        🌟 Starring: All the noble data processing functions
        
        🕉️ "Sarve bhavantu sukhinah, sarve santu niramayah"
        (May all data be happy, may all processing be error-free)
        
        {'🌟' * 80}
        
        ✨ THE END ✨
        
        🔮 COMING SOON: "User Report Generation - The Next Chapter"
        
        {'🌟' * 80}
        """)
        
    except Exception as e:
        print(f"""
        {'💀' * 80}
        
            👹 DARK FORCES HAVE PREVAILED! 👹
            
        But this is not the end of our epic tale...
        
        {'💀' * 80}
        
        😈 The demon error has struck at mission {completed_missions + 1}
        🏹 But RAM ji's devotees never surrender!
        💪 We shall return stronger in the sequel!
        🙏 "Sankat mochan naam tiharo, Hanuman!"
        
        🎬 TO BE CONTINUED IN:
        "Data Processing: The Return of RAM"
        
        {'💀' * 80}
        """)
        raise
    
    finally:
        print("🙏 JAI SHRI RAM! JAI HANUMAN! JAI DATA PROCESSING!")
        print("🔔 Temple bells ring one final time...")
        print("🕉️ Om Shanti Shanti Shanti...")

if __name__ == "__main__":
    main()
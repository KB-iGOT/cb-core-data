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
    Initializes and returns a SparkSession - Lagaan cricket team style!
    
    Like Bhuvan assembling his cricket team with diverse skills,
    we prepare our Spark configuration with optimized settings.
    """
    print("🏏 LAGAAN DATA PROCESSING - TEAM ASSEMBLY BEGINS!")
    print("⚡ Spark Session ko Bhuvan ki team ki tarah powerful banayenge!")
    print("💪 12GB executor memory - Kachra ki spin bowling jaisi effective!")
    print("🧠 10GB driver memory - Bhuvan ke strategy jaisa intelligent!")
    print("🎯 64 partitions - Puri cricket team ka coordination!")
    print("🏆 Legacy time parser - Traditional techniques jo kabhi fail nahi hote!")
    
    spark = SparkSession.builder \
        .appName("Lagaan_DataProcessing_Cricket_Championship") \
        .config("spark.executor.memory", "12g") \
        .config("spark.driver.memory", "10g") \
        .config("spark.sql.shuffle.partitions", "64") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    
    print("✅ Cricket team assembled and ready!")
    print("🎵 'Chale Chalo' spirit - March towards data victory!")
    return spark

def run_stage(name: str, func, spark):
    """
    Runs a stage with Lagaan cricket match excitement!

    Like each phase of the cricket match in Lagaan had its drama,
    each ETL stage is a crucial phase of our data processing match.

    Parameters:
    - name: Name of the cricket phase
    - func: The match strategy (our processing function)
    - spark: The cricket team (our SparkSession)
    """
    
    # Cricket match phases with Lagaan character connections
    stage_strategies = {
        "Org Hierarchy Computation": {
            "character": "👑 Raja sahab's Court",
            "strategy": "Building the village hierarchy like royal court!",
            "encouragement": "May organizations unite like villagers against British!",
            "mood": "🎵 Royal meeting music with serious discussions"
        },
        "Content Ratings & Summary": {
            "character": "📚 Pandit ji's Wisdom",
            "strategy": "Rating knowledge like village elder's guidance!",
            "encouragement": "May wisdom be valued like Pandit ji's advice!",
            "mood": "🎵 Thoughtful village council music"
        },
        "All Course/Program (ES)": {
            "character": "🎓 Cricket Rules Learning",
            "strategy": "Understanding all cricket rules and courses!",
            "encouragement": "May education spread like cricket fever!",
            "mood": "🎵 Learning and practice session tunes"
        },
        "Content Master Data": {
            "character": "📖 Village Chronicle Master",
            "strategy": "Creating the master record of village stories!",
            "encouragement": "May data be as eternal as village tales!",
            "mood": "🎵 Storytelling music with dhol rhythms"
        },
        "External Content": {
            "character": "🌍 British Intelligence Reports",
            "strategy": "Gathering information about British team tactics!",
            "encouragement": "May external data help us like spy reports!",
            "mood": "🎵 Cautious investigation background music"
        },
        "User Profile Computation": {
            "character": "👤 Team Player Profiles",
            "strategy": "Profiling each cricket team member's skills!",
            "encouragement": "May every player shine like our village heroes!",
            "mood": "🎵 Individual player theme music"
        },
        "Enrolment Master Data": {
            "character": "📋 Cricket Team Registration",
            "strategy": "Enrolling village warriors for the big match!",
            "encouragement": "May all enrollments lead to victory!",
            "mood": "🎵 Team assembly drums and excitement"
        },
        "External Enrolment": {
            "character": "🤝 Allied Village Support",
            "strategy": "Other villages supporting our cricket cause!",
            "encouragement": "May alliances be strong like village unity!",
            "mood": "🎵 Community celebration music"
        },
        "Org-User Mapping with Hierarchy": {
            "character": "🗺️ Cricket Field Mapping",
            "strategy": "Mapping field positions and team hierarchy!",
            "encouragement": "May connections be strong like team coordination!",
            "mood": "🎵 Strategic planning music with tabla beats"
        },
        "ACBP Enrolment Computation": {
            "character": "🏏 Final Match Preparation",
            "strategy": "Ultimate preparation for the championship match!",
            "encouragement": "May this be our winning strategy!",
            "mood": "🎵 Victory anticipation music with building drums"
        }
    }
    
    strategy = stage_strategies.get(name, {
        "character": "🏏 Cricket Action",
        "strategy": "Executing match-winning move!",
        "encouragement": "May this phase bring us closer to victory!",
        "mood": "🎵 Cricket match excitement music"
    })
    
    print(f"""
    {'🏏' * 70}
    {'⚾' * 20} CRICKET PHASE BEGINS {'⚾' * 20}
    {'🏏' * 70}
    
    🎭 PHASE: {strategy['character']}
    📖 MISSION: {name}
    🎯 STRATEGY: {strategy['strategy']}
    {strategy['mood']}
    
    🙏 BHUVAN'S BLESSING: {strategy['encouragement']}
    {'🏆' * 70}
    """)
    
    start_time = time.time()
    
    try:
        print("🔥 Match phase starting... team taking positions!")
        print("⏳ Bhuvan's confidence: 'Hum kar sakte hain!'")
        
        result = func(spark)
        
        duration = time.time() - start_time
        
        if hasattr(result, "count"):
            record_count = result.count()
            print(f"""
    {'🏆' * 50}
    🥳 PHASE WON! EXCELLENT PERFORMANCE!
    {'🏆' * 50}
    
    ✅ Cricket Phase: {name}
    📊 Runs Scored: {record_count:,}
    ⏱️  Over Duration: {duration:.2f} seconds
    💫 Performance: {'Lightning fast like Bhura!' if duration < 30 else 'Steady like Bhuvan!' if duration < 120 else 'Patient like Gauri!'}
    
    🎉 Celebration: Village crowd cheers loudly!
    🎵 Victory song: 'Mitwa sun mitwa!'
            """)
        else:
            print(f"""
    {'🏆' * 50}
    🥳 PHASE COMPLETED SUCCESSFULLY!
    {'🏆' * 50}
    
    ✅ Cricket Phase: {name}
    ⏱️  Phase Time: {duration:.2f} seconds
    🌟 Status: Perfect execution!
    
    🎊 Bhuvan smiles with satisfaction!
            """)
            
        # Cricket performance commentary
        if duration < 10:
            print("⚡ Bhura ki speed! Faster than running between wickets!")
        elif duration < 30:
            print("🏏 Bhuvan's perfect shot! Clean and precise!")
        elif duration < 60:
            print("🚶‍♂️ Steady progress like building partnership!")
        else:
            print("🧘‍♂️ Patient batting like saving wickets - wise strategy!")
            
    except Exception as e:
        duration = time.time() - start_time
        print(f"""
    {'💥' * 70}
    🚨 BRITISH TEAM STRIKES! FOUL PLAY!
    {'💥' * 70}
    
    😠 Troubled Phase: {name}
    💀 British Captain's Trick: {str(e)}
    ⏱️  Struggle Duration: {duration:.2f} seconds
    
    🔥 But Bhuvan never gives up!
    🏏 'Haar nahi maanenge!' - We won't accept defeat!
    🙏 Village support: 'Tum kar sakte ho!'
    
    💪 Comeback strategy being planned...
    {'💥' * 70}
        """)
        raise e
    
    print(f"{'🌟' * 70}")
    print("🎭 Phase completed! Moving to next cricket strategy...")
    print(f"{'🌟' * 70}\n")

def main():
    """
    Main Cricket Championship - The Complete Lagaan Data Processing!
    
    This is the legendary tale of how impossible data challenges were
    conquered through teamwork, determination, and cricket spirit!
    
    Featuring all village heroes in data processing roles!
    """
    
    print(f"""
    {'🏏' * 80}
    
            🎬 LAGAAN DATA PROCESSING! 🎬
            
        🏆 THE CRICKET CHAMPIONSHIP OF DATA 🏆
        
            📜 PRESENTED BY VILLAGE PRODUCTIONS 📜
            
    {'🏏' * 80}
    
    🎪 CRICKET TEAM & CREW:
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    👑 Captain: Bhuvan (Data Team Leader)
    🎵 Music: Village Musicians (Inspirational Soundtrack)
    🎬 Story: Village vs British Data Challenge
    📝 Strategy: Teamwork + Determination = Victory
    
    🌟 PLAYING XI:
    👑 Apache Spark as Captain Bhuvan (The Inspiring Leader)
    💪 Data Processing as Kachra (The Specialist)
    🛡️ Exception Handling as Lakha (The Dependable)
    👸 Clean Data as Gauri (The Pure & Beautiful)
    🏃‍♂️ Utility Functions as Village Team (The United Squad)
    😈 Bugs & Errors as British Team (The Opposition)
    
    🎯 CRICKET SAGA: 10 Crucial Overs to Win the Match!
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    """)
    
    # Match day preparation
    print("🔔 Village temple bells ring for good luck...")
    print("🕯️ Oil lamps lit in every village house...")
    print("🙏 Village priest blesses the cricket team...")
    print("📿 'Ishwar Allah Tere Naam' - Seeking divine help...")
    print("🎵 Village musicians start the encouraging songs...")
    
    # Initialize the cricket team (Spark session)
    spark = initialize_spark()
    
    print(f"""
    {'🌅' * 60}
    🌄 DAWN OF THE MATCH DAY!
    🏏 THE CRICKET CHAMPIONSHIP BEGINS...
    {'🌅' * 60}
    """)
    
    # Track the cricket match progress
    total_start_time = time.time()
    completed_overs = 0
    total_overs = 10
    
    # The 10 Cricket Overs of Data Processing Championship!
    cricket_overs = [
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
        for over_name, over_strategy in cricket_overs:
            run_stage(over_name, over_strategy, spark)
            completed_overs += 1
            
            # Match progress celebration
            progress = (completed_overs / total_overs) * 100
            print(f"📊 MATCH PROGRESS: {completed_overs}/{total_overs} overs completed ({progress:.0f}%)")
            
            if completed_overs == 5:
                print("🎊 HALFWAY CELEBRATION! Team performing brilliantly!")
            elif completed_overs == 8:
                print("🏏 FINAL OVERS APPROACH! Victory within reach!")
        
        # Cricket match conclusion
        total_duration = time.time() - total_start_time
        
        print(f"""
        {'🏆' * 80}
        
                🎉 CRICKET MATCH WON! 🎉
                
            👑 LAGAAN VICTORY! DATA PROCESSING CHAMPION! 👑
            
        {'🏆' * 80}
        
        📊 MATCH STATISTICS:
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        ✅ Overs Completed: {completed_overs}/{total_overs}
        ⏱️  Total Match Duration: {total_duration/60:.1f} minutes
        🎯 Win Percentage: 100% (Village team victory!)
        💫 Performance: Championship winning!
        
        🎭 VICTORY CELEBRATION:
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        🌅 Victory sun shines over the village
        👑 Bhuvan lifts the championship trophy
        🎊 Entire village celebrates wildly
        🕊️ Three years tax-free! (Bug-free processing!)
        📚 Match will be remembered for generations
        
        🎵 VICTORY SONG: "Radha Kaise Na Jale"
        
        🙏 CREDITS:
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        🎬 Directed by: Village Wisdom
        🎵 Music by: Traditional Musicians + Modern Beats
        💻 Special Effects: Cricket Team Coordination
        🌟 Starring: All brave village data processors
        
        🏏 "Jeet gaye! Jeet gaye! Hamne kar dikhaya!"
        (We won! We won! We proved ourselves!)
        
        {'🌟' * 80}
        
        ✨ MATCH END ✨
        
        🔮 COMING SOON: "User Report Championship - The Next Season"
        
        {'🌟' * 80}
        """)
        
    except Exception as e:
        print(f"""
        {'💀' * 80}
        
            😠 BRITISH TEAM CHEATED! 😠
            
        But this village never gives up...
        
        {'💀' * 80}
        
        😈 British captain's dirty trick at over {completed_overs + 1}
        🏏 But Bhuvan's team never surrenders!
        💪 Village spirit is stronger than any challenge!
        🙏 "Haar nahi maanenge!" - We won't accept defeat!
        
        🎬 TO BE CONTINUED IN:
        "Data Processing: The Comeback Match"
        
        {'💀' * 80}
        """)
        raise
    
    finally:
        print("🙏 JAI HO! VILLAGE VICTORIOUS! JAI DATA PROCESSING!")
        print("🔔 Victory bells ring across all villages...")
        print("🏏 Cricket bats raised in celebration...")

if __name__ == "__main__":
    main()
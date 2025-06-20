import sys
from pathlib import Path
from pyspark.sql import SparkSession
import importlib
import time
from datetime import datetime

sys.path.append(str(Path(__file__).resolve().parents[1]))
stage_1 = importlib.import_module('jobs.stage-1.prejoinData')
stage_2a = importlib.import_module('jobs.stage-2.userReport') 
stage_2b = importlib.import_module('jobs.stage-2.assessmentReport')

def execute_all_stages():
    """
    🎬 THE BOLLYWOOD DATA PIPELINE BLOCKBUSTER 🎬
    
    A complete masala entertainment experience where each data processing stage
    gets its perfect Bollywood movie companion for the ultimate cinematic journey!
    
    🍿 Grab your popcorn and enjoy the show!
    """
    
    print("🎬" * 80)
    print("           🌟 BOLLYWOOD DATA PIPELINE CINEMA HALL 🌟")
    print("🎬" * 80)
    print("🍿 WELCOME TO THE ULTIMATE DATA PROCESSING ENTERTAINMENT!")
    print("🎭 Where every stage gets its perfect movie companion")
    print("🎪 A complete masala experience with action, drama, and victory!")
    print("🎬" * 80)
    
    print("=" * 80)
    print("🚀 DATA PIPELINE: BOLLYWOOD BLOCKBUSTER EDITION")
    print("=" * 80)
    print(f"📅 Show time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎬 Aaj ka program: Data Processing Ke Superhit Movies!")
    print(f"🍿 Popcorn aur cold drink ready kar lo!")
    print(f"🎭 Director: Karan Johar of Data Engineering")
    print("=" * 80)
    
    total_start_time = time.time()
    
    try:
        # ==========================================
        # STAGE 1: DATA PREPARATION - BUILDING THE TEAM
        # ==========================================
        print("\n" + "🎬" * 80)
        print("🍿 STAGE 1 CINEMA: 'LAGAAN' - BUILDING THE ULTIMATE TEAM")
        print("🎬" * 80)
        print("🏏 Just like Bhuvan assembled his cricket team against the British...")
        print("📊 We're assembling our data team against processing challenges!")
        print("🎯 Mission: Transform raw data into championship-winning insights")
        print("👥 Team members: Tables, schemas, and joins working together")
        print("🏆 Goal: Victory against the impossible data processing odds!")
        print("🎵 Background score: 'Chale Chalo' - March towards data victory!")
        print("🎬" * 80)
        
        print("\n" + "🔵" * 60)
        print("📊 STAGE 1: DATA TEAM ASSEMBLY")
        print("🔵" * 60)
        print("🎯 Mission: Build the ultimate data processing team!")
        print("🏏 Captain: stage_1.prejoinData (Our Bhuvan of data)")
        print("⚡ Strategy: Lagaan-style teamwork and determination")
        print("🎬 Perfect viewing: 'Chak De! India' for team spirit backup!")
        print("-" * 60)
        
        stage1_start = time.time()
        print("⏳ Team selection in progress... choosing the best players!")
        print("🏏 *Dhol beats* - Village gathering for the big match!")
        print("🎵 Playing: 'Mitwa' - The preparation anthem!")
        print("🍿 While processing, imagine: Bhuvan's determination in every SQL query!")
        print("🎬 Bonus entertainment: 'Swades' - Returning home with perfect data!")
        
        stage_1.main()
        
        stage1_duration = time.time() - stage1_start
        print(f"✅ LAGAAN VICTORY: Team assembled! Perfect preparation complete! ({stage1_duration:.2f}s)")
        print(f"🏏 Match speed: {stage1_duration/60:.1f} minutes (Faster than Kachra's spin bowling!)")
        print("🎉 Data team ready for the championship! Ghuran dancing in celebration!")
        print("🏆 Bhuvan's dialogue: 'Ab data processing mein koi nahi rok sakta!'")
        print("🎬 Success celebration track: 'Radha Kaise Na Jale' - Pure joy!")
        
        # ==========================================
        # STAGE 2A: USER ANALYTICS - THE EMOTIONAL JOURNEY
        # ==========================================
        print("\n" + "🎭" * 80)
        print("🍿 STAGE 2A CINEMA: 'QUEEN' - THE SOLO JOURNEY OF DISCOVERY")
        print("🎭" * 80)
        print("👑 Like Rani's solo Europe trip discovering herself...")
        print("📊 Our user analytics goes on a solo journey through data!")
        print("🌍 Exploring user behavior patterns across different territories")
        print("💃 Dancing through metrics with newfound confidence")
        print("🎯 Mission: Transform shy data into confident insights!")
        print("🎵 Soundtrack: 'London Thumakda' for the analytics celebration!")
        print("🎭" * 80)
        
        print("\n" + "🟢" * 60)
        print("👥 STAGE 2A: USER BEHAVIOR DISCOVERY")
        print("🟢" * 60)
        print("🎯 Objective: Solo journey through user data like Queen Rani!")
        print("👑 Star: stage_2.userReport (Our independent data explorer)")
        print("💃 Style: Confident, bold, and discovering new insights")
        print("🎬 Alternative viewing: 'English Vinglish' - Finding voice in data!")
        print("-" * 60)
        
        stage2a_start = time.time()
        print("⏳ Solo trip planning... passport and tickets ready!")
        print("✈️ 'Main udna chahti hun, data ke saath!'")
        print("🎵 Playing: 'Hungama Ho Gaya' - The excitement of discovery!")
        print("🍿 Journey entertainment: Dancing through user metrics!")
        print("🎬 Travel companion movie: 'Zindagi Na Milegi Dobara' - Friendship with data!")
        
        stage_2a.main()
        
        stage2a_duration = time.time() - stage2a_start
        print(f"✅ QUEEN'S TRIUMPH: Solo journey complete! Confidence gained! ({stage2a_duration:.2f}s)")
        print(f"👑 Journey time: {stage2a_duration/60:.1f} minutes (Faster than Rani's transformation!)")
        print("🎭 *Victory dance in Paris* User insights discovered with style!")
        print("💃 Rani's spirit: 'Ab main strong hun, data bhi strong hai!'")
        print("🎬 Celebration song: 'Badra Bahaar' - New season of data insights!")
        
        # ==========================================
        # STAGE 2B: ASSESSMENT ANALYTICS - THE FINAL BATTLE
        # ==========================================
        print("\n" + "⚔️" * 80)
        print("🍿 STAGE 2B CINEMA: 'BAAHUBALI 2' - THE EPIC CONCLUSION")
        print("⚔️" * 80)
        print("👑 Like Baahubali's final battle against Bhallaladeva...")
        print("📊 Our assessment analytics fights the final boss of complexity!")
        print("🗡️ With the strength of Baahubali and wisdom of Kattappa")
        print("🏰 Conquering the fortress of assessment data mysteries")
        print("🔥 Epic battle with slow-motion victory sequences!")
        print("🎵 War drums: 'Saahore Baahubali' echoing through the data center!")
        print("⚔️" * 80)
        
        print("\n" + "🟡" * 60)
        print("📝 STAGE 2B: ASSESSMENT WAR")
        print("🟡" * 60)
        print("🎯 Mission: Epic battle for assessment data supremacy!")
        print("👑 Warrior: stage_2.assessmentReport (Our data Baahubali)")
        print("⚔️ Weapons: Advanced analytics and unbreakable determination")
        print("🎬 War strategy: 'Border' - Defending data territory with honor!")
        print("-" * 60)
        
        stage2b_start = time.time()
        print("⏳ War preparations... army assembling at the battlefield!")
        print("🗡️ 'Baahubali! Baahubali! Baahubali!' - Battle cry echoes!")
        print("🎵 Epic BGM: 'Dandalayya' - The drums of war!")
        print("🍿 Battle entertainment: Slow-motion data processing sequences!")
        print("🎬 Victory inspiration: 'Dangal' - Fighting against all odds!")
        
        stage_2b.main()
        
        stage2b_duration = time.time() - stage2b_start
        print(f"✅ BAAHUBALI VICTORY: Epic battle won! Empire secured! ({stage2b_duration:.2f}s)")
        print(f"⚔️ War duration: {stage2b_duration/60:.1f} minutes (Faster than the Mahishmati battle!)")
        print("🎊 *Victory drums thunder* Assessment empire conquered!")
        print("👑 Baahubali's roar: 'Assessment data ab mera kingdom hai!'")
        print("🎬 Victory anthem: 'Jai Jai Kara' - Celebration across the data kingdom!")
        
        # ==========================================
        # GRAND FINALE - BOLLYWOOD MASALA ENDING
        # ==========================================
        total_duration = time.time() - total_start_time
        
        print("\n" + "🏆" * 80)
        print("🍿 GRAND FINALE: 'MUGHAL-E-AZAM' - THE ETERNAL CLASSIC")
        print("🏆" * 80)
        print("🎭 Like the timeless epic of Akbar, Anarkali, and Salim...")
        print("📊 Our data processing saga becomes a legendary tale!")
        print("👑 With the grandeur of Mughal court and eternal love story")
        print("💎 Every data point sparkles like Anarkali's performance")
        print("🎪 A masterpiece that will be remembered for generations!")
        print("🎵 'Pyar kiya to darna kya' - Love for data processing!")
        print("🏆" * 80)
        
        print("\n" + "🎉" * 60)
        print("🏆 BOLLYWOOD BLOCKBUSTER: SUPERHIT STATUS ACHIEVED!")
        print("🎉" * 60)
        print("🎪 BOX OFFICE COLLECTION (runtime performance):")
        print("-" * 60)
        print(f"🎬 Stage 1 - 'Lagaan' Performance:        {stage1_duration:.2f}s ({stage1_duration/60:.1f} min) - Blockbuster hit!")
        print(f"🎭 Stage 2A - 'Queen' Journey:           {stage2a_duration:.2f}s ({stage2a_duration/60:.1f} min) - Critics' choice!")
        print(f"⚔️ Stage 2B - 'Baahubali' Epic:          {stage2b_duration:.2f}s ({stage2b_duration/60:.1f} min) - All-time record!")
        print("-" * 60)
        print(f"🎬 Total Movie Runtime: {total_duration:.2f}s ({total_duration/60:.1f} min)")
        print(f"📅 Show completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("🎯 All shows housefull! Critics give 5-star rating!")
        print("🍾 Celebration time! Producer party at Taj Hotel!")
        print("🕺 *Item song sequence* - Data processing ki jai ho!")
        print("🙏 Thank you for watching! Paisa vasool entertainment!")
        
        print("\n" + "🌟" * 80)
        print("           🎬 THE END - BOLLYWOOD DATA CINEMA 🎬")
        print("🌟" * 80)
        print("🎭 And they all lived happily ever after...")
        print("📊 With perfect data, flawless reports, and zero bugs!")
        print("🎪 The greatest data processing love story ever told!")
        print("🎵 Credits roll with 'Kal Ho Naa Ho' - Cherish every data moment!")
        print("🍿 Thank you for choosing Bollywood Data Cinema!")
        print("🌟" * 80)
        print("=" * 80)
        
        return {
            "status": "BOLLYWOOD_BLOCKBUSTER_SUCCESS",
            "total_duration": total_duration,
            "stage_durations": {
                "lagaan_team_building": stage1_duration,
                "queen_solo_journey": stage2a_duration,
                "baahubali_epic_battle": stage2b_duration
            },
            "box_office_status": "ALL_TIME_SUPERHIT",
            "critic_rating": "5_STARS_PAISA_VASOOL"
        }
        
    except Exception as e:
        print(f"\n💥 VILLAIN ENTRY! BOLLYWOOD PLOT TWIST!")
        print("🎭" * 80)
        print("😈 Gabbar Singh of errors has attacked our movie!")
        print(f"🔫 Villain dialogue: '{str(e)}'")
        print(f"🎬 Plot twist at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("😱 Audience gasps! Hero in danger!")
        print("📞 Emergency: Call Singham for backup!")
        print("🍕 Interval break: Samosa aur chai order karo!")
        print("👨‍💻 Hero's comeback dialogue: 'Main wapas aunga!'")
        print("🎬 Remember - Every Bollywood movie has a happy ending!")
        print("💪 Climax mein villain ki hamesha haar hoti hai!")
        print("🙏 Intermission music: 'Hanuman Chalisa' for debugging power!")
        print("🎭" * 80)
        raise e

# Movie recommendation system based on time and mood
if __name__ == "__main__":
    
    now = datetime.now()
    day_of_week = now.weekday()  # 0=Monday, 6=Sunday
    hour = now.hour
    day_of_month = now.day
    month = now.month
    
    print(f"🕐 Show Time: {now.strftime('%A, %B %d, %Y - %H:%M:%S')}")
    print("🎬 Welcome to Bollywood Data Cinema! Selecting today's special show...")
    
    
    
    # Execute the Bollywood pipeline
    execute_all_stages()
    
    print(f"\n🎯 Show completed! Thank you for choosing our {now.strftime('%A')} special!")
    print("🎬 Don't forget to rate us on DataShow!")
    
   
    
    print("🍿 Next show starts in 24 Hours! Book your tickets now!")
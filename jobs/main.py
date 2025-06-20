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
    """Execute all pipeline stages with full desi tadka and masala"""
    
    # Pipeline header with Bollywood style
    print("=" * 80)
    print("🚀 DATA PIPELINE: DABANGG RETURNS - THE SPARK STRIKES BACK")
    print("=" * 80)
    print(f"📅 Muhurat time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎬 Aaj ka program: Multi-Stage Data Processing Ka Mahabharat!")
    print(f"🍿 Samosas ready kar lo, data ka nasha hone wala hai!")
    print(f"💃 Director: Rohit Shetty of Data Engineering")
    print("=" * 80)
    
    total_start_time = time.time()
    
    try:
        # ==========================================
        # STAGE 1: PRE-JOIN DATA PROCESSING
        # ==========================================
        print("\n" + "🔵" * 60)
        print("📊 STAGE 1: DATA KA GREAT INDIAN KITCHEN")
        print("🔵" * 60)
        print("🎯 Mission: Maggi se biryani banane ka magic!")
        print("👨‍🍳 Chef: stage_1.prejoinData (aka 'Data ka Sanjeev Kapoor')")
        print("🌶️ Masala level: Extra spicy with SQL tadka")
        print("🏠 Fun fact: Iska code dekh ke Sharma uncle ka beta engineer ban gaya")
        print("-" * 60)
        
        stage1_start = time.time()
        print("⏳ Data ke aaloo pyaaz ko chop kar rahe hain... pressure cooker ready!")
        print("🔥 *Cooker ki siti bajti hai* - 3 whistles and we're done!")
        
        stage_1.main()
        
        stage1_duration = time.time() - stage1_start
        print(f"✅ STAGE 1: Wah! Perfect banaa! Mummy proud! ({stage1_duration:.2f}s)")
        print(f"🏃‍♂️ Speed: {stage1_duration/60:.1f} minutes (Domino's se tez delivery!)")
        print("🎉 Data ab bilkul marriage-ready hai! 💒")
        
        # ==========================================
        # STAGE 2A: USER REPORT GENERATION
        # ==========================================
        print("\n" + "🟢" * 60)
        print("👥 STAGE 2A: SAAS BAHU AUR USER REPORTS")
        print("🟢" * 60)
        print("🎯 Objective: Boring numbers ko K-serial drama mein convert karna")
        print("📺 Star Plus presents: stage_2.userReport (Ekta Kapoor ki latest hit)")
        print("💎 Plot: Users ke dil mein kya hai? Hamara report batayega!")
        print("🎭 Guest appearance by: Exception handling aunty")
        print("-" * 60)
        
        stage2a_start = time.time()
        print("⏳ Report wale uncle ko chai-biscuit de rahe hain...")
        print("☕ 'Bas 5 minute beta, abhi WhatsApp forward kar raha hun'")
        
        stage_2a.main()
        
        stage2a_duration = time.time() - stage2a_start
        print(f"✅ STAGE 2A: Zabardast! Reports bilkul butter chicken jaisi smooth! ({stage2a_duration:.2f}s)")
        print(f"📈 Performance: {stage2a_duration/60:.1f} minutes (Rajinikanth style - impossible is nothing!)")
        print("🎭 *Slow motion dance sequence* Tujhe dekha to ye jaana sanam!")
        
        # ==========================================
        # STAGE 2B: ASSESSMENT DATA PROCESSING
        # ==========================================
        print("\n" + "🟡" * 60)
        print("📝 STAGE 2B: CID - SPECIAL ASSESSMENT BUREAU")
        print("🟡" * 60)
        print("🎯 Mission: Assessment ka raaz khol dena hai!")
        print("🕵️ ACP Pradyuman presents: stage_2.assessmentReport")
        print("💼 Daya: 'Sir, yeh assessment normal nahi hai!'")
        print("🔍 Abhijeet: 'Kuch to gadbad hai!'")
        print("-" * 60)
        
        stage2b_start = time.time()
        print("⏳ Forensic team ko bulaya gaya hai... unka coffee break chal raha hai")
        print("🔍 *Magnifying glass sound effect from 90s TV shows*")
        print("📞 'Hello, IT department? Have you tried turning it off and on again?'")
        
        stage_2b.main()
        
        stage2b_duration = time.time() - stage2b_start
        print(f"✅ STAGE 2B: Case solved! Culprit pakda gaya! ({stage2b_duration:.2f}s)")
        print(f"🕰️ Investigation time: {stage2b_duration/60:.1f} minutes (CID walo se bhi tez!)")
        print("🎊 *Rahul Dravid style celebration* - Calm and composed victory!")
        
        # ==========================================
        # PIPELINE COMPLETION CELEBRATION
        # ==========================================
        total_duration = time.time() - total_start_time
        
        print("\n" + "🎉" * 60)
        print("🏆 BHAIYON AUR BEHNO, HAMNE KAR DIKHAYA!")
        print("🎉" * 60)
        print("🎪 FINAL SCORECARD (tabla beats please...):")
        print("-" * 60)
        print(f"🥇 Stage 1 (Kitchen Master):     {stage1_duration:.2f}s ({stage1_duration/60:.1f} min) - Ekdum jhakkas!")
        print(f"🥈 Stage 2A (Drama Queen):       {stage2a_duration:.2f}s ({stage2a_duration/60:.1f} min) - Superhit!")
        print(f"🥉 Stage 2B (CID Officer):       {stage2b_duration:.2f}s ({stage2b_duration/60:.1f} min) - Kamaal!")
        print("-" * 60)
        print(f"🕐 Total Time: {total_duration:.2f}s ({total_duration/60:.1f} min)")
        print(f"📅 Khatam shuru: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("🎯 Sab stages without any error! Server abhi tak zinda hai!")
        print("🍾 Celebration time! Kulfi-falooda order karo!")
        print("🕺 *Bhangra sequence with full orchestra*")
        print("🙏 Dhanyawaad! Agle janam mein bhi data engineer banenge!")
        print("=" * 80)
        
        return {
            "status": "FIRST_CLASS_SUCCESS",
            "total_duration": total_duration,
            "stage_durations": {
                "stage_1": stage1_duration,
                "stage_2a": stage2a_duration,
                "stage_2b": stage2b_duration
            },
            "tadka_level": "EXTRA_MASALA_MAXIMUM_DHAMAKA"
        }
        
    except Exception as e:
        print(f"\n💥 ARREY YAAR! KUCH TO GADBAD HAI!")
        print("🚨" * 60)
        print(f"🔥 Error uncle ne kaha: {str(e)}")
        print(f"📱 Panic time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("😭 Servers ne hartal kar diya hai!")
        print("☎️  Emergency helpline: 1-800-ITNA-KYON")
        print("🍕 Recommendation: Chai-pakoda order karo aur kal fir try karo")
        print("👨‍💻 WhatsApp pe family group mein message: 'Code nahi chal raha'")
        print("🚨" * 60)
        raise e

def execute_all_stages_delhi_style():
    """Delhi walon ke liye special aggressive version"""
    
    print("\n😤 Oye! Ek aur data pipeline... yahan bhi line!")
    print("=" * 50)
    print("📊 Dekhte hain aaj data sahab cooperation karte hain ya nahi!")
    
    total_start = time.time()
    
    try:
        # Stage 1
        print("\n📊 [1/3] Pre-join processing (bilkul bakwas kaam hai)...")
        print("😴 'Yaar ye kab khatam hoga?'")
        start = time.time()
        stage_1.main()
        duration = time.time() - start
        print(f"    😲 Arey! Chal gaya! {duration:.1f}s mein! Dilli ki power!")
        
        # Stage 2A
        print("\n👥 [2/3] User reports (users ki bhi na...)...")
        print("🤷‍♀️ 'Bhai report padhta kaun hai nowadays...'")
        start = time.time()
        stage_2a.main()
        duration = time.time() - start
        print(f"    🎭 Hogaya boss! {duration:.1f}s mein - CP wali speed!")
        
        # Stage 2B
        print("\n📝 [3/3] Assessment data (assessment ka assessment)...")
        print("🕵️‍♀️ 'Bhai kya scene hai ye?'")
        start = time.time()
        stage_2b.main()
        duration = time.time() - start
        print(f"    🎪 Tata-bye-bye! {duration:.1f}s mein complete!")
        
        total_duration = time.time() - total_start
        print(f"\n🎉 Sab sahi! Total time: {total_duration:.1f}s")
        print("🏆 LinkedIn pe update karo: 'Data Pipeline Ke Baadshah'")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n🤦‍♀️ Lo kar diya na! Error: {e}")
        print("☕ Chai peeney ka time hai aur LinkedIn pe job search karne ka")
        raise

def execute_all_stages_mumbai_style():
    """Mumbai local train ki tarah fast and efficient"""
    
    print("\n🚂 Mumbai Local Express Pipeline - Next station: Success!")
    print("🌸" * 50)
    print("🍃 Bheed mein bhi kaam karna hai... local train style!")
    
    total_start = time.time()
    
    try:
        # Stage 1
        print("\n🌱 [1/3] Stage 1: Dadar se Bandra - Data sorting...")
        print("🧘‍♂️ 'Seedha khada raho, ladies compartment nahi hai'")
        start = time.time()
        stage_1.main()
        duration = time.time() - start
        print(f"    ☯️  {duration:.1f}s mein pahunch gaye! Exactly on time!")
        
        # Stage 2A
        print("\n🌸 [2/3] Stage 2A: Andheri se Borivali - Report generation...")
        print("🕯️  'Bhagwan bharose hai, signal green rahega'")
        start = time.time()
        stage_2a.main()
        duration = time.time() - start
        print(f"    🙏 {duration:.1f}s - No delay! Mumbai spirit!")
        
        # Stage 2B
        print("\n🌙 [3/3] Stage 2B: Churchgate se CST - Final processing...")
        print("🔮 'Boss, traffic jam mein fas gaye, but data ready hai'")
        start = time.time()
        stage_2b.main()
        duration = time.time() - start
        print(f"    ✨ {duration:.1f}s - Destination reached!")
        
        total_duration = time.time() - total_start
        print(f"\n🌅 Journey complete! Total time: {total_duration:.1f}s")
        print("🧘‍♀️ 'Bas pehle ki local pakad li, sab set hai'")
        print("🌸" * 50)
        
    except Exception as e:
        print(f"\n🌪️  Signal fail ho gaya: {e}")
        print("🍵 Platform pe chai-biscuit khaao aur next local ka wait karo...")
        raise

def execute_all_stages_south_indian_style():
    """South Indian cinema ke style mein - logic defying action"""
    
    print("\n🎬 *Mass BGM plays* - Data Processing: The Rajinikanth Way")
    print("=" * 60)
    print("🎭 Physics? Logic? We don't need that here!")
    print("💥 Data will fly in slow motion and land perfectly!")
    print("🕺 Thalaiva style execution incoming...")
    print("=" * 60)
    
    total_start = time.time()
    
    try:
        print("\n🎥 SCENE 1: The Hero's Entry")
        print("⚡ *Coconut falls from tree in slow motion*")
        print("🥥 Hero catches it without looking - that's our Stage 1!")
        start = time.time()
        stage_1.main()
        duration = time.time() - start
        print(f"🔥 'Impossible!' - Physics Professor ({duration:.1f}s)")
        print("💪 *Flexes bicep* - Data sorted with swag!")
        
        print("\n🎥 SCENE 2: The Mass Sequence")
        print("💀 *100 goons surround our hero*")
        print("🤸‍♂️ Hero fights them with... a banana! That's Stage 2A!")
        start = time.time()
        stage_2a.main()
        duration = time.time() - start
        print(f"😱 'Anna, you are too good!' - Random villager ({duration:.1f}s)")
        print("🌪️ *Tornado appears out of nowhere*")
        
        print("\n🎥 SCENE 3: The Climax")
        print("🌋 *Hero surfs on a helicopter to reach the villain*")
        print("🦁 Villain is a... corrupt database! Stage 2B to the rescue!")
        start = time.time()
        stage_2b.main()
        duration = time.time() - start
        print(f"🎯 'Naan oruthadava sonna, nooru dhadava sonna!' ({duration:.1f}s)")
        print("🎊 *Confetti falls from helicopter*")
        
        total_duration = time.time() - total_start
        print(f"\n🏆 THALAIVAAA! Total runtime: {total_duration:.1f}s")
        print("⭐ 'Style', 'Mass', 'Class' - All in one pipeline!")
        print("🎵 *Anirudh BGM intensifies*")
        print("🙏 'Vanakkam Chennai!' - Hero's final dialogue")
        
    except Exception as e:
        print(f"\n💀 VILLAIN WINS: {e}")
        print("🎬 'Sequel confirmed!' - Producer")
        print("🍿 Coming soon: Pipeline 2 - The Debugging Begins")
        raise

def execute_all_stages_bengali_intellectual():
    """Bengali intellectual style - everything is profound"""
    
    print("\n📚 Data Pipeline: An Intellectual Discourse on Information Processing")
    print("☕" * 50)
    print("🎭 As Tagore would say... 'Data jekhane bishoy, sekhane joy'")
    print("🐟 Like ilish maach in monsoon, data too has its season")
    
    total_start = time.time()
    
    try:
        print("\n📖 Chapter 1: The Metamorphosis of Raw Data")
        print("🎨 'Ei je dekho, data o shilpo hote pare!'")
        print("📰 Like Anandabazar Patrika's morning edition - fresh and informative")
        start = time.time()
        stage_1.main()
        duration = time.time() - start
        print(f"✍️ Profound! Like Ray's cinematography! ({duration:.1f}s)")
        
        print("\n🎪 Chapter 2: The User's Dilemma - A Sociological Study")
        print("🍵 Over addha and mishti, we contemplate user behavior")
        print("🎭 'Manusher upor bhalobasar moto, user report-er upor trust'")
        start = time.time()
        stage_2a.main()
        duration = time.time() - start
        print(f"📝 Extraordinary! Feluda would be proud! ({duration:.1f}s)")
        
        print("\n🎵 Chapter 3: Assessment - The Final Reckoning")
        print("🏛️ In the style of Calcutta University's exam system")
        print("📊 'Marks die manush hoyna, kintu statistics die hoy'")
        start = time.time()
        stage_2b.main()
        duration = time.time() - start
        print(f"🎯 Magnificent! Better than Park Street's phuchka! ({duration:.1f}s)")
        
        total_duration = time.time() - total_start
        print(f"\n🏆 Epilogue: Success in {total_duration:.1f}s")
        print("📚 'Jodi tor dak shune keu na ashe tobe ekla cholo re' - Even data agrees!")
        print("☕ Time for addha and adda at Coffee House")
        
    except Exception as e:
        print(f"\n💔 Tragedy strikes like a Rituparno Ghosh film: {e}")
        print("📖 'Ei rokom hoy, eita jibon' - That's life, said the philosopher")
        raise

# Date-based logic with Indian festival calendar awareness
if __name__ == "__main__":
    
    now = datetime.now()
    day_of_week = now.weekday()  # 0=Monday, 6=Sunday
    hour = now.hour
    day_of_month = now.day
    month = now.month
    
    print(f"🕐 Samay: {now.strftime('%A, %B %d, %Y - %H:%M:%S')}")
    print("🎭 Aaj ka mood check kar rahe hain...")
    
    # Monday Morning Blues - Need Bollywood masala
    if day_of_week == 0 and hour < 12:  # Monday morning
        print("😫 Monday morning detected! Full Bollywood treatment needed!")
        execute_all_stages()
    
    # Delhi office hours - Aggressive style
    elif day_of_week < 5 and 9 <= hour <= 18:  # Weekday office hours
        print("💼 Office time hai boss! Delhi style execution!")
        execute_all_stages_delhi_style()
    
    # Mumbai rush hour - Local train efficiency
    elif (hour >= 8 and hour <= 10) or (hour >= 18 and hour <= 21):
        print("🚂 Rush hour! Mumbai local train ki speed mein!")
        execute_all_stages_mumbai_style()
    
    # Late night (after 10 PM) - South Indian mass style
    elif hour >= 22:
        print("🌙 Late night mass execution! Thalaiva style!")
        execute_all_stages_south_indian_style()
    
    # Early morning intellectual hour (5-8 AM)
    elif 5 <= hour < 8:
        print("🌅 Subah ka intellectual session! Bengali style!")
        execute_all_stages_bengali_intellectual()
    
    # Diwali season (October-November)
    elif month in [10, 11]:
        print("🪔 Diwali season! Extra dhoom-dham execution!")
        print("🎆 Data processing mein bhi pataka!")
        execute_all_stages()
    
    # Holi season (March) - Colorful execution
    elif month == 3:
        print("🌈 Holi hai! Rangon se bhara execution!")
        execute_all_stages()
    
    # Monsoon season (June-September) - Contemplative
    elif month in [6, 7, 8, 9]:
        print("🌧️ Monsoon mood! Contemplative execution!")
        execute_all_stages_south_indian_style()
    
    # Weekend South Indian movie time
    elif day_of_week in [5, 6]:  # Saturday, Sunday
        print("🎬 Weekend! South Indian blockbuster time!")
        execute_all_stages_south_indian_style()
    
    # Default - Full masala treatment
    else:
        print("🎲 Aaj kuch special nahi... regular Bollywood masala!")
        execute_all_stages()
    
    print(f"\n🎯 Execution ho gaya with perfect {now.strftime('%A')} wala josh!")
    
    # Indian time-based easter eggs
    if hour == 4 and now.minute == 20:
        print("🌅 Brahma muhurat! Data processing bhi auspicious time pe!")
    elif hour == 12 and now.minute == 0:
        print("🌞 Exact dupahir! Time for lunch break aur data success!")
    elif hour == 21 and now.minute == 0:
        print("📺 9 PM - Prime time! Hamara pipeline bhi prime time ready!")
    elif day_of_month == 15:
        print("💰 Salary day! Data bhi paisa kamane mein help karega!")
    elif day_of_week == 0 and hour == 9:
        print("😴 Monday 9 AM - Coffee double shot leke aao!")
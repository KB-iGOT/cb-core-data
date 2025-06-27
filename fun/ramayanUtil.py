"""
ramayanUtil.py - Epic Ramayana Style Humor Utilities

A sacred module containing divine humor functions for data processing entertainment.
Like the eternal Ramayana epic, these functions bring joy and wisdom to your code.

Author: Digital Sage Valmiki
Version: Ram Rajya 1.0
Blessed by: All the gods of distributed computing
"""

import random
from datetime import datetime
from typing import List, Optional

class RamayanaPrinter:
    """
    The sacred printer class that channels divine humor energy.
    Like Hanuman carrying messages, this class carries entertainment across your codebase.
    """
    
    # Epic character database for dynamic quotes
    CHARACTERS = {
        "ram": {
            "name": "👑 Lord RAM",
            "quotes": [
                "Dharma ki jeet hamesha hoti hai!",
                "Sabka saath, sabka vikas!",
                "Patience aur precision - ye hai success ka mantra!",
                "Data mein bhi satya hona chahiye!"
            ]
        },
        "hanuman": {
            "name": "💪 Hanuman ji",
            "quotes": [
                "Mission accomplished, Prabhu!",
                "Sankat mochan naam tiharo!",
                "Jai Bajrang Bali! Bug fix ho gaya!",
                "RAM naam ki shakti se sab possible hai!"
            ]
        },
        "lakshman": {
            "name": "🛡️ Lakshman",
            "quotes": [
                "Bhaiya, main hamesha ready hun!",
                "Error handling mere zimme!",
                "Loyalty aur protection - ye mera kaam!",
                "Koi problem ho to mujhe bulao!"
            ]
        },
        "sita": {
            "name": "👸 Sita Mata",
            "quotes": [
                "Pavitrata aur sundarata essential hai!",
                "Clean data clean heart!",
                "Patience mein hi shakti hai!",
                "Satyam shivam sundaram!"
            ]
        }
    }
    
    # Epic icons for different purposes
    ICONS = {
        "chapter": ["🏹", "📚", "⏰", "🎭", "💒", "🌪️", "🎨", "🏛️", "🎊"],
        "success": ["✅", "🎉", "🏆", "🌟", "💫", "⚡"],
        "error": ["👹", "💀", "🚨", "🔥", "💥"],
        "celebration": ["🎊", "🎉", "🥳", "🕺", "💃", "🎆"]
    }

    @staticmethod
    def print_spark_initialization():
        """Initialize Spark with epic Ramayana style"""
        messages = [
            "🚀 Spark Session ko RAM ji ki tarah powerful banate hain!",
            "💪 Memory allocation - Hanuman ji ki shakti se!",
            "🧠 Driver configuration - Ravana ke dimag se bhi tez!",
            "⚡ Partitions - Vanar Sena ke troops ki tarah organized!",
            "🔮 Legacy policies - ancient wisdom preserved!"
        ]
        
        print("🕉️ JAI SHRI RAM! Divine Spark Initialization begins...")
        for msg in messages:
            print(msg)
        print("✅ Spark Session blessed and ready! Data processing shuru!")

    @staticmethod
    def print_chapter_header(chapter_num: int, title: str, custom_icon: Optional[str] = None):
        """
        Print epic chapter headers with divine styling
        
        Args:
            chapter_num: Chapter number (1-9)
            title: Chapter title
            custom_icon: Optional custom icon, otherwise uses predefined ones
        """
        icon = custom_icon or RamayanaPrinter.ICONS["chapter"][(chapter_num - 1) % len(RamayanaPrinter.ICONS["chapter"])]
        
        print(f"\n{icon * 60}")
        print(f"📊 CHAPTER {chapter_num}: {title}")
        print(f"{icon * 60}")

    @staticmethod
    def print_ramayana_messages(messages: List[str], show_divider: bool = True):
        """
        Print a set of related Ramayana-style messages
        
        Args:
            messages: List of messages to print
            show_divider: Whether to show bottom divider
        """
        for msg in messages:
            print(msg)
        if show_divider:
            print("-" * 50)

    @staticmethod
    def print_success_celebration(achievement: str, character_quote: str, character: str = "hanuman"):
        """
        Print success messages with character quotes
        
        Args:
            achievement: The achievement description
            character_quote: Quote from the character
            character: Character name (ram, hanuman, lakshman, sita)
        """
        success_icon = random.choice(RamayanaPrinter.ICONS["success"])
        celebration_icon = random.choice(RamayanaPrinter.ICONS["celebration"])
        
        print(f"{success_icon} {achievement}")
        print(f"{celebration_icon} {character_quote}")

    @staticmethod
    def print_epic_conclusion():
        """Print the grand finale messages"""
        conclusion_messages = [
            "🏆 RAM RAJYA ESTABLISHED - EPIC SUCCESS!",
            "👑 Data Processing - A Divine Epic Complete!",
            "🏹 Where ancient wisdom meets modern computing",
            "📊 RAM's principles applied to data governance", 
            "⚡ Hanuman's speed in data processing",
            "💪 Lakshman's loyalty in error handling",
            "🎯 Precision like RAM's divine arrows",
            "🙏 'Raghuveer Samartha, Data Processing Samartha!'",
            "🌟 May your data always be as pure as RAM Rajya!"
        ]
        
        print(f"\n{'🌅' * 60}")
        for msg in conclusion_messages:
            print(msg)
        print("=" * 60)

    @staticmethod
    def print_ravana_error(error: Exception, custom_messages: Optional[List[str]] = None):
        """
        Print error messages in epic Ravana demon style
        
        Args:
            error: The exception object
            custom_messages: Optional custom error messages
        """
        default_error_msgs = [
            "🚨 RAVANA STRIKES! DEMON ATTACK ON DATA!",
            f"😈 Ravana's curse: {str(error)}",
            "💀 Ten-headed error demon has attacked our processing!",
            "🏹 But fear not! RAM ji ke devotees haar nahi mante!",
            "🔥 Hanuman Chalisa path karo, bugs bhag jayenge!",
            "💪 'Sankat mochan naam tiharo!' - problem solver!",
            "🛡️ Divine protection activated - retry mechanism!",
            "⚡ 'Jai Bajrang Bali!' - debugging power on!",
            "🙏 RAM ji ki kripa se sab theek ho jayega!"
        ]
        
        error_messages = custom_messages or default_error_msgs
        
        print(f"\n{'👹' * 60}")
        for msg in error_messages:
            print(msg)
        print("👹" * 60)

    @staticmethod
    def print_character_quote(character: str = "hanuman", custom_quote: Optional[str] = None):
        """
        Print a random quote from specified character
        
        Args:
            character: Character name (ram, hanuman, lakshman, sita)
            custom_quote: Optional custom quote to override random selection
        """
        if custom_quote:
            quote = custom_quote
        else:
            char_data = RamayanaPrinter.CHARACTERS.get(character.lower(), RamayanaPrinter.CHARACTERS["hanuman"])
            quote = random.choice(char_data["quotes"])
            
        char_name = RamayanaPrinter.CHARACTERS.get(character.lower(), RamayanaPrinter.CHARACTERS["hanuman"])["name"]
        print(f"{char_name}: '{quote}'")

    @staticmethod
    def print_epic_intro(title: str, subtitle: str = ""):
        """
        Print epic introduction for main programs
        
        Args:
            title: Main title
            subtitle: Optional subtitle
        """
        intro_lines = [
            "🕉️ JAI SHRI RAM! DIGITAL RAMAYANA PRESENTS",
            f"📊 {title.upper()}",
            "👑 RAM RAJYA DATA GOVERNANCE"
        ]
        
        if subtitle:
            intro_lines.append(f"🎭 {subtitle}")
            
        cast_lines = [
            "🎭 Epic Cast:",
            "👑 Apache Spark as Lord RAM (The Perfect Leader)",
            "💪 Data Transformations as Hanuman (The Devoted Servant)",
            "🛡️ Exception Handling as Lakshman (The Protective Brother)", 
            "👸 Clean Schema as Sita Mata (The Pure and Beautiful)",
            "🏹 Query Optimizer as Divine Arrows (Precision Weapons)",
            "🐒 Utility Functions as Vanar Sena (The Loyal Army)",
            "🎵 Background Score: Vedic chants + Server humming"
        ]
        
        print(f"\n{'🏹' * 80}")
        for line in intro_lines:
            print(line)
        print("🏹" * 80)
        for line in cast_lines:
            print(line)
        print("🏹" * 80)

    @staticmethod
    def print_epic_finale(custom_messages: Optional[List[str]] = None):
        """
        Print epic finale celebration
        
        Args:
            custom_messages: Optional custom finale messages
        """
        default_finale = [
            "🌅 Suryoday! Victory sun rises over digital Ayodhya!",
            "🏆 MISSION RAMAYANA ACCOMPLISHED!",
            "🎊 All of Ayodhya celebrates the data victory!",
            "👑 RAM Rajya established in data processing!",
            "🙏 Dhanyawad to all divine forces!",
            "🔔 Temple bells ring in eternal celebration!",
            "🕉️ 'Sab ke data mein RAM ka ashirwad!' - Blessings to all!",
            "🌟 May the force (and RAM) be with your data!"
        ]
        
        finale_messages = custom_messages or default_finale
        
        print(f"\n{'🎆' * 60}")
        for msg in finale_messages:
            print(msg)
        print(f"{'🎆' * 60}")

    @staticmethod
    def print_progress_update(current: int, total: int, task_name: str = "Divine Mission"):
        """
        Print progress updates in epic style
        
        Args:
            current: Current progress count
            total: Total count
            task_name: Name of the task being tracked
        """
        progress_percent = (current / total) * 100
        
        print(f"📊 EPIC PROGRESS: {current}/{total} {task_name.lower()}s complete ({progress_percent:.0f}%)")
        
        # Special milestone celebrations
        if current == total // 2:
            print("🎊 HALFWAY CELEBRATION! Hanuman has crossed the ocean!")
        elif current == int(total * 0.8):
            print("🏰 LANKA IN SIGHT! Final battles approaching!")
        elif current == total:
            print("🏆 ALL MISSIONS COMPLETE! RAM RAJYA ACHIEVED!")

    @staticmethod
    def print_performance_comment(duration_seconds: float):
        """
        Print performance comments based on execution time
        
        Args:
            duration_seconds: Execution time in seconds
        """
        if duration_seconds < 10:
            print("⚡ Hanuman speed! Faster than crossing the ocean!")
        elif duration_seconds < 30:
            print("🏹 RAM's arrow precision! Swift and accurate!")
        elif duration_seconds < 60:
            print("🚶‍♂️ Steady progress like RAM's march to Lanka!")
        elif duration_seconds < 300:
            print("🧘‍♂️ Patience like Sita in Ashok Vatika - good things take time!")
        else:
            print("🐌 Kumbhakarna speed! Time for optimization yagna!")

    @staticmethod
    def print_data_quality_comment(record_count: int, quality_score: Optional[float] = None):
        """
        Print data quality comments based on record count and quality
        
        Args:
            record_count: Number of records processed
            quality_score: Optional quality score (0-1)
        """
        if record_count > 1000000:
            print(f"👑 RAM Rajya scale! {record_count:,} records - empire level data!")
        elif record_count > 100000:
            print(f"🏰 Kingdom scale! {record_count:,} records - royal collection!")
        elif record_count > 10000:
            print(f"🏘️ Village scale! {record_count:,} records - community size!")
        else:
            print(f"🏠 Family scale! {record_count:,} records - intimate gathering!")
            
        if quality_score:
            if quality_score > 0.95:
                print("💎 Sita Mata level purity! Absolutely divine quality!")
            elif quality_score > 0.85:
                print("🌟 RAM ji approved quality! Very good standards!")
            elif quality_score > 0.75:
                print("👍 Hanuman level quality! Solid and reliable!")
            else:
                print("⚠️ Ravana influence detected! Quality needs divine intervention!")

# Convenience functions for quick access
def chapter_header(num: int, title: str, icon: str = None):
    """Quick chapter header function"""
    RamayanaPrinter.print_chapter_header(num, title, icon)

def ramayana_msg(messages: List[str]):
    """Quick messages function"""
    RamayanaPrinter.print_ramayana_messages(messages)

def success_msg(achievement: str, quote: str, character: str = "hanuman"):
    """Quick success function"""
    RamayanaPrinter.print_success_celebration(achievement, quote, character)

def error_msg(error: Exception):
    """Quick error function"""
    RamayanaPrinter.print_ravana_error(error)

def epic_intro(title: str, subtitle: str = ""):
    """Quick epic intro function"""
    RamayanaPrinter.print_epic_intro(title, subtitle)

def epic_finale(messages: List[str] = None):
    """Quick finale function"""
    RamayanaPrinter.print_epic_finale(messages)

def character_quote(character: str = "hanuman", quote: str = None):
    """Quick character quote function"""
    RamayanaPrinter.print_character_quote(character, quote)

def performance_comment(duration: float):
    """Quick performance comment function"""
    RamayanaPrinter.print_performance_comment(duration)

def data_quality_comment(count: int, quality: float = None):
    """Quick data quality comment function"""
    RamayanaPrinter.print_data_quality_comment(count, quality)

def progress_update(current: int, total: int, task: str = "Divine Mission"):
    """Quick progress update function"""
    RamayanaPrinter.print_progress_update(current, total, task)

# Special themed message sets for different data operations
class RamayanaThemes:
    """Pre-defined message themes for common data operations"""
    
    DATA_LOADING = [
        "👑 RAM ji says: 'Pehle apne data count karte hain!'",
        "📋 Like royal census, systematic data gathering...",
        "🎯 Hanuman ji style - ek jump mein file cross!",
        "💭 'Database mein kitne records hain?' - divine counting..."
    ]
    
    DATA_TRANSFORMATION = [
        "⚡ Transformation like RAM's divine arrows - precise and powerful!",
        "🔮 Sage Vishwamitra's alchemy - turning raw data into wisdom!",
        "💎 Each transformation adds divine value!",
        "🌟 Data makeover begins - Sita Mata style beauty!"
    ]
    
    DATA_JOINING = [
        "💒 Sacred data marriage ceremony begins!",
        "🤵👰 Perfect union of datasets - made in heaven!",
        "🌺 Seven pheras of joins - eternal bond!",
        "🔔 Temple bells ring - join operation blessed!"
    ]
    
    DATA_EXPORT = [
        "📁 Divine distribution begins - prasadam for all!",
        "🎊 Victory celebration - data reaches every kingdom!",
        "🕯️ Light the diyas - success illumination!",
        "🎵 'Raghupati Raghav Raja RAM' - export celebration song!"
    ]

# Example usage and testing
if __name__ == "__main__":
    print("🧪 Testing ramayanUtil module...")
    
    # Test various functions
    RamayanaPrinter.print_spark_initialization()
    chapter_header(1, "TEST CHAPTER", "🧪")
    ramayana_msg(RamayanaThemes.DATA_LOADING)
    success_msg("Test successful!", "All functions working perfectly!")
    character_quote("ram", "Testing complete ho gaya!")
    performance_comment(15.5)
    data_quality_comment(50000, 0.92)
    progress_update(3, 5)
    
    print("\n✅ ramayanUtil module ready for epic adventures!")
    print("🙏 JAI SHRI RAM! Module testing complete!")
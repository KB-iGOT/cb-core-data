import sys
from pathlib import Path
import os
import time

# Ensure the parent directory is in sys.path for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[2]))

from constants.ParquetFileConstants import ParquetFileConstants
from constants.QueryConstants import QueryConstants

# ==============================
# 1. Configuration and Constants
# ==============================

def main():


    print(f"""
        ##########################################################
        ###            
        ###             Content
        ### 
        ##########################################################
    """)
   

    print(f"""
        ##########################################################
        ###            
        ###             User
        ### 
        ##########################################################
    """)
    
    print(f"""
        ##########################################################
        ###            
        ###             Enrolment
        ### 
        ##########################################################
    """)
   
    print(f"""
        ##########################################################
        ###            
        ###             ACBP Enrolment
        ### 
        ##########################################################
    """)
    

if __name__ == "__main__":
    main()

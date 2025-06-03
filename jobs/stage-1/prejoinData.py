import sys
from pathlib import Path
from pyspark.sql import SparkSession


# Ensure the parent directory is in sys.path for absolute imports
sys.path.append(str(Path(__file__).resolve().parents[2]))

from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.enrolment.acbp import acbpDFUtil

spark = SparkSession.builder \
    .appName("MySparkApp") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()
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
    userDFUtil.preComputeUser(spark)

    print(f"""
        ##########################################################
        ###            
        ###             Org
        ### 
        ##########################################################
    """)
    userDFUtil.preComputeOrgWithHierarchy(spark)

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
 

    userDFUtil.preComputeOrgHierarchyWithUser(spark)

    acbpDFUtil.preComputeACBPData(spark)
   
    

if __name__ == "__main__":
    main()

from sqlite3 import Date
import sys
from pathlib import Path
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark import SparkConf
from pyspark.sql import SparkSession

# 1. Set up SparkConf with memory settings
conf = SparkConf() \
    .set("spark.executor.memory", "8g") \
    .set("spark.driver.memory", "4g") \
    .setAppName("PandasToSpark")

# 2. Create SparkSession with the config
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# 3. Import internal modules after sys.path setup
sys.path.append(str(Path(__file__).resolve().parents[2]))
from duckutil import duckutil, datautil, schemas
from dfutil.content import contentDFUtil
from dfutil.user import userDFUtil
from dfutil.enrolment.acbp import acbpDFUtil


def main():
    acbpDF = acbpDFUtil.getACBPDetailsDF(spark, schemas.cbplan_draft_data_schema)
    print(acbpDF.count())

    userOrgHierarchyDF = userDFUtil.getUserOrgHierarchy(spark)

    primaryCategories = ["Course", "Program", "Blended Program", "Curated Program", "Standalone Assessment"]
    all_course_program_details_dataframe = contentDFUtil.content_es_dataframe(spark,primaryCategories)
    print(all_course_program_details_dataframe.count())
    selectColumns = [
        "userID", "fullName", "userPrimaryEmail", "userMobile", "designation", "group",
        "userOrgID", "ministry_name", "dept_name", "userOrgName", "userStatus", "acbpID",
        "assignmentType", "completionDueDate", "allocatedOn", "acbpCourseIDList",
        "acbpStatus", "acbpCreatedBy", "cbPlanName"
    ]

    acbpAllotmentDF = acbpDFUtil.exploded_acbp_details(acbpDF, userOrgHierarchyDF, selectColumns)
    print(acbpAllotmentDF.count())

     

   
    #cbplan_warehouse_df = createCbPlanWareHouseDF(exploded_acbp_allotment_df)
    #print(cbplan_warehouse_df.count())

    

    



def createCbPlanWareHouseDF(exploded_acbp_allotment_df):
      # Current timestamp and date formats
    current_datetime = F.current_timestamp()
    date_format_str = "yyyy-MM-dd"
    datetime_format_str = "yyyy-MM-dd HH:mm:ss"

     # Step 2: Prepare data for warehouse
    cbplan_warehouse_df = (
          exploded_acbp_allotment_df
          .withColumn("allotment_to", F.expr("""
               CASE 
                    WHEN assignmentType = 'CustomUser' THEN userID
                    WHEN assignmentType = 'Designation' THEN designation
                    WHEN assignmentType = 'AllUser' THEN 'All Users'
                    ELSE 'No Records'
               END
          """))
          .withColumn("data_last_generated_on", current_datetime)
          .select(
               F.col("userOrgID").alias("org_id"),
               F.col("acbpCreatedBy").alias("created_by"),
               F.col("acbpID").alias("cb_plan_id"),
               F.col("cbPlanName").alias("plan_name"),
               F.col("assignmentType").alias("allotment_type"),
               F.col("allotment_to"),
               F.col("courseID").alias("content_id"),
               F.date_format(F.col("allocatedOn"), datetime_format_str).alias("allocated_on"),
               F.date_format(F.col("completionDueDate"), date_format_str).alias("due_by"),
               F.col("acbpStatus").alias("status"),
               F.col("data_last_generated_on")
          )
          .dropDuplicates()
          .orderBy("org_id", "created_by", "plan_name")
     )
    return cbplan_warehouse_df;


if __name__ == "__main__":
    main()

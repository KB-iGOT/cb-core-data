from pathlib import Path

class ParquetFileConstants:
    # Define the output directory
    OUTPUT_DIR = Path(__file__).resolve().parents[1] / "output"
    OUTPUT_COMPUTED_DIR = Path(__file__).resolve().parents[1] / "output/computed"
    OUTPUT_COMPUTED_DIR.mkdir(parents=True, exist_ok=True)

    # Static constants for each Parquet file
    ACBP_PARQUET_FILE = str(OUTPUT_DIR / "acbp_combined_data.parquet")
    BATCH_PARQUET_FILE = str(OUTPUT_DIR / "batch_combined_data.parquet")
    ENROLMENT_PARQUET_FILE = str(OUTPUT_DIR / "enrolment_combined_data.parquet")
    ESCONTENT_PARQUET_FILE = str(OUTPUT_DIR / "esContent_combined_data.parquet")
    EXTERNAL_COURSE_ENROLMENTS_PARQUET_FILE = str(OUTPUT_DIR / "externalCourseEnrolments_combined_data.parquet")
    HIERARCHY_PARQUET_FILE = str(OUTPUT_DIR / "hierarchy_combined_data.parquet")
    KCMV6_PARQUET_FILE = str(OUTPUT_DIR / "kcmV6_combined_data.parquet")
    LEARNER_LEADERBOARD_PARQUET_FILE = str(OUTPUT_DIR / "learnerLeaderBoard_combined_data.parquet")
    NLW_CONTENT_CERTIFICATE_GENERATED_COUNT_PARQUET_FILE = str(OUTPUT_DIR / "nlwContentCertificateGeneratedCount_combined_data.parquet")
    NLW_CONTENT_LEARNING_HOURS_PARQUET_FILE = str(OUTPUT_DIR / "nlwContentLearningHours_combined_data.parquet")
    ORG_PARQUET_FILE=str(OUTPUT_DIR / "org_combined_data.parquet")
    ORG_COMPLETE_HIERARCHY_PARQUET_FILE = str(OUTPUT_DIR / "orgCompleteHierarchy_combined_data.parquet")
    ORG_HIERARCHY_PARQUET_FILE = str(OUTPUT_DIR / "orgHierarchy_combined_data.parquet")
    RATING_PARQUET_FILE = str(OUTPUT_DIR / "rating_combined_data.parquet")
    RATING_SUMMARY_PARQUET_FILE = str(OUTPUT_DIR / "ratingSummary_combined_data.parquet")
    ROLE_PARQUET_FILE = str(OUTPUT_DIR / "role_combined_data.parquet")
    USER_PARQUET_FILE = str(OUTPUT_DIR / "user_combined_data.parquet")
    USER_KARMA_POINTS_PARQUET_FILE = str(OUTPUT_DIR / "userKarmaPoints_combined_data.parquet")
    USER_KARMA_POINTS_SUMMARY_PARQUET_FILE = str(OUTPUT_DIR / "userKarmaPointsSummary_combined_data.parquet")


    ###
    ###  Computed Parquet File
    ###
    USER_COMPUTED_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "user_computed_data.parquet")
    ACBP_COMPUTED_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "acbp_computed_data.parquet")
    USER_ORG_COMPUTED_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "user_org_computed_data.parquet")
    USER_ORG_ROLE_COMPUTED_PARQUET_FILE = str(OUTPUT_COMPUTED_DIR / "user_org_role_computed_data.parquet")
    COURSE_PROGRAM_ENROLMENT_COMPUTED_FILE = str(OUTPUT_COMPUTED_DIR / "user_course_program_enrolment_computed_data.parquet")
    USER_ORG_COURSE_PROGRAM_ENROLMENT_COMPUTED_FILE = str(OUTPUT_COMPUTED_DIR / "user_org_course_program_enrolment_computed_data.parquet")
    #ACBP_USER_COURSE_PROGRAM_ENROLMENT_COMPUTED_FILE = str(OUTPUT_COMPUTED_DIR / "acbp_user_course_program_enrolment_computed_data.parquet")
# Example Usage:
def main():
    print("Defined Static Parquet File Constants:")

if __name__ == "__main__":
    main()

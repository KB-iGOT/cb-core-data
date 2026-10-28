import findspark
findspark.init()
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel
from pyspark.sql.functions import (
    col, lit, coalesce, when, expr, explode_outer, from_json,
    to_date, date_format, current_timestamp, trim, element_at,
    array_distinct, collect_list, collect_set, count as F_count
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from datetime import datetime
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))
from util import schemas
from jobs.config import get_environment_config
from jobs.default_config import create_config

from constants.ParquetFileConstants import ParquetFileConstants


MANASVI_USER_ID = "9cce1943-84d0-4df5-91a2-678a49244e35"

STATE_NAMES = [
    "ANDAMAN and NICOBAR", "ANDHRA PRADESH", "ARUNACHAL PRADESH", "ASSAM", "BIHAR",
    "CHANDIGARH", "CHHATTISGARH", "Dadra and Nagar Haveli and Daman and Diu", "DELHI",
    "GOA", "GUJARAT", "HARYANA", "HIMACHAL PRADESH", "JAMMU and KASHMIR", "JHARKHAND",
    "KARNATAKA", "KERALA", "LADAKH", "LAKSHADWEEP", "MADHYA PRADESH", "MAHARASHTRA",
    "MANIPUR", "MEGHALAYA", "MIZORAM", "NAGALAND", "ODISHA", "PUDUCHERRY", "PUNJAB",
    "RAJASTHAN", "SIKKIM", "TAMIL NADU", "TELANGANA", "TRIPURA", "UTTAR PRADESH",
    "UTTARAKHAND", "WEST BENGAL",
]
STATE_NAMES_UPPER = [s.upper() for s in STATE_NAMES]

# Which approval-request statuses count as "Sent for Approval" in the funnel.
# Unchanged behaviour (PENDING only). NOTE: a plan only exists once its request
# is approved, so with PENDING-only the later funnel stages are NOT a subset of
# this stage. Use ["PENDING", "APPROVED", "REJECTED"] to make it a true subset.
SENT_FOR_APPROVAL_STATUSES = ["PENDING", "APPROVED", "REJECTED"]

COMPETENCY_LIST_TYPE = "array<struct<area:string,theme:string,sub_theme:string>>"


class AICBPDashboardFactModel:
    """
    ONE date basis for everything: role_mappings.created_at (stored as
    metric_date / event_date). Role mapping is the first funnel stage and every
    later stage is a subset of it, all connected through role_mapping_id:

        role_mapping -> recommendation -> saved -> sent_for_approval
                     -> CBP created (Cassandra plan) -> published (Live)

      - Role Mappings Generated:      role_mappings, own created_at
      - Recommendations Generated:    recommended_courses WHERE role_mapping_id
                                       IN (role_mappings)
      - Saved CBPs:                   cbp_plans WHERE role_mapping_id IN (same set)
      - Sent for Approval:            approval_request_items.source_role_mapping_id
                                       joined to approval_requests, DISTINCT
                                       role_mapping_id, status in
                                       SENT_FOR_APPROVAL_STATUSES
      - CBPs Created / Published:     Cassandra cb_plan_v2 plans, linked to a
                                       role mapping via
                                         mdo_approval.igot_cbp_plan_id = plan_id
                                         mdo_approval.approval_request_item_id
                                           = approval_request_items.id
                                         approval_request_items.source_role_mapping_id
                                           = role_mappings.id
                                       and dated by THAT role mapping's created_at.
                                       Plans with no such link get metric_date NULL
                                       (counted and printed; they fall outside every
                                       date filter).
      - Competency Gap Analysis:      master list = role_mappings.competencies;
                                       covered = recommended_courses.filtered_courses'
                                       embedded competencies, same role_mapping set.

    Tables produced:
      1. aiCbpFact                    — one row per CBP plan; role_mapping_id +
                                         metric_date link it to its role mapping
      2. FunnelStageFact                — one row per (stage, org, date) (Filter B),
                                         stages: role_mapping, recommendation, saved,
                                         sent_for_approval. role_mapping rows also
                                         carry competency_list (master list, same
                                         date basis as everything else here now).
      3. RecommendedCourseCompetencyMap  — one row per (role_mapping_id, course,
                                         competency), from recommended_courses.
                                         filtered_courses (Filter B) — source of
                                         truth for "number of courses recommended"
                                         and the covered side of gap analysis.
      4. SavedCourseMap                   — one row per (role_mapping_id, course),
                                         from cbp_plans.selected_courses (Filter B)
                                         — source for "number of courses saved".
      5. LearnerCoverageMap                 — unified: one row per (event_type,
                                         user_id, org, date). event_type in
                                         {cbp, role_mapping,
                                         recommendation, saved, sent_for_approval},
                                         all dated by the ROLE MAPPING's own
                                         created_at (event_date).
      6. orgDeptUserSummary                  — static reference: one row per
                                         (ministry_id, department_id), user_count.
      7. competencyMasterList                — every distinct (area, theme, sub_theme)
                                         ever seen in role_mappings; no org/date.

    `level` (State / Centre) computed once, on aiCbpFact and FunnelStageFact.
    """

    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.AICBPDashboardFactModel"

    def name(self):
        return "AICBPDashboardFactModel"

    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def current_date_time():
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def contextdata_schema():
        return StructType([
            StructField("accessControl", StructType([
                StructField("userGroups", ArrayType(StructType([
                    StructField("userGroupName", StringType()),
                    StructField("userGroupCriteriaList", ArrayType(StructType([
                        StructField("criteriaKey", StringType()),
                        StructField("criteriaValue", ArrayType(StringType())),
                    ]))),
                ]))),
                StructField("version", IntegerType()),
            ])),
        ])

    @staticmethod
    def course_list_schema():
        """
        Shared schema for both recommended_courses.filtered_courses AND
        cbp_plans.selected_courses. CONFIRMED against a real
        filtered_courses sample row — identical shape to selected_courses.
        """
        return ArrayType(StructType([
            StructField("course", StringType()),
            StructField("identifier", StringType()),
            StructField("duration", StringType()),
            StructField("relevancy", IntegerType()),
            StructField("rationale", StringType()),
            StructField("competencies", ArrayType(StructType([
                StructField("competencyAreaName", StringType()),
                StructField("competencyThemeName", StringType()),
                StructField("competencySubThemeName", StringType()),
            ]))),
            StructField("organisation", ArrayType(StringType())),
        ]))

    @staticmethod
    def role_mapping_competencies_schema():
        return ArrayType(StructType([
            StructField("type", StringType()),
            StructField("theme", StringType()),
            StructField("source", StringType()),
            StructField("sub_theme", StringType()),
        ]))

    def read_cassandra_table(self, keyspace: str, table: str) -> "DataFrame":
        """Read data from Cassandra table"""
        return self.spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=table, keyspace=keyspace) \
            .load()

    def read_postgres_table(self, url: str, table: str, username: str, password: str) -> "DataFrame":
        """Read data from PostgreSQL table.

        `fetchsize` matters: without it the Postgres JDBC driver loads the
        ENTIRE result set into memory at once — fatal for tables with big
        JSON columns (recommended_courses.filtered_courses, cbp_plans.selected_courses).
        With it set, Spark's PostgresDialect turns autocommit off and the
        driver streams rows in batches instead."""
        return self.spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", table) \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .option("fetchsize", "5000") \
            .load()

    # ==================================================================
    # Shared helpers
    # ==================================================================

    @staticmethod
    def normalize_area(area_col):
        return F.when(F.lower(F.trim(area_col)) == "behavioural", F.lit("Behavioral")) \
            .when(F.lower(F.trim(area_col)) == "behavioral", F.lit("Behavioral")) \
            .otherwise(F.initcap(F.trim(area_col)))

    @staticmethod
    def compute_level(ministry_name_col):
        return F.when(
            F.upper(F.trim(ministry_name_col)).isin(STATE_NAMES_UPPER),
            F.lit("State"),
        ).otherwise(F.lit("Centre"))

    @staticmethod
    def compute_org_type(ministry_name_col, level_col):
        """Dashboard 'Type' column: State Govt. / Central Ministry / Central Dept.
        State Govt. comes from `level`. Central is split by NAME: a name starting
        with 'Ministry' is a Central Ministry, everything else Central Dept.
        (org_hierarchy has no explicit type column that we rely on - confirm the
        name rule against the per-type listing printed by the sanity check.)"""
        return F.when(level_col == "State", F.lit("State Govt.")) \
            .when(F.lower(F.trim(ministry_name_col)).rlike(r"^(ministry|ministries)\b"),
                  F.lit("Central Ministry")) \
            .otherwise(F.lit("Central Dept."))

    # ==================================================================
    # PART 1 — Cassandra: aiCbpFact + cbp-slice of LearnerCoverageMap
    # (Filter A — CBP created_date)
    # ==================================================================

    def parse_plans(self, plansDF):
        parsed = (
            plansDF
            .filter(col("plantype") == "AICBP")
            .withColumn("plan_id", col("planid"))
            .withColumn("created_date", to_date(col("createdat")))
            .withColumn("published_date", to_date(col("publishedat")))
            .withColumn("is_published", when(col("status") == "Live", lit(1)).otherwise(lit(0)))
            .withColumn("status_raw", col("status"))
            .withColumn("plan_type", col("plantype"))
            .withColumn("department_id", element_at(col("orgidlist"), 1))
            .withColumn("content_ids", col("contentlist"))
            .withColumn("created_at_ts", col("createdat"))
            .withColumn("context_parsed", from_json(col("contextdata"), self.contextdata_schema()))
        )

        exploded = (
            parsed
            .select("plan_id", explode_outer("context_parsed.accessControl.userGroups").alias("ug"))
            .select("plan_id", explode_outer("ug.userGroupCriteriaList").alias("criteria"))
        )

        designationsDF = (
            exploded
            .filter(col("criteria.criteriaKey") == "designation")
            .select("plan_id", explode_outer("criteria.criteriaValue").alias("designation"))
            .groupBy("plan_id")
            .agg(collect_set("designation").alias("designations"))
        )

        parsed = parsed.join(designationsDF, on="plan_id", how="left")

        return parsed.select(
            "plan_id", "created_date", "published_date", "is_published", "status_raw", "plan_type",
            "department_id", "content_ids", "designations", "created_at_ts",
        )

    def attach_ministry(self, plansDF, orgHierarchyDF):
        orgLookupDF = (
            orgHierarchyDF
            .select(
                trim(col("mdo_id")).alias("department_id"),
                trim(col("mdo_name")).alias("department_name"),
                trim(col("ministry_id")).alias("ministry_id_raw"),
                trim(col("ministry")).alias("ministry_name_raw"),
            )
            .dropDuplicates(["department_id"])
        )

        before = plansDF.count()
        resultDF = plansDF.join(orgLookupDF, on="department_id", how="left")
        after = resultDF.count()
        print(f"[attach_ministry] Before: {before}, After: {after}")
        assert before == after, "Row count changed while attaching ministry_id — check for duplicate mdo_id in org_hierarchy"

        resultDF = resultDF.withColumn(
            "ministry_id",
            F.when(
                (col("ministry_id_raw").isNull()) | (col("ministry_id_raw") == ""),
                col("department_id"),
                ).otherwise(col("ministry_id_raw")),
        ).withColumn(
            "ministry_name",
            F.when(
                (col("ministry_name_raw").isNull()) | (col("ministry_name_raw") == ""),
                col("department_name"),
                ).otherwise(col("ministry_name_raw")),
        ).drop("ministry_id_raw", "ministry_name_raw")

        resultDF = resultDF.withColumn("level", self.compute_level(col("ministry_name")))
        resultDF = resultDF.withColumn("org_type", self.compute_org_type(col("ministry_name"), col("level")))

        unmatchedDF = resultDF.filter(col("department_name").isNull()).select("plan_id", "department_id").distinct()
        unmatchedCount = unmatchedDF.count()
        if unmatchedCount > 0:
            print(f"  WARNING: {unmatchedCount} plans have a department_id not found in org_hierarchy at all")
            unmatchedDF.show(50, truncate=False)

        return resultDF

    def attach_role_mapping(self, plansDF, mdoApprovalDF, approvalRequestItemsDF, leafDF):
        """Link each Cassandra plan to its role mapping (and so to metric_date):
        plan_id = mdo_approval.igot_cbp_plan_id -> approval_request_item_id ->
        approval_request_items.source_role_mapping_id -> role_mappings (leaf).
        Only role mappings present in leafDF count (COMPLETED, Manasvi excluded).
        If a plan somehow links to several role mappings, the earliest is used."""
        mdo = mdoApprovalDF.select(
            F.lower(trim(col("igot_cbp_plan_id").cast("string"))).alias("plan_key"),
            col("approval_request_item_id").cast("string").alias("item_id"),
        ).filter(col("plan_key").isNotNull() & col("item_id").isNotNull())
        items = approvalRequestItemsDF.select(
            col("id").cast("string").alias("item_id"),
            col("source_role_mapping_id").cast("string").alias("role_mapping_id"),
        )
        rm = leafDF.select(col("id").cast("string").alias("role_mapping_id"), "metric_date")
        linkDF = (
            mdo.join(items, on="item_id", how="inner")
            .join(rm, on="role_mapping_id", how="inner")
            .groupBy("plan_key")
            .agg(F.min(F.struct("metric_date", "role_mapping_id")).alias("m"))
            .select("plan_key", col("m.metric_date").alias("metric_date"),
                    col("m.role_mapping_id").alias("role_mapping_id"))
        )
        return (
            plansDF
            .withColumn("plan_key", F.lower(trim(col("plan_id").cast("string"))))
            .join(linkDF, on="plan_key", how="left")
            .drop("plan_key")
        )

    def build_plan_facts(self, plansDF):
        return plansDF.select(
            "plan_id", "role_mapping_id", "metric_date", "created_date", "published_date", "is_published", "status_raw", "plan_type",
            "department_id", "department_name", "ministry_id", "ministry_name", "level", "org_type",
            col("designations").alias("designation_list"), "content_ids",
            "created_at_ts", current_timestamp().alias("computed_at"),
        )

    def build_cbp_learner_slice(self, plansDF, userDetailDF):
        activeUsersDF = userDetailDF.filter(col("status") == 1).select(
            col("user_id"), trim(col("mdo_id")).alias("department_id"), col("designation"),
        )
        planDesignationsDF = (
            plansDF
            .select(
                "plan_id", "department_id", "ministry_id", "metric_date", "is_published",
                explode_outer("designations").alias("designation"),
            )
            .filter(col("metric_date").isNotNull())
        )
        return planDesignationsDF.join(
            activeUsersDF, on=["department_id", "designation"], how="inner",
        ).select(
            "plan_id", "user_id", "department_id", "ministry_id", "metric_date", "is_published",
        ).dropDuplicates(["plan_id", "user_id"])

    # ==================================================================
    # PART 2 — Postgres: everything keyed on role_mappings.created_at
    # (Filter B)
    # ==================================================================

    def leaf_role_mappings(self, roleMappingsDF):
        return (
            roleMappingsDF
            .filter(col("status") == "COMPLETED")
            .filter(col("user_id") != MANASVI_USER_ID)
            .withColumn("ministry_id", trim(col("state_center_id")))
            .withColumn("ministry_name", trim(col("state_center_name")))
            .withColumn(
                "department_id",
                coalesce(
                    F.when(trim(col("department_id")) != "", trim(col("department_id"))),
                    trim(col("state_center_id")),
                )
            )
            .withColumn("level", self.compute_level(col("ministry_name")))
            .withColumn("org_type", self.compute_org_type(col("ministry_name"), col("level")))
            .withColumn("metric_date", to_date(col("created_at")))
            # Designation this role mapping is for (iGOT name preferred), normalised
            # so it can be matched to user_detail.designation -> covered learners.
            .withColumn(
                "designation_key",
                F.lower(trim(coalesce(
                    F.when(trim(col("igot_designation_name")) != "", col("igot_designation_name")),
                    col("designation_name"),
                ))),
            )
            .select("id", "user_id", "ministry_id", "ministry_name", "level", "org_type", "department_id",
                    "metric_date", "designation_key", "competencies")
        )

    def expand_covered_learners(self, rmIdsDF, event_type):
        """COVERED LEARNERS for a funnel stage.

        rmIdsDF: one column `rm_id` = role_mappings.id that reached this stage.
        Each role mapping is for ONE (department, designation). Every ACTIVE user
        in user_detail with that same department (mdo_id) and designation is a
        covered learner. Replaces the old logic that stored the admin who clicked
        (role_mappings / recommended_courses / cbp_plans .user_id).
        One row per (user, org, date) per event_type.
        """
        rmDF = (
            rmIdsDF.distinct()
            .join(
                self.leafDF.select(
                    col("id").alias("rm_id"), "department_id", "ministry_id",
                    col("metric_date").alias("event_date"), "designation_key",
                ),
                on="rm_id", how="inner",
            )
            .filter(col("designation_key").isNotNull() & (col("designation_key") != ""))
        )
        return (
            rmDF.join(self.activeUsersDF, on=["department_id", "designation_key"], how="inner")
            .select("user_id", "department_id", "ministry_id", "event_date")
            .dropDuplicates(["user_id", "department_id", "ministry_id", "event_date"])
            .withColumn("event_type", lit(event_type))
            .withColumn("is_published", lit(None).cast("int"))
            .withColumn("plan_id", lit(None).cast("string"))
        )

    def build_role_mapping_competency_agg(self, leafDF):
        parsedDF = leafDF.withColumn(
            "competencies_parsed", from_json(col("competencies"), self.role_mapping_competencies_schema())
        )
        exploded = (
            parsedDF
            .select("department_id", "ministry_id", "metric_date", explode_outer("competencies_parsed").alias("comp"))
            .filter(col("comp").isNotNull())
        )
        compStruct = F.struct(
            self.normalize_area(col("comp.type")).alias("area"),
            col("comp.theme").alias("theme"),
            col("comp.sub_theme").alias("sub_theme"),
        )
        return (
            exploded
            .withColumn("comp_struct", compStruct)
            .groupBy("department_id", "ministry_id", "metric_date")
            .agg(array_distinct(collect_list("comp_struct")).alias("competency_list"))
        )

    def build_competency_master(self, leafDF):
        """CompetencyMasterList: every DISTINCT (area, theme, sub_theme) that has
        ever appeared in role_mappings.competencies (COMPLETED, Manasvi excluded).
        Not tied to any org or date. Denominator for Course Coverage / Gap Analysis."""
        return (
            leafDF
            .withColumn("competencies_parsed", from_json(col("competencies"), self.role_mapping_competencies_schema()))
            .select(explode_outer("competencies_parsed").alias("comp"))
            .filter(col("comp").isNotNull())
            .select(
                self.normalize_area(col("comp.type")).alias("competency_area"),
                trim(col("comp.theme")).alias("competency_theme"),
                trim(col("comp.sub_theme")).alias("competency_sub_theme"),
            )
            .filter(col("competency_sub_theme").isNotNull() & (col("competency_sub_theme") != ""))
            .dropDuplicates()
        )

    def build_role_mapping_stage(self, leafDF, competencyAggDF):
        stageDF = (
            leafDF
            .groupBy("department_id", "ministry_id", "ministry_name", "level", "org_type", "metric_date")
            .agg(F_count("id").alias("stage_count"))
            .withColumn("stage", lit("role_mapping"))
            .join(competencyAggDF, on=["department_id", "ministry_id", "metric_date"], how="left")
            .withColumn("competency_list", coalesce(col("competency_list"), F.array().cast(COMPETENCY_LIST_TYPE)))
        )
        learnersDF = self.expand_covered_learners(leafDF.select(col("id").alias("rm_id")), "role_mapping")
        return stageDF, learnersDF

    def build_recommendation_stage(self, leafDF, recommendedCoursesDF):
        """
        Date from LEAF (role_mapping's own created_at), NOT
        recommended_courses.created_at — all four Postgres stages share
        one date basis via role_mapping_id. Only status='COMPLETED'
        recommendations count (recommended_courses.status confirmed to
        exist, e.g. 'COMPLETED' vs failed/errored attempts).
        Learner user_id comes from recommended_courses' own user_id column.
        """
        rc = recommendedCoursesDF.filter(
            (col("status") == "COMPLETED") & (col("user_id") != MANASVI_USER_ID)
        )
        joined = leafDF.join(rc, leafDF["id"] == rc["role_mapping_id"], "inner")

        stageDF = (
            joined
            .groupBy(leafDF["department_id"], leafDF["ministry_id"], leafDF["ministry_name"], leafDF["level"], leafDF["org_type"], leafDF["metric_date"])
            .agg(F_count(rc["id"]).alias("stage_count"))
            .withColumn("stage", lit("recommendation"))
            .withColumn("competency_list", F.array().cast(COMPETENCY_LIST_TYPE))
        )
        learnersDF = self.expand_covered_learners(joined.select(leafDF["id"].alias("rm_id")), "recommendation")
        return stageDF, learnersDF

    def build_saved_stage(self, leafDF, cbpPlansDF):
        """
        "All IDs matching the role_mapping_id" — no extra status/column
        filter on cbp_plans. Date from leaf (role_mapping's created_at).
        Learner user_id from cbp_plans' own user_id column.
        """
        cp = cbpPlansDF.filter(col("user_id") != MANASVI_USER_ID)
        joined = leafDF.join(cp, leafDF["id"] == cp["role_mapping_id"], "inner")

        stageDF = (
            joined
            .groupBy(leafDF["department_id"], leafDF["ministry_id"], leafDF["ministry_name"], leafDF["level"], leafDF["org_type"], leafDF["metric_date"])
            .agg(F_count(cp["id"]).alias("stage_count"))
            .withColumn("stage", lit("saved"))
            .withColumn("competency_list", F.array().cast(COMPETENCY_LIST_TYPE))
        )
        learnersDF = self.expand_covered_learners(joined.select(leafDF["id"].alias("rm_id")), "saved")
        return stageDF, learnersDF

    def build_sent_for_approval_stage(self, leafDF, approvalRequestItemsDF, approvalRequestsDF):
        """
        Counts DISTINCT role_mapping_id (leaf.id) whose approval request is
        currently PENDING — not raw item/request row counts, for consistency
        with how role_mappings_generated counts role mapping rows. Date
        from leaf (role_mapping's own created_at), not approval_requests'.
        """
        ar = approvalRequestsDF.filter(col("user_id") != MANASVI_USER_ID).filter(col("status").isin(SENT_FOR_APPROVAL_STATUSES))
        joined = (
            leafDF
            .join(approvalRequestItemsDF, leafDF["id"] == approvalRequestItemsDF["source_role_mapping_id"], "inner")
            .join(ar, approvalRequestItemsDF["approval_request_id"] == ar["id"], "inner")
        )

        stageDF = (
            joined
            .groupBy(leafDF["department_id"], leafDF["ministry_id"], leafDF["ministry_name"], leafDF["level"], leafDF["org_type"], leafDF["metric_date"])
            .agg(F.countDistinct(leafDF["id"]).alias("stage_count"))
            .withColumn("stage", lit("sent_for_approval"))
            .withColumn("competency_list", F.array().cast(COMPETENCY_LIST_TYPE))
        )
        learnersDF = self.expand_covered_learners(joined.select(leafDF["id"].alias("rm_id")), "sent_for_approval")
        return stageDF, learnersDF

    def build_recommended_course_competency_map(self, leafDF, recommendedCoursesDF):
        """
        Source of truth for "number of courses recommended" and the
        COVERED side of gap analysis. Exploded from
        recommended_courses.filtered_courses — schema CONFIRMED against a
        real sample row: identifier + competencies array with
        competencyAreaName/competencyThemeName/competencySubThemeName.
        Only status='COMPLETED' rows count, same as build_recommendation_stage.
        """
        rc = recommendedCoursesDF.filter(
            (col("status") == "COMPLETED") & (col("user_id") != MANASVI_USER_ID)
        )
        rcParsedDF = rc.withColumn(
            "filtered_courses_parsed", from_json(col("filtered_courses"), self.course_list_schema())
        )
        joined = leafDF.join(rcParsedDF, leafDF["id"] == rcParsedDF["role_mapping_id"], "inner")

        exploded = (
            joined
            .select(
                leafDF["id"].alias("role_mapping_id"), leafDF["department_id"], leafDF["ministry_id"],
                leafDF["metric_date"],
                explode_outer("filtered_courses_parsed").alias("course"),
            )
            .select(
                "role_mapping_id", "department_id", "ministry_id", "metric_date",
                col("course.identifier").alias("course_id"),
                explode_outer("course.competencies").alias("comp"),
            )
            .withColumn("competency_area", self.normalize_area(col("comp.competencyAreaName")))
            .withColumn("competency_theme", col("comp.competencyThemeName"))
            .withColumn("competency_sub_theme", col("comp.competencySubThemeName"))
            .select("role_mapping_id", "department_id", "ministry_id", "metric_date",
                    "course_id", "competency_area", "competency_theme", "competency_sub_theme")
            .dropDuplicates()
        )
        return exploded

    def build_saved_course_map(self, leafDF, cbpPlansDF):
        """Source for 'number of courses saved' — course IDs only, from
        cbp_plans.selected_courses, no competency breakdown (gap analysis
        uses the recommended side, not saved, per current requirement)."""
        cp = cbpPlansDF.filter(col("user_id") != MANASVI_USER_ID)
        cpParsedDF = cp.withColumn(
            "selected_courses_parsed", from_json(col("selected_courses"), self.course_list_schema())
        )
        joined = leafDF.join(cpParsedDF, leafDF["id"] == cpParsedDF["role_mapping_id"], "inner")

        exploded = (
            joined
            .select(
                leafDF["id"].alias("role_mapping_id"), leafDF["department_id"], leafDF["ministry_id"],
                leafDF["metric_date"],
                explode_outer("selected_courses_parsed").alias("course"),
            )
            .select(
                "role_mapping_id", "department_id", "ministry_id", "metric_date",
                col("course.identifier").alias("course_id"),
            )
            .dropDuplicates()
        )
        return exploded

    # ==================================================================
    # PART 3 — orgDeptUserSummary
    # ==================================================================

    def build_org_dept_user_summary(self, orgHierarchyDF, userDetailDF):
        orgDF = (
            orgHierarchyDF
            .select(
                trim(col("mdo_id")).alias("department_id"),
                trim(col("mdo_name")).alias("department_name"),
                trim(col("ministry_id")).alias("ministry_id_raw"),
                trim(col("ministry")).alias("ministry_name_raw"),
            )
            .dropDuplicates(["department_id"])
            .withColumn(
                "ministry_id",
                F.when(
                    (col("ministry_id_raw").isNull()) | (col("ministry_id_raw") == ""),
                    col("department_id"),
                    ).otherwise(col("ministry_id_raw")),
            )
            .withColumn(
                "ministry_name",
                F.when(
                    (col("ministry_name_raw").isNull()) | (col("ministry_name_raw") == ""),
                    col("department_name"),
                    ).otherwise(col("ministry_name_raw")),
            )
            .withColumn("level", self.compute_level(col("ministry_name")))
            .withColumn("org_type", self.compute_org_type(col("ministry_name"), col("level")))
            .drop("ministry_id_raw", "ministry_name_raw")
        )
        userCountsDF = (
            userDetailDF
            .filter(col("status") == 1)
            .groupBy(trim(col("mdo_id")).alias("department_id"))
            .agg(F.countDistinct("user_id").alias("user_count"))
        )
        resultDF = (
            orgDF
            .join(userCountsDF, on="department_id", how="left")
            .withColumn("user_count", coalesce(col("user_count"), lit(0)))
        )
        return resultDF.select(
            "ministry_id", "ministry_name", "level", "org_type",
            "department_id", "department_name", "user_count",
        )

    # ==================================================================
    # Sanity checks
    # ==================================================================

    def sanity_check_plan_facts(self, df):
        print("\n--- aiCbpFact sanity checks ---")
        print(f"Total rows: {df.count()}")
        df.groupBy("status_raw").agg(F_count(lit(1)).alias("cnt")).show(truncate=False)
        df.groupBy("level").agg(F_count(lit(1)).alias("cnt")).show(truncate=False)
        print(f"is_published = 1 rows: {df.filter(col('is_published') == 1).count()}")
        dupDF = df.groupBy("plan_id").agg(F_count(lit(1)).alias("cnt")).filter(col("cnt") > 1)
        print(f"Duplicate plan_id rows: {dupDF.count()}")
        total = df.count()
        linked = df.filter(col("metric_date").isNotNull()).count()
        print(f"Plans linked to a role mapping (metric_date set): {linked} of {total}; UNLINKED: {total - linked}")
        print(f"Linked plans with is_published = 1: {df.filter(col('metric_date').isNotNull() & (col('is_published') == 1)).count()}")
        print(f"Null department_id: {df.filter(col('department_id').isNull()).count()}")
        print(f"Null ministry_id: {df.filter(col('ministry_id').isNull()).count()}")
        df.printSchema()
        df.show(5, truncate=False)

    def sanity_check_funnel_stage_facts(self, df):
        print("\n--- FunnelStageFact sanity checks ---")
        print(f"Total rows: {df.count()}")
        df.groupBy("stage").agg(
            F_count(lit(1)).alias("rows"), F.sum("stage_count").alias("total_count"),
        ).show(truncate=False)
        df.groupBy("level").agg(F_count(lit(1)).alias("cnt")).show(truncate=False)
        roleRows = df.filter(col("stage") == "role_mapping")
        withCompCount = roleRows.filter(F.size(col("competency_list")) > 0).count()
        print(f"role_mapping rows with a non-empty competency_list: {withCompCount} of {roleRows.count()}")
        dupDF = (
            df.groupBy("stage", "department_id", "ministry_id", "metric_date")
            .agg(F_count(lit(1)).alias("cnt")).filter(col("cnt") > 1)
        )
        print(f"Duplicate (stage, department_id, ministry_id, metric_date) rows: {dupDF.count()}")
        df.printSchema()
        df.show(10, truncate=False)

    def sanity_check_learner_coverage_map(self, df):
        print("\n--- LearnerCoverageMap sanity checks ---")
        print(f"Total rows: {df.count()}")
        df.groupBy("event_type").agg(
            F_count(lit(1)).alias("rows"), F.countDistinct("user_id").alias("distinct_users"),
        ).show(truncate=False)
        df.printSchema()
        df.show(10, truncate=False)

    def sanity_check_course_map(self, df, name):
        print(f"\n--- {name} sanity checks ---")
        print(f"Total rows: {df.count()}")
        print(f"Distinct course_ids: {df.select('course_id').distinct().count()}")
        df.printSchema()
        df.show(10, truncate=False)

    def sanity_check_org_dept_user_summary(self, df):
        print("\n--- orgDeptUserSummary sanity checks ---")
        print(f"Total rows: {df.count()}")
        print(f"Distinct ministries: {df.select('ministry_id').distinct().count()}")
        print(f"Departments with 0 active users: {df.filter(col('user_count') == 0).count()}")
        print("Ministries per level (expect State ~36 names, rest Centre):")
        df.select("ministry_id", "ministry_name", "level").distinct() \
            .groupBy("level").agg(F_count(lit(1)).alias("ministries")).show(truncate=False)
        print("Ministries per org_type (Central Ministry is decided by name starting with 'Ministry'):")
        df.select("ministry_id", "ministry_name", "org_type").distinct() \
            .groupBy("org_type").agg(F_count(lit(1)).alias("ministries")).show(truncate=False)
        print("Sample Central Dept. names (check none of these should be a Ministry):")
        df.filter(col("org_type") == "Central Dept.").select("ministry_name").distinct().show(40, truncate=False)
        df.printSchema()
        df.show(10, truncate=False)

    # ==================================================================
    # Write helper
    # ==================================================================

    def write_parquet_and_reload(self, df, path, num_files):
        """
        Writes df as flat snappy parquet (num_files part-files, no partition
        subfolders), then RE-READS what was written and returns that.

        Why: sanity checks used to run BEFORE the write, so every count()/
        show() re-executed the whole (often huge) lineage — the exploded
        course map and the learner union were being computed 3-4 times per
        table. Now the lineage runs exactly once (in the write), and all
        sanity checks read the cheap, already-materialized parquet.
        """
        df.repartition(num_files).write.mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(path)
        return self.spark.read.parquet(path)

    # ==================================================================
    # Main processing entry point
    # ==================================================================

    def process_data(self, spark, config):
        self.spark = spark
        # State/Centre list comes from config (comma-separated 'nlwStatesList');
        # the hardcoded STATE_NAMES above is only the fallback.
        global STATE_NAMES_UPPER
        cfgStates = getattr(config, "nlwStatesList", None)
        if cfgStates:
            STATE_NAMES_UPPER = [s.strip().upper() for s in str(cfgStates).split(",") if s.strip()]
        print(f"State list in use ({len(STATE_NAMES_UPPER)} names, "
              f"{'config nlwStatesList' if cfgStates else 'hardcoded fallback'})")
        try:
            currentDateTime = date_format(current_timestamp(), ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT)
            aiCBPPostgresUrl =  f"jdbc:postgresql://{config.aiCBPPostgresHost}/{config.aiCBPPostgresSchema}"

            print("=" * 60)
            print("PART 1 — Cassandra: read and parse CBP plans (written after role mappings are read)")
            print("=" * 60)

            print("Reading CBP plan data from Cassandra...")
            cbpPlansRawDF = self.read_cassandra_table(config.cassandraUserKeyspace, "cb_plan_v2")
            print(f"  -> {cbpPlansRawDF.count()} rows")

            print("Reading org_hierarchy...")
            orgHierarchyDF = spark.read.parquet("/home/analytics/pyspark/warehouse/org_hierarchy") \
                .persist(StorageLevel.MEMORY_AND_DISK)
            print(f"  -> {orgHierarchyDF.count()} rows")

            print("Reading user_detail (only the 4 columns actually used)...")
            userDetailDF = spark.read.parquet("/home/analytics/pyspark/warehouse/user_detail") \
                .select("user_id", "mdo_id", "designation", "status") \
                .persist(StorageLevel.MEMORY_AND_DISK)
            print(f"  -> {userDetailDF.count()} rows")

            plansDF = self.parse_plans(cbpPlansRawDF)
            plansDF = self.attach_ministry(plansDF, orgHierarchyDF).persist(StorageLevel.MEMORY_AND_DISK)

            print("\n" + "=" * 60)
            print("PART 2 — Postgres pipeline (role_mappings.created_at drives everything)")
            print("=" * 60)

            print("Reading role_mappings from Postgres...")
            roleMappingsDF = self.read_postgres_table(
                aiCBPPostgresUrl, "role_mappings", config.aiCBPPostgresUsername, config.aiCBPPostgresCredential,
            )
            print(f"  -> {roleMappingsDF.count()} rows")

            print("Reading recommended_courses from Postgres (column-pruned — excludes embedding/vector_query/actual_courses)...")
            recommendedCoursesDF = self.read_postgres_table(
                aiCBPPostgresUrl,
                "(SELECT id, user_id, role_mapping_id, status, filtered_courses, created_at FROM recommended_courses) AS rc",
                config.aiCBPPostgresUsername, config.aiCBPPostgresCredential,
            ).repartition(32)  # JDBC read is 1 partition; spread JSON parse/explode across tasks
            print(f"  -> {recommendedCoursesDF.count()} rows")

            print("Reading cbp_plans from Postgres...")
            cbpPlansPostgresDF = self.read_postgres_table(
                aiCBPPostgresUrl, "cbp_plans", config.aiCBPPostgresUsername, config.aiCBPPostgresCredential,
            ).repartition(16)
            print(f"  -> {cbpPlansPostgresDF.count()} rows")

            print("Reading approval_request_items from Postgres...")
            approvalRequestItemsDF = self.read_postgres_table(
                aiCBPPostgresUrl, "approval_request_items", config.aiCBPPostgresUsername, config.aiCBPPostgresCredential,
            )
            print(f"  -> {approvalRequestItemsDF.count()} rows")

            print("Reading approval_requests from Postgres...")
            approvalRequestsDF = self.read_postgres_table(
                aiCBPPostgresUrl, "approval_requests", config.aiCBPPostgresUsername, config.aiCBPPostgresCredential,
            )
            print(f"  -> {approvalRequestsDF.count()} rows")

            print("Reading mdo_approval from Postgres (plan <-> approval item bridge)...")
            mdoApprovalDF = self.read_postgres_table(
                aiCBPPostgresUrl,
                "(SELECT approval_request_item_id, igot_cbp_plan_id FROM mdo_approval) AS ma",
                config.aiCBPPostgresUsername, config.aiCBPPostgresCredential,
            )
            print(f"  -> {mdoApprovalDF.count()} rows")

            leafDF = self.leaf_role_mappings(roleMappingsDF).persist(StorageLevel.MEMORY_AND_DISK)
            self.leafDF = leafDF

            # Active users keyed by (department, normalised designation) for covered-learner expansion
            self.activeUsersDF = (
                userDetailDF.filter(col("status") == 1)
                .select(
                    col("user_id"),
                    trim(col("mdo_id")).alias("department_id"),
                    F.lower(trim(col("designation"))).alias("designation_key"),
                )
                .filter(col("designation_key").isNotNull() & (col("designation_key") != ""))
                .dropDuplicates(["user_id", "department_id", "designation_key"])
                .persist(StorageLevel.MEMORY_AND_DISK)
            )

            # Match-rate check: how many role mappings find at least one learner
            rmKeys = leafDF.select("department_id", "designation_key").distinct()
            matchedKeys = rmKeys.join(
                self.activeUsersDF.select("department_id", "designation_key").distinct(),
                on=["department_id", "designation_key"], how="inner",
            ).count()
            print(f"[covered learners] (department, designation) pairs in role mappings: {rmKeys.count()}; "
                  f"with >=1 active user: {matchedKeys}")

            print("\n" + "=" * 60)
            print("PART 2a — aiCbpFact + CBP learner slice (plans linked to role mappings)")
            print("=" * 60)
            plansDF = self.attach_role_mapping(plansDF, mdoApprovalDF, approvalRequestItemsDF, leafDF) \
                .persist(StorageLevel.MEMORY_AND_DISK)
            planFactsDF = self.build_plan_facts(plansDF).withColumn("data_last_generated_on", currentDateTime)
            cbpLearnerSliceDF = self.build_cbp_learner_slice(plansDF, userDetailDF)

            print("Writing aiCbpFact to warehouse...")
            planFactsWritten = self.write_parquet_and_reload(
                planFactsDF, f"{config.warehouseReportDir}/aiCBPFactTable", 8
            )
            self.sanity_check_plan_facts(planFactsWritten)

            competencyAggDF = self.build_role_mapping_competency_agg(leafDF)

            roleMappingStageDF, roleMappingLearnersDF = self.build_role_mapping_stage(leafDF, competencyAggDF)
            recommendationStageDF, recommendationLearnersDF = self.build_recommendation_stage(leafDF, recommendedCoursesDF)
            savedStageDF, savedLearnersDF = self.build_saved_stage(leafDF, cbpPlansPostgresDF)
            sentForApprovalStageDF, sentForApprovalLearnersDF = self.build_sent_for_approval_stage(
                leafDF, approvalRequestItemsDF, approvalRequestsDF
            )

            funnelStageFactsDF = (
                roleMappingStageDF
                .unionByName(recommendationStageDF)
                .unionByName(savedStageDF)
                .unionByName(sentForApprovalStageDF)
                .withColumn("computed_at", current_timestamp())
                .withColumn("data_last_generated_on", currentDateTime)
                .select(
                    "stage", "department_id", "ministry_id", "ministry_name", "level", "org_type", "metric_date",
                    "stage_count", "competency_list", "computed_at", "data_last_generated_on",
                )
            )

            print("Writing FunnelStageFact to warehouse...")
            funnelWritten = self.write_parquet_and_reload(
                funnelStageFactsDF, f"{config.warehouseReportDir}/funnelStageFactsTable", 8
            )
            self.sanity_check_funnel_stage_facts(funnelWritten)

            print("\n" + "=" * 60)
            print("PART 3 — RecommendedCourseCompetencyMap, SavedCourseMap")
            print("=" * 60)

            recommendedCourseCompetencyMapDF = self.build_recommended_course_competency_map(leafDF, recommendedCoursesDF) \
                .withColumn("computed_at", current_timestamp()) \
                .withColumn("data_last_generated_on", currentDateTime)

            savedCourseMapDF = self.build_saved_course_map(leafDF, cbpPlansPostgresDF) \
                .withColumn("computed_at", current_timestamp()) \
                .withColumn("data_last_generated_on", currentDateTime)

            print("Writing RecommendedCourseCompetencyMap to warehouse...")
            recommendedWritten = self.write_parquet_and_reload(
                recommendedCourseCompetencyMapDF,
                f"{config.warehouseReportDir}/recommendedCourseCompetencyMapTable", 16,
            )
            self.sanity_check_course_map(recommendedWritten, "RecommendedCourseCompetencyMap")

            print("Writing SavedCourseMap to warehouse...")
            savedWritten = self.write_parquet_and_reload(
                savedCourseMapDF, f"{config.warehouseReportDir}/savedCourseMapTable", 8
            )
            self.sanity_check_course_map(savedWritten, "SavedCourseMap")

            print("\n" + "=" * 60)
            print("PART 3b — CompetencyMasterList (all distinct competencies from role_mappings)")
            print("=" * 60)
            competencyMasterDF = self.build_competency_master(leafDF) \
                .withColumn("computed_at", current_timestamp())
            print("Writing CompetencyMasterList to warehouse...")
            competencyMasterWritten = self.write_parquet_and_reload(
                competencyMasterDF, f"{config.warehouseReportDir}/competencyMasterListTable", 1
            )
            print(f"CompetencyMasterList rows: {competencyMasterWritten.count()}")
            competencyMasterWritten.groupBy("competency_area").agg(F_count(lit(1)).alias("cnt")).show(truncate=False)
            competencyMasterWritten.show(10, truncate=False)

            print("\n" + "=" * 60)
            print("PART 4 — Unified LearnerCoverageMap")
            print("=" * 60)

            cbpLearnersUnifiedDF = (
                cbpLearnerSliceDF
                .select(
                    "user_id", "department_id", "ministry_id",
                    col("metric_date").alias("event_date"), "is_published", "plan_id",
                )
                .withColumn("event_type", lit("cbp"))
            )

            learnerCoverageMapDF = (
                cbpLearnersUnifiedDF
                .unionByName(roleMappingLearnersDF.select(cbpLearnersUnifiedDF.columns))
                .unionByName(recommendationLearnersDF.select(cbpLearnersUnifiedDF.columns))
                .unionByName(savedLearnersDF.select(cbpLearnersUnifiedDF.columns))
                .unionByName(sentForApprovalLearnersDF.select(cbpLearnersUnifiedDF.columns))
                .withColumn("computed_at", current_timestamp())
                .withColumn("data_last_generated_on", currentDateTime)
            )

            print("Writing LearnerCoverageMap to warehouse...")
            learnerWritten = self.write_parquet_and_reload(
                learnerCoverageMapDF, f"{config.warehouseReportDir}/learnerCoverageMapTable", 16
            )
            self.sanity_check_learner_coverage_map(learnerWritten)

            print("\n" + "=" * 60)
            print("PART 5 — orgDeptUserSummary")
            print("=" * 60)

            orgDeptUserSummaryDF = self.build_org_dept_user_summary(orgHierarchyDF, userDetailDF) \
                .withColumn("computed_at", current_timestamp())

            print("Writing orgDeptUserSummary to warehouse...")
            orgSummaryWritten = self.write_parquet_and_reload(
                orgDeptUserSummaryDF, f"{config.warehouseReportDir}/orgDeptUserSummaryTable", 4
            )
            self.sanity_check_org_dept_user_summary(orgSummaryWritten)

            print("\n✅ AICBPDashboardFactModel processing complete — 6 tables written: "
                  "aiCbpFact, FunnelStageFact, RecommendedCourseCompetencyMap, SavedCourseMap, "
                  "LearnerCoverageMap, orgDeptUserSummary, CompetencyMasterList.")

        except Exception as e:
            print(f"❌ Error occurred during AICBPDashboardFactModel processing: {str(e)}")
            raise e


def main():
    # Packages only. Do NOT put --driver-memory here: a flag in PYSPARK_SUBMIT_ARGS
    # would override the builder's spark.driver.memory below.
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,org.postgresql:postgresql:42.6.0 pyspark-shell'
    config_dict = get_environment_config()
    config = create_config(config_dict)

    # Sizing mirrors the UserEnrolmentModel job (local mode, 256 GB box).
    # In local mode everything runs inside ONE JVM, so spark.driver.memory is the
    # only memory setting that matters; spark.executor.* / memoryOverhead /
    # *.memoryFraction are no-ops here and are intentionally left out.
    spark = SparkSession.builder \
        .appName("AI CBP Dashboard Fact Model") \
        .config("spark.master", "local[16]") \
        .config("spark.sql.shuffle.partitions", "240") \
        .config("spark.driver.memory", "128g") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.debug.maxToStringFields", "200") \
        .config("spark.cassandra.connection.host", config.sparkCassandraConnectionHost) \
        .config("spark.cassandra.connection.port", '9042') \
        .getOrCreate()

    # Confirms the heap the JVM actually got (should print ~128 GB).
    max_heap_gb = spark._jvm.java.lang.Runtime.getRuntime().maxMemory() / (1024 ** 3)
    print(f"[INFO] JVM max heap available to Spark: {max_heap_gb:.1f} GB "
          f"(spark.master = {spark.sparkContext.master})")

    start_time = datetime.now()
    print(f"[START] AICBPDashboardFactModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    model = AICBPDashboardFactModel()
    model.process_data(spark, config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] AICBPDashboardFactModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()


if __name__ == "__main__":
    main()

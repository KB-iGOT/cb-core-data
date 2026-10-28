import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[3]))
from util import schemas
from constants.ParquetFileConstants import ParquetFileConstants
import psutil
import os
import shutil
import time
import duckdb
from dfutil.user.userDFUtil import exportDFToParquet
from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    struct, explode, col, from_json, when, expr, concat_ws, array_join, lit, lower, trim, split, array_position, size
)
from pyspark.sql import functions as F
from pyspark import StorageLevel


IS_ON_CENTRAL_DEPUTATION_EXPR = """
    CASE
        WHEN isOnCentralDeputation IS NULL
          OR TRIM(CAST(isOnCentralDeputation AS VARCHAR)) = ''
        THEN 'false'
        ELSE LOWER(TRIM(CAST(isOnCentralDeputation AS VARCHAR)))
    END AS is_on_central_deputation_lower
"""

STANDARD_CRITERIA_TYPES = (
    "'rootorgid','alluser','user','customuser',"
    "'designation','cadre','group','batch',"
    "'service','isoncentraldeputation',"
    "'ministryorstateid'"
)


def preComputeACBPData(spark):
    print("=" * 80)
    print("ACBP Pre-Processing - Version 6.0 (single-pass DuckDB)")
    print("=" * 80)

    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

    acbp_df = spark.read.parquet(ParquetFileConstants.ACBP_PARQUET_FILE)
    user_group_info = spark.read.parquet(ParquetFileConstants.USER_GROUP_INFO_PARQUET_FILE)

    usergroup_df = (user_group_info
                    .filter(F.lower(F.trim(col("status"))) == "active")
                    .withColumn("ug_criteria", expr("""
             flatten(
               transform(
                 filter(criteria, m -> m IS NOT NULL),
                 m -> transform(map_keys(m), k ->
                        named_struct('criteriaKey', k, 'criteriaValue', m[k]))
               )
             )
         """))
                    .select(F.trim(col("usergroupid")).alias("ug_id"),
                            col("orgid").alias("ug_orgid"),
                            col("ug_criteria"))
                    .dropDuplicates(["ug_id"]))

    acbp_df = acbp_df.withColumn("contextdata",
                                 F.regexp_replace(col("contextdata"),
                                                  '"criteriaValue":((?!\\[)(true|false|"[^"]*"|[0-9]+(?:\\.[0-9]+)?))',
                                                  '"criteriaValue":[$1]'
                                                  )
                                 )

    acbp_select_df = (acbp_df
                      .withColumn("context_data", from_json(col("contextdata"), schemas.accessControlSchema))
                      .withColumn("userGroup", explode(col("context_data.accessControl.userGroups")))
                      .join(F.broadcast(usergroup_df), F.trim(col("userGroup.userGroupId")) == col("ug_id"), "left")
                      .withColumn("criteriaList",
                                  F.when(F.size(col("userGroup.userGroupCriteriaList")) > 0,
                                         col("userGroup.userGroupCriteriaList"))
                                  .otherwise(col("ug_criteria")))
                      .withColumn("criteria_keys", expr("transform(criteriaList, x -> lower(x.criteriaKey))"))
                      .withColumn("json_criteria_values", expr("transform(criteriaList, x -> to_json(x.criteriaValue))"))
                      .withColumn("assignmentType", array_join(col("criteria_keys"), "|"))
                      .withColumn("assignmentTypeInfo", array_join(col("json_criteria_values"), "|"))
                      .withColumn("userOrgID", F.coalesce(
        expr("element_at(filter(criteriaList, x -> lower(x.criteriaKey) = 'rootorgid')[0].criteriaValue, 1)"),
        col("ug_orgid")))
                      # json_criteria_values: array of JSON-serialized arrays, used for assignmentTypeInfo
                      # preserves multi-value entries like ["deputy director (research, statistics and analysis)","deputy director"]
                      # so DuckDB can unnest them without comma-splitting ambiguity
                      .select(
        col("planid").alias("acbpID"),
        col("userOrgID").alias("orgID"),
        col("draftdata"),
        col("status").alias("acbpStatus"),
        col("createdby").alias("acbpCreatedBy"),
        col("isapar"),
        col("name").alias("cbPlanName"),
        col("enddate").cast("string").alias("completionDueDate"),
        col("publishedat").cast("string").alias("allocatedOn"),
        col("contentlist").alias("acbpCourseIDList"),
        col("assignmentType"),
        col("assignmentTypeInfo"),
        col("planyear"),
        col("plantype"),
        col("calinkedid")
    )
                      .na.fill({"cbPlanName": ""})
                      )

    # PERF: cache once — previously every .count() below re-ran from_json + explode + join from scratch
    acbp_select_df = acbp_select_df.persist(StorageLevel.MEMORY_AND_DISK)

    acbp_select_count = acbp_select_df.count()
    print(f"  ACBP select data: {acbp_select_count:,} rows")

    draft_cbp_data = (acbp_select_df
                      .filter((col("acbpStatus") == "draft") & col("draftdata").isNotNull())
                      .select("acbpID", "orgID", "draftdata", "acbpStatus", "acbpCreatedBy", "isapar", "planyear", "plantype", "calinkedid")
                      .withColumn("draftData", from_json(col("draftdata"), schemas.cbplan_draft_data_schema))
                      .withColumn("cbPlanName", col("draftData.name"))
                      .withColumn("assignmentType", col("draftData.assignmentType"))
                      .withColumn("assignmentTypeInfo", array_join(col("draftData.assignmentTypeInfo"), ","))
                      .withColumn("completionDueDate", col("draftData.endDate").cast("string"))
                      .withColumn("allocatedOn", lit(None).cast("string"))
                      .withColumn("acbpCourseIDList", col("draftData.contentList"))
                      .drop("draftData"))

    draft_count = draft_cbp_data.count()
    print(f"  Draft CBP data: {draft_count:,} rows")

    non_draft_cbp_data = acbp_select_df.filter(col("acbpStatus") != "draft")
    non_draft_count = non_draft_cbp_data.count()
    print(f"  Non-draft CBP data: {non_draft_count:,} rows")

    draft_cbp_data = draft_cbp_data.withColumn("draftdata", lit(None).cast("string"))

    final_df = non_draft_cbp_data.unionByName(draft_cbp_data).persist(StorageLevel.MEMORY_AND_DISK)
    total_plans = final_df.count()
    print(f"  Total plans (final_df): {total_plans:,} rows")

    exportDFToParquet(final_df, ParquetFileConstants.ACBP_SELECT_FILE)

    live_acbp_df = final_df.filter(col("acbpStatus") == "Live")
    live_count = live_acbp_df.count()
    print(f"  Live plans for processing: {live_count:,} rows")
    user_extended_profile_df = spark.read.parquet("/home/analytics/pyspark/data-res/pq_files/cache_pq/userExtendedProfile")
    explodeAcbpData(spark, live_acbp_df, user_extended_profile_df=user_extended_profile_df)

    final_df.unpersist()
    acbp_select_df.unpersist()


def get_safe_duck_budget():
    free_gb = psutil.virtual_memory().available / (1024 ** 3)
    budget = max(8, int(free_gb * 0.60))
    print(f"  Free RAM: {free_gb:.1f}GB → DuckDB budget: {budget}GB")
    return f"{budget}GB"


def flattenCustomFields(spark, user_extended_profile_df, temp_dir):
    print("  Flattening user extended profile custom fields...")

    custom_field_schema = ArrayType(StructType([
        StructField("organisationId", StringType(), True),
        StructField("customFieldValues", ArrayType(StructType([
            StructField("customFieldId", StringType(), True),
            StructField("attributeName", StringType(), True),
            StructField("type", StringType(), True),
            StructField("values", ArrayType(StructType([
                StructField("attributeName", StringType(), True),
                StructField("value", StringType(), True),
                StructField("level", StringType(), True)
            ])), True)
        ])), True)
    ]))

    org_props_df = user_extended_profile_df.filter(
        col("contexttype") == "orgAdditionalProperties"
    )

    flattened = (
        org_props_df
        .withColumn("parsed",       from_json(col("contextdata"), custom_field_schema))
        .withColumn("org_entry",    explode(col("parsed")))
        .withColumn("custom_field", explode(col("org_entry.customFieldValues")))
        .withColumn("field_value",  explode(col("custom_field.values")))
        .select(
            lower(col("userid")).alias("userid_lower"),          # same as old LOWER(ucf.userid), done once here
            col("org_entry.organisationId").alias("orgID"),
            lower(trim(
                F.regexp_replace(col("custom_field.attributeName"), "_", " ")
            )).alias("custom_field_name"),
            lower(trim(col("field_value.value"))).alias("custom_field_value")
        )
        .filter(
            col("custom_field_name").isNotNull()  & (F.length(col("custom_field_name"))  > 0) &
            col("custom_field_value").isNotNull() & (F.length(col("custom_field_value")) > 0)
        )
        .dropDuplicates()                                        # matching is set-based; dups only cost time
    )

    custom_fields_path = f"{temp_dir}/user_custom_fields.parquet"
    flattened.write.mode("overwrite").parquet(custom_fields_path)

    # PERF: count the written file instead of re-running the JSON parse
    cf_count = spark.read.parquet(custom_fields_path).count()
    print(f"  Custom field rows flattened: {cf_count:,}")
    print(f"  Written to: {custom_fields_path}")

    return custom_fields_path


def explodeAcbpData(spark, acbp_df: DataFrame, user_extended_profile_df: DataFrame = None):
    """
    Single-pass user ↔ plan allocation.

    A user qualifies for a (plan, criteria group) when they match EVERY criteria type in the group.
    Same rule as before, computed differently:
      * rootorgid is applied as a FILTER on the other criteria instead of emitting one match row per
        (org user × plan). It is only expanded for groups whose sole criterion is rootorgid.
      * all attribute criteria are matched with ONE hash join against an unpivoted user-attribute set,
        instead of ~11 separate joins per user chunk.
      * org-based and global (no orgID) plans go through the same query.
      * no chunking / checkpoints — DuckDB spills to disk itself.
    """
    print("\n" + "=" * 80)
    print("ACBP User Allocation - Version 6.0 (single-pass DuckDB)")
    print("=" * 80)

    start_time = time.time()

    project_dir = str(Path(__file__).resolve().parents[3])
    temp_dir = f"{project_dir}/temp_acbp_duckdb"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)

    # ------------------------------------------------------------------ 1. Spark: plan criteria
    print(f"\n[1/5] Parsing ACBP assignment types (Spark)...")
    print(f"  Temp directory: {temp_dir}")

    acbp_expanded = acbp_df \
        .withColumn("criteria_types_arr", split(lower(col("assignmentType")), "\\|")) \
        .withColumn("criteria_values_arr", split(col("assignmentTypeInfo"), "\\|")) \
        .withColumn("criteria_idx", F.expr("sequence(0, size(criteria_types_arr) - 1)")) \
        .withColumn("criteria_exploded", F.explode(col("criteria_idx"))) \
        .withColumn("criteria_type", F.expr("criteria_types_arr[criteria_exploded]")) \
        .withColumn("criteria_value", F.expr("criteria_values_arr[criteria_exploded]")) \
        .withColumn("criteria_value_lower", lower(trim(col("criteria_value")))) \
        .select(
        "acbpID", "orgID", "acbpStatus", "acbpCreatedBy", "isapar", "cbPlanName",
        "completionDueDate", "allocatedOn", "acbpCourseIDList",
        "assignmentType", "assignmentTypeInfo",
        "planyear", "plantype",
        "criteria_type", "criteria_value", "criteria_value_lower"
    )

    acbp_criteria_path = f"{temp_dir}/acbp_criteria.parquet"
    acbp_expanded.write.mode("overwrite").parquet(acbp_criteria_path)

    has_custom_fields = user_extended_profile_df is not None
    custom_fields_path = None
    if has_custom_fields:
        custom_fields_path = flattenCustomFields(spark, user_extended_profile_df, temp_dir)
    else:
        print("  No user_extended_profile_df provided — skipping custom field matching")

    user_data_path = ParquetFileConstants.USER_ORG_COMPUTED_FILE
    output_path    = ParquetFileConstants.ACBP_COMPUTED_FILE

    # ------------------------------------------------------------------ 2. DuckDB setup
    print(f"\n[2/5] Initializing DuckDB (in-memory, spills to disk)...")

    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    os.makedirs(output_path, exist_ok=True)
    output_file = f"{output_path}/part-00000.parquet"

    spill_dir = f"{temp_dir}/spill"
    os.makedirs(spill_dir, exist_ok=True)

    # PERF: in-memory DB (old version wrote every intermediate table into a .duckdb file on disk)
    con = duckdb.connect()
    con.execute(f"SET temp_directory='{spill_dir}'")
    con.execute(f"SET memory_limit='{get_safe_duck_budget()}'")
    con.execute(f"SET threads={os.cpu_count() or 4}")
    con.execute("SET preserve_insertion_order=false")

    # ------------------------------------------------------------------ 3. Lookup tables
    print("\n[3/5] Building criteria / plan / user tables...")
    t = time.time()

    # criteria_value_lower is a JSON array string like
    # '["deputy director (research, statistics and analysis)","deputy director"]'
    con.execute(f"""
        CREATE TABLE criteria_exploded AS
        SELECT acbpID, orgID, criteria_type,
               LOWER(TRIM(v)) AS criteria_value,
               criteria_group_id
        FROM (
            SELECT acbpID, orgID, criteria_type,
                   UNNEST(json_extract_string(criteria_value_lower, '$[*]')::VARCHAR[]) AS v,
                   MD5(assignmentType || '|' || assignmentTypeInfo)                    AS criteria_group_id
            FROM read_parquet('{acbp_criteria_path}/*.parquet')
        )
        WHERE LENGTH(TRIM(v)) > 0
    """)

    # Per group: number of NON-rootorgid types that must match + whether rootorgid restricts the group.
    # (Old logic: matched_types = total_types incl. rootorgid. Equivalent, since rootorgid is now a filter.)
    con.execute("""
                CREATE TABLE group_req AS
                SELECT acbpID, criteria_group_id,
                       COUNT(DISTINCT criteria_type) FILTER (WHERE criteria_type <> 'rootorgid') AS required_types,
                    BOOL_OR(criteria_type = 'rootorgid')                                       AS has_rootorg
                FROM criteria_exploded
                GROUP BY acbpID, criteria_group_id
                """)

    con.execute("""
                CREATE TABLE group_org AS
                SELECT DISTINCT acbpID, criteria_group_id, criteria_value AS org_value
                FROM criteria_exploded
                WHERE criteria_type = 'rootorgid'
                """)

    # join_type: old code compared ministryOrStateId RAW for org plans and LOWER() for global plans.
    # Kept exactly: org plans join on 'ministryorstateid' (raw user value),
    # global plans join on 'ministryorstateid#lc' (lowercased user value).
    # org_value: the group's rootorgid (NULL if the group has none). One row per allowed org.
    # PERF: the org is part of the JOIN KEY in step 4, so common values (e.g. isoncentraldeputation=false,
    # popular designations) only match users of the right org instead of all 18M users.
    con.execute("""
                CREATE TABLE attr_criteria AS
                SELECT DISTINCT
                    c.acbpID, c.criteria_group_id, c.criteria_type, c.criteria_value, c.join_type,
                    o.org_value
                FROM (
                         SELECT
                             acbpID, criteria_group_id, criteria_type, criteria_value,
                             CASE
                                 WHEN criteria_type = 'ministryorstateid' AND (orgID IS NULL OR orgID = '')
                                     THEN 'ministryorstateid#lc'
                                 ELSE criteria_type
                                 END AS join_type
                         FROM criteria_exploded
                         WHERE criteria_type NOT IN ('rootorgid', 'alluser')
                     ) c
                         LEFT JOIN group_org o
                                   ON o.acbpID = c.acbpID
                                       AND o.criteria_group_id = c.criteria_group_id
                """)

    con.execute("""
                CREATE TABLE alluser_groups AS
                SELECT DISTINCT acbpID, criteria_group_id, orgID
                FROM criteria_exploded
                WHERE criteria_type = 'alluser' AND orgID IS NOT NULL AND orgID <> ''
                """)

    # PERF: ONE row per plan. Old plan_info had one row per user-group of the plan, joined on acbpID
    # only, which multiplied output rows and then needed a huge DISTINCT to collapse them.
    con.execute(f"""
        CREATE TABLE plan_meta AS
        SELECT acbpID,
               any_value(isapar)            AS isapar,
               any_value(completionDueDate) AS completionDueDate,
               any_value(allocatedOn)       AS allocatedOn,
               any_value(acbpCourseIDList)  AS acbpCourseIDList,
               any_value(acbpStatus)        AS acbpStatus,
               any_value(acbpCreatedBy)     AS acbpCreatedBy,
               any_value(cbPlanName)        AS cbPlanName,
               any_value(planyear)          AS planyear,
               any_value(plantype)          AS plantype
        FROM read_parquet('{acbp_criteria_path}/*.parquet')
        GROUP BY acbpID
    """)

    # PERF: assignmentTypeInfo formatting done ONCE per group (old code did json_extract per output row).
    # Each pipe-segment is a JSON array like ["val1","val2"] -> "val1", "val2"
    con.execute(f"""
        CREATE TABLE group_info AS
        SELECT DISTINCT
            acbpID,
            MD5(assignmentType || '|' || assignmentTypeInfo) AS criteria_group_id,
            assignmentType,
            array_to_string(
                list_transform(
                    string_split(assignmentTypeInfo, '|'),
                    seg -> array_to_string(
                        list_transform(
                            json_extract_string(seg, '$[*]')::VARCHAR[],
                            v -> '"' || v || '"'
                        ),
                        ', '
                    )
                ),
                '|'
            ) AS assignmentTypeInfo
        FROM read_parquet('{acbp_criteria_path}/*.parquet')
    """)

    # Users read ONCE (old code re-read the user parquet in every chunk / every join)
    con.execute(f"""
        CREATE TABLE users_base AS
        SELECT DISTINCT
            userID, fullName, userPrimaryEmail, userMobile,
            designation, "group", userOrgID, ministryOrStateId,
            ministry_name, dept_name, userOrgName,
            cadreName, civilServiceType, civilServiceName,
            cadreBatch, organised_service, userStatus,
            employmentDetails.employeeCode                     AS employeeCode,
            LOWER(TRIM(COALESCE(designation, '')))             AS designation_lower,
            LOWER(TRIM(COALESCE("group", '')))                 AS group_lower,
            LOWER(TRIM(COALESCE(cadreName, '')))               AS cadre_lower,
            LOWER(TRIM(COALESCE(civilServiceName, '')))        AS service_lower,
            LOWER(TRIM(COALESCE(cadreBatch, '')))              AS batch_lower,
            {IS_ON_CENTRAL_DEPUTATION_EXPR}
        FROM read_parquet('{user_data_path}/*.parquet')
    """)

    crit_rows  = con.execute("SELECT COUNT(*) FROM criteria_exploded").fetchone()[0]
    group_rows = con.execute("SELECT COUNT(*) FROM group_req").fetchone()[0]
    user_count = con.execute("SELECT COUNT(*) FROM users_base").fetchone()[0]
    print(f"  Criteria rows:   {crit_rows:,}")
    print(f"  Criteria groups: {group_rows:,}")
    print(f"  Users:           {user_count:,}")
    print(f"  Step time: {time.time() - t:.1f}s")

    # ------------------------------------------------------------------ 4. Single-pass matching
    print("\n[4/5] Matching users to criteria (single pass)...")
    t = time.time()

    custom_attrs_sql = ""
    if has_custom_fields:
        custom_attrs_sql = f"""
            UNION ALL
            SELECT u.userID, u.userOrgID,
                   ucf.custom_field_name  AS attr_type,
                   ucf.custom_field_value AS attr_value
            FROM read_parquet('{custom_fields_path}/*.parquet') ucf
            JOIN users_base u
              ON LOWER(u.userID) = ucf.userid_lower
             AND u.userOrgID     = ucf.orgID
            WHERE ucf.custom_field_name NOT IN ({STANDARD_CRITERIA_TYPES})
        """

    con.execute(f"""
        CREATE TABLE qualified_pairs AS
        WITH user_attrs AS (
            -- every user unpivoted to (attr_type, attr_value): one hash join replaces ~11 queries
            SELECT userID, userOrgID, attr_type, attr_value
            FROM (
                SELECT userID, userOrgID,
                       UNNEST(['user', 'customuser', 'designation', 'cadre', 'group',
                               'batch', 'service', 'isoncentraldeputation',
                               'ministryorstateid', 'ministryorstateid#lc'])            AS attr_type,
                       UNNEST([LOWER(userID), LOWER(userID), designation_lower, cadre_lower, group_lower,
                               batch_lower, service_lower, is_on_central_deputation_lower,
                               CAST(ministryOrStateId AS VARCHAR),
                               LOWER(CAST(ministryOrStateId AS VARCHAR))])             AS attr_value
                FROM users_base
            )
            WHERE attr_value IS NOT NULL AND attr_value <> ''
            {custom_attrs_sql}
        ),
        org_filtered AS (
            -- groups WITH rootorgid: org is part of the join key -> only that org's users are touched
            SELECT ua.userID, c.acbpID, c.criteria_group_id, c.criteria_type AS matched_type
            FROM user_attrs ua
            JOIN (SELECT * FROM attr_criteria WHERE org_value IS NOT NULL) c
              ON c.join_type      = ua.attr_type
             AND c.criteria_value = ua.attr_value
             AND c.org_value      = ua.userOrgID

            UNION ALL

            -- groups WITHOUT rootorgid: match on type + value only
            SELECT ua.userID, c.acbpID, c.criteria_group_id, c.criteria_type AS matched_type
            FROM user_attrs ua
            JOIN (SELECT * FROM attr_criteria WHERE org_value IS NULL) c
              ON c.join_type      = ua.attr_type
             AND c.criteria_value = ua.attr_value

            UNION ALL

            -- alluser (still restricted by rootorgid when the group has one)
            SELECT u.userID, g.acbpID, g.criteria_group_id, 'alluser' AS matched_type
            FROM users_base u
            JOIN alluser_groups g ON u.userOrgID = g.orgID
            JOIN group_req r
              ON r.acbpID = g.acbpID AND r.criteria_group_id = g.criteria_group_id
            LEFT JOIN group_org o
              ON o.acbpID = g.acbpID AND o.criteria_group_id = g.criteria_group_id
             AND o.org_value = u.userOrgID
            WHERE NOT r.has_rootorg OR o.org_value IS NOT NULL
        ),
        with_req AS (
            SELECT m.userID, m.acbpID, m.criteria_group_id, m.matched_type, r.required_types
            FROM org_filtered m
            JOIN group_req r
              ON r.acbpID = m.acbpID
             AND r.criteria_group_id = m.criteria_group_id
        )
        SELECT userID, acbpID, criteria_group_id
        FROM with_req
        GROUP BY userID, acbpID, criteria_group_id, required_types
        HAVING COUNT(DISTINCT matched_type) = required_types

        UNION ALL

        -- groups whose ONLY criterion is rootorgid -> every user of that org
        SELECT DISTINCT u.userID, r.acbpID, r.criteria_group_id
        FROM group_req r
        JOIN group_org o
          ON o.acbpID = r.acbpID
         AND o.criteria_group_id = r.criteria_group_id
        JOIN users_base u ON u.userOrgID = o.org_value
        WHERE r.required_types = 0
    """)

    qp_count = con.execute("SELECT COUNT(*) FROM qualified_pairs").fetchone()[0]
    print(f"  Qualified (user, plan, group) pairs: {qp_count:,}")
    print(f"  Step time: {time.time() - t:.1f}s")

    # ------------------------------------------------------------------ 5. Final output
    print("\n[5/5] Writing final output...")
    t = time.time()

    # No DISTINCT needed: qualified_pairs is unique per (user, plan, group), users_base is distinct,
    # plan_meta has one row per plan, group_info one row per (plan, group).
    con.execute(f"""
        COPY (
            SELECT
                u.userID, u.fullName, u.userPrimaryEmail, u.userMobile,
                u.designation, u."group", u.userOrgID,
                u.ministry_name, u.dept_name, u.userOrgName,
                u.cadreName, u.civilServiceType, u.civilServiceName,
                u.cadreBatch, u.organised_service, u.userStatus, u.employeeCode,
                p.isapar, p.acbpID,
                gi.assignmentType,
                gi.assignmentTypeInfo,
                p.completionDueDate, p.allocatedOn, p.acbpCourseIDList,
                p.acbpStatus, p.acbpCreatedBy, p.cbPlanName,
                p.planyear, p.plantype
            FROM qualified_pairs qp
            JOIN users_base u
              ON u.userID = qp.userID
            JOIN plan_meta p
              ON p.acbpID = qp.acbpID
            JOIN group_info gi
              ON gi.acbpID = qp.acbpID
             AND gi.criteria_group_id = qp.criteria_group_id
        ) TO '{output_file}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)
    """)
    print(f"  Step time: {time.time() - t:.1f}s")

    count_result, unique_users, unique_plans = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT userID), COUNT(DISTINCT acbpID)
        FROM read_parquet('{output_file}')
    """).fetchone()

    elapsed_time = time.time() - start_time

    print("\n" + "=" * 80)
    print("PROCESSING COMPLETE!")
    print("=" * 80)
    print(f"Total time:              {elapsed_time:.1f}s ({elapsed_time / 60:.1f} min)")
    print(f"Total user-plan matches: {count_result:,}")
    print(f"Unique users matched:    {unique_users:,}")
    print(f"Unique plans matched:    {unique_plans:,}")
    print(f"Processing rate:         {count_result / max(elapsed_time, 1):,.0f} matches/second")
    print(f"Output location:         {output_path}")
    print("=" * 80 + "\n")

    print("Cleaning up temporary files...")
    con.close()

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    print("DuckDB-based ACBP explosion complete!")


def cast_ntz_to_string_recursively(schema, prefix=""):
    fields = []
    for field in schema.schema.fields:
        full_name = f"{prefix}.{field.name}" if prefix else field.name
        if isinstance(field.dataType, TimestampNTZType):
            fields.append(col(full_name).cast("string").alias(field.name))
        elif isinstance(field.dataType, StructType):
            nested_cols = cast_ntz_to_string_recursively(field.dataType, prefix=full_name)
            fields.append(struct(*nested_cols).alias(field.name))
        elif isinstance(field.dataType, ArrayType):
            elemType = field.dataType.elementType
            if isinstance(elemType, TimestampNTZType):
                fields.append(expr(f"transform({full_name}, x -> CAST(x AS STRING))").alias(field.name))
            elif isinstance(elemType, StructType):
                nested_cols = cast_ntz_to_string_recursively(elemType, prefix="x")
                struct_expr = f"struct({', '.join([f'x.{c.name} as {c.name}' for c in elemType.fields])})"
                fields.append(expr(f"transform({full_name}, x -> {struct_expr})").alias(field.name))
            else:
                fields.append(col(full_name).alias(field.name))
        else:
            fields.append(col(full_name).alias(field.name))
    return fields


def drop_all_ntz_fields(df: DataFrame) -> DataFrame:
    return df.drop("completionDueDate", "allocatedOn")


def cast_ntz_to_string(df):
    new_cols = cast_ntz_to_string_recursively(df.schema)
    return df.select(*new_cols)


def print_nested_schema(df, prefix=""):
    for field in df.schema.fields:
        dt = field.dataType
        name = prefix + field.name
        if isinstance(dt, StructType):
            print_nested_schema(df.select(f"{name}.*"), prefix=name + ".")
        else:
            print(f"{name}: {dt}")

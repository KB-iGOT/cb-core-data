#!/usr/bin/env python3
"""
org_hierarchy_job.py

Consolidated replacement for the three scripts that currently run in
sequence in production:

    stage-2/org_hierarchy_export/org_hierarchy_all.py
    stage-2/org_hierarchy_export/org_hierarchy_updated_empty.py
    stage-2/org_hierarchy_export/postgres_to_parquet.py

Run with no arguments:

    python3 org_hierarchy_job.py

What it writes
---------------
  Stage A (org_hierarchy_all.py logic):
      -> Postgres tables org_hierarchy_lookup and mdo_children_lookup.
      -> Built from the PLAIN Elasticsearch org set: NO hardcoded-ID
         merge, NO empty-framework fix. (unchanged from today)

  Stage B (org_hierarchy_updated_empty.py logic):
      -> Parquet file only, at
         /home/analytics/pyspark/warehouse/org_hierarchy_new/part-00000.snappy.parquet
      -> Built from ES query + hardcoded-ID merge + empty-framework fix.
      -> Postgres org_hierarchy_new is NOT written (per instruction:
         Postgres write for this table is not required, Parquet only).
      -> Rows are written to Parquet directly from the same in-memory
         data used to build them - no round trip back through Postgres
         (the original postgres_to_parquet.py re-SELECTed the table it
         had just written, which is redundant once everything runs in
         one process).

Known, preserved discrepancy (matches current production, not "fixed"
here): org_hierarchy_new (now Parquet) and org_hierarchy_lookup /
mdo_children_lookup (Postgres) are built from two different org sets.
Orgs with an empty framework, and the 2 hardcoded org IDs, will appear
in the org_hierarchy_new Parquet output but not in the lookup/children
Postgres tables.
"""

import datetime
import logging
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import psycopg2
import psycopg2.extras
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from elasticsearch import Elasticsearch
from elasticsearch.connection import RequestsHttpConnection

sys.path.append(str(Path(__file__).resolve().parents[2]))
from jobs.config import get_environment_config
from jobs.default_config import create_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger("org_hierarchy_job")

LEVEL_NAMES = [
    "LevelOne", "LevelTwo", "LevelThree", "LevelFour", "LevelFive",
    "LevelSix", "LevelSeven", "LevelEight", "LevelNine", "LevelTen",
]

# Merged into the org set for Stage B (org_hierarchy_new / Parquet) ONLY.
# Stage A (lookup / children tables) does not include these.
HARDCODED_ORG_IDS = ["0133783095823810560", "0136855852269322243827"]

ES_SOURCE_FIELDS = [
    "identifier", "orgName", "orgHierarchyFrameworkId",
    "ministryorstatetype", "createdDate", "sbOrgType", "updatedDate",
]

ES_INDEX_NAME = "org_v4"
REQUEST_DELAY_SECONDS = 1  # matches original scripts' time.sleep(1)

PARQUET_OUTPUT_DIR = "/home/analytics/pyspark/warehouse/org_hierarchy_new"

MAIN_QUERY = {
    "query": {
        "bool": {
            "must": [
                {"terms": {"sbOrgType": ["state", "ministry"]}},
                {"exists": {"field": "orgHierarchyFrameworkId"}},
            ]
        }
    },
    "_source": ES_SOURCE_FIELDS,
}

HARDCODED_QUERY = {
    "query": {
        "bool": {
            "must": [
                {"terms": {"identifier.raw": HARDCODED_ORG_IDS}},
                {"exists": {"field": "orgHierarchyFrameworkId"}},
            ]
        }
    },
    "_source": ES_SOURCE_FIELDS,
}

# Column order for the org_hierarchy_new Parquet output - matches the
# columns org_hierarchy_new (Postgres) was created with in the originals.
HIERARCHY_COLUMNS = ["created_date", "updated_date"]
for _i in range(len(LEVEL_NAMES)):
    HIERARCHY_COLUMNS.extend([f"l{_i + 1}_id", f"l{_i + 1}_name"])
HIERARCHY_COLUMNS.extend(["center_state_id", "center_state_name"])


class OrgHierarchyJob:
    def __init__(self):
        config_dict = get_environment_config()
        self.config = create_config(config_dict)

        self.es: Optional[Elasticsearch] = None
        self.pg_conn: Optional[psycopg2.extensions.connection] = None
        self.pg_cursor: Optional[psycopg2.extensions.cursor] = None

    # ------------------------------------------------------------------
    # Setup / teardown
    # ------------------------------------------------------------------
    def connect(self) -> None:
        self.es = Elasticsearch(
            hosts=[{
                "host": self.config.sparkElasticsearchConnectionHost,
                "port": int(self.config.sparkElasticsearchConnectionPort),
            }],
            headers={"Content-Type": "application/json"},
            connection_class=RequestsHttpConnection,
        )

        host, port = self.config.appPostgresHost.split(":")
        self.pg_conn = psycopg2.connect(
            dbname=self.config.appPostgresSchema,
            user=self.config.appPostgresUsername,
            password=self.config.appPostgresCredential,
            host=host,
            port=port,
        )
        self.pg_cursor = self.pg_conn.cursor()
        logger.info("Connected to Elasticsearch and PostgreSQL.")

    def close(self) -> None:
        if self.pg_cursor:
            self.pg_cursor.close()
        if self.pg_conn:
            self.pg_conn.close()
        logger.info("Connections closed.")

    # ------------------------------------------------------------------
    # Elasticsearch fetch (shared scroll helper, two distinct org sets)
    # ------------------------------------------------------------------
    def _scroll_search(self, query_body: dict) -> pd.DataFrame:
        scroll = "2m"
        query_body = dict(query_body)
        query_body["size"] = 1000

        results = []
        page = self.es.search(index=ES_INDEX_NAME, body=query_body, scroll=scroll)
        sid = page["_scroll_id"]
        scroll_size = len(page["hits"]["hits"])
        results.extend(page["hits"]["hits"])

        while scroll_size > 0:
            page = self.es.scroll(scroll_id=sid, scroll=scroll)
            sid = page["_scroll_id"]
            scroll_size = len(page["hits"]["hits"])
            results.extend(page["hits"]["hits"])

        if not results:
            return pd.DataFrame(columns=ES_SOURCE_FIELDS)
        return pd.DataFrame([r["_source"] for r in results])

    def get_stage_a_orgs(self) -> pd.DataFrame:
        """Plain org set for org_hierarchy_lookup / mdo_children_lookup.
        Matches org_hierarchy_all.py exactly: no hardcoded-ID merge."""
        df = self._scroll_search(MAIN_QUERY)
        logger.info("[Stage A] Fetched %d source orgs (no hardcoded merge).", len(df))
        return df

    def get_stage_b_orgs(self) -> pd.DataFrame:
        """Org set for org_hierarchy_new (Parquet). Matches
        org_hierarchy_updated_empty.py exactly: main query + hardcoded-ID
        merge, deduplicated."""
        df_main = self._scroll_search(MAIN_QUERY)
        df_hard = self._scroll_search(HARDCODED_QUERY)

        if df_main.empty and df_hard.empty:
            logger.info("[Stage B] No records fetched from Elasticsearch.")
            return pd.DataFrame(columns=ES_SOURCE_FIELDS)

        df = pd.concat([df_main, df_hard], ignore_index=True)
        df = df.drop_duplicates(subset=["identifier"], keep="first").reset_index(drop=True)
        logger.info("[Stage B] Fetched %d unique source orgs (%d main + %d hardcoded, before dedup).",
                    len(df), len(df_main), len(df_hard))
        return df

    # ------------------------------------------------------------------
    # Framework parsing
    # ------------------------------------------------------------------
    @staticmethod
    def parse_framework_data(data: dict) -> List[Dict[str, Tuple[Optional[str], Optional[str]]]]:
        """Flatten a framework's categories into L1..L10 hierarchy dicts.
        Empty-framework fix: an org with no LevelOne terms still gets a
        single all-None row instead of being dropped. (org_hierarchy_updated_empty.py behavior)"""
        categories = data.get("result", {}).get("framework", {}).get("categories", [])

        level_term_lookup: Dict[str, dict] = {}
        for cat in categories:
            code = cat.get("code")
            terms = cat.get("terms", [])
            level_term_lookup[code] = {t["identifier"]: t for t in terms}

        results: List[Dict[str, Tuple[Optional[str], Optional[str]]]] = []

        def walk(term, level_idx, hierarchy):
            if level_idx >= len(LEVEL_NAMES):
                for lvl in LEVEL_NAMES:
                    hierarchy.setdefault(lvl, (None, None))
                results.append(hierarchy.copy())
                return

            level = LEVEL_NAMES[level_idx]
            org_id = term.get("additionalProperties", {}).get("orgId")
            name = term.get("name")
            hierarchy[level] = (org_id, name)

            associations = term.get("associations", [])
            if associations:
                next_level = LEVEL_NAMES[level_idx + 1] if level_idx + 1 < len(LEVEL_NAMES) else None
                for assoc in associations:
                    assoc_term = assoc
                    if next_level and "identifier" in assoc and next_level in level_term_lookup:
                        assoc_term = level_term_lookup[next_level].get(assoc["identifier"], assoc)
                    walk(assoc_term, level_idx + 1, hierarchy.copy())
            else:
                for idx in range(level_idx + 1, len(LEVEL_NAMES)):
                    hierarchy[LEVEL_NAMES[idx]] = (None, None)
                results.append(hierarchy.copy())

        level_one_terms = []
        for cat in categories:
            if cat.get("code") == "LevelOne":
                level_one_terms = cat.get("terms", [])
                break

        if not level_one_terms:
            results.append({lvl: (None, None) for lvl in LEVEL_NAMES})
        else:
            for term in level_one_terms:
                walk(term, 0, {})

        for h in results:
            for lvl in LEVEL_NAMES:
                h.setdefault(lvl, (None, None))

        return results

    @staticmethod
    def build_flat_descendant_map(categories: list) -> Dict[str, List[str]]:
        """{org_id: [all descendant org_ids, any depth]} - matches
        org_hierarchy_all.py's build_flat_descendant_map exactly."""
        id_to_term: Dict[str, dict] = {}
        orgid_to_term: Dict[str, dict] = {}
        for cat in categories:
            for t in cat.get("terms", []):
                id_to_term[t["identifier"]] = t
                org_id = t.get("additionalProperties", {}).get("orgId")
                if org_id:
                    orgid_to_term[org_id] = t

        def collect(term) -> List[str]:
            descendants = []
            for assoc in term.get("associations", []):
                assoc_org_id = assoc.get("additionalProperties", {}).get("orgId")
                assoc_term = id_to_term.get(assoc.get("identifier"))
                if assoc_org_id:
                    descendants.append(assoc_org_id)
                if assoc_term:
                    descendants.extend(collect(assoc_term))
            return descendants

        flat_map = {}
        for org_id, term in orgid_to_term.items():
            flat_map[org_id] = list(dict.fromkeys(collect(term)))  # dedup, keep order
        return flat_map

    @staticmethod
    def parse_pg_timestamp(ts) -> Optional[datetime.datetime]:
        if ts is None or (isinstance(ts, float) and pd.isna(ts)):
            return None
        try:
            if ts.count(":") == 3:
                base, tz = ts.rsplit("+", 1)
                base = base[::-1].replace(":", ".", 1)[::-1]
                ts_fixed = f"{base}+{tz}"
            else:
                ts_fixed = ts
            return datetime.datetime.strptime(ts_fixed, "%Y-%m-%d %H:%M:%S.%f%z")
        except Exception as e:
            logger.warning("Could not parse timestamp %r: %s", ts, e)
            return None

    # ------------------------------------------------------------------
    # Batched Postgres writes (Stage A only)
    # ------------------------------------------------------------------
    def insert_lookup_rows(self, rows: List[tuple]) -> None:
        if not rows:
            return
        sql = """
              INSERT INTO org_hierarchy_lookup (
                  mdo_name, mdo_id, mdo_level,
                  department_id, department_name, department_level,
                  center_state_id, center_state_name
              ) VALUES %s \
              """
        psycopg2.extras.execute_values(self.pg_cursor, sql, rows, page_size=500)

    def insert_children_rows(self, rows: List[tuple]) -> None:
        """rows: list of (mdo_id, children_ids_list)"""
        if not rows:
            return
        formatted = [
            (mdo_id, ",".join(str(c) for c in children if c) if children else None)
            for mdo_id, children in rows
        ]
        sql = """
              INSERT INTO mdo_children_lookup (mdo_id, children_id)
              VALUES %s
                  ON CONFLICT (mdo_id) DO UPDATE SET children_id = EXCLUDED.children_id \
              """
        psycopg2.extras.execute_values(self.pg_cursor, sql, formatted, page_size=500)

    def build_lookup_rows(self, org_id: str, org_name: str, categories: list) -> List[tuple]:
        level_term_lookup: Dict[str, dict] = {}
        for cat in categories:
            code = cat.get("code")
            level_term_lookup[code] = {t["identifier"]: t for t in cat.get("terms", [])}

        rows = []
        for idx, level in enumerate(LEVEL_NAMES):
            if level not in level_term_lookup:
                continue
            for term_id, term in level_term_lookup[level].items():
                mdo_id = term.get("additionalProperties", {}).get("orgId")
                mdo_name = term.get("name")

                department_id = department_name = department_level = None
                if idx > 0:
                    prev_level = LEVEL_NAMES[idx - 1]
                    for prev_term in level_term_lookup.get(prev_level, {}).values():
                        if any(a.get("identifier") == term_id for a in prev_term.get("associations", [])):
                            department_id = prev_term.get("additionalProperties", {}).get("orgId")
                            department_name = prev_term.get("name")
                            department_level = prev_level
                            break

                rows.append((
                    mdo_name, mdo_id, level,
                    department_id, department_name, department_level,
                    org_id, org_name,
                ))
        return rows

    def fetch_framework(self, framework_id: str) -> Optional[dict]:
        api_url_template = self.config.api_url_template
        url = api_url_template.format(framework_id)
        response = requests.get(url)
        if response.status_code != 200:
            logger.warning("Framework fetch failed for %s: HTTP %s", framework_id, response.status_code)
            return None
        return response.json()

    # ------------------------------------------------------------------
    # Stage A: org_hierarchy_lookup + mdo_children_lookup (Postgres only)
    # (from org_hierarchy_all.py - plain org set, no hardcoded merge)
    # ------------------------------------------------------------------
    def run_stage_a(self) -> None:
        logger.info("=== Stage A: org_hierarchy_lookup / mdo_children_lookup (Postgres) ===")
        self.pg_cursor.execute("TRUNCATE TABLE org_hierarchy_lookup;")
        self.pg_cursor.execute("TRUNCATE TABLE mdo_children_lookup;")
        self.pg_conn.commit()

        df = self.get_stage_a_orgs()
        total = len(df)

        for i, row in df.iterrows():
            org_name = row["orgName"]
            org_id = row["identifier"]
            framework_id = row["orgHierarchyFrameworkId"]

            try:
                data = self.fetch_framework(framework_id)
                if data is None:
                    continue
                categories = data.get("result", {}).get("framework", {}).get("categories", [])

                lookup_rows = self.build_lookup_rows(org_id, org_name, categories)
                self.insert_lookup_rows(lookup_rows)

                flat_map = self.build_flat_descendant_map(categories)
                children_rows = [(mdo_id, children) for mdo_id, children in flat_map.items() if mdo_id]
                all_children = set(flat_map.keys())
                for children in flat_map.values():
                    all_children.update(children)
                children_rows.append((org_id, list(all_children)))
                self.insert_children_rows(children_rows)

                self.pg_conn.commit()

                if (i + 1) % 25 == 0 or (i + 1) == total:
                    logger.info("[Stage A] Processed %d/%d orgs.", i + 1, total)
            except Exception as e:
                self.pg_conn.rollback()
                logger.error("[Stage A] Error processing framework %s (org %s): %s", framework_id, org_id, e)

            time.sleep(REQUEST_DELAY_SECONDS)

    # ------------------------------------------------------------------
    # Stage B: org_hierarchy_new rows, built in memory (Parquet only,
    # no Postgres write)
    # (from org_hierarchy_updated_empty.py - hardcoded merge + empty-framework fix)
    # ------------------------------------------------------------------
    def build_stage_b_rows(self) -> List[tuple]:
        logger.info("=== Stage B: org_hierarchy_new rows (Parquet only) ===")
        df = self.get_stage_b_orgs()
        total = len(df)
        all_rows: List[tuple] = []

        for i, row in df.iterrows():
            org_name = row["orgName"]
            org_id = row["identifier"]
            framework_id = row["orgHierarchyFrameworkId"]
            created_date = self.parse_pg_timestamp(row.get("createdDate"))
            updated_date = self.parse_pg_timestamp(row.get("updatedDate"))

            try:
                data = self.fetch_framework(framework_id)
                if data is None:
                    continue

                hierarchies = self.parse_framework_data(data)
                for h in hierarchies:
                    values = [created_date, updated_date]
                    for lvl in LEVEL_NAMES:
                        values.extend(h.get(lvl, (None, None)))
                    values.extend([org_id, org_name])
                    all_rows.append(tuple(values))

                if (i + 1) % 25 == 0 or (i + 1) == total:
                    logger.info("[Stage B] Processed %d/%d orgs (%d rows so far).",
                                i + 1, total, len(all_rows))
            except Exception as e:
                logger.error("[Stage B] Error processing framework %s (org %s): %s", framework_id, org_id, e)

            time.sleep(REQUEST_DELAY_SECONDS)

        return all_rows

    # ------------------------------------------------------------------
    # Stage C: Parquet export - writes the rows Stage B already built,
    # no re-read from Postgres, no Spark. Plain pandas/pyarrow is enough
    # here: the row count is small and everything is already in memory,
    # so a SparkSession would only add JVM/session startup overhead with
    # no parallel work for it to actually do.
    # ------------------------------------------------------------------
    def write_parquet(self, rows: List[tuple]) -> str:
        logger.info("=== Stage C: Parquet export ===")
        output_dir = Path(PARQUET_OUTPUT_DIR)
        output_dir.mkdir(parents=True, exist_ok=True)

        df = pd.DataFrame(rows, columns=HIERARCHY_COLUMNS)
        logger.info("Writing %d rows to Parquet.", len(df))

        # Fixed "part-" prefixed filename so downstream readers that glob
        # on part-*.parquet (Spark-dataset-style directory reads) still
        # find it, and each run overwrites this same file instead of
        # accumulating stale files in the directory.
        output_path = str(output_dir / "part-00000.snappy.parquet")
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, output_path, compression="snappy")

        size_mb = Path(output_path).stat().st_size / (1024 * 1024)
        logger.info("Wrote Parquet file: %s (%.2f MB)", output_path, size_mb)
        return output_path

    # ------------------------------------------------------------------
    def run(self) -> None:
        self.connect()
        try:
            self.run_stage_a()
            stage_b_rows = self.build_stage_b_rows()
            self.write_parquet(stage_b_rows)
            logger.info("Job complete.")
        finally:
            self.close()


def main() -> int:
    job = OrgHierarchyJob()
    try:
        job.run()
        return 0
    except Exception as e:
        logger.error("Job failed: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
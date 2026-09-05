"""
24_reconcile_status_and_identity.py
===================================

Phase 7/8 cleanup:

1. Reconcile historical_extraction_status so it no longer says
   "not_started" after historical staging/candidate data exists.
2. Populate evidence_identity and evidence_observation from already-cleaned
   staging tables.

This script does not clean, match, price, or alter TMV. It only promotes
existing lineage/status facts into the metadata tables designed for them.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb

BASE_DIR = Path(__file__).parent.parent
DEFAULT_DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"
SCHEMA_PATH = BASE_DIR / "scripts" / "schema.sql"


def _count(conn: duckdb.DuckDBPyConnection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def apply_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(SCHEMA_PATH.read_text())


def reconcile_historical_extraction_status(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    before_not_started = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM historical_extraction_status
            WHERE extraction_status = 'not_started'
            """
        ).fetchone()[0]
    )

    conn.execute("DROP TABLE IF EXISTS tmp_historical_candidate_status")
    conn.execute(
        """
        CREATE TEMP TABLE tmp_historical_candidate_status AS
        WITH hist_candidates AS (
            SELECT inventory_uid, 'eBay sold' AS source_label
            FROM match_candidates_ebay_sold
            UNION ALL
            SELECT inventory_uid, 'VCP aggregate' AS source_label
            FROM match_candidates_vcp
        ),
        by_item AS (
            SELECT
                inventory_uid,
                COUNT(*) AS candidate_count,
                string_agg(DISTINCT source_label, ', ' ORDER BY source_label) AS source_label
            FROM hist_candidates
            GROUP BY inventory_uid
        ),
        source_files AS (
            SELECT
                string_agg(DISTINCT source_filename, ', ' ORDER BY source_filename) AS source_filename,
                MAX(sold_date) AS latest_date
            FROM stg_historical_ebay_sold
            WHERE source_filename IS NOT NULL
            UNION ALL
            SELECT
                string_agg(DISTINCT COALESCE(original_source_file, source_file), ', ' ORDER BY COALESCE(original_source_file, source_file)) AS source_filename,
                MAX(last_sold_date) AS latest_date
            FROM stg_historical_vcp_aggregate
            WHERE COALESCE(original_source_file, source_file) IS NOT NULL
        ),
        source_summary AS (
            SELECT
                string_agg(DISTINCT source_filename, ', ' ORDER BY source_filename) AS source_filename,
                MAX(latest_date) AS latest_date
            FROM source_files
            WHERE source_filename IS NOT NULL
        )
        SELECT
            h.inventory_uid,
            CASE
                WHEN b.candidate_count > 0 THEN 'done'
                ELSE 'done'
            END AS extraction_status,
            'ingested' AS ingestion_status,
            COALESCE(s.source_filename, h.source_filename) AS source_filename,
            COALESCE(s.latest_date, h.extraction_date, CURRENT_DATE) AS extraction_date,
            CASE
                WHEN b.candidate_count > 0 THEN
                    'Historical exports ingested; ' || CAST(b.candidate_count AS VARCHAR) ||
                    ' historical candidate(s) linked via ' || b.source_label || '.'
                ELSE
                    'Historical exports ingested; no item-level historical candidate currently linked.'
            END AS notes
        FROM historical_extraction_status h
        LEFT JOIN by_item b ON b.inventory_uid = h.inventory_uid
        CROSS JOIN source_summary s
        """
    )

    updated = int(
        conn.execute(
            """
            UPDATE historical_extraction_status AS h
            SET
                extraction_status = s.extraction_status,
                ingestion_status = s.ingestion_status,
                source_filename = s.source_filename,
                extraction_date = s.extraction_date,
                notes = s.notes,
                updated_at = CURRENT_TIMESTAMP
            FROM tmp_historical_candidate_status AS s
            WHERE h.inventory_uid = s.inventory_uid
              AND h.time_bucket = 'all'
              AND (
                  h.extraction_status IS DISTINCT FROM s.extraction_status
               OR h.ingestion_status IS DISTINCT FROM s.ingestion_status
               OR h.source_filename IS DISTINCT FROM s.source_filename
               OR h.extraction_date IS DISTINCT FROM s.extraction_date
               OR h.notes IS DISTINCT FROM s.notes
              )
            """
        ).fetchone()[0]
    )

    done = int(
        conn.execute(
            "SELECT COUNT(*) FROM historical_extraction_status WHERE extraction_status = 'done'"
        ).fetchone()[0]
    )
    still_not_started = int(
        conn.execute(
            "SELECT COUNT(*) FROM historical_extraction_status WHERE extraction_status = 'not_started'"
        ).fetchone()[0]
    )
    with_candidates = int(
        conn.execute(
            """
            SELECT COUNT(DISTINCT inventory_uid)
            FROM (
                SELECT inventory_uid FROM match_candidates_ebay_sold
                UNION ALL
                SELECT inventory_uid FROM match_candidates_vcp
            )
            """
        ).fetchone()[0]
    )
    return {
        "before_not_started": before_not_started,
        "updated": updated,
        "done": done,
        "still_not_started": still_not_started,
        "with_historical_candidates": with_candidates,
    }


def populate_evidence_registries(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    identity_before = _count(conn, "evidence_identity")
    observation_before = _count(conn, "evidence_observation")

    conn.execute("DROP TABLE IF EXISTS tmp_evidence_identity")
    conn.execute("DROP TABLE IF EXISTS tmp_evidence_observation")

    conn.execute(
        """
        CREATE TEMP TABLE tmp_evidence_identity AS
        SELECT
            stable_evidence_uid,
            identity_type,
            identity_source,
            identity_confidence,
            source_system,
            natural_key_type,
            marketplace,
            natural_key_value
        FROM (
            SELECT DISTINCT
                stable_evidence_uid,
                'INDIVIDUAL_LISTING' AS identity_type,
                'natural_key' AS identity_source,
                'HIGH' AS identity_confidence,
                'EBAY_ACTIVE_TARGETED' AS source_system,
                'ITEM_ID' AS natural_key_type,
                marketplace,
                item_id AS natural_key_value
            FROM stg_active_targeted
            WHERE stable_evidence_uid IS NOT NULL

            UNION ALL

            SELECT DISTINCT
                stable_evidence_uid,
                'INDIVIDUAL_LISTING' AS identity_type,
                'natural_key' AS identity_source,
                'HIGH' AS identity_confidence,
                'EBAY_ACTIVE_BROAD' AS source_system,
                'ITEM_ID' AS natural_key_type,
                marketplace,
                item_id AS natural_key_value
            FROM stg_active_broad
            WHERE stable_evidence_uid IS NOT NULL

            UNION ALL

            SELECT DISTINCT
                stable_evidence_uid,
                'INDIVIDUAL_LISTING' AS identity_type,
                'natural_key' AS identity_source,
                'HIGH' AS identity_confidence,
                'EBAY_SOLD_EXPORT' AS source_system,
                'ITEM_NUMBER' AS natural_key_type,
                NULL AS marketplace,
                item_number AS natural_key_value
            FROM stg_historical_ebay_sold
            WHERE stable_evidence_uid IS NOT NULL

            UNION ALL

            SELECT DISTINCT
                stable_evidence_uid,
                'AGGREGATE_CLUSTER' AS identity_type,
                'natural_key' AS identity_source,
                'HIGH' AS identity_confidence,
                'VCP_AGGREGATE' AS source_system,
                'DUPLICATE_GROUP_ID' AS natural_key_type,
                NULL AS marketplace,
                duplicate_group_id AS natural_key_value
            FROM stg_historical_vcp_aggregate
            WHERE stable_evidence_uid IS NOT NULL
        )
        QUALIFY row_number() OVER (
            PARTITION BY stable_evidence_uid
            ORDER BY source_system
        ) = 1
        """
    )

    conn.execute(
        """
        INSERT INTO evidence_identity (
            stable_evidence_uid, identity_type, identity_source, identity_confidence,
            source_system, natural_key_type, marketplace, natural_key_value
        )
        SELECT
            stable_evidence_uid, identity_type, identity_source, identity_confidence,
            source_system, natural_key_type, marketplace, natural_key_value
        FROM tmp_evidence_identity
        ON CONFLICT (stable_evidence_uid) DO NOTHING
        """
    )

    conn.execute(
        """
        CREATE TEMP TABLE tmp_evidence_observation AS
        SELECT *
        FROM (
            SELECT
                observation_uid,
                stable_evidence_uid,
                raw_id,
                COALESCE(fetched_at, item_creation_date, created_at) AS observed_at,
                price_eur AS price,
                condition_standard AS condition,
                title AS title_snapshot
            FROM stg_active_targeted
            WHERE observation_uid IS NOT NULL

            UNION ALL

            SELECT
                observation_uid,
                stable_evidence_uid,
                raw_id,
                COALESCE(collected_at_utc, item_creation_date, created_at) AS observed_at,
                price_eur AS price,
                condition_standard AS condition,
                title AS title_snapshot
            FROM stg_active_broad
            WHERE observation_uid IS NOT NULL

            UNION ALL

            SELECT
                observation_uid,
                stable_evidence_uid,
                raw_id,
                CAST(COALESCE(sold_date, cleaned_at) AS TIMESTAMP) AS observed_at,
                price_eur AS price,
                condition_standard AS condition,
                title AS title_snapshot
            FROM stg_historical_ebay_sold
            WHERE observation_uid IS NOT NULL

            UNION ALL

            SELECT
                observation_uid,
                stable_evidence_uid,
                raw_id,
                CAST(COALESCE(last_sold_date, cleaned_at) AS TIMESTAMP) AS observed_at,
                avg_price_eur AS price,
                format_standard AS condition,
                title AS title_snapshot
            FROM stg_historical_vcp_aggregate
            WHERE observation_uid IS NOT NULL
        )
        QUALIFY row_number() OVER (
            PARTITION BY observation_uid
            ORDER BY observed_at DESC NULLS LAST, raw_id DESC NULLS LAST
        ) = 1
        """
    )

    conn.execute(
        """
        INSERT INTO evidence_observation (
            observation_uid, stable_evidence_uid, raw_id, observed_at,
            price, condition, title_snapshot, is_current
        )
        SELECT
            observation_uid, stable_evidence_uid, raw_id, observed_at,
            price, condition, title_snapshot, FALSE AS is_current
        FROM tmp_evidence_observation
        ON CONFLICT (observation_uid) DO NOTHING
        """
    )

    conn.execute(
        """
        UPDATE evidence_observation AS o
        SET
            stable_evidence_uid = s.stable_evidence_uid,
            raw_id = s.raw_id,
            observed_at = s.observed_at,
            price = s.price,
            condition = s.condition,
            title_snapshot = s.title_snapshot
        FROM tmp_evidence_observation AS s
        WHERE o.observation_uid = s.observation_uid
        """
    )

    conn.execute(
        """
        UPDATE evidence_observation
        SET is_current = FALSE
        WHERE stable_evidence_uid IN (
            SELECT stable_evidence_uid FROM tmp_evidence_observation
        )
        """
    )
    conn.execute(
        """
        UPDATE evidence_observation AS o
        SET is_current = TRUE
        FROM (
            SELECT observation_uid
            FROM evidence_observation
            WHERE stable_evidence_uid IN (
                SELECT stable_evidence_uid FROM tmp_evidence_observation
            )
            QUALIFY row_number() OVER (
                PARTITION BY stable_evidence_uid
                ORDER BY observed_at DESC NULLS LAST, raw_id DESC NULLS LAST, observation_uid DESC
            ) = 1
        ) AS latest
        WHERE o.observation_uid = latest.observation_uid
        """
    )

    identity_after = _count(conn, "evidence_identity")
    observation_after = _count(conn, "evidence_observation")
    current_count = int(
        conn.execute("SELECT COUNT(*) FROM evidence_observation WHERE is_current").fetchone()[0]
    )
    return {
        "identity_before": identity_before,
        "identity_after": identity_after,
        "identity_inserted": identity_after - identity_before,
        "observation_before": observation_before,
        "observation_after": observation_after,
        "observation_inserted": observation_after - observation_before,
        "current_observations": current_count,
    }


def run(db_path: str | os.PathLike) -> dict[str, dict[str, int]]:
    conn = duckdb.connect(str(db_path))
    try:
        apply_schema(conn)
        hist = reconcile_historical_extraction_status(conn)
        evidence = populate_evidence_registries(conn)
        return {"historical_status": hist, "evidence_registry": evidence}
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(os.environ.get("WATCHPARTS_DB", DEFAULT_DB_PATH)))
    args = parser.parse_args()

    result = run(args.db)
    print(f"Database target: {args.db}")
    print("Historical extraction status:")
    for key, value in result["historical_status"].items():
        print(f"  {key}: {value:,}")
    print("Evidence registry:")
    for key, value in result["evidence_registry"].items():
        print(f"  {key}: {value:,}")
    print("Phase 7/8 cleanup complete.")


if __name__ == "__main__":
    main()

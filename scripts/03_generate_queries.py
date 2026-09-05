"""
03_generate_queries.py
=======================
Module 2.5: generates eBay search query candidates from staging_inventory.

Does NOT:
- mine or bootstrap ref_part_name_lexicon (future prompt)
- generate Tier 3 descriptive queries beyond a graceful lexicon lookup
  that currently returns nothing (lexicon is empty by design here)
- historical extraction, matching, feature computation, TMV, turnover,
  or the dashboard

Usage:
    python scripts/03_generate_queries.py
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import duckdb
import pandas as pd

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"
SCHEMA_PATH = BASE_DIR / "scripts" / "schema.sql"
LOG_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"

CALIBER_PREFIXES = ["Cal", "Calibre", "Caliber", "Kaliber"]
QUERY_TEMPLATE_VERSION = "v1"

QUERY_COLUMNS = [
    "inventory_uid", "canonical_inventory_id", "tier",
    "query_text", "uses_lexicon", "query_template_version",
]


def setup_logging(log_dir: Path = LOG_DIR) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=log_dir / "03_generate_queries.log",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        force=True,
    )


def log_and_print(message: str = "") -> None:
    print(message)
    logging.info(message)


def table_exists(conn, table_name: str) -> bool:
    result = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return bool(result and result[0])


def get_connection():
    last_exc = None
    for attempt in range(31):
        try:
            conn = duckdb.connect(str(DB_PATH))
            break
        except duckdb.IOException as exc:
            last_exc = exc
            if "lock" not in str(exc).lower() or attempt == 30:
                raise
            time.sleep(0.5)
    else:
        raise last_exc
    conn.execute(SCHEMA_PATH.read_text())
    migrate_historical_extraction_status_primary_key(conn)
    migrate_search_queries_schema(conn)
    return conn


# ══════════════════════════════════════════════════════════════════════════════
# SCHEMA MIGRATIONS
# ══════════════════════════════════════════════════════════════════════════════

def migrate_historical_extraction_status_primary_key(conn) -> int:
    """
    One-time, idempotent migration: historical_extraction_status's primary
    key moves from (canonical_inventory_id, time_bucket) to
    (inventory_uid, time_bucket). Backfills inventory_uid by joining
    staging_inventory on canonical_inventory_id. Never rebuilds or drops
    rows that can be matched — this table may carry real human progress.

    Returns the number of rows successfully backfilled (0 if the table was
    already empty or already migrated).
    """
    current_pk = conn.execute(
        """
        SELECT constraint_column_names FROM duckdb_constraints()
        WHERE table_name = 'historical_extraction_status' AND constraint_type = 'PRIMARY KEY'
        """
    ).fetchone()
    if not current_pk or list(current_pk[0]) == ["inventory_uid", "time_bucket"]:
        return 0

    df = conn.execute("SELECT * FROM historical_extraction_status").df()
    before_count = len(df)
    if before_count == 0:
        conn.execute("DROP TABLE historical_extraction_status")
        _create_historical_extraction_status(conn)
        return 0

    uid_by_canonical = dict(
        conn.execute("SELECT canonical_inventory_id, inventory_uid FROM staging_inventory").fetchall()
    )
    df["inventory_uid"] = df["canonical_inventory_id"].map(uid_by_canonical)
    backfilled = int(df["inventory_uid"].notna().sum())
    unmatched = before_count - backfilled
    if unmatched:
        log_and_print(
            f"   ⚠ historical_extraction_status PK migration: {unmatched} row(s) have no matching "
            "staging_inventory canonical_inventory_id and cannot be migrated — they will be dropped."
        )
    df = df.dropna(subset=["inventory_uid"])
    df = df.sort_values("updated_at").drop_duplicates(subset=["inventory_uid", "time_bucket"], keep="last")

    conn.register("tmp_hes_migrated", df)
    conn.execute("DROP TABLE historical_extraction_status")
    _create_historical_extraction_status(conn)
    cols = ["inventory_uid", "canonical_inventory_id", "time_bucket", "extraction_status",
            "source_filename", "extraction_date", "ingestion_status", "notes", "updated_at"]
    conn.execute(
        f"INSERT INTO historical_extraction_status ({','.join(cols)}) "
        f"SELECT {','.join(cols)} FROM tmp_hes_migrated"
    )
    conn.unregister("tmp_hes_migrated")

    log_and_print(
        f"   ✓ historical_extraction_status PK migrated to (inventory_uid, time_bucket): "
        f"{backfilled} row(s) backfilled, {unmatched} dropped"
    )
    return backfilled


def _create_historical_extraction_status(conn) -> None:
    conn.execute(
        """
        CREATE TABLE historical_extraction_status (
            inventory_uid           VARCHAR,
            canonical_inventory_id  VARCHAR,
            time_bucket             VARCHAR,
            extraction_status       VARCHAR,
            source_filename         VARCHAR,
            extraction_date         DATE,
            ingestion_status        VARCHAR,
            notes                   VARCHAR,
            updated_at              TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (inventory_uid, time_bucket)
        )
        """
    )


def migrate_search_queries_schema(conn) -> None:
    """
    search_queries is purely derived and fully rebuilt every run — no
    backfill needed, just ensure the table has the current desired shape.
    """
    existing_cols = (
        set(row[0] for row in conn.execute("DESCRIBE search_queries").fetchall())
        if table_exists(conn, "search_queries")
        else set()
    )
    desired_cols = set(QUERY_COLUMNS) | {"created_at"}
    if existing_cols == desired_cols:
        return
    if existing_cols:
        log_and_print(
            "   Migrating search_queries to the new inventory_uid-keyed schema "
            "(purely derived data, safe to recreate)..."
        )
        conn.execute("DROP TABLE search_queries")
    conn.execute(
        """
        CREATE TABLE search_queries (
            inventory_uid            VARCHAR,
            canonical_inventory_id   VARCHAR,
            tier                     INTEGER,
            query_text               VARCHAR,
            uses_lexicon             BOOLEAN,
            query_template_version   VARCHAR,
            created_at               TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (inventory_uid, tier, query_text)
        )
        """
    )


# ══════════════════════════════════════════════════════════════════════════════
# QUERY GENERATION
# ══════════════════════════════════════════════════════════════════════════════

def _query_row(inventory_uid, canonical_inventory_id, tier, query_text, uses_lexicon):
    return {
        "inventory_uid": inventory_uid,
        "canonical_inventory_id": canonical_inventory_id,
        "tier": tier,
        "query_text": query_text,
        "uses_lexicon": uses_lexicon,
        "query_template_version": QUERY_TEMPLATE_VERSION,
    }


def build_queries_for_inventory(inventory_df: pd.DataFrame, lexicon_df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    query_rows: list[dict] = []
    manual_review: list[dict] = []

    for row in inventory_df.itertuples():
        status = row.validation_status
        uid = row.inventory_uid
        canon_id = row.canonical_inventory_id

        if status == "FAIL":
            manual_review.append({
                "inventory_uid": uid,
                "canonical_inventory_id": canon_id,
                "reason_category": "FAIL",
                "reason": "validation_status=FAIL — skipped entirely, no query attempted",
            })
            continue

        if status == "WARNING":
            if row.part_number_is_distinctive:
                pn = str(row.part_number).strip()
                query_rows.append(_query_row(uid, canon_id, 5, pn, False))
            else:
                manual_review.append({
                    "inventory_uid": uid,
                    "canonical_inventory_id": canon_id,
                    "reason_category": "WARNING-non-distinctive",
                    "reason": "blank caliber and part_number not distinctive — no safe query possible",
                })
            continue

        # PASS
        brand_s = str(row.brand).strip()
        caliber_s = str(row.caliber).strip()
        pn_s = str(row.part_number).strip()

        query_rows.append(_query_row(uid, canon_id, 1, f"{brand_s} {caliber_s} {pn_s}", False))

        for prefix in CALIBER_PREFIXES:
            query_rows.append(_query_row(uid, canon_id, 2, f"{brand_s} {prefix} {caliber_s} {pn_s}", False))

        query_rows.append(_query_row(uid, canon_id, 4, f"{brand_s} {caliber_s}", False))

        # Tier 3: lexicon lookup — ref_part_name_lexicon is empty in this
        # prompt's scope, so this must produce nothing without erroring.
        if not lexicon_df.empty:
            match = lexicon_df[
                (lexicon_df["caliber"] == row.caliber) & (lexicon_df["part_number"] == row.part_number)
            ]
            if not match.empty:
                part_name = str(match.iloc[0]["part_name"]).strip()
                query_rows.append(_query_row(uid, canon_id, 3, f"{brand_s} {caliber_s} {part_name}", True))

    query_df = pd.DataFrame(query_rows, columns=QUERY_COLUMNS) if query_rows else pd.DataFrame(columns=QUERY_COLUMNS)
    return query_df, manual_review


def compute_priority_order(inventory_df: pd.DataFrame) -> pd.DataFrame:
    """
    Priority: stock descending, but stratified across calibers so a few
    high-stock calibers can't push every other caliber to the bottom.
    Each caliber's highest-stock item gets priority first (sorted by stock
    across calibers), then each caliber's second-highest, and so on —
    a round-robin "snake draft" by caliber.
    """
    eligible = inventory_df[
        (inventory_df["validation_status"] == "PASS")
        | ((inventory_df["validation_status"] == "WARNING") & (inventory_df["part_number_is_distinctive"]))
    ].copy()
    if eligible.empty:
        return eligible.assign(priority_rank=pd.Series(dtype="int64"))

    eligible["caliber_key"] = eligible["caliber"].fillna("__unknown__")
    eligible = eligible.sort_values(["caliber_key", "stock"], ascending=[True, False])
    eligible["rank_within_caliber"] = eligible.groupby("caliber_key").cumcount()
    eligible = eligible.sort_values(["rank_within_caliber", "stock"], ascending=[True, False])
    eligible["priority_rank"] = range(1, len(eligible) + 1)
    return eligible


def export_worklist(query_df: pd.DataFrame, priority_df: pd.DataFrame, reports_dir: Path = REPORTS_DIR) -> Path:
    reports_dir.mkdir(parents=True, exist_ok=True)
    out_path = reports_dir / "search_queries_worklist.csv"

    if query_df.empty or priority_df.empty:
        pd.DataFrame(columns=[
            "priority_rank", "canonical_inventory_id", "brand", "caliber", "part_number", "tier", "query_text",
        ]).to_csv(out_path, index=False)
        return out_path

    merged = query_df.merge(
        priority_df[["inventory_uid", "brand", "caliber", "part_number", "priority_rank"]],
        on="inventory_uid", how="inner",
    )
    merged = merged.sort_values(["priority_rank", "tier", "query_text"])
    worklist = merged[[
        "priority_rank", "canonical_inventory_id", "brand", "caliber", "part_number", "tier", "query_text",
    ]]
    worklist.to_csv(out_path, index=False)
    return out_path


def generate_search_queries(conn, reports_dir: Path = REPORTS_DIR) -> dict:
    log_and_print("\nGenerating search queries...")

    inventory_df = conn.execute(
        """
        SELECT canonical_inventory_id, inventory_uid, brand, caliber, part_number, stock,
               validation_status, part_number_is_distinctive
        FROM staging_inventory
        """
    ).df()
    log_and_print(f"   Input: {len(inventory_df):,} staging_inventory rows")

    lexicon_df = conn.execute("SELECT caliber, part_number, part_name FROM ref_part_name_lexicon").df()

    query_df, manual_review = build_queries_for_inventory(inventory_df, lexicon_df)

    conn.execute("DELETE FROM search_queries")
    if not query_df.empty:
        conn.register("tmp_search_queries", query_df)
        conn.execute(
            f"INSERT INTO search_queries ({','.join(QUERY_COLUMNS)}) "
            f"SELECT {','.join(QUERY_COLUMNS)} FROM tmp_search_queries"
        )
        conn.unregister("tmp_search_queries")

    tier_counts = query_df["tier"].value_counts().sort_index().to_dict() if not query_df.empty else {}
    log_and_print(f"   Queries generated: {len(query_df):,} total")
    for tier, count in tier_counts.items():
        log_and_print(f"     Tier {tier}: {count:,}")
    review_counts: dict[str, int] = {"FAIL": 0, "WARNING-non-distinctive": 0}
    for item in manual_review:
        review_counts[item["reason_category"]] = review_counts.get(item["reason_category"], 0) + 1
    review_breakdown = ", ".join(f"{category}: {count}" for category, count in review_counts.items())
    log_and_print(f"   Manual review list: {review_breakdown}")

    # historical_extraction_status: append-only, never touch existing rows.
    hes_new = inventory_df[["inventory_uid", "canonical_inventory_id"]].copy()
    hes_new["time_bucket"] = "all"
    hes_new["extraction_status"] = "not_started"
    conn.register("tmp_hes_new", hes_new)
    conn.execute(
        """
        INSERT INTO historical_extraction_status
            (inventory_uid, canonical_inventory_id, time_bucket, extraction_status)
        SELECT inventory_uid, canonical_inventory_id, time_bucket, extraction_status
        FROM tmp_hes_new
        ON CONFLICT (inventory_uid, time_bucket) DO NOTHING
        """
    )
    conn.unregister("tmp_hes_new")

    priority_df = compute_priority_order(inventory_df)
    worklist_path = export_worklist(query_df, priority_df, reports_dir)
    worklist_rows = len(query_df.merge(priority_df[["inventory_uid"]], on="inventory_uid", how="inner")) if not query_df.empty and not priority_df.empty else 0

    search_queries_count = conn.execute("SELECT COUNT(*) FROM search_queries").fetchone()[0]
    hes_count = conn.execute("SELECT COUNT(*) FROM historical_extraction_status").fetchone()[0]
    log_and_print(f"   ✓ search_queries: {search_queries_count:,} rows (full rebuild)")
    log_and_print(f"   ✓ historical_extraction_status: {hes_count:,} rows (append-only)")
    log_and_print(f"   ✓ Worklist exported: {worklist_path} ({worklist_rows:,} rows)")

    return {
        "query_df": query_df,
        "manual_review": manual_review,
        "priority_df": priority_df,
        "worklist_path": worklist_path,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    setup_logging()
    log_and_print("=" * 60)
    log_and_print("WATCHPARTS — STEP 2.5: GENERATE SEARCH QUERIES")
    log_and_print("=" * 60)
    log_and_print(f"Database path: {DB_PATH}")

    conn = get_connection()
    try:
        generate_search_queries(conn)
    finally:
        conn.close()

    log_and_print("\n✓ Query generation complete.")


if __name__ == "__main__":
    main()

"""
01_ingest.py
============
Single raw-data ingestion entry point for the Watchparts project.

This script only loads raw data exactly as received.

It does not:
- clean data
- standardize inventory
- create canonical IDs
- generate search queries
- match listings
- calculate landed cost, TMV, turnover, or dashboard metrics
"""

from __future__ import annotations

import argparse
import logging
import hashlib
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import duckdb
import pandas as pd


BASE_DIR = Path(__file__).parent.parent
# DB target resolution order: --db CLI arg (applied in main()) > WATCHPARTS_DB
# env var (read here at import, so it also reaches this script when it is
# launched as a subprocess by 04_collect_targeted_active.py) > the default
# live database. Default behaviour is unchanged when neither is set.
DEFAULT_DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"
DB_PATH = Path(os.environ["WATCHPARTS_DB"]) if os.environ.get("WATCHPARTS_DB") else DEFAULT_DB_PATH
SCHEMA_PATH = BASE_DIR / "scripts" / "schema.sql"
RAW_DIR = BASE_DIR / "data" / "raw"
LOG_DIR = BASE_DIR / "logs"

INVENTORY_CSV = RAW_DIR / "inventory.csv"
CURRENT_LISTINGS_CSV = RAW_DIR / "latest.csv"
HISTORICAL_EXPORTS_DIR = RAW_DIR / "historical_exports"
TARGETED_ACTIVE_DIR = RAW_DIR / "targeted_active"
# Deliberately a SEPARATE directory from HISTORICAL_EXPORTS_DIR, not the
# same one with a distinguishing filename: insert_historical_exports()
# globs *.csv in HISTORICAL_EXPORTS_DIR and would otherwise try to ingest
# an eBay-item-wise file as if it were VCP-aggregate-shaped (wrong
# columns, silently defaulting to blank via its own `if col in df.columns
# else ""` fallback) — the two sources must never be discoverable by the
# same glob.
EBAY_SOLD_EXPORTS_DIR = RAW_DIR / "historical_exports_ebay_sold"

EXPECTED_EBAY_SOLD_COLUMNS = [
    "item_number", "title", "price_eur", "currency", "condition", "seller_type",
    "sold_date_iso", "sold_date_raw", "is_sold", "shipping_eur", "free_shipping",
    "best_offer", "location", "seller", "url", "source_page",
]

EXPECTED_INVENTORY_COLUMNS = ["Rolex/Tudor", "Calibre", "P-number", "Stock"]

EXPECTED_TARGETED_COLUMNS = [
    "collection_batch_id",
    "inventory_uid",
    "canonical_inventory_id",
    "query_text",
    "query_tier",
    "query_template_version",
    "marketplace_id",
    "fetched_at",
    "item_id",
    "title",
    "price_value",
    "price_currency",
    "condition",
    "condition_id",
    "buying_options",
    "item_web_url",
    "image_url",
    "seller_username",
    "seller_feedback_score",
    "seller_feedback_percentage",
    "shipping_cost_value",
    "shipping_cost_currency",
    "item_location_country",
    "item_location_city",
    "item_creation_date",
]


def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=LOG_DIR / "01_ingest.log",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def log_and_print(message: str) -> None:
    print(message)
    logging.info(message)


def stop_with_error(message: str) -> None:
    logging.error(message)
    raise SystemExit(message)


def get_connection() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
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
    ensure_raw_tables_current(conn)
    migrate_raw_active_targeted_schema(conn)
    return conn


def migrate_raw_active_targeted_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """
    raw_active_targeted predates Module 3's lineage requirements (it used
    product_id instead of inventory_uid/canonical_inventory_id, and had no
    row_hash). CREATE TABLE IF NOT EXISTS never retrofits an existing table,
    so this checks the shape directly. Only ever recreates when the table is
    empty — never touches a populated raw table.
    """
    # Compare against the columns declared in schema.sql for this table by
    # re-creating it in an isolated in-memory connection (cheap, avoids
    # parsing SQL by hand).
    probe = duckdb.connect(":memory:")
    probe.execute(SCHEMA_PATH.read_text())
    desired_cols = set(row[0] for row in probe.execute("DESCRIBE raw_active_targeted").fetchall())
    probe.close()

    if not table_exists(conn, "raw_active_targeted"):
        return

    current_cols = table_columns(conn, "raw_active_targeted")
    if current_cols == desired_cols:
        return

    row_count = conn.execute("SELECT COUNT(*) FROM raw_active_targeted").fetchone()[0]
    if row_count > 0:
        stop_with_error(
            "raw_active_targeted has an outdated schema but already contains "
            f"{row_count} row(s) — refusing to drop a populated raw table. "
            "Manual migration required."
        )

    log_and_print(
        "raw_active_targeted has an outdated (pre-Module-3) schema and is empty — recreating with current columns."
    )
    conn.execute("DROP TABLE raw_active_targeted")
    conn.execute(SCHEMA_PATH.read_text())


def table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    if not table_exists(conn, table_name):
        return set()
    rows = conn.execute(f"DESCRIBE {table_name}").fetchall()
    return {row[0] for row in rows}


def add_missing_column(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    column_name: str,
    column_type: str,
) -> None:
    if column_name not in table_columns(conn, table_name):
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


REQUIRED_RAW_COLUMNS: dict[str, dict[str, str]] = {
    "raw_inventory": {
        "row_hash": "VARCHAR",
        "upload_batch_id": "VARCHAR",
        "source_filename": "VARCHAR",
        "file_hash": "VARCHAR",
        "raw_rolex_tudor": "VARCHAR",
        "raw_calibre": "VARCHAR",
        "raw_p_number": "VARCHAR",
        "raw_stock": "VARCHAR",
        "validation_status": "VARCHAR",
        "validation_notes": "VARCHAR",
    },
    "raw_active_broad": {
        "row_hash": "VARCHAR",
        "source_country": "VARCHAR",
        "source_marketplace_id": "VARCHAR",
        "item_id": "VARCHAR",
        "title": "VARCHAR",
        "price_value": "DOUBLE",
        "shipping_cost_value": "DOUBLE",
    },
    "raw_historical": {
        "row_hash": "VARCHAR",
        "title": "VARCHAR",
        "avg_price_eur": "DOUBLE",
        "avg_shipping_eur": "DOUBLE",
        "source_file": "VARCHAR",
        "original_source_file": "VARCHAR",
        "physical_container_file": "VARCHAR",
        "ingested_at": "TIMESTAMP",
    },
}


def ensure_raw_tables_current(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(SCHEMA_PATH.read_text())

    for table_name, columns in REQUIRED_RAW_COLUMNS.items():
        if not table_exists(conn, table_name):
            continue
        for column_name, column_type in columns.items():
            add_missing_column(conn, table_name, column_name, column_type)
    backfill_raw_inventory_row_hash(conn)


def backfill_raw_inventory_row_hash(conn: duckdb.DuckDBPyConnection) -> None:
    """Populate row_hash for inventory rows created before the row-level
    idempotency migration. Without this, the first stock edit after the
    migration would reinsert every old inventory row because their hashes
    were still NULL."""
    if not table_exists(conn, "raw_inventory") or "row_hash" not in table_columns(conn, "raw_inventory"):
        return
    rows = conn.execute(
        """
        SELECT id, raw_rolex_tudor, raw_calibre, raw_p_number, raw_stock
        FROM raw_inventory
        WHERE row_hash IS NULL OR row_hash = ''
        """
    ).fetchall()
    if not rows:
        return
    hashes = pd.DataFrame(
        {
            "id": [row[0] for row in rows],
            "row_hash_new": [
                text_hash([row[1], row[2], row[3], row[4]])
                for row in rows
            ],
        }
    )
    conn.register("raw_inventory_hash_backfill", hashes)
    conn.execute(
        """
        UPDATE raw_inventory
        SET row_hash = raw_inventory_hash_backfill.row_hash_new
        FROM raw_inventory_hash_backfill
        WHERE raw_inventory.id = raw_inventory_hash_backfill.id
        """
    )
    conn.unregister("raw_inventory_hash_backfill")


def read_csv_file(path: Path, *, dtype: type | str | None = None) -> pd.DataFrame:
    try:
        if dtype is None:
            return pd.read_csv(path)
        return pd.read_csv(path, dtype=dtype, keep_default_na=False)
    except Exception as exc:
        stop_with_error(f"Could not read CSV file: {path}\nError: {exc}")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def text_hash(values: list[object]) -> str:
    safe_values = ["" if pd.isna(value) else str(value) for value in values]
    return hashlib.sha256("||".join(safe_values).encode("utf-8")).hexdigest()


def log_ingestion(
    conn: duckdb.DuckDBPyConnection,
    *,
    source_type: str,
    source_filename: str,
    file_hash: str,
    upload_batch_id: str,
    rows_inserted: int,
    status: str,
) -> None:
    ingested_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    ingestion_log_row = pd.DataFrame(
        [
            {
                "source_type": source_type,
                "source_filename": source_filename,
                "file_hash": file_hash,
                "upload_batch_id": upload_batch_id,
                "ingested_at": ingested_at,
                "rows_inserted": rows_inserted,
                "status": status,
            }
        ]
    )
    conn.execute(
        """
        INSERT INTO ingestion_log (
            source_type,
            source_filename,
            file_hash,
            upload_batch_id,
            ingested_at,
            rows_inserted,
            status
        )
        SELECT
            source_type,
            source_filename,
            file_hash,
            upload_batch_id,
            ingested_at,
            rows_inserted,
            status
        FROM ingestion_log_row
        ON CONFLICT (source_type, source_filename, file_hash) DO UPDATE SET
            upload_batch_id = CASE
                WHEN excluded.rows_inserted > ingestion_log.rows_inserted THEN excluded.upload_batch_id
                ELSE ingestion_log.upload_batch_id
            END,
            ingested_at = CASE
                WHEN excluded.rows_inserted > ingestion_log.rows_inserted THEN CAST(excluded.ingested_at AS TIMESTAMP)
                ELSE ingestion_log.ingested_at
            END,
            rows_inserted = GREATEST(ingestion_log.rows_inserted, excluded.rows_inserted),
            status = CASE
                WHEN excluded.rows_inserted > ingestion_log.rows_inserted THEN excluded.status
                ELSE ingestion_log.status
            END
        """
    )


def successful_file_ingested(
    conn: duckdb.DuckDBPyConnection,
    *,
    source_type: str,
    source_filename: str,
    file_hash: str,
) -> bool:
    result = conn.execute(
        """
        SELECT COUNT(*)
        FROM ingestion_log
        WHERE source_type = ?
          AND source_filename = ?
          AND file_hash = ?
          AND status = 'success'
        """,
        [source_type, source_filename, file_hash],
    ).fetchone()
    return bool(result and result[0])


def table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    result = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(result and result[0])


def next_id(conn: duckdb.DuckDBPyConnection, table_name: str) -> int:
    if not table_exists(conn, table_name):
        return 1
    result = conn.execute(f"SELECT COALESCE(MAX(id), 0) + 1 FROM {table_name}").fetchone()
    return int(result[0])


def insert_inventory(conn: duckdb.DuckDBPyConnection, upload_batch_id: str) -> int:
    if not INVENTORY_CSV.exists():
        stop_with_error(
            "Inventory file missing. Expected file:\n"
            f"{INVENTORY_CSV}\n"
            "Please place inventory.csv in data/raw/ and run again."
        )

    file_hash = file_sha256(INVENTORY_CSV)
    inventory_snapshot_exists = conn.execute(
        """
        SELECT COUNT(*)
        FROM ingestion_log
        WHERE source_type = 'inventory'
          AND source_filename = ?
          AND file_hash = ?
          AND status = 'success'
          AND rows_inserted > 0
        """,
        [INVENTORY_CSV.name, file_hash],
    ).fetchone()[0]
    if inventory_snapshot_exists:
        log_and_print("Inventory file already ingested. Skipping.")
        return 0

    df = read_csv_file(INVENTORY_CSV, dtype=str)
    missing_columns = [col for col in EXPECTED_INVENTORY_COLUMNS if col not in df.columns]
    if missing_columns:
        stop_with_error(
            "Inventory file is missing expected columns:\n"
            f"{missing_columns}\n"
            f"Expected columns: {EXPECTED_INVENTORY_COLUMNS}"
        )

    df = df.copy()
    df["row_hash"] = df.apply(
        lambda row: text_hash([
            row["Rolex/Tudor"], row["Calibre"], row["P-number"], row["Stock"],
        ]),
        axis=1,
    )
    df = df.drop_duplicates(subset=["row_hash"], keep="last").reset_index(drop=True)
    if df.empty:
        log_and_print("Inventory file has no rows to ingest.")
        log_ingestion(
            conn,
            source_type="inventory",
            source_filename=INVENTORY_CSV.name,
            file_hash=file_hash,
            upload_batch_id=upload_batch_id,
            rows_inserted=0,
            status="success",
        )
        return 0

    start_id = next_id(conn, "raw_inventory")
    ingested_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    inventory_rows_for_insert = pd.DataFrame(
        {
            "id": range(start_id, start_id + len(df)),
            "row_hash": df["row_hash"],
            "upload_batch_id": upload_batch_id,
            "source_filename": INVENTORY_CSV.name,
            "file_hash": file_hash,
            "ingested_at": ingested_at,
            "raw_rolex_tudor": df["Rolex/Tudor"],
            "raw_calibre": df["Calibre"],
            "raw_p_number": df["P-number"],
            "raw_stock": df["Stock"],
            "validation_status": "not_validated",
            "validation_notes": "",
        }
    )

    conn.execute(
        """
        INSERT INTO raw_inventory (
            id,
            row_hash,
            upload_batch_id,
            source_filename,
            file_hash,
            ingested_at,
            raw_rolex_tudor,
            raw_calibre,
            raw_p_number,
            raw_stock,
            validation_status,
            validation_notes
        )
        SELECT
            id,
            row_hash,
            upload_batch_id,
            source_filename,
            file_hash,
            ingested_at,
            raw_rolex_tudor,
            raw_calibre,
            raw_p_number,
            raw_stock,
            validation_status,
            validation_notes
        FROM inventory_rows_for_insert
        """
    )
    log_ingestion(
        conn,
        source_type="inventory",
        source_filename=INVENTORY_CSV.name,
        file_hash=file_hash,
        upload_batch_id=upload_batch_id,
        rows_inserted=len(inventory_rows_for_insert),
        status="success",
    )
    return len(inventory_rows_for_insert)


def insert_current_listings(conn: duckdb.DuckDBPyConnection) -> int:
    if not CURRENT_LISTINGS_CSV.exists():
        stop_with_error(
            "Current listings file missing. Expected file:\n"
            f"{CURRENT_LISTINGS_CSV}\n"
            "Please place latest.csv in data/raw/ and run again."
        )

    file_hash = file_sha256(CURRENT_LISTINGS_CSV)
    df = read_csv_file(CURRENT_LISTINGS_CSV, dtype=str)
    source_rows = len(df)

    expected_columns = [
        "id",
        "row_hash",
        "collected_at_utc",
        "keyword",
        "source_country",
        "source_marketplace_id",
        "item_id",
        "legacy_item_id",
        "title",
        "price_value",
        "price_currency",
        "condition",
        "condition_id",
        "buying_options",
        "item_web_url",
        "image_url",
        "seller_username",
        "seller_feedback_score",
        "seller_feedback_percentage",
        "shipping_cost_value",
        "shipping_cost_currency",
        "item_location_country",
        "item_location_city",
        "category_ids",
        "category_names",
        "listing_marketplace_id",
        "item_creation_date",
    ]

    file_columns = [col for col in expected_columns if col not in {"id", "row_hash"}]
    missing_columns = [col for col in file_columns if col not in df.columns]
    if missing_columns:
        stop_with_error(
            "Current listings file is missing expected columns:\n"
            f"{missing_columns}\n"
            f"Expected file: {CURRENT_LISTINGS_CSV}"
        )

    active_rows_for_insert = df[file_columns].copy()
    for col in [
        "price_value",
        "condition_id",
        "seller_feedback_score",
        "seller_feedback_percentage",
        "shipping_cost_value",
    ]:
        active_rows_for_insert[col] = pd.to_numeric(active_rows_for_insert[col], errors="coerce")
    active_rows_for_insert["row_hash"] = active_rows_for_insert.apply(
        lambda row: text_hash(
            [
                row.get("item_id", ""),
                row.get("title", ""),
                row.get("price_value", ""),
                row.get("price_currency", ""),
                row.get("seller_username", ""),
                row.get("item_location_country", ""),
                row.get("item_location_city", ""),
            ]
        ),
        axis=1,
    )

    # Idempotent observation ingestion: unchanged rows are skipped by their
    # source-value fingerprint, while a genuinely changed active listing
    # (for example a new price for the same item_id) is retained as a new
    # observation. Dashboard add/update jobs use --inventory-only and do not
    # touch this broad current-listing file.
    existing_row_hashes = set(
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT row_hash FROM raw_active_broad WHERE row_hash IS NOT NULL AND row_hash <> ''"
        ).fetchall()
    )
    is_duplicate = active_rows_for_insert["row_hash"].isin(existing_row_hashes)
    active_rows_for_insert = active_rows_for_insert.loc[~is_duplicate].copy()
    active_rows_for_insert = active_rows_for_insert.drop_duplicates(subset=["row_hash"], keep="first")
    start_id = next_id(conn, "raw_active_broad")
    active_rows_for_insert.insert(0, "id", range(start_id, start_id + len(active_rows_for_insert)))
    active_rows_for_insert = active_rows_for_insert[expected_columns]

    conn.execute(
        f"""
        INSERT INTO raw_active_broad ({",".join(expected_columns)})
        SELECT {",".join(expected_columns)}
        FROM active_rows_for_insert
        """
    )
    duplicate_rows = source_rows - len(active_rows_for_insert)
    log_ingestion(
        conn,
        source_type="current_listings",
        source_filename=CURRENT_LISTINGS_CSV.name,
        file_hash=file_hash,
        upload_batch_id="",
        rows_inserted=len(active_rows_for_insert),
        status="success",
    )
    log_and_print(f"Current listings source rows: {source_rows:,}")
    log_and_print(f"Current listings new rows inserted: {len(active_rows_for_insert):,}")
    log_and_print(f"Current listings duplicate rows skipped: {duplicate_rows:,}")
    return len(active_rows_for_insert)


def insert_historical_exports(conn: duckdb.DuckDBPyConnection) -> int:
    if not HISTORICAL_EXPORTS_DIR.exists():
        log_and_print("No historical exports found.")
        return 0

    csv_files = sorted(HISTORICAL_EXPORTS_DIR.glob("*.csv"))
    if not csv_files:
        log_and_print("No historical exports found.")
        return 0

    total_rows = 0
    start_id = next_id(conn, "raw_historical")
    ingested_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    for csv_file in csv_files:
        file_hash = file_sha256(csv_file)
        if successful_file_ingested(
            conn,
            source_type="historical",
            source_filename=csv_file.name,
            file_hash=file_hash,
        ):
            log_and_print(f"Historical file already ingested. Skipping: {csv_file.name}")
            continue

        df = read_csv_file(csv_file)
        source_rows = len(df)

        # original_source_file (the row's OWN provenance, e.g. which search/page
        # produced it) and physical_container_file (the file this process actually
        # opened) are two different facts and must never be collapsed into one
        # column — that collapse is exactly the bug this fix corrects. "Valid
        # row-level provenance" means the CSV's own source_file value is present
        # and non-empty for that row; only then is it used. A row with no
        # row-level value of its own (column absent, or blank/whitespace-only)
        # falls back to the physical container filename as the best available
        # provenance — a fallback, never the normal case.
        if "source_file" in df.columns:
            row_provenance = df["source_file"].astype(str).str.strip()
            has_valid_row_provenance = (
                df["source_file"].notna()
                & (row_provenance != "")
                & (row_provenance.str.lower() != "nan")
            )
            original_source_file = row_provenance.where(has_valid_row_provenance, csv_file.name)
        else:
            original_source_file = csv_file.name

        historical_rows_for_insert = pd.DataFrame(
            {
                "title": df["title"] if "title" in df.columns else "",
                "avg_price_eur": df["avg_price_eur"] if "avg_price_eur" in df.columns else "",
                "format": df["format"] if "format" in df.columns else "",
                "avg_shipping_eur": df["avg_shipping_eur"] if "avg_shipping_eur" in df.columns else "",
                "free_shipping_pct": df["free_shipping_pct"] if "free_shipping_pct" in df.columns else "",
                "total_sold": df["total_sold"] if "total_sold" in df.columns else "",
                "total_sales_eur": df["total_sales_eur"] if "total_sales_eur" in df.columns else "",
                "last_sold": df["last_sold"] if "last_sold" in df.columns else "",
                "bids": df["bids"] if "bids" in df.columns else "",
                "removed": df["removed"] if "removed" in df.columns else "",
                "source_file": csv_file.name,
                "original_source_file": original_source_file,
                "physical_container_file": csv_file.name,
                "ingested_at": ingested_at,
            }
        )
        historical_rows_for_insert["row_hash"] = historical_rows_for_insert.apply(
            lambda row: text_hash(
                [
                    row.get("title", ""),
                    row.get("avg_price_eur", ""),
                    row.get("total_sales_eur", ""),
                    row.get("last_sold", ""),
                    row.get("source_file", ""),
                ]
            ),
            axis=1,
        )
        existing_row_hashes = set(
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT row_hash FROM raw_historical WHERE row_hash IS NOT NULL AND row_hash <> ''"
            ).fetchall()
        )
        historical_rows_for_insert = historical_rows_for_insert.loc[
            ~historical_rows_for_insert["row_hash"].isin(existing_row_hashes)
        ].copy()
        historical_rows_for_insert = historical_rows_for_insert.drop_duplicates(subset=["row_hash"], keep="first")
        historical_rows_for_insert.insert(0, "id", range(start_id, start_id + len(historical_rows_for_insert)))
        total_rows += len(historical_rows_for_insert)

        conn.execute(
            """
            INSERT INTO raw_historical (
                id,
                row_hash,
                title,
                avg_price_eur,
                format,
                avg_shipping_eur,
                free_shipping_pct,
                total_sold,
                total_sales_eur,
                last_sold,
                bids,
                removed,
                source_file,
                original_source_file,
                physical_container_file,
                ingested_at
            )
            SELECT
                id,
                row_hash,
                title,
                avg_price_eur,
                format,
                avg_shipping_eur,
                free_shipping_pct,
                total_sold,
                total_sales_eur,
                last_sold,
                bids,
                removed,
                source_file,
                original_source_file,
                physical_container_file,
                ingested_at
            FROM historical_rows_for_insert
            """
        )
        log_ingestion(
            conn,
            source_type="historical",
            source_filename=csv_file.name,
            file_hash=file_hash,
            upload_batch_id="",
            rows_inserted=len(historical_rows_for_insert),
            status="success",
        )
        log_and_print(f"Historical file {csv_file.name}: {source_rows:,} source rows, {len(historical_rows_for_insert):,} new rows inserted")
        start_id += len(historical_rows_for_insert)

    return total_rows


def insert_historical_ebay_sold_exports(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Module 4: full ingestion for the eBay item-wise sold-listing source
    (EBAY_SOLD_LISTING / 'listing' row_grain — see stg_historical's
    source_type/row_grain columns), into its own raw_historical_ebay_sold
    table — deliberately never raw_historical, per the locked
    source-separated-estimators recommendation
    (docs/module4_historical_source_strategy.md §8): the two sources must
    never be concatenated into one rowset, starting at the raw layer.

    Idempotency: whole-file skip via file-hash (successful_file_ingested,
    matching insert_historical_exports' pattern), plus row-level dedup by
    item_number — eBay's own listing id is a natural, stable identity for
    a sold-listing observation; the same item_number reappearing (a
    duplicate scrape across search-result pages within one file, or the
    same file re-ingested) is the same real listing, not a new
    observation, and is never duplicated. row_hash = sha256(item_number)
    for consistency with this file's row_hash convention elsewhere, even
    though item_number alone is already a sufficient key.

    Each eBay sold-listing row is one individual sold-listing observation,
    not a proven one-unit transaction — best_offer/price semantics are
    still unconfirmed per the source-strategy audit (§3); this function
    only ingests the row as-is, it does not interpret or adjust price.
    """
    if not EBAY_SOLD_EXPORTS_DIR.exists():
        log_and_print("No eBay sold-listing exports found.")
        return 0

    csv_files = sorted(EBAY_SOLD_EXPORTS_DIR.glob("*.csv"))
    if not csv_files:
        log_and_print("No eBay sold-listing exports found.")
        return 0

    total_rows = 0
    start_id = next_id(conn, "raw_historical_ebay_sold")
    ingested_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    for csv_file in csv_files:
        file_hash = file_sha256(csv_file)
        if successful_file_ingested(
            conn,
            source_type="historical_ebay_sold",
            source_filename=csv_file.name,
            file_hash=file_hash,
        ):
            log_and_print(f"eBay sold-listing file already ingested. Skipping: {csv_file.name}")
            continue

        df = read_csv_file(csv_file, dtype=str)
        source_rows = len(df)
        missing_columns = [col for col in EXPECTED_EBAY_SOLD_COLUMNS if col not in df.columns]
        if missing_columns:
            stop_with_error(
                f"eBay sold-listing file {csv_file.name} is missing expected columns:\n"
                f"{missing_columns}\nExpected columns: {EXPECTED_EBAY_SOLD_COLUMNS}"
            )

        def _bool_col(series):
            return series.astype(str).str.strip().isin(["1", "true", "True", "TRUE"])

        rows_for_insert = pd.DataFrame({
            "item_number": df["item_number"],
            "title": df["title"],
            "price_eur": pd.to_numeric(df["price_eur"], errors="coerce"),
            "currency": df["currency"],
            "condition": df["condition"],
            "seller_type": df["seller_type"],
            "sold_date_iso": df["sold_date_iso"],
            "sold_date_raw": df["sold_date_raw"],
            "is_sold": _bool_col(df["is_sold"]),
            "shipping_eur": pd.to_numeric(df["shipping_eur"], errors="coerce"),
            "free_shipping": _bool_col(df["free_shipping"]),
            "best_offer": _bool_col(df["best_offer"]),
            "location": df["location"],
            "seller": df["seller"],
            "url": df["url"],
            "source_page": df["source_page"],
            "upload_batch_id": "",
            "source_filename": csv_file.name,
            "file_hash": file_hash,
            "ingested_at": ingested_at,
        })
        rows_for_insert["row_hash"] = rows_for_insert["item_number"].apply(lambda v: text_hash([v]))

        existing_row_hashes = set(
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT row_hash FROM raw_historical_ebay_sold WHERE row_hash IS NOT NULL AND row_hash <> ''"
            ).fetchall()
        )
        rows_for_insert = rows_for_insert.loc[~rows_for_insert["row_hash"].isin(existing_row_hashes)].copy()
        rows_for_insert = rows_for_insert.drop_duplicates(subset=["row_hash"], keep="first")
        rows_for_insert.insert(0, "id", range(start_id, start_id + len(rows_for_insert)))
        total_rows += len(rows_for_insert)

        conn.execute(
            """
            INSERT INTO raw_historical_ebay_sold (
                id, row_hash, item_number, title, price_eur, currency, condition, seller_type,
                sold_date_iso, sold_date_raw, is_sold, shipping_eur, free_shipping, best_offer,
                location, seller, url, source_page, upload_batch_id, source_filename, file_hash, ingested_at
            )
            SELECT
                id, row_hash, item_number, title, price_eur, currency, condition, seller_type,
                sold_date_iso, sold_date_raw, is_sold, shipping_eur, free_shipping, best_offer,
                location, seller, url, source_page, upload_batch_id, source_filename, file_hash, ingested_at
            FROM rows_for_insert
            """
        )
        log_ingestion(
            conn,
            source_type="historical_ebay_sold",
            source_filename=csv_file.name,
            file_hash=file_hash,
            upload_batch_id="",
            rows_inserted=len(rows_for_insert),
            status="success",
        )
        log_and_print(
            f"eBay sold-listing file {csv_file.name}: {source_rows:,} source rows, "
            f"{len(rows_for_insert):,} new rows inserted"
        )
        start_id += len(rows_for_insert)

    return total_rows


def insert_targeted_listings(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Guarantee: at-least-once ingestion execution with idempotent final
    database effects. This is NOT exactly-once and NOT fully transactional
    across the two writes involved — the INSERT INTO raw_active_targeted
    and the subsequent log_ingestion(status="success") are two separate,
    independently-auto-committed DuckDB statements (no explicit
    BEGIN/COMMIT wraps them together). A crash between those two commits
    is possible and leaves raw_active_targeted with the real rows already
    durable but ingestion_log NOT yet reflecting success for that file —
    the next run re-attempts that file (at-least-once execution), and the
    (inventory_uid, item_id, marketplace_id) row-level dedup below ensures
    that retry inserts zero duplicate rows (idempotent final effect), then
    correctly writes the success log entry it missed the first time. See
    test_ingestion_crash_between_raw_insert_and_log_commit_is_idempotent_on_retry
    in tests/test_collect_targeted_active.py for a direct reproduction.

    Ingest scripts/04_collect_targeted_active.py's CSV output. Idempotency
    strategy verified against both existing patterns before choosing this
    one (see Module 3 planning notes):
      - targeted_active_<batch>.csv files are distinct, immutable,
        uniquely-named snapshots — one per collection batch — matching
        insert_historical_exports' shape, not insert_current_listings'
        single-evolving-file shape. So file-level dedup via ingestion_log
        (source_type='targeted_active') skips a whole file that's already
        been ingested, exactly like historical exports.
      - The same real eBay listing can legitimately reappear across two
        different batch files (an item re-escalated on a later run), so
        row-level dedup against already-stored raw_active_targeted rows
        also applies, exactly like insert_current_listings' logic — this
        is the defense-in-depth layer a pure file-hash check alone would
        miss.

    Dedup key is (inventory_uid, item_id, marketplace_id), never
    (item_id, marketplace_id) alone: the same real eBay listing can
    legitimately be returned via more than one marketplace (preserved as
    separate rows, each marketplace's own price/currency/shipping
    observation kept), AND the same real eBay listing can legitimately
    surface as a candidate for two DIFFERENT inventory items sharing an
    overlapping query — that is two distinct evidence relationships, not
    one duplicated observation, and raw_active_targeted must never
    silently collapse either case into a single row. A global
    (item_id, marketplace_id)-only key previously discarded a later item's
    entire candidate set whenever an earlier item's query returned an
    overlapping listing first — confirmed data loss on real collected
    data, fixed here and in write_batch_csv (04_collect_targeted_active.py)
    together. Deduplicating the same item_id into a single valuation
    comparable is downstream matching's job (04_match.py, out of scope
    here), not raw ingestion's.

    KNOWN LIMITATION — cross-batch tier provenance is not upgraded once a
    triple has been ingested. escalate_for_marketplace() (04_collect_
    targeted_active.py) now retains the lowest (most specific) query_tier
    within a single collection run, but that guarantee is per-run only. If
    a (inventory_uid, item_id, marketplace_id) row was already ingested
    from an earlier batch at a HIGHER (less specific) tier, and a later
    batch's file contains the same triple resolved at a LOWER, more
    specific tier, the row-level dedup below skips the later file's row
    entirely — the existing raw_active_targeted row keeps its original,
    less-specific query_tier/query_text forever. Updating it in place
    would violate this table's raw-layer contract (raw_* = exact copy of
    source data, never modified — see schema.sql header) and is
    deliberately NOT implemented here. This is a real, currently
    undetected staleness risk for repeated collection runs against the
    same inventory item, left as an open item for Module 4/5 rather than
    silently patched by mutating raw data.
    """
    if not TARGETED_ACTIVE_DIR.exists():
        log_and_print("No targeted active collection files found.")
        return 0

    csv_files = sorted(TARGETED_ACTIVE_DIR.glob("targeted_active_*.csv"))
    if not csv_files:
        log_and_print("No targeted active collection files found.")
        return 0

    total_rows = 0
    start_id = next_id(conn, "raw_active_targeted")

    for csv_file in csv_files:
        file_hash = file_sha256(csv_file)
        if successful_file_ingested(
            conn,
            source_type="targeted_active",
            source_filename=csv_file.name,
            file_hash=file_hash,
        ):
            log_and_print(f"Targeted active file already ingested. Skipping: {csv_file.name}")
            continue

        df = read_csv_file(csv_file)
        source_rows = len(df)

        missing_columns = [col for col in EXPECTED_TARGETED_COLUMNS if col not in df.columns]
        if missing_columns:
            stop_with_error(
                f"Targeted active file {csv_file.name} is missing expected columns:\n"
                f"{missing_columns}\n"
                f"Expected columns: {EXPECTED_TARGETED_COLUMNS}"
            )

        rows_for_insert = df[EXPECTED_TARGETED_COLUMNS].copy()
        rows_for_insert["row_hash"] = rows_for_insert.apply(
            lambda row: text_hash(
                [
                    row.get("inventory_uid", ""),
                    row.get("item_id", ""),
                    row.get("title", ""),
                    row.get("price_value", ""),
                    row.get("price_currency", ""),
                    row.get("seller_username", ""),
                    row.get("marketplace_id", ""),
                ]
            ),
            axis=1,
        )

        existing_keys = set(
            (row[0], row[1], row[2])
            for row in conn.execute(
                "SELECT DISTINCT inventory_uid, item_id, marketplace_id FROM raw_active_targeted "
                "WHERE item_id IS NOT NULL AND item_id <> ''"
            ).fetchall()
        )
        existing_row_hashes = set(
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT row_hash FROM raw_active_targeted WHERE row_hash IS NOT NULL AND row_hash <> ''"
            ).fetchall()
        )

        has_item_id = rows_for_insert["item_id"].notna() & (rows_for_insert["item_id"].astype(str) != "")
        row_keys = list(zip(
            rows_for_insert["inventory_uid"].astype(str),
            rows_for_insert["item_id"].astype(str),
            rows_for_insert["marketplace_id"].astype(str),
        ))
        is_duplicate = (
            (has_item_id & pd.Series(row_keys, index=rows_for_insert.index).isin(existing_keys))
            | (~has_item_id & rows_for_insert["row_hash"].isin(existing_row_hashes))
        )
        rows_for_insert = rows_for_insert.loc[~is_duplicate].copy()
        rows_for_insert = rows_for_insert.drop_duplicates(subset=["inventory_uid", "item_id", "marketplace_id"], keep="first")

        rows_for_insert.insert(0, "id", range(start_id, start_id + len(rows_for_insert)))
        insert_cols = ["id"] + EXPECTED_TARGETED_COLUMNS + ["row_hash"]
        rows_for_insert = rows_for_insert[insert_cols]

        conn.register("tmp_targeted_insert", rows_for_insert)
        conn.execute(
            f"""
            INSERT INTO raw_active_targeted ({",".join(insert_cols)})
            SELECT {",".join(insert_cols)}
            FROM tmp_targeted_insert
            """
        )
        conn.unregister("tmp_targeted_insert")

        duplicate_rows = source_rows - len(rows_for_insert)
        log_ingestion(
            conn,
            source_type="targeted_active",
            source_filename=csv_file.name,
            file_hash=file_hash,
            upload_batch_id="",
            rows_inserted=len(rows_for_insert),
            status="success",
        )
        log_and_print(
            f"Targeted active file {csv_file.name}: {source_rows:,} source rows, "
            f"{len(rows_for_insert):,} new rows inserted, {duplicate_rows:,} duplicates skipped"
        )
        total_rows += len(rows_for_insert)
        start_id += len(rows_for_insert)

    return total_rows


MAX_DB_SIZE_BYTES = 500 * 1024 * 1024


def log_database_size() -> None:
    size_bytes = DB_PATH.stat().st_size
    size_mb = size_bytes / (1024 * 1024)
    log_and_print(f"Database file size: {size_mb:,.2f} MB")
    if size_bytes > MAX_DB_SIZE_BYTES:
        log_and_print(
            f"⚠ WARNING: database file size ({size_mb:,.2f} MB) exceeds 500 MB threshold."
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Watchparts raw-data ingestion.")
    parser.add_argument(
        "--targeted",
        action="store_true",
        help="Ingest only scripts/04_collect_targeted_active.py's CSV output into "
        "raw_active_targeted. Skips inventory/current-listings/historical ingestion.",
    )
    parser.add_argument(
        "--historical-ebay-sold",
        action="store_true",
        help="Ingest only the eBay item-wise sold-listing export(s) in "
        "data/raw/historical_exports_ebay_sold/ into raw_historical_ebay_sold. "
        "Isolated from the default flow and from --targeted — Module 4's second "
        "historical source is never concatenated with raw_historical (VCP aggregate).",
    )
    parser.add_argument(
        "--inventory-only",
        action="store_true",
        help="Ingest only data/raw/inventory.csv. Used by dashboard add/update jobs so they do "
        "not reprocess broad current listings or historical exports.",
    )
    parser.add_argument(
        "--db", default=None,
        help="Target DuckDB file. Overrides the WATCHPARTS_DB env var and the default "
        "live database. Use a disposable copy for tests/pilots so the live database is "
        "never touched.",
    )
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()

    if args.db:
        # Reassign the module global BEFORE any get_connection() call. Every
        # reference here reads DB_PATH at call time (get_connection has no
        # default-bound db_path parameter), so this cleanly redirects the run.
        global DB_PATH
        DB_PATH = Path(args.db)

    if args.targeted:
        log_and_print("=" * 60)
        log_and_print("WATCHPARTS — STEP 1: RAW INGESTION (targeted active listings only)")
        log_and_print("=" * 60)
        log_and_print(f"Database path: {DB_PATH}")

        conn = get_connection()
        try:
            targeted_rows = insert_targeted_listings(conn)
        finally:
            conn.close()

        log_and_print("")
        log_and_print("Success summary")
        log_and_print(f"Targeted active listing rows imported: {targeted_rows:,}")
        log_and_print(f"Database path: {DB_PATH}")
        log_and_print(f"Log file: {LOG_DIR / '01_ingest.log'}")
        log_database_size()
        log_and_print("✓ Raw ingestion complete.")
        return

    if args.historical_ebay_sold:
        log_and_print("=" * 60)
        log_and_print("WATCHPARTS — STEP 1: RAW INGESTION (eBay sold-listing exports only)")
        log_and_print("=" * 60)
        log_and_print(f"Database path: {DB_PATH}")

        conn = get_connection()
        try:
            ebay_sold_rows = insert_historical_ebay_sold_exports(conn)
        finally:
            conn.close()

        log_and_print("")
        log_and_print("Success summary")
        log_and_print(f"eBay sold-listing rows imported: {ebay_sold_rows:,}")
        log_and_print(f"Database path: {DB_PATH}")
        log_and_print(f"Log file: {LOG_DIR / '01_ingest.log'}")
        log_database_size()
        log_and_print("✓ Raw ingestion complete.")
        return

    if args.inventory_only:
        upload_batch_id = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"

        log_and_print("=" * 60)
        log_and_print("WATCHPARTS — STEP 1: RAW INGESTION (inventory only)")
        log_and_print("=" * 60)
        log_and_print(f"Batch ID: {upload_batch_id}")
        log_and_print(f"Database path: {DB_PATH}")

        conn = get_connection()
        try:
            inventory_rows = insert_inventory(conn, upload_batch_id)
        finally:
            conn.close()

        log_and_print("")
        log_and_print("Success summary")
        log_and_print(f"Inventory rows imported: {inventory_rows:,}")
        log_and_print("Current listing rows imported: 0")
        log_and_print("Historical rows imported: 0")
        log_and_print(f"Batch ID: {upload_batch_id}")
        log_and_print(f"Database path: {DB_PATH}")
        log_and_print(f"Log file: {LOG_DIR / '01_ingest.log'}")
        log_database_size()
        log_and_print("✓ Raw ingestion complete.")
        return

    upload_batch_id = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"

    log_and_print("=" * 60)
    log_and_print("WATCHPARTS — STEP 1: RAW INGESTION")
    log_and_print("=" * 60)
    log_and_print(f"Batch ID: {upload_batch_id}")
    log_and_print(f"Database path: {DB_PATH}")

    conn = get_connection()
    try:
        inventory_rows = insert_inventory(conn, upload_batch_id)
        current_rows = insert_current_listings(conn)
        historical_rows = insert_historical_exports(conn)
    finally:
        conn.close()

    log_and_print("")
    log_and_print("Success summary")
    log_and_print(f"Inventory rows imported: {inventory_rows:,}")
    log_and_print(f"Current listing rows imported: {current_rows:,}")
    log_and_print(f"Historical rows imported: {historical_rows:,}")
    log_and_print(f"Batch ID: {upload_batch_id}")
    log_and_print(f"Database path: {DB_PATH}")
    log_and_print(f"Log file: {LOG_DIR / '01_ingest.log'}")
    log_database_size()
    log_and_print("✓ Raw ingestion complete.")


if __name__ == "__main__":
    main()

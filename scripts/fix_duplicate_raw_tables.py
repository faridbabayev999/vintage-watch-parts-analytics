#!/usr/bin/env python3
"""
One-time raw table duplicate fixer.

This script:
- backs up raw_inventory, raw_active_broad, and raw_historical
- deduplicates raw_inventory by raw_rolex_tudor + raw_calibre + raw_p_number + raw_stock
- deduplicates raw_active_broad by item_id when item_id exists, otherwise row_hash
- deduplicates raw_historical by row_hash only when row_hash exists
- does not touch staging tables
"""

from __future__ import annotations

import hashlib
import shutil
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd


BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"
BACKUP_DB_PATH = BASE_DIR / "database" / f"watchparts_before_raw_dedup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.duckdb"


def text_hash(values: list[object]) -> str:
    safe_values = ["" if pd.isna(value) else str(value) for value in values]
    return hashlib.sha256("||".join(safe_values).encode("utf-8")).hexdigest()


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return bool(
        con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = ?
            """,
            [table_name],
        ).fetchone()[0]
    )


def table_count(con: duckdb.DuckDBPyConnection, table_name: str) -> int:
    if not table_exists(con, table_name):
        return 0
    return con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def backup_table(con: duckdb.DuckDBPyConnection, table_name: str, suffix: str) -> str | None:
    if not table_exists(con, table_name):
        return None
    backup_name = f"{table_name}_backup_{suffix}"
    con.execute(f"CREATE TABLE {backup_name} AS SELECT * FROM {table_name}")
    return backup_name


def replace_table_from_df(con: duckdb.DuckDBPyConnection, table_name: str, df: pd.DataFrame) -> None:
    con.execute(f"DROP TABLE {table_name}")
    con.register("dedup_df", df)
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM dedup_df")
    con.unregister("dedup_df")


def dedup_raw_inventory(con: duckdb.DuckDBPyConnection) -> tuple[int, int]:
    before = table_count(con, "raw_inventory")
    if before == 0:
        return before, before

    df = con.sql("SELECT * FROM raw_inventory").fetchdf()
    sort_columns = ["raw_rolex_tudor", "raw_calibre", "raw_p_number", "raw_stock"]
    df["_has_file_hash"] = df.get("file_hash", "").fillna("").astype(str) != ""
    df = df.sort_values(["_has_file_hash", "id"], ascending=[False, True])
    df = df.drop_duplicates(subset=sort_columns, keep="first")
    df = df.drop(columns=["_has_file_hash"])
    df = df.sort_values("id")
    replace_table_from_df(con, "raw_inventory", df)
    return before, len(df)


def dedup_raw_active_broad(con: duckdb.DuckDBPyConnection) -> tuple[int, int]:
    before = table_count(con, "raw_active_broad")
    if before == 0:
        return before, before

    df = con.sql("SELECT * FROM raw_active_broad").fetchdf()
    if "row_hash" not in df.columns:
        df["row_hash"] = ""

    df["row_hash"] = df.apply(
        lambda row: row["row_hash"]
        if pd.notna(row["row_hash"]) and str(row["row_hash"]) != ""
        else text_hash(
            [
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
    df["_dedup_key"] = df["item_id"].fillna("").astype(str)
    missing_item_id = df["_dedup_key"] == ""
    df.loc[missing_item_id, "_dedup_key"] = df.loc[missing_item_id, "row_hash"]
    df = df.sort_values("id").drop_duplicates(subset=["_dedup_key"], keep="first")
    df = df.drop(columns=["_dedup_key"])
    replace_table_from_df(con, "raw_active_broad", df)
    return before, len(df)


def dedup_raw_historical(con: duckdb.DuckDBPyConnection) -> tuple[int, int, str]:
    before = table_count(con, "raw_historical")
    if before == 0:
        return before, before, "empty"

    df = con.sql("SELECT * FROM raw_historical").fetchdf()
    if "row_hash" not in df.columns or df["row_hash"].fillna("").astype(str).eq("").all():
        return before, before, "left untouched because no stable row_hash exists"

    df = df.sort_values("id").drop_duplicates(subset=["row_hash"], keep="first")
    replace_table_from_df(con, "raw_historical", df)
    return before, len(df), "deduplicated by row_hash"


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"Database not found: {DB_PATH}")

    shutil.copy2(DB_PATH, BACKUP_DB_PATH)
    print(f"Database file backup created: {BACKUP_DB_PATH}")

    suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
    con = duckdb.connect(str(DB_PATH))
    try:
        backup_tables = []
        for table in ["raw_inventory", "raw_active_broad", "raw_historical"]:
            backup_name = backup_table(con, table, suffix)
            if backup_name:
                backup_tables.append(backup_name)
        print("Backup tables created:")
        for table in backup_tables:
            print(f"  {table}")

        inv_before, inv_after = dedup_raw_inventory(con)
        active_before, active_after = dedup_raw_active_broad(con)
        hist_before, hist_after, hist_note = dedup_raw_historical(con)

        print()
        print("Before/after counts:")
        print(f"raw_inventory: {inv_before:,} -> {inv_after:,}")
        print(f"raw_active_broad: {active_before:,} -> {active_after:,}")
        print(f"raw_historical: {hist_before:,} -> {hist_after:,} ({hist_note})")
        print()
        print("Staging tables were not touched.")
    finally:
        con.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Describe one table in database/watchparts.duckdb."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"


def main() -> None:
    parser = argparse.ArgumentParser(description="Describe a DuckDB table.")
    parser.add_argument("table", help="Table name, for example raw_inventory.")
    args = parser.parse_args()

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        df = con.sql(f"DESCRIBE {args.table}").fetchdf()
        print(df.to_string(index=False))
    finally:
        con.close()


if __name__ == "__main__":
    main()

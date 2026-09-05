#!/usr/bin/env python3
"""Run a SQL query against database/watchparts.duckdb."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SQL against the Watchparts DuckDB database.")
    parser.add_argument("sql", help="SQL query to run. Put it in quotes.")
    parser.add_argument("--limit", type=int, default=50, help="Maximum rows to print for SELECT queries.")
    args = parser.parse_args()

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 240)
    pd.set_option("display.max_colwidth", 120)

    read_only = args.sql.strip().lower().startswith(("select", "show", "describe", "with"))
    con = duckdb.connect(str(DB_PATH), read_only=read_only)
    try:
        result = con.sql(args.sql)
        if args.sql.strip().lower().startswith(("select", "show", "describe", "with")):
            df = result.limit(args.limit).fetchdf()
            print(df.to_string(index=False))
        else:
            print("Query executed successfully.")
    finally:
        con.close()


if __name__ == "__main__":
    main()

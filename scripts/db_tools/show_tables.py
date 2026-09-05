#!/usr/bin/env python3
"""Show all tables in database/watchparts.duckdb."""

from __future__ import annotations

from pathlib import Path

import duckdb


BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"


def main() -> None:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        df = con.sql("SHOW TABLES").fetchdf()
        print(df.to_string(index=False))
    finally:
        con.close()


if __name__ == "__main__":
    main()

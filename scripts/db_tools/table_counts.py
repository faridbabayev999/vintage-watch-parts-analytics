#!/usr/bin/env python3
"""Print row counts for every table in database/watchparts.duckdb."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"


def main() -> None:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = [row[0] for row in con.sql("SHOW TABLES").fetchall()]
        rows = []
        for table in tables:
            count = con.sql(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            rows.append({"table": table, "rows": count})
        df = pd.DataFrame(rows).sort_values("table")
        print(df.to_string(index=False))
    finally:
        con.close()


if __name__ == "__main__":
    main()

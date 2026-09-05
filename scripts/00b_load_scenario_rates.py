"""
00b_load_scenario_rates.py
===========================
Seeds ref_shipping_rates / ref_customs_rates / ref_tax_rates for Module 6
(TMV scenario engine, scripts/15_scenario_engine.py). Same discipline as
scripts/00_load_fx_rates.py: reference DATA, not code constants -- every rate
is dated and carries a `source` citation, none is a bare number buried in
calculation logic.

Values are the same sourced figures Implementation B's scenario_landed()
already cited (not fabricated here) -- reused as DATA under the "pure math/
sourced values are reusable, B's identity model is not" rule
(docs/TMV_B_AUDIT_AND_A_NATIVE_DESIGN.md). B's own comments already flagged
the customs rate as a DEFAULT needing confirmation; that caveat is carried
into the `source` column verbatim, not silently dropped.

Idempotent: re-running with the same valid_from is a primary-key upsert
(INSERT OR REPLACE), never a duplicate row.

Usage:
    python scripts/00b_load_scenario_rates.py
    python scripts/00b_load_scenario_rates.py --db /tmp/copy.duckdb
"""
from __future__ import annotations

import argparse
import os
from datetime import date
from pathlib import Path

import duckdb

BASE_DIR = Path(__file__).parent.parent
DEFAULT_DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"
SCHEMA_PATH = BASE_DIR / "scripts" / "schema.sql"

VALID_FROM = date(2026, 1, 1)  # effective date for this initial seed

SHIPPING_ROWS = [
    # country, shipping_cost, currency, source
    ("DE", 5.0, "EUR", "Project baseline flat-rate assumption (Module 1 utils.py original SHIPPING_DE_EUR), unconfirmed with a carrier"),
    ("US", 25.0, "EUR", "Project baseline flat-rate assumption (Module 1 utils.py original SHIPPING_US_EUR), unconfirmed with a carrier"),
]

CUSTOMS_ROWS = [
    # hs_code, country, duty_rate, source
    ("9114.90", "US", 0.03, "DEFAULT ad-valorem duty for HS 9114.90 (other clock/watch parts) -- DEFAULT, confirm exact subheading at hts.usitc.gov (carried from Implementation B's own comment)"),
    ("9114.90", "DE", 0.0, "intra-EU / domestic destination -- no import duty applies"),
]

TAX_ROWS = [
    # country, tax_type, rate, source
    ("US", "sales_tax", 0.0975, "Beverly Hills ZIP 90210 combined rate (6% CA + 0.25% LA Co + 3.5% special) -- SalesTaxHandbook/CDTFA 2026 (carried from Implementation B's own comment); a single US rate is a simplification -- real US sales tax varies by destination state/ZIP"),
    ("DE", "import_tax", 0.0, "intra-EU / domestic destination -- no import tax applies"),
]


def resolve_db_path(db_path=None) -> Path:
    if db_path:
        return Path(db_path)
    if os.environ.get("WATCHPARTS_DB"):
        return Path(os.environ["WATCHPARTS_DB"])
    return DEFAULT_DB_PATH


def load(conn) -> dict:
    conn.execute(SCHEMA_PATH.read_text())
    for country, cost, ccy, source in SHIPPING_ROWS:
        conn.execute("""
            INSERT INTO ref_shipping_rates (country, shipping_cost, currency, valid_from, source)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (country, valid_from) DO UPDATE SET
                shipping_cost = excluded.shipping_cost, currency = excluded.currency, source = excluded.source
        """, [country, cost, ccy, VALID_FROM, source])
    for hs_code, country, rate, source in CUSTOMS_ROWS:
        conn.execute("""
            INSERT INTO ref_customs_rates (hs_code, country, duty_rate, valid_from, source)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (hs_code, country, valid_from) DO UPDATE SET
                duty_rate = excluded.duty_rate, source = excluded.source
        """, [hs_code, country, rate, VALID_FROM, source])
    for country, tax_type, rate, source in TAX_ROWS:
        conn.execute("""
            INSERT INTO ref_tax_rates (country, tax_type, rate, valid_from, source)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (country, tax_type, valid_from) DO UPDATE SET
                rate = excluded.rate, source = excluded.source
        """, [country, tax_type, rate, VALID_FROM, source])
    return {
        "shipping_rows": len(SHIPPING_ROWS),
        "customs_rows": len(CUSTOMS_ROWS),
        "tax_rows": len(TAX_ROWS),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None)
    args = ap.parse_args()
    db_path = resolve_db_path(args.db)
    conn = duckdb.connect(str(db_path))
    try:
        summary = load(conn)
    finally:
        conn.close()
    print(f"Database target: {db_path}")
    print(f"Shipping rates loaded: {summary['shipping_rows']}")
    print(f"Customs rates loaded: {summary['customs_rows']}")
    print(f"Tax rates loaded: {summary['tax_rows']}")


if __name__ == "__main__":
    main()

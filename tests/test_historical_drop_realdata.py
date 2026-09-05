"""
Real-data-path verification for scripts/15_historical_drop_ingest.py.

Unlike test_historical_drop_ingest.py (fake ingest01), this drives the REAL
01_ingest.py ingestion end to end on a disposable DuckDB with synthetic files
matching the exact production export schemas (EXPECTED_EBAY_SOLD_COLUMNS / VCP).
Verifies actual row insertion into the correct raw tables, provenance/currency/
market preservation, and hash-based idempotency on a second run.
"""
import importlib.util
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
SCHEMA = SCRIPTS / "schema.sql"


def _load(name, fname):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / fname)
    m = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPTS))
    spec.loader.exec_module(m)
    return m


EBAY_COLS = ["item_number", "title", "price_eur", "currency", "condition", "seller_type",
             "sold_date_iso", "sold_date_raw", "is_sold", "shipping_eur", "free_shipping",
             "best_offer", "location", "seller", "url", "source_page"]


def _ebay_file(path):
    pd.DataFrame([
        # EU (EUR) confirmed sale
        {"item_number": "111", "title": "Rolex 3135 escape wheel", "price_eur": "180.00",
         "currency": "EUR", "condition": "Used", "seller_type": "business",
         "sold_date_iso": "2025-06-01", "sold_date_raw": "Jun 1, 2025", "is_sold": "true",
         "shipping_eur": "8.00", "free_shipping": "false", "best_offer": "false",
         "location": "Germany", "seller": "watchparts_de", "url": "http://x/111", "source_page": "1"},
        # US (USD) best-offer sale
        {"item_number": "222", "title": "Rolex 3135 balance", "price_eur": "210.00",
         "currency": "USD", "condition": "Used", "seller_type": "private",
         "sold_date_iso": "2025-05-15", "sold_date_raw": "May 15, 2025", "is_sold": "true",
         "shipping_eur": "12.00", "free_shipping": "false", "best_offer": "true",
         "location": "United States", "seller": "us_seller", "url": "http://x/222", "source_page": "1"},
    ], columns=EBAY_COLS).to_csv(path, index=False)


def _vcp_file(path):
    pd.DataFrame([
        {"title": "Rolex 3135 mainspring", "avg_price_eur": "95.00", "format": "FixedPrice",
         "avg_shipping_eur": "6.00", "free_shipping_pct": "10", "total_sold": "12",
         "total_sales_eur": "1140.00", "last_sold": "2025-06-10", "bids": "0", "removed": "false"},
    ]).to_csv(path, index=False)


@pytest.fixture
def env(tmp_path, monkeypatch):
    ingest01 = _load("ingest01_rd", "01_ingest.py")
    drop15 = _load("drop15_rd", "15_historical_drop_ingest.py")
    # redirect ingest export dirs + drop-folder side dirs to tmp (never touch real data/)
    monkeypatch.setattr(ingest01, "EBAY_SOLD_EXPORTS_DIR", tmp_path / "ex_ebay")
    monkeypatch.setattr(ingest01, "HISTORICAL_EXPORTS_DIR", tmp_path / "ex_vcp")
    monkeypatch.setattr(drop15, "PROCESSED_DIR", tmp_path / "processed")
    monkeypatch.setattr(drop15, "REJECTED_DIR", tmp_path / "rejected")
    db = tmp_path / "rd.duckdb"
    duckdb.connect(str(db)).execute(SCHEMA.read_text())
    # CRITICAL: pin ingest01's resolved DB_PATH to the disposable DB. The module
    # resolved DB_PATH at import (from env at that instant), so setting an env
    # var now would be too late and get_connection() would hit the LIVE DB.
    # get_connection() reads the module global at call time, so this redirects it.
    monkeypatch.setattr(ingest01, "DB_PATH", db)
    monkeypatch.setenv("WATCHPARTS_DB", str(db))
    drop = tmp_path / "drop"; drop.mkdir()
    return ingest01, drop15, db, drop


def _counts(db):
    c = duckdb.connect(str(db), read_only=True)
    out = {
        "ebay": c.execute("SELECT COUNT(*) FROM raw_historical_ebay_sold").fetchone()[0],
        "vcp": c.execute("SELECT COUNT(*) FROM raw_historical").fetchone()[0],
        "log": c.execute("SELECT COUNT(*) FROM ingestion_log WHERE status='success'").fetchone()[0],
    }
    c.close()
    return out


def test_real_data_path_ingests_both_sources(env):
    ingest01, drop15, db, drop = env
    _ebay_file(drop / "ebay_sold_export.csv")
    _vcp_file(drop / "vcp_export.csv")

    conn = ingest01.get_connection()
    summary = drop15.process_drop_folder(conn, ingest01, drop_dir=drop)
    conn.close()

    assert {e["source_type"] for e in summary["ingested"]} == {"ebay_sold", "vcp"}
    assert summary["rejected"] == []
    counts = _counts(db)
    assert counts["ebay"] == 2, f"expected 2 eBay rows, got {counts['ebay']}"
    assert counts["vcp"] == 1, f"expected 1 VCP row, got {counts['vcp']}"

    # provenance/currency/market preserved
    c = duckdb.connect(str(db), read_only=True)
    usd = c.execute("SELECT currency, location FROM raw_historical_ebay_sold WHERE item_number='222'").fetchone()
    assert usd[0] == "USD" and "United States" in (usd[1] or "")
    eur = c.execute("SELECT currency, best_offer FROM raw_historical_ebay_sold WHERE item_number='111'").fetchone()
    assert eur[0] == "EUR"
    c.close()

    # archived out of the drop folder
    assert not (drop / "ebay_sold_export.csv").exists()
    assert (drop15.PROCESSED_DIR / "ebay_sold_export.csv").exists()


def test_real_data_path_idempotent_second_run(env):
    ingest01, drop15, db, drop = env
    _ebay_file(drop / "ebay_sold_export.csv")
    _vcp_file(drop / "vcp_export.csv")
    # run 1
    conn = ingest01.get_connection()
    drop15.process_drop_folder(conn, ingest01, drop_dir=drop)
    conn.close()
    first = _counts(db)
    assert first["ebay"] == 2 and first["vcp"] == 1
    # drop the SAME files again
    _ebay_file(drop / "ebay_sold_export.csv")
    _vcp_file(drop / "vcp_export.csv")
    conn = ingest01.get_connection()
    summary = drop15.process_drop_folder(conn, ingest01, drop_dir=drop)
    conn.close()
    second = _counts(db)
    assert second["ebay"] == 2 and second["vcp"] == 1, "idempotency: second run must not duplicate rows"
    # BOTH sources must be reported as skipped (VCP source_type maps to 'historical')
    skipped = {s["file"] for s in summary["skipped"]}
    assert skipped == {"ebay_sold_export.csv", "vcp_export.csv"}
    assert summary["ingested"] == []


def test_real_data_path_rejects_bad_schema(env):
    ingest01, drop15, db, drop = env
    pd.DataFrame({"foo": [1], "bar": [2]}).to_csv(drop / "junk.csv", index=False)
    conn = ingest01.get_connection()
    summary = drop15.process_drop_folder(conn, ingest01, drop_dir=drop)
    conn.close()
    assert summary["ingested"] == [] and summary["rejected"][0]["file"] == "junk.csv"
    assert _counts(db)["ebay"] == 0

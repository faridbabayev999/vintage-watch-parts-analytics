"""
tests/test_historical_ingestion.py
====================================
Pytest tests for historical ingestion (scripts/01_ingest.py::insert_historical_exports)
and historical cleaning (scripts/02_clean.py::clean_historical), covering the
Module 4 pre-implementation foundation:

  - row-level source provenance (original_source_file) preserved separately
    from the physical container filename (physical_container_file)
  - deterministic file hashing and idempotent same-file reingestion
  - separate provenance for different container files
  - German date parsing, including malformed input
  - aggregate arithmetic (total_sales_eur ~= avg_price_eur * total_sold)
    surviving ingestion + cleaning unchanged
  - FX (EUR->USD) ASOF-join normalization, including the earliest-rate fallback
  - legitimate repeated snapshots (same title, different price/date) not
    being silently deduplicated away

Every test runs against an isolated on-disk DuckDB file under pytest's own
tmp_path, and an isolated HISTORICAL_EXPORTS_DIR monkeypatched onto the
01_ingest module — never database/watchparts.duckdb, never
data/raw/historical_exports/. A module-scoped autouse fixture hashes/stats
the real project files before and after this file's tests run and fails
loudly if any of them changed.

This file does NOT implement or exercise: historical acquisition
orchestration, matching, TMV, turnover, scraping, or any live eBay call.
"""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pytest

TESTS_DIR = Path(__file__).parent
BASE_DIR = TESTS_DIR.parent
SCRIPTS_DIR = BASE_DIR / "scripts"
SCHEMA_PATH = SCRIPTS_DIR / "schema.sql"


def _load_module(name: str, path: Path):
    sys.path.insert(0, str(SCRIPTS_DIR))
    sys.path.insert(0, str(BASE_DIR))
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ingest01 = _load_module("ingest01_hist", SCRIPTS_DIR / "01_ingest.py")
clean02 = _load_module("clean02_hist", SCRIPTS_DIR / "02_clean.py")


HISTORICAL_CSV_COLUMNS = [
    "title", "avg_price_eur", "format", "avg_shipping_eur", "free_shipping_pct",
    "total_sold", "total_sales_eur", "last_sold", "bids", "removed", "source_file",
]


def _write_historical_csv(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    for col in HISTORICAL_CSV_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[HISTORICAL_CSV_COLUMNS]
    df.to_csv(path, index=False)
    return path


def _file_digest(path: Path) -> str | None:
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ── Isolation guard ────────────────────────────────────────────────────────────

@pytest.fixture(scope="module", autouse=True)
def guard_production_untouched():
    real_db = ingest01.DB_PATH
    real_exports_dir = ingest01.HISTORICAL_EXPORTS_DIR
    real_ingest_log = ingest01.LOG_DIR / "01_ingest.log"
    real_clean_log = clean02.LOG_DIR / "02_clean.log"

    def _listing(directory: Path) -> set[str]:
        return set(p.name for p in directory.iterdir()) if directory.exists() else set()

    before = {
        "db": _file_digest(real_db),
        "exports_dir_listing": _listing(real_exports_dir),
        "ingest_log_mtime": real_ingest_log.stat().st_mtime if real_ingest_log.exists() else None,
        "clean_log_mtime": real_clean_log.stat().st_mtime if real_clean_log.exists() else None,
    }
    yield
    after = {
        "db": _file_digest(real_db),
        "exports_dir_listing": _listing(real_exports_dir),
        "ingest_log_mtime": real_ingest_log.stat().st_mtime if real_ingest_log.exists() else None,
        "clean_log_mtime": real_clean_log.stat().st_mtime if real_clean_log.exists() else None,
    }
    assert before == after, "A test touched the production database, historical exports dir, or logs — isolation is broken"


@pytest.fixture()
def db_path(tmp_path):
    path = tmp_path / "test.duckdb"
    assert path.resolve() != ingest01.DB_PATH.resolve()
    return path


@pytest.fixture()
def conn(db_path):
    connection = duckdb.connect(str(db_path))
    connection.execute(SCHEMA_PATH.read_text())
    yield connection
    connection.close()


@pytest.fixture()
def isolated_exports_dir(tmp_path, monkeypatch):
    exports_dir = tmp_path / "historical_exports"
    exports_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(ingest01, "HISTORICAL_EXPORTS_DIR", exports_dir)
    return exports_dir


# ── 1/2. Row-level provenance vs. physical container filename ─────────────────

def test_row_level_provenance_preserved_separately_from_container_filename(conn, isolated_exports_dir):
    csv_path = _write_historical_csv(isolated_exports_dir / "terapeak_sold_last.csv", [
        dict(title="Rolex part A", avg_price_eur=50.0, format="Festpreis", avg_shipping_eur=5.0,
             free_shipping_pct=0, total_sold=1, total_sales_eur=50.0, last_sold="7. Aug 2025",
             bids="-", removed="FALSE", source_file="terapeak_rolex_caliber_p1.html"),
        dict(title="Rolex part B", avg_price_eur=80.0, format="Festpreis", avg_shipping_eur=6.0,
             free_shipping_pct=0, total_sold=2, total_sales_eur=160.0, last_sold="17. Nov 2023",
             bids="-", removed="FALSE", source_file="terapeak_rolex_original_p2.html"),
    ])

    n = ingest01.insert_historical_exports(conn)
    assert n == 2

    rows = conn.execute(
        "SELECT title, source_file, original_source_file, physical_container_file FROM raw_historical ORDER BY title"
    ).fetchall()
    assert rows[0] == ("Rolex part A", csv_path.name, "terapeak_rolex_caliber_p1.html", csv_path.name)
    assert rows[1] == ("Rolex part B", csv_path.name, "terapeak_rolex_original_p2.html", csv_path.name)

    # The physical container filename must never equal the row's own genuine
    # provenance here — proving the two concepts were not collapsed back
    # into one value (the exact bug this pass fixes).
    for _, _, original, container in rows:
        assert original != container


def test_physical_container_filename_correct_across_multiple_files(conn, isolated_exports_dir):
    _write_historical_csv(isolated_exports_dir / "export_a.csv", [
        dict(title="Item from file A", avg_price_eur=10.0, total_sold=1, total_sales_eur=10.0,
             last_sold="1. Jan 2024", source_file="terapeak_rolex_p1.html"),
    ])
    _write_historical_csv(isolated_exports_dir / "export_b.csv", [
        dict(title="Item from file B", avg_price_eur=20.0, total_sold=1, total_sales_eur=20.0,
             last_sold="2. Feb 2024", source_file="terapeak_rolex_p1.html"),
    ])

    n = ingest01.insert_historical_exports(conn)
    assert n == 2

    row_a = conn.execute(
        "SELECT physical_container_file FROM raw_historical WHERE title = 'Item from file A'"
    ).fetchone()
    row_b = conn.execute(
        "SELECT physical_container_file FROM raw_historical WHERE title = 'Item from file B'"
    ).fetchone()
    assert row_a[0] == "export_a.csv"
    assert row_b[0] == "export_b.csv"


def test_missing_row_level_provenance_falls_back_to_container_filename(conn, isolated_exports_dir):
    """'Valid row-level provenance' means the CSV's own source_file value is
    present and non-empty for that row. A row with a blank/whitespace-only
    source_file must fall back to the physical container filename — but only
    that row, not its siblings that do have genuine provenance."""
    csv_path = _write_historical_csv(isolated_exports_dir / "mixed_provenance.csv", [
        dict(title="Has real provenance", avg_price_eur=30.0, total_sold=1, total_sales_eur=30.0,
             last_sold="3. Mrz 2024", source_file="terapeak_rolex_movement_p1.html"),
        dict(title="Blank provenance", avg_price_eur=40.0, total_sold=1, total_sales_eur=40.0,
             last_sold="4. Apr 2024", source_file="   "),
    ])

    ingest01.insert_historical_exports(conn)

    real = conn.execute(
        "SELECT original_source_file FROM raw_historical WHERE title = 'Has real provenance'"
    ).fetchone()
    blank = conn.execute(
        "SELECT original_source_file FROM raw_historical WHERE title = 'Blank provenance'"
    ).fetchone()
    assert real[0] == "terapeak_rolex_movement_p1.html"
    assert blank[0] == csv_path.name, "a genuinely blank row-level source_file must fall back to the container filename"


def test_brand_and_keyword_not_derived_from_physical_container_filename(conn, isolated_exports_dir):
    """The container filename ('terapeak_sold_last.csv') must never be fed to
    extract_brand/extract_keyword when valid row-level provenance exists —
    that filename doesn't match the brand_keyword_pN.html pattern those
    functions parse, and would silently produce garbage (e.g. brand='Sold')."""
    _seed_fx_rates(conn, [("2020-01-01", 1.10)])
    _write_historical_csv(isolated_exports_dir / "terapeak_sold_last.csv", [
        dict(title="Rolex caliber part", avg_price_eur=65.1, format="Festpreis", avg_shipping_eur=10.91,
             free_shipping_pct=0, total_sold=3, total_sales_eur=195.28, last_sold="7. Aug 2025",
             bids="-", removed="FALSE", source_file="terapeak_rolex_caliber_p1.html"),
    ])
    ingest01.insert_historical_exports(conn)
    clean02.clean_historical(conn)

    row = conn.execute("SELECT brand, search_keyword FROM stg_historical WHERE title = 'Rolex caliber part'").fetchone()
    assert row[0] == "Rolex", f"expected brand derived from genuine row-level provenance, got {row[0]!r}"
    assert row[1] == "caliber", f"expected search_keyword derived from genuine row-level provenance, got {row[1]!r}"


# ── 3/4. Deterministic hashing and idempotent reingestion ─────────────────────

def test_file_hash_is_deterministic(isolated_exports_dir):
    csv_path = _write_historical_csv(isolated_exports_dir / "det_hash.csv", [
        dict(title="X", avg_price_eur=1.0, total_sold=1, total_sales_eur=1.0, last_sold="1. Jan 2024"),
    ])
    h1 = ingest01.file_sha256(csv_path)
    h2 = ingest01.file_sha256(csv_path)
    assert h1 == h2
    assert len(h1) == 64  # sha256 hex digest


def test_idempotent_same_file_reingestion(conn, isolated_exports_dir):
    _write_historical_csv(isolated_exports_dir / "repeat_ingest.csv", [
        dict(title="Idempotency item", avg_price_eur=99.0, total_sold=1, total_sales_eur=99.0,
             last_sold="5. Mai 2024", source_file="terapeak_rolex_p5.html"),
    ])

    first = ingest01.insert_historical_exports(conn)
    second = ingest01.insert_historical_exports(conn)

    assert first == 1
    assert second == 0, "re-ingesting the same, unchanged file must insert zero new rows"
    total = conn.execute("SELECT COUNT(*) FROM raw_historical").fetchone()[0]
    assert total == 1

    log_rows = conn.execute(
        "SELECT COUNT(*) FROM ingestion_log WHERE source_type='historical' AND status='success'"
    ).fetchone()[0]
    assert log_rows == 1, "a skipped re-ingestion must not create a second success log entry"


# ── 5. Separate provenance for different container files ──────────────────────

def test_separate_provenance_preserved_across_different_container_files(conn, isolated_exports_dir):
    """Two different container files may legitimately share the same
    row-level (original_source_file) provenance value (e.g. the same search
    page name, re-exported at a different time) — both rows must survive,
    each correctly tagged with its OWN physical_container_file."""
    _write_historical_csv(isolated_exports_dir / "batch_2024.csv", [
        dict(title="Recurring search result", avg_price_eur=45.0, total_sold=1, total_sales_eur=45.0,
             last_sold="1. Jun 2024", source_file="terapeak_rolex_original_p1.html"),
    ])
    _write_historical_csv(isolated_exports_dir / "batch_2025.csv", [
        dict(title="Recurring search result", avg_price_eur=55.0, total_sold=1, total_sales_eur=55.0,
             last_sold="1. Jun 2025", source_file="terapeak_rolex_original_p1.html"),
    ])

    n = ingest01.insert_historical_exports(conn)
    assert n == 2, "both rows are distinct real observations and must both survive"

    rows = conn.execute(
        "SELECT physical_container_file, original_source_file, last_sold FROM raw_historical "
        "WHERE title = 'Recurring search result' ORDER BY physical_container_file"
    ).fetchall()
    assert [r[0] for r in rows] == ["batch_2024.csv", "batch_2025.csv"]
    assert all(r[1] == "terapeak_rolex_original_p1.html" for r in rows)


# ── 6/7. German date parsing ───────────────────────────────────────────────────

@pytest.mark.parametrize("raw, expected", [
    ("7. Aug 2025", "2025-08-07"),
    ("2. Mrz 2026", "2026-03-02"),
    ("17. Nov 2023", "2023-11-17"),
    ("1. Okt 2023", "2023-10-01"),
    ("31. Dez 2024", "2024-12-31"),
])
def test_german_date_parsing_valid_inputs(raw, expected):
    assert clean02.parse_german_date(raw) == expected


@pytest.mark.parametrize("raw", [None, "", "-", "nan", "not a date", "99. Foo 2024", "  "])
def test_german_date_parsing_malformed_inputs_return_none(raw):
    assert clean02.parse_german_date(raw) is None


# ── 8. Aggregate arithmetic consistency survives ingestion + cleaning ─────────

def test_aggregate_arithmetic_consistency_survives_pipeline(conn, isolated_exports_dir):
    """total_sales_eur ~= avg_price_eur * total_sold must still hold, and the
    exact raw values must still be recoverable, after ingestion + cleaning —
    proving neither step corrupts the aggregate arithmetic via type
    coercion, rounding, or column mixups."""
    _seed_fx_rates(conn, [("2020-01-01", 1.10)])
    _write_historical_csv(isolated_exports_dir / "arithmetic.csv", [
        dict(title="Consistent aggregate", avg_price_eur=65.10, format="Festpreis", avg_shipping_eur=10.91,
             free_shipping_pct=0, total_sold=3, total_sales_eur=195.30, last_sold="7. Aug 2025",
             bids="-", removed="FALSE", source_file="terapeak_rolex_p1.html"),
    ])
    ingest01.insert_historical_exports(conn)
    clean02.clean_historical(conn)

    row = conn.execute(
        "SELECT avg_price_eur, total_sold, total_sales_eur FROM stg_historical WHERE title = 'Consistent aggregate'"
    ).fetchone()
    avg_price_eur, total_sold, total_sales_eur = row
    assert avg_price_eur == 65.10
    assert total_sold == 3
    assert total_sales_eur == 195.30
    expected = avg_price_eur * total_sold
    assert abs(total_sales_eur - expected) < 0.01, (
        f"total_sales_eur ({total_sales_eur}) no longer consistent with avg_price_eur*total_sold ({expected})"
    )


# ── 9. FX normalization behavior ───────────────────────────────────────────────

def _seed_fx_rates(conn, rates: list[tuple[str, float]]) -> None:
    for valid_date, rate in rates:
        conn.execute(
            "INSERT INTO ref_exchange_rates (from_currency, to_currency, rate, valid_date, source) "
            "VALUES ('EUR', 'USD', ?, ?, 'test')",
            [rate, valid_date],
        )


def test_fx_asof_join_uses_rate_on_or_before_sale_date(conn, isolated_exports_dir):
    _seed_fx_rates(conn, [("2025-08-01", 1.10), ("2025-08-10", 1.20)])
    _write_historical_csv(isolated_exports_dir / "fx_test.csv", [
        dict(title="FX mid-window item", avg_price_eur=100.0, total_sold=1, total_sales_eur=100.0,
             last_sold="5. Aug 2025", source_file="terapeak_rolex_p1.html"),
    ])
    ingest01.insert_historical_exports(conn)
    clean02.clean_historical(conn)

    row = conn.execute(
        "SELECT eur_usd_rate_used, fx_rate_is_fallback, avg_price_usd FROM stg_historical "
        "WHERE title = 'FX mid-window item'"
    ).fetchone()
    rate_used, is_fallback, price_usd = row
    assert rate_used == 1.10, "ASOF join must use the most recent rate ON OR BEFORE the sale date, not the next one"
    assert is_fallback is False
    assert price_usd == round(100.0 * 1.10, 2)


def test_fx_fallback_used_when_date_predates_earliest_rate(conn, isolated_exports_dir):
    _seed_fx_rates(conn, [("2025-08-01", 1.10), ("2025-08-10", 1.20)])
    _write_historical_csv(isolated_exports_dir / "fx_fallback_test.csv", [
        dict(title="FX before earliest rate", avg_price_eur=50.0, total_sold=1, total_sales_eur=50.0,
             last_sold="1. Jan 2024", source_file="terapeak_rolex_p1.html"),
    ])
    ingest01.insert_historical_exports(conn)
    clean02.clean_historical(conn)

    row = conn.execute(
        "SELECT eur_usd_rate_used, fx_rate_is_fallback FROM stg_historical WHERE title = 'FX before earliest rate'"
    ).fetchone()
    rate_used, is_fallback = row
    assert is_fallback is True, "a date older than the earliest fetched rate must be flagged as a fallback, not silently NULL"
    assert rate_used == 1.10, "the fallback must use the earliest available rate, not an arbitrary default"


def test_fx_fallback_used_when_date_unparseable(conn, isolated_exports_dir):
    _seed_fx_rates(conn, [("2025-08-01", 1.10)])
    _write_historical_csv(isolated_exports_dir / "fx_unparseable_date.csv", [
        dict(title="Unparseable date item", avg_price_eur=20.0, total_sold=1, total_sales_eur=20.0,
             last_sold="not a real date", source_file="terapeak_rolex_p1.html"),
    ])
    ingest01.insert_historical_exports(conn)
    clean02.clean_historical(conn)

    row = conn.execute(
        "SELECT last_sold_date, eur_usd_rate_used, fx_rate_is_fallback FROM stg_historical "
        "WHERE title = 'Unparseable date item'"
    ).fetchone()
    last_sold_date, rate_used, is_fallback = row
    assert last_sold_date is None
    assert is_fallback is True
    assert rate_used == 1.10


# ── 10. Legitimate repeated snapshots are not deduplicated away ────────────────

def test_legitimate_repeated_snapshot_same_title_different_observation_survives(conn, isolated_exports_dir):
    """Two rows can legitimately share (title, source_file) while
    representing genuinely different re-observed snapshots (different
    price/total_sold/last_sold) — exactly the real pattern found in the live
    Verkaufer Cockpit export. Both must survive; only a truly identical row
    (same title, price, total_sales_eur, last_sold, source_file) is a
    duplicate."""
    _write_historical_csv(isolated_exports_dir / "repeated_snapshots.csv", [
        dict(title="Original ROLEX Unruhe vintage Balance", avg_price_eur=389.44, total_sold=2,
             total_sales_eur=778.87, last_sold="18. Jan 2026", source_file="terapeak_rolex_ersatzteil_p1.html"),
        dict(title="Original ROLEX Unruhe vintage Balance", avg_price_eur=345.80, total_sold=1,
             total_sales_eur=345.80, last_sold="29. Sep 2023", source_file="terapeak_rolex_ersatzteil_p1.html"),
        # a true duplicate of the first row (identical in every hashed field) — this one must be dropped
        dict(title="Original ROLEX Unruhe vintage Balance", avg_price_eur=389.44, total_sold=2,
             total_sales_eur=778.87, last_sold="18. Jan 2026", source_file="terapeak_rolex_ersatzteil_p1.html"),
    ])

    n = ingest01.insert_historical_exports(conn)
    assert n == 2, "the two genuinely distinct snapshots must survive; the exact duplicate third row must be dropped"

    rows = conn.execute(
        "SELECT last_sold, total_sold FROM raw_historical WHERE title = 'Original ROLEX Unruhe vintage Balance' "
        "ORDER BY last_sold"
    ).fetchall()
    assert len(rows) == 2
    assert set(rows) == {("18. Jan 2026", 2), ("29. Sep 2023", 1)}


# ── Schema upgrade tested on a disposable/fresh database only ─────────────────

def test_additive_migration_adds_new_columns_to_old_shaped_raw_historical(tmp_path):
    """The additive migration path (ensure_raw_tables_current) must retrofit
    original_source_file/physical_container_file onto a pre-existing,
    old-shaped raw_historical table without losing existing rows — tested
    here on a disposable, freshly-created database only, never the live one."""
    disposable_db = tmp_path / "old_shape.duckdb"
    assert disposable_db.resolve() != ingest01.DB_PATH.resolve()

    old_conn = duckdb.connect(str(disposable_db))
    old_conn.execute(
        """
        CREATE TABLE raw_historical (
            id INTEGER, row_hash VARCHAR, title VARCHAR, avg_price_eur DOUBLE,
            format VARCHAR, avg_shipping_eur DOUBLE, free_shipping_pct INTEGER,
            total_sold INTEGER, total_sales_eur DOUBLE, last_sold VARCHAR,
            bids VARCHAR, removed VARCHAR, source_file VARCHAR,
            ingested_at TIMESTAMP DEFAULT current_timestamp
        )
        """
    )
    old_conn.execute(
        "INSERT INTO raw_historical (id, title, source_file) VALUES (1, 'pre-existing row', 'terapeak_rolex_p1.html')"
    )
    before_cols = {r[0] for r in old_conn.execute("DESCRIBE raw_historical").fetchall()}
    assert "original_source_file" not in before_cols
    assert "physical_container_file" not in before_cols

    ingest01.ensure_raw_tables_current(old_conn)

    after_cols = {r[0] for r in old_conn.execute("DESCRIBE raw_historical").fetchall()}
    assert "original_source_file" in after_cols
    assert "physical_container_file" in after_cols

    row = old_conn.execute(
        "SELECT id, title, source_file, original_source_file, physical_container_file FROM raw_historical"
    ).fetchall()
    assert row == [(1, "pre-existing row", "terapeak_rolex_p1.html", None, None)], (
        "pre-existing rows must survive unchanged, with the new columns NULL rather than fabricated"
    )

    # idempotent: running the migration again must not error or duplicate anything
    ingest01.ensure_raw_tables_current(old_conn)
    count = old_conn.execute("SELECT COUNT(*) FROM raw_historical").fetchone()[0]
    assert count == 1
    old_conn.close()


def test_schema_sql_applies_cleanly_and_idempotently_to_fresh_database(tmp_path):
    """schema.sql itself (the fresh-database path, as opposed to the additive
    ensure_raw_tables_current retrofit path) must apply cleanly and
    idempotently, and produce the full source-aware contract on both
    raw_historical and stg_historical — tested on a disposable database."""
    disposable_db = tmp_path / "fresh.duckdb"
    conn = duckdb.connect(str(disposable_db))
    conn.execute(SCHEMA_PATH.read_text())
    conn.execute(SCHEMA_PATH.read_text())  # idempotent rerun

    raw_cols = {r[0] for r in conn.execute("DESCRIBE raw_historical").fetchall()}
    stg_cols = {r[0] for r in conn.execute("DESCRIBE stg_historical").fetchall()}
    assert {"original_source_file", "physical_container_file"} <= raw_cols
    assert {
        "source_type", "row_grain", "source_record_id",
        "observed_price_eur", "condition",
        "original_source_file", "physical_container_file",
    } <= stg_cols
    conn.close()


def test_source_type_is_verkaeufer_cockpit_aggregate_and_nothing_populates_ebay_sold_listing(conn, isolated_exports_dir):
    """Confirms the groundwork claim explicitly: today, every stg_historical
    row produced by the real pipeline is tagged VERKAEUFER_COCKPIT_AGGREGATE
    / row_grain='aggregate' — no code path exists yet that writes
    EBAY_SOLD_LISTING rows, so the schema is prepared but not populated by a
    second source."""
    _seed_fx_rates(conn, [("2020-01-01", 1.10)])
    _write_historical_csv(isolated_exports_dir / "source_type_check.csv", [
        dict(title="Tagged row", avg_price_eur=10.0, total_sold=1, total_sales_eur=10.0,
             last_sold="1. Jan 2024", source_file="terapeak_rolex_p1.html"),
    ])
    ingest01.insert_historical_exports(conn)
    clean02.clean_historical(conn)

    distinct_source_types = conn.execute("SELECT DISTINCT source_type FROM stg_historical").fetchall()
    distinct_row_grains = conn.execute("SELECT DISTINCT row_grain FROM stg_historical").fetchall()
    assert distinct_source_types == [("VERKAEUFER_COCKPIT_AGGREGATE",)]
    assert distinct_row_grains == [("aggregate",)]

    ebay_rows = conn.execute(
        "SELECT COUNT(*) FROM stg_historical WHERE source_type = 'EBAY_SOLD_LISTING'"
    ).fetchone()[0]
    assert ebay_rows == 0, "no ingestion path exists yet that should ever populate EBAY_SOLD_LISTING rows"

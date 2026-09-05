"""
tests/test_load_fx_rates.py
=============================
Pytest tests for scripts/00_load_fx_rates.py.

No real network calls in this suite — build_fx_rows() and write_fx_rows()
are pure/DB-only functions deliberately separated from the HTTP-calling
fetch_historical_daily()/fetch_latest() so they're testable with injected
fetch results instead of mocking requests. The actual live-API roundtrip
(schema-only DB -> real fetch -> cleaning succeeds) is validated
separately as a one-off script run, not as part of this always-run suite.

Isolation: every test runs against a duckdb file under pytest's tmp_path —
never database/watchparts.duckdb. A module-scoped autouse fixture hashes
the real project database before/after and fails loudly if it changed.
"""

import hashlib
import importlib.util
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pytest

TESTS_DIR = Path(__file__).parent
BASE_DIR = TESTS_DIR.parent
SCRIPTS_DIR = BASE_DIR / "scripts"
SCHEMA_PATH = SCRIPTS_DIR / "schema.sql"


def _load_module(name: str, path: Path):
    sys.path.insert(0, str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fx = _load_module("load_fx_rates", SCRIPTS_DIR / "00_load_fx_rates.py")


def _file_digest(path: Path):
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module", autouse=True)
def guard_production_files_untouched():
    real_db = fx.DB_PATH
    before = _file_digest(real_db)
    yield
    after = _file_digest(real_db)
    assert before == after, "database/watchparts.duckdb changed — test isolation is broken"


@pytest.fixture()
def conn(tmp_path):
    db_path = tmp_path / "test.duckdb"
    assert db_path.resolve() != fx.DB_PATH.resolve()
    connection = duckdb.connect(str(db_path))
    connection.execute(SCHEMA_PATH.read_text())
    yield connection
    connection.close()


def _sample_df():
    return fx.build_fx_rows(
        historical=[
            (date(2024, 1, 2), 1.0956),
            (date(2024, 1, 3), 1.0919),
        ],
        latest_date=date(2026, 7, 23),
        latest_rates={"GBP": 0.85318},
        today=date(2026, 7, 23),
        retrieved_at=datetime(2026, 7, 23, 9, 0, 0, tzinfo=timezone.utc),
    )


def test_true_schema_only_db_has_empty_ref_exchange_rates(conn):
    """Confirms the reproducibility gap this script exists to close: a
    freshly-schema'd DB has zero FX rows before this loader runs."""
    count = conn.execute("SELECT COUNT(*) FROM ref_exchange_rates").fetchone()[0]
    assert count == 0


def test_build_fx_rows_includes_all_required_provenance(conn):
    df = _sample_df()
    assert set(["from_currency", "to_currency", "rate", "valid_date", "source", "created_at"]).issubset(df.columns)
    assert df["created_at"].nunique() == 1, "retrieved_at must be a single, consistent timestamp per run"


def test_build_fx_rows_includes_historical_latest_inverted_and_identity(conn):
    df = _sample_df()
    pairs = set(zip(df["from_currency"], df["to_currency"]))
    assert ("EUR", "USD") in pairs
    assert ("EUR", "GBP") in pairs
    assert ("GBP", "EUR") in pairs, "inverted latest rate must be computed for the reverse direction"
    assert ("EUR", "EUR") in pairs, "identity row required by 02_clean.py's EUR-denominated ASOF join"

    gbp_eur_row = df[(df["from_currency"] == "GBP") & (df["to_currency"] == "EUR")].iloc[0]
    assert gbp_eur_row["rate"] == pytest.approx(1.0 / 0.85318, rel=1e-6)
    assert "inverted" in gbp_eur_row["source"]


# ── Bidirectional daily history (USD->EUR completeness fix) ────────────────

def test_usd_eur_daily_rows_generated_for_every_historical_date(conn):
    """The core fix: every EUR->USD historical row must produce a
    corresponding USD->EUR row at the SAME rate_date, derived locally
    (1/rate), not just a single latest-only snapshot."""
    df = _sample_df()
    eur_usd_dates = set(df[(df["from_currency"] == "EUR") & (df["to_currency"] == "USD")]["valid_date"])
    usd_eur_dates = set(df[(df["from_currency"] == "USD") & (df["to_currency"] == "EUR")]["valid_date"])
    assert eur_usd_dates == {date(2024, 1, 2), date(2024, 1, 3)}
    assert usd_eur_dates == eur_usd_dates, "EUR->USD and USD->EUR must contain the exact same set of rate dates"

    row1 = df[(df["from_currency"] == "USD") & (df["to_currency"] == "EUR") & (df["valid_date"] == date(2024, 1, 2))].iloc[0]
    assert row1["rate"] == pytest.approx(1.0 / 1.0956, rel=1e-6)
    assert row1["source"] == "ECB via Frankfurter API (historical daily, inverted)"


def test_usd_no_longer_double_counted_via_latest_targets(conn):
    """USD must not appear via the latest-snapshot path anymore — it's
    fully covered by the daily historical inversion instead."""
    assert "USD" not in fx.FX_LATEST_TARGETS


def test_no_second_api_call_needed_for_usd_eur(conn):
    """Structural proof, not just behavioral: build_fx_rows is a pure
    function with no network access, and it alone is what produces the
    USD->EUR daily rows — confirming they come from local inversion of
    already-fetched data, not a second fetch."""
    import inspect
    assert "requests" not in inspect.getsource(fx.build_fx_rows)


# ── Rate validation ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("bad_rate", [0.0, -1.5, None, float("nan"), float("inf")])
def test_invalid_historical_rate_produces_no_rows_either_direction(conn, bad_rate):
    df = fx.build_fx_rows(
        historical=[(date(2024, 1, 2), bad_rate)],
        latest_date=date(2026, 7, 23), latest_rates={},
        today=date(2026, 7, 23), retrieved_at=datetime(2026, 7, 23, tzinfo=timezone.utc),
    )
    pairs = set(zip(df["from_currency"], df["to_currency"]))
    assert ("EUR", "USD") not in pairs, f"a bad rate ({bad_rate!r}) must not produce a direct row"
    assert ("USD", "EUR") not in pairs, f"a bad rate ({bad_rate!r}) must not produce an inverted row either"


@pytest.mark.parametrize("bad_rate", [0.0, -1.5, None, float("nan")])
def test_invalid_latest_rate_produces_no_rows_either_direction(conn, bad_rate):
    df = fx.build_fx_rows(
        historical=[], latest_date=date(2026, 7, 23), latest_rates={"GBP": bad_rate},
        today=date(2026, 7, 23), retrieved_at=datetime(2026, 7, 23, tzinfo=timezone.utc),
    )
    pairs = set(zip(df["from_currency"], df["to_currency"]))
    assert ("EUR", "GBP") not in pairs
    assert ("GBP", "EUR") not in pairs


def test_mixed_valid_and_invalid_historical_rates_only_valid_ones_survive(conn):
    df = fx.build_fx_rows(
        historical=[(date(2024, 1, 2), 1.10), (date(2024, 1, 3), 0.0), (date(2024, 1, 4), -2.0), (date(2024, 1, 5), None)],
        latest_date=date(2026, 7, 23), latest_rates={},
        today=date(2026, 7, 23), retrieved_at=datetime(2026, 7, 23, tzinfo=timezone.utc),
    )
    eur_usd_dates = set(df[(df["from_currency"] == "EUR") & (df["to_currency"] == "USD")]["valid_date"])
    assert eur_usd_dates == {date(2024, 1, 2)}


def test_write_fx_rows_inserts_all_rows(conn):
    df = _sample_df()
    result = fx.write_fx_rows(conn, df)
    assert result["rows_inserted"] == len(df)
    assert result["rows_already_present"] == 0
    assert conn.execute("SELECT COUNT(*) FROM ref_exchange_rates").fetchone()[0] == len(df)


def test_write_fx_rows_is_idempotent_on_rerun(conn):
    df = _sample_df()
    fx.write_fx_rows(conn, df)
    result = fx.write_fx_rows(conn, df)
    assert result["rows_inserted"] == 0
    assert result["rows_already_present"] == len(df)
    assert conn.execute("SELECT COUNT(*) FROM ref_exchange_rates").fetchone()[0] == len(df)


def test_write_fx_rows_never_duplicates_on_primary_key(conn):
    df = _sample_df()
    fx.write_fx_rows(conn, df)
    fx.write_fx_rows(conn, df)
    fx.write_fx_rows(conn, df)
    dupes = conn.execute("""
        SELECT from_currency, to_currency, valid_date, COUNT(*) AS n
        FROM ref_exchange_rates GROUP BY 1,2,3 HAVING COUNT(*) > 1
    """).fetchall()
    assert dupes == []


def test_write_fx_rows_extends_historical_series_without_touching_existing_rows(conn):
    """Simulates a later rerun that extends the historical daily series by
    a few more days — existing rows must be untouched, only new dates added."""
    first = fx.build_fx_rows(
        historical=[(date(2024, 1, 2), 1.0956)],
        latest_date=date(2026, 7, 1), latest_rates={"USD": 1.10},
        today=date(2026, 7, 1), retrieved_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
    )
    fx.write_fx_rows(conn, first)

    second = fx.build_fx_rows(
        historical=[(date(2024, 1, 2), 1.0956), (date(2024, 1, 3), 1.0919)],
        latest_date=date(2026, 7, 23), latest_rates={"USD": 1.1392},
        today=date(2026, 7, 23), retrieved_at=datetime(2026, 7, 23, tzinfo=timezone.utc),
    )
    result = fx.write_fx_rows(conn, second)

    old_row = conn.execute(
        "SELECT rate FROM ref_exchange_rates WHERE from_currency='EUR' AND to_currency='USD' AND valid_date = DATE '2024-01-02'"
    ).fetchone()
    assert old_row[0] == pytest.approx(1.0956), "an already-stored historical rate must never be overwritten"
    new_row = conn.execute(
        "SELECT rate FROM ref_exchange_rates WHERE from_currency='EUR' AND to_currency='USD' AND valid_date = DATE '2024-01-03'"
    ).fetchone()
    assert new_row is not None, "a genuinely new historical date must be inserted"
    assert result["rows_inserted"] >= 1


def test_true_schema_only_db_plus_fx_ingestion_unblocks_cleaning(conn):
    """The exact validation chain required: schema-only DB -> FX ingestion
    -> cleaning succeeds. Loads FX via build_fx_rows/write_fx_rows (no
    network), seeds one minimal raw_historical row whose date falls inside
    the loaded FX coverage, and confirms clean_historical() completes
    without the pre-fix crash (unhandled TypeError formatting a None USD
    price) and produces a real, non-null avg_price_usd."""
    fx.write_fx_rows(conn, fx.build_fx_rows(
        historical=[(date(2024, 6, 1), 1.09)],
        latest_date=date(2026, 7, 23), latest_rates={"GBP": 0.85},
        today=date(2026, 7, 23), retrieved_at=datetime(2026, 7, 23, tzinfo=timezone.utc),
    ))

    conn.execute("""
        INSERT INTO raw_historical (id, row_hash, title, avg_price_eur, format, avg_shipping_eur,
            free_shipping_pct, total_sold, total_sales_eur, last_sold, bids, removed, source_file)
        VALUES (1, 'h1', 'Rolex test part', 100.0, 'Auction', 5.0, 50, 3, 15.0, '1. Jun 2024', '-', 'No', 'test.html')
    """)

    clean02_spec = importlib.util.spec_from_file_location("clean02_fx_chain", SCRIPTS_DIR / "02_clean.py")
    clean02 = importlib.util.module_from_spec(clean02_spec)
    clean02_spec.loader.exec_module(clean02)
    clean02.clean_historical(conn)  # must not raise

    row = conn.execute("SELECT avg_price_usd, eur_usd_rate_used FROM stg_historical WHERE id = 1").fetchone()
    assert row[0] is not None, "avg_price_usd must be a real value once ref_exchange_rates is populated"
    assert row[0] == pytest.approx(100.0 * 1.09)

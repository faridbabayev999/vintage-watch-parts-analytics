"""
tests/test_clean_active_broad.py
==================================
Pytest tests for clean_active_broad() in scripts/02_clean.py.

Covers the staging-layer half of the active-broad freshness fix: now that
raw_active_broad can hold more than one observation per item_id (see
scripts/01_ingest.py::insert_current_listings' row_hash fix), staging must
select the LATEST observation per item_id by collected_at_utc — not
whatever row order the table happens to return.

Isolation: every test runs against a duckdb file under pytest's tmp_path —
never database/watchparts.duckdb. A module-scoped autouse fixture hashes
the real project database before/after and fails loudly if it changed.
"""

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


def _load_clean_module():
    sys.path.insert(0, str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location("clean02_broad", SCRIPTS_DIR / "02_clean.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


clean02 = _load_clean_module()


def _file_digest(path: Path):
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module", autouse=True)
def guard_production_files_untouched():
    real_db = clean02.DB_PATH
    before = _file_digest(real_db)
    yield
    after = _file_digest(real_db)
    assert before == after, "database/watchparts.duckdb changed — test isolation is broken"


def _seed_ref_tables(connection) -> None:
    connection.execute("""
        INSERT INTO ref_exchange_rates (from_currency, to_currency, rate, valid_date, source) VALUES
        ('USD', 'EUR', 0.90, DATE '2025-12-01', 'test'),
        ('EUR', 'USD', 1.10, DATE '2025-12-01', 'test'),
        ('EUR', 'EUR', 1.00, DATE '2025-12-01', 'identity')
    """)
    connection.execute("""
        INSERT INTO ref_condition_map (condition_raw, condition_standard, language) VALUES
        ('Used', 'Good', 'EN')
    """)


def _insert_raw_broad(connection, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    defaults = {
        "row_hash": "", "keyword": "rolex", "source_country": "DE",
        "source_marketplace_id": "EBAY_DE", "legacy_item_id": None,
        "price_currency": "EUR", "condition": "Used", "condition_id": 3000.0,
        "buying_options": "FIXED_PRICE", "item_web_url": "", "image_url": "",
        "seller_username": "seller1", "seller_feedback_score": 100,
        "seller_feedback_percentage": 99.0, "shipping_cost_value": 5.0,
        "shipping_cost_currency": "EUR", "item_location_country": "DE",
        "item_location_city": "Berlin", "category_ids": "173696",
        "category_names": "Watch Parts", "listing_marketplace_id": "EBAY_DE",
        "item_creation_date": "2026-01-01T00:00:00Z",
    }
    for key, value in defaults.items():
        if key not in df.columns:
            df[key] = value
    connection.register("tmp_raw_broad", df)
    cols = list(df.columns)
    connection.execute(
        f"INSERT INTO raw_active_broad ({','.join(cols)}) SELECT {','.join(cols)} FROM tmp_raw_broad"
    )
    connection.unregister("tmp_raw_broad")


@pytest.fixture()
def conn(tmp_path):
    db_path = tmp_path / "test.duckdb"
    assert db_path.resolve() != clean02.DB_PATH.resolve()
    connection = duckdb.connect(str(db_path))
    connection.execute(SCHEMA_PATH.read_text())
    _seed_ref_tables(connection)
    yield connection
    connection.close()


def test_single_observation_per_item_unaffected(conn):
    _insert_raw_broad(conn, [
        dict(id=1, item_id="i1", title="T", price_value=100.0, collected_at_utc="2026-07-10T09:00:00Z"),
        dict(id=2, item_id="i2", title="T", price_value=200.0, collected_at_utc="2026-07-10T09:00:00Z"),
    ])
    clean02.clean_active_broad(conn)
    count = conn.execute("SELECT COUNT(*) FROM stg_active_broad").fetchone()[0]
    assert count == 2


def test_latest_observation_selected_when_price_changed(conn):
    """The core staging-side fix: two raw observations of the same item_id
    at different prices/times must resolve to exactly one staged row, and
    it must be the NEWER one, not whichever the table returns first."""
    _insert_raw_broad(conn, [
        dict(id=1, item_id="i1", title="T", price_value=100.0, collected_at_utc="2026-07-10T09:00:00Z"),
        dict(id=2, item_id="i1", title="T", price_value=130.0, collected_at_utc="2026-07-20T09:00:00Z"),
    ])
    clean02.clean_active_broad(conn)
    rows = conn.execute("SELECT price_original FROM stg_active_broad WHERE item_id = 'i1'").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 130.0, "the newer observation must win, not the older one"


def test_latest_observation_selected_regardless_of_raw_insertion_order(conn):
    """Same scenario, but the OLDER row is inserted into raw with a HIGHER
    id / later in table scan order than the newer one — proves selection
    is driven by collected_at_utc, not row/id order."""
    _insert_raw_broad(conn, [
        dict(id=1, item_id="i1", title="T", price_value=130.0, collected_at_utc="2026-07-20T09:00:00Z"),
        dict(id=2, item_id="i1", title="T", price_value=100.0, collected_at_utc="2026-07-10T09:00:00Z"),
    ])
    clean02.clean_active_broad(conn)
    rows = conn.execute("SELECT price_original FROM stg_active_broad WHERE item_id = 'i1'").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 130.0, "must select by timestamp, not by raw row order"


def test_unparseable_timestamp_never_wins_over_a_valid_one(conn):
    _insert_raw_broad(conn, [
        dict(id=1, item_id="i1", title="T", price_value=999.0, collected_at_utc="not-a-date"),
        dict(id=2, item_id="i1", title="T", price_value=100.0, collected_at_utc="2026-07-10T09:00:00Z"),
    ])
    clean02.clean_active_broad(conn)
    rows = conn.execute("SELECT price_original FROM stg_active_broad WHERE item_id = 'i1'").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 100.0, "a row with an unparseable timestamp must not be treated as 'the latest'"


# ── FX fallback flag correctness ────────────────────────────────────────────
#
# clean_active_broad() previously computed fx_rate_is_fallback by checking
# whether price_to_eur_rate/eur_usd_rate were still null AFTER the
# fallback-fill step already ran — by then every successfully-substituted
# rate is non-null, so the flag could only ever catch the doubly-
# unresolved case. Fixed to capture fallback status BEFORE the fill,
# matching clean_active_targeted's already-correct pattern.

def _seed_direct_rate(connection) -> None:
    connection.execute("""
        INSERT INTO ref_exchange_rates (from_currency, to_currency, rate, valid_date, source) VALUES
        ('USD', 'EUR', 0.90, DATE '2026-01-01', 'test'),
        ('EUR', 'USD', 1.10, DATE '2026-01-01', 'test')
    """)


def test_direct_fx_rate_found_flag_is_false(conn):
    """A row whose currency/date has a direct ASOF match must NOT be
    flagged as fallback."""
    _seed_direct_rate(conn)
    _insert_raw_broad(conn, [
        dict(id=1, item_id="i1", title="T", price_value=100.0, price_currency="USD",
             collected_at_utc="2026-01-10T09:00:00Z"),
    ])
    clean02.clean_active_broad(conn)
    row = conn.execute(
        "SELECT price_eur, fx_rate_is_fallback FROM stg_active_broad WHERE item_id='i1'"
    ).fetchone()
    price_eur, is_fallback = row
    assert price_eur == pytest.approx(90.0)
    assert is_fallback is False


def test_missing_fx_rate_uses_fallback_flag_is_true(conn):
    """A currency/date with no direct ASOF match must fall back to the
    latest known rate AND be flagged True — not silently read as False
    just because the substitution succeeded."""
    _seed_direct_rate(conn)
    _insert_raw_broad(conn, [
        # collected well before the only seeded rate's valid_date -> no
        # direct ASOF match, must use the latest-known-rate fallback
        dict(id=1, item_id="i1", title="T", price_value=100.0, price_currency="USD",
             collected_at_utc="2025-01-01T09:00:00Z", item_creation_date="2024-01-01T00:00:00Z"),
    ])
    clean02.clean_active_broad(conn)
    row = conn.execute(
        "SELECT price_eur, fx_rate_is_fallback FROM stg_active_broad WHERE item_id='i1'"
    ).fetchone()
    price_eur, is_fallback = row
    assert price_eur == pytest.approx(90.0), "fallback substitution must still produce the correct converted value"
    assert is_fallback is True


def test_converted_value_correct_in_both_direct_and_fallback_cases(conn):
    """The fallback-flag fix must not change any converted price —
    only the flag's accuracy. Both a direct-match row and a fallback row
    must convert to the same correct EUR value given the same rate."""
    _seed_direct_rate(conn)
    _insert_raw_broad(conn, [
        dict(id=1, item_id="direct", title="T", price_value=200.0, price_currency="USD",
             collected_at_utc="2026-01-10T09:00:00Z"),
        dict(id=2, item_id="fallback", title="T", price_value=200.0, price_currency="USD",
             collected_at_utc="2025-01-01T09:00:00Z", item_creation_date="2024-01-01T00:00:00Z"),
    ])
    clean02.clean_active_broad(conn)
    rows = dict(conn.execute(
        "SELECT item_id, price_eur FROM stg_active_broad WHERE item_id IN ('direct','fallback')"
    ).fetchall())
    assert rows["direct"] == pytest.approx(180.0)
    assert rows["fallback"] == pytest.approx(180.0)


def test_existing_behaviour_unchanged_when_rates_exist_for_all_rows(conn):
    """Regression guard: when every row has a direct FX match, the fix
    must not introduce any new fallback flags or change any prices."""
    _seed_direct_rate(conn)
    _insert_raw_broad(conn, [
        dict(id=1, item_id="i1", title="T", price_value=100.0, price_currency="EUR",
             collected_at_utc="2026-01-10T09:00:00Z"),
        dict(id=2, item_id="i2", title="T", price_value=100.0, price_currency="USD",
             collected_at_utc="2026-01-10T09:00:00Z"),
    ])
    clean02.clean_active_broad(conn)
    rows = conn.execute(
        "SELECT item_id, price_eur, fx_rate_is_fallback FROM stg_active_broad ORDER BY item_id"
    ).fetchall()
    assert rows == [("i1", 100.0, False), ("i2", 90.0, False)]


def test_rerun_is_idempotent(conn):
    _insert_raw_broad(conn, [
        dict(id=1, item_id="i1", title="T", price_value=100.0, collected_at_utc="2026-07-10T09:00:00Z"),
    ])
    clean02.clean_active_broad(conn)
    clean02.clean_active_broad(conn)
    count = conn.execute("SELECT COUNT(*) FROM stg_active_broad").fetchone()[0]
    assert count == 1


# ── EUR->EUR identity conversion (date-independent) ─────────────────────────
#
# Confirmed bug: a EUR-priced row could be marked fx_rate_is_fallback=True
# purely because its collection date preceded the single dated EUR->EUR
# reference row in ref_exchange_rates, even though EUR->EUR is exactly 1.0
# for any date. Fixed by resolving EUR identity directly (rate=1.0, no ASOF
# lookup, never a fallback) rather than depending on that row's own date.
# _seed_ref_tables (module fixture) seeds EUR->EUR dated 2025-12-01.

def test_eur_row_dated_before_identity_reference_row(conn):
    # A separate EUR->USD bridge rate covering this early date isolates the
    # EUR-identity question from the (legitimate, different) concern of
    # whether the USD bridge rate itself is available for this date —
    # matches how clean_historical treats bridge-rate availability as its
    # own dimension, not something this fix touches.
    conn.execute("INSERT INTO ref_exchange_rates (from_currency, to_currency, rate, valid_date, source) "
                 "VALUES ('EUR', 'USD', 1.08, DATE '2023-06-01', 'test')")
    _insert_raw_broad(conn, [
        dict(id=1, item_id="i1", title="T", price_value=100.0, price_currency="EUR",
             shipping_cost_currency="EUR", collected_at_utc="2024-01-01T09:00:00Z",
             item_creation_date="2023-01-01T00:00:00Z"),
    ])
    clean02.clean_active_broad(conn)
    row = conn.execute(
        "SELECT price_eur, fx_to_eur_rate_used, fx_rate_is_fallback FROM stg_active_broad WHERE item_id='i1'"
    ).fetchone()
    price_eur, rate_used, is_fallback = row
    assert price_eur == pytest.approx(100.0), "the original EUR amount must be preserved exactly"
    assert rate_used == pytest.approx(1.0)
    assert is_fallback is False, "EUR->EUR is date-independent — must never be flagged fallback"


def test_eur_row_dated_after_identity_reference_row(conn):
    _insert_raw_broad(conn, [
        dict(id=1, item_id="i1", title="T", price_value=100.0, price_currency="EUR",
             shipping_cost_currency="EUR", collected_at_utc="2026-06-01T09:00:00Z"),
    ])
    clean02.clean_active_broad(conn)
    row = conn.execute(
        "SELECT price_eur, fx_to_eur_rate_used, fx_rate_is_fallback FROM stg_active_broad WHERE item_id='i1'"
    ).fetchone()
    price_eur, rate_used, is_fallback = row
    assert price_eur == pytest.approx(100.0)
    assert rate_used == pytest.approx(1.0)
    assert is_fallback is False


def test_usd_row_with_direct_historical_rate(conn):
    _seed_direct_rate(conn)
    _insert_raw_broad(conn, [
        dict(id=1, item_id="i1", title="T", price_value=100.0, price_currency="USD",
             shipping_cost_currency="USD", collected_at_utc="2026-01-10T09:00:00Z"),
    ])
    clean02.clean_active_broad(conn)
    row = conn.execute(
        "SELECT price_eur, fx_rate_is_fallback FROM stg_active_broad WHERE item_id='i1'"
    ).fetchone()
    assert row[0] == pytest.approx(90.0)
    assert row[1] is False


def test_usd_row_without_usable_rate_existing_fallback_behaviour_intact(conn):
    """No USD->EUR rate exists at all for this scenario — existing
    fallback policy (unresolvable currency, price_eur left NULL,
    flagged) must remain exactly as it was, unaffected by the EUR fix."""
    _insert_raw_broad(conn, [
        dict(id=1, item_id="i1", title="T", price_value=100.0, price_currency="USD",
             shipping_cost_currency="USD", collected_at_utc="2020-01-01T09:00:00Z",
             item_creation_date="2019-01-01T00:00:00Z"),
    ])
    clean02.clean_active_broad(conn)
    row = conn.execute(
        "SELECT price_eur, fx_rate_is_fallback FROM stg_active_broad WHERE item_id='i1'"
    ).fetchone()
    # No USD->EUR rate seeded anywhere before 2025-12-01 in this fixture
    # set, and none at all in the module-level ref table for USD->EUR
    # directly — falls back to the latest known rate if one resolves,
    # or stays NULL/flagged if not. Either way this must still be flagged.
    assert row[1] is True, "an unresolvable/fallback USD rate must still be flagged — EUR fix must not suppress this"


def test_null_shipping_currency_not_silently_reclassified_as_eur(conn):
    """A NULL shipping_cost_currency must retain the existing missing-
    currency/fallback policy — never silently treated as EUR (there is
    no documented rule that shipping inherits the item's price currency)."""
    _insert_raw_broad(conn, [
        dict(id=1, item_id="i1", title="T", price_value=100.0, price_currency="EUR",
             shipping_cost_currency=None, shipping_cost_value=None,
             collected_at_utc="2024-01-01T09:00:00Z", item_creation_date="2023-01-01T00:00:00Z"),
    ])
    clean02.clean_active_broad(conn)
    row = conn.execute(
        "SELECT price_eur, shipping_eur, fx_rate_is_fallback FROM stg_active_broad WHERE item_id='i1'"
    ).fetchone()
    price_eur, shipping_eur, is_fallback = row
    assert price_eur == pytest.approx(100.0), "price (currency=EUR) resolves via identity, unaffected"
    assert shipping_eur == 0, "documented policy: unknown shipping assumed 0, unchanged by this fix"
    assert is_fallback is True, "missing shipping currency must still be traceable as a fallback — not silently EUR"

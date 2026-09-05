"""
tests/test_clean_active_targeted.py
====================================
Pytest tests for clean_active_targeted() in scripts/02_clean.py.

Isolation: every test runs against a duckdb file under pytest's tmp_path —
never database/watchparts.duckdb. A module-scoped autouse fixture
hashes the real project database before and after this file's tests run
and fails loudly if it changed.

The one behavior this file exists to pin down: clean_active_targeted()
must NOT deduplicate by item_id alone. raw_active_targeted's grain is
(inventory_uid, item_id, marketplace_id) — the same listing can be a
genuine candidate for two different inventory items — and a naive
item_id-only dedup step (copied from clean_active_broad, where it IS
correct) would silently re-collapse that evidence one layer downstream.
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
    sys.path.insert(0, str(SCRIPTS_DIR))  # so `import utils` inside 02_clean.py resolves
    spec = importlib.util.spec_from_file_location("clean02_targeted", SCRIPTS_DIR / "02_clean.py")
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
        ('GBP', 'EUR', 1.15, DATE '2025-12-01', 'test'),
        ('EUR', 'USD', 1.10, DATE '2025-12-01', 'test')
    """)
    connection.execute("""
        INSERT INTO ref_condition_map (condition_raw, condition_standard, language) VALUES
        ('Used', 'Good', 'EN'),
        ('New', 'New', 'EN')
    """)


def _insert_raw_targeted(connection, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    defaults = {
        "collection_batch_id": "batch_test1",
        "query_text": "Rolex 1030 6941",
        "query_tier": 1,
        "query_template_version": "v1",
        "fetched_at": "2026-01-05T00:00:00",
        "row_hash": "",
        "price_currency": "EUR",
        "condition": "Used",
        "condition_id": 3000.0,
        "buying_options": "FIXED_PRICE",
        "item_web_url": "",
        "image_url": "",
        "seller_username": "seller1",
        "seller_feedback_score": 100,
        "seller_feedback_percentage": 99.0,
        "shipping_cost_value": 5.0,
        "shipping_cost_currency": "EUR",
        "item_location_country": "DE",
        "item_location_city": "Berlin",
        "item_creation_date": "2026-01-01T00:00:00",
    }
    for key, value in defaults.items():
        if key not in df.columns:
            df[key] = value
    connection.register("tmp_raw_targeted", df)
    cols = list(df.columns)
    connection.execute(
        f"INSERT INTO raw_active_targeted ({','.join(cols)}) SELECT {','.join(cols)} FROM tmp_raw_targeted"
    )
    connection.unregister("tmp_raw_targeted")


@pytest.fixture()
def conn(tmp_path):
    db_path = tmp_path / "test.duckdb"
    assert db_path.resolve() != clean02.DB_PATH.resolve()
    connection = duckdb.connect(str(db_path))
    connection.execute(SCHEMA_PATH.read_text())
    _seed_ref_tables(connection)
    yield connection
    connection.close()


def test_empty_raw_table_writes_zero_rows(conn):
    clean02.clean_active_targeted(conn)
    count = conn.execute("SELECT COUNT(*) FROM stg_active_targeted").fetchone()[0]
    assert count == 0


def test_cross_item_evidence_survives_cleaning(conn):
    """The exact scenario Task 1/2's grain fix was built to preserve: the
    same (item_id, marketplace_id) listing appearing as a candidate for two
    different inventory items must produce TWO rows in stg_active_targeted,
    not one. A naive item_id-only dedup (as in clean_active_broad) would
    collapse this to 1."""
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_item_a", canonical_inventory_id="rolex_32_557b",
             item_id="shared_listing", marketplace_id="EBAY_DE",
             title="Item A's view of it", price_value=50.0),
        dict(id=2, inventory_uid="iuid_item_b", canonical_inventory_id="rolex_32_20764",
             item_id="shared_listing", marketplace_id="EBAY_DE",
             title="Item B's view of it", price_value=50.0),
    ])
    clean02.clean_active_targeted(conn)

    rows = conn.execute(
        "SELECT inventory_uid, canonical_inventory_id, item_id FROM stg_active_targeted ORDER BY inventory_uid"
    ).fetchall()
    assert rows == [
        ("iuid_item_a", "rolex_32_557b", "shared_listing"),
        ("iuid_item_b", "rolex_32_20764", "shared_listing"),
    ], f"Expected both inventory items' evidence preserved, got {rows}"


def test_price_and_shipping_converted_to_eur_and_usd(conn):
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_1", canonical_inventory_id="rolex_1030_6941",
             item_id="i1", marketplace_id="EBAY_US",
             title="T", price_value=100.0, price_currency="USD",
             shipping_cost_value=10.0, shipping_cost_currency="USD"),
    ])
    clean02.clean_active_targeted(conn)

    row = conn.execute(
        "SELECT price_eur, shipping_eur, landed_cost_eur, price_usd, landed_cost_usd, fx_rate_is_fallback "
        "FROM stg_active_targeted WHERE item_id = 'i1'"
    ).fetchone()
    price_eur, shipping_eur, landed_cost_eur, price_usd, landed_cost_usd, is_fallback = row

    assert price_eur == pytest.approx(90.0)
    assert shipping_eur == pytest.approx(9.0)
    assert landed_cost_eur == pytest.approx(99.0)
    # Already USD — used directly, not bridged through EUR (no FX error introduced)
    assert price_usd == pytest.approx(100.0)
    assert landed_cost_usd == pytest.approx(110.0)
    assert is_fallback is False


def test_provenance_and_linkage_columns_pass_through(conn):
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_1", canonical_inventory_id="rolex_1030_6941",
             item_id="i1", marketplace_id="EBAY_DE", title="T", price_value=50.0,
             query_text="Rolex 1030 6941", query_tier=2, query_template_version="v2",
             collection_batch_id="batch_xyz", row_hash="abc123"),
    ])
    clean02.clean_active_targeted(conn)

    row = conn.execute("""
        SELECT inventory_uid, canonical_inventory_id, query_text, query_tier,
               query_template_version, collection_batch_id, row_hash,
               matched_canonical_inventory_id, match_confidence
        FROM stg_active_targeted WHERE item_id = 'i1'
    """).fetchone()
    assert row == ("iuid_1", "rolex_1030_6941", "Rolex 1030 6941", 2, "v2", "batch_xyz", "abc123", None, None)


def test_condition_mapped_and_buying_options_parsed(conn):
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_1", canonical_inventory_id="c1",
             item_id="i1", marketplace_id="EBAY_DE", title="T", price_value=50.0,
             condition="New", buying_options="AUCTION|BEST_OFFER"),
        dict(id=2, inventory_uid="iuid_2", canonical_inventory_id="c2",
             item_id="i2", marketplace_id="EBAY_DE", title="T", price_value=50.0,
             condition="Some Unmapped String", buying_options="FIXED_PRICE"),
    ])
    clean02.clean_active_targeted(conn)

    rows = dict(conn.execute(
        "SELECT item_id, condition_standard FROM stg_active_targeted ORDER BY item_id"
    ).fetchall())
    assert rows["i1"] == "New"
    assert rows["i2"] is None  # unmapped condition — not silently guessed

    buying = conn.execute(
        "SELECT item_id, is_auction, accepts_best_offer FROM stg_active_targeted ORDER BY item_id"
    ).fetchall()
    assert buying == [("i1", True, True), ("i2", False, False)]


def test_zero_or_missing_price_rows_dropped(conn):
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_1", canonical_inventory_id="c1",
             item_id="i1", marketplace_id="EBAY_DE", title="T", price_value=0.0),
        dict(id=2, inventory_uid="iuid_2", canonical_inventory_id="c2",
             item_id="i2", marketplace_id="EBAY_DE", title="T", price_value=None),
        dict(id=3, inventory_uid="iuid_3", canonical_inventory_id="c3",
             item_id="i3", marketplace_id="EBAY_DE", title="T", price_value=50.0),
    ])
    clean02.clean_active_targeted(conn)

    item_ids = [r[0] for r in conn.execute("SELECT item_id FROM stg_active_targeted").fetchall()]
    assert item_ids == ["i3"]


def test_rerun_is_idempotent_replaces_not_appends(conn):
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_1", canonical_inventory_id="c1",
             item_id="i1", marketplace_id="EBAY_DE", title="T", price_value=50.0),
    ])
    clean02.clean_active_targeted(conn)
    clean02.clean_active_targeted(conn)
    count = conn.execute("SELECT COUNT(*) FROM stg_active_targeted").fetchone()[0]
    assert count == 1


# ── FX fallback flag reconciliation ─────────────────────────────────────────
#
# Root cause of the reported inconsistency (26 field-level fallbacks logged,
# 0 rows flagged fx_rate_is_fallback): the flag used to be computed AFTER
# the fallback-fill step, checking `price_to_eur_rate.isna()` — but by then
# every successfully-substituted rate was already non-null, so the flag
# only ever caught the doubly-unresolved case. It also never looked at
# shipping's rate at all, only price and the EUR->USD bridge. The fix
# captures a pre-fill "no direct ASOF match" boolean per field (price,
# shipping, eur_usd bridge) and ORs them into the final flag.
#
# CHF is used below as a currency with exactly one, LATER-dated rate (valid
# 2026-06-01) — deliberately absent for rows fetched in December/January —
# so its ASOF join always misses and its only path to a value is the
# latest-known-rate fallback. 'ZZZ' is used as a currency with zero rows in
# ref_exchange_rates at all, so even the fallback cannot resolve it.

def _seed_chf_rate(connection) -> None:
    connection.execute("""
        INSERT INTO ref_exchange_rates (from_currency, to_currency, rate, valid_date, source) VALUES
        ('CHF', 'EUR', 1.05, DATE '2026-06-01', 'test')
    """)


def test_direct_rate_conversion_no_fallback(conn):
    """Both price and shipping in currencies with a direct ASOF match —
    fx_rate_is_fallback must be False."""
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_1", canonical_inventory_id="c1",
             item_id="i1", marketplace_id="EBAY_DE", title="T", price_value=100.0,
             price_currency="USD", shipping_cost_value=10.0, shipping_cost_currency="USD"),
    ])
    clean02.clean_active_targeted(conn)
    row = conn.execute(
        "SELECT fx_rate_is_fallback FROM stg_active_targeted WHERE item_id = 'i1'"
    ).fetchone()
    assert row[0] is False


def test_price_fallback_only(conn):
    """Price currency (CHF) has no ASOF match before the fetch date, so the
    latest known rate is substituted — must be flagged. Shipping (USD)
    resolves directly and must not itself trigger the flag."""
    _seed_chf_rate(conn)
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_1", canonical_inventory_id="c1",
             item_id="i1", marketplace_id="EBAY_DE", title="T", price_value=100.0,
             price_currency="CHF", shipping_cost_value=10.0, shipping_cost_currency="USD",
             fetched_at="2026-01-05T00:00:00"),
    ])
    clean02.clean_active_targeted(conn)
    row = conn.execute(
        "SELECT price_eur, shipping_eur, fx_rate_is_fallback FROM stg_active_targeted WHERE item_id = 'i1'"
    ).fetchone()
    price_eur, shipping_eur, is_fallback = row
    assert price_eur == pytest.approx(105.0)  # 100 * 1.05, the only (fallback) CHF rate available
    assert shipping_eur == pytest.approx(9.0)  # direct USD conversion, unaffected
    assert is_fallback is True


def test_shipping_fallback_only(conn):
    """Shipping currency (CHF) has no ASOF match before the fetch date;
    price (USD) resolves directly. Must still be flagged at the row level
    even though only the shipping field needed a fallback."""
    _seed_chf_rate(conn)
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_1", canonical_inventory_id="c1",
             item_id="i1", marketplace_id="EBAY_DE", title="T", price_value=100.0,
             price_currency="USD", shipping_cost_value=10.0, shipping_cost_currency="CHF",
             fetched_at="2026-01-05T00:00:00"),
    ])
    clean02.clean_active_targeted(conn)
    row = conn.execute(
        "SELECT price_eur, shipping_eur, fx_rate_is_fallback FROM stg_active_targeted WHERE item_id = 'i1'"
    ).fetchone()
    price_eur, shipping_eur, is_fallback = row
    assert price_eur == pytest.approx(90.0)  # direct USD conversion, unaffected
    assert shipping_eur == pytest.approx(10.5)  # 10 * 1.05, the only (fallback) CHF rate available
    assert is_fallback is True, "shipping-only fallback must still surface as a row-level flag"


def test_both_price_and_shipping_fallback(conn):
    """Both price and shipping priced in CHF, both needing the fallback."""
    _seed_chf_rate(conn)
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_1", canonical_inventory_id="c1",
             item_id="i1", marketplace_id="EBAY_DE", title="T", price_value=100.0,
             price_currency="CHF", shipping_cost_value=10.0, shipping_cost_currency="CHF",
             fetched_at="2026-01-05T00:00:00"),
    ])
    clean02.clean_active_targeted(conn)
    row = conn.execute(
        "SELECT price_eur, shipping_eur, fx_rate_is_fallback FROM stg_active_targeted WHERE item_id = 'i1'"
    ).fetchone()
    price_eur, shipping_eur, is_fallback = row
    assert price_eur == pytest.approx(105.0)
    assert shipping_eur == pytest.approx(10.5)
    assert is_fallback is True


def test_no_fx_match_flags_and_nulls_price_eur(conn):
    """A currency with zero rows in ref_exchange_rates at all (not even a
    fallback candidate) cannot be resolved by any path. Existing policy
    (unchanged here): price_eur is left NULL rather than guessed — but this
    must now be traceable via fx_rate_is_fallback, which the pre-fix
    implementation did not guarantee for every such case."""
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_1", canonical_inventory_id="c1",
             item_id="i1", marketplace_id="EBAY_DE", title="T", price_value=100.0,
             price_currency="ZZZ", shipping_cost_value=10.0, shipping_cost_currency="EUR"),
    ])
    clean02.clean_active_targeted(conn)
    row = conn.execute(
        "SELECT price_eur, fx_rate_is_fallback FROM stg_active_targeted WHERE item_id = 'i1'"
    ).fetchone()
    price_eur, is_fallback = row
    assert price_eur is None, "unresolvable currency must not silently guess a price_eur value"
    assert is_fallback is True, "an unresolvable currency must still be flagged, not silently pass as False"


# ── Latest-snapshot selection (freshness fix) ───────────────────────────────

def test_latest_observation_selected_per_triple(conn):
    """Two raw observations of the SAME (inventory_uid, item_id,
    marketplace_id) triple at different times/prices must resolve to
    exactly one staged row — the newer one."""
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_1", canonical_inventory_id="c1",
             item_id="i1", marketplace_id="EBAY_DE", title="T", price_value=100.0,
             fetched_at="2026-07-10T09:00:00", collection_batch_id="batch_old"),
        dict(id=2, inventory_uid="iuid_1", canonical_inventory_id="c1",
             item_id="i1", marketplace_id="EBAY_DE", title="T", price_value=130.0,
             fetched_at="2026-07-20T09:00:00", collection_batch_id="batch_new"),
    ])
    clean02.clean_active_targeted(conn)
    rows = conn.execute(
        "SELECT price_original, collection_batch_id FROM stg_active_targeted "
        "WHERE inventory_uid='iuid_1' AND item_id='i1' AND marketplace='EBAY_DE'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0] == (130.0, "batch_new"), "the newer observation must win"


def test_latest_selection_does_not_collapse_different_inventory_uid(conn):
    """Same (item_id, marketplace_id), different inventory_uid — this is
    the cross-item evidence case and must NOT be collapsed by the new
    freshness filter, only genuine re-observations of the exact same
    triple should be."""
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_a", canonical_inventory_id="c_a",
             item_id="shared", marketplace_id="EBAY_DE", title="A", price_value=50.0,
             fetched_at="2026-07-10T09:00:00"),
        dict(id=2, inventory_uid="iuid_b", canonical_inventory_id="c_b",
             item_id="shared", marketplace_id="EBAY_DE", title="B", price_value=50.0,
             fetched_at="2026-07-10T09:00:00"),
    ])
    clean02.clean_active_targeted(conn)
    uids = sorted(r[0] for r in conn.execute(
        "SELECT inventory_uid FROM stg_active_targeted WHERE item_id='shared'"
    ).fetchall())
    assert uids == ["iuid_a", "iuid_b"]


def test_latest_selection_does_not_collapse_different_marketplace(conn):
    """Marketplace separation must be preserved — DE and US observations
    of the same (inventory_uid, item_id) are different partitions."""
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_1", canonical_inventory_id="c1",
             item_id="i1", marketplace_id="EBAY_DE", title="T", price_value=100.0,
             price_currency="EUR", fetched_at="2026-07-10T09:00:00"),
        dict(id=2, inventory_uid="iuid_1", canonical_inventory_id="c1",
             item_id="i1", marketplace_id="EBAY_US", title="T", price_value=110.0,
             price_currency="USD", fetched_at="2026-07-10T09:00:00"),
    ])
    clean02.clean_active_targeted(conn)
    rows = sorted(conn.execute(
        "SELECT marketplace, price_original FROM stg_active_targeted WHERE item_id='i1'"
    ).fetchall())
    assert rows == [("EBAY_DE", 100.0), ("EBAY_US", 110.0)]


def test_latest_selection_ignores_raw_row_order(conn):
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_1", canonical_inventory_id="c1",
             item_id="i1", marketplace_id="EBAY_DE", title="T", price_value=130.0,
             fetched_at="2026-07-20T09:00:00"),
        dict(id=2, inventory_uid="iuid_1", canonical_inventory_id="c1",
             item_id="i1", marketplace_id="EBAY_DE", title="T", price_value=100.0,
             fetched_at="2026-07-10T09:00:00"),
    ])
    clean02.clean_active_targeted(conn)
    rows = conn.execute(
        "SELECT price_original FROM stg_active_targeted WHERE item_id='i1'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 130.0, "must select by fetched_at, not raw row/id order"


def test_missing_shipping_currency_flags_fallback(conn):
    """The exact live-data pattern this bug was found from: shipping_cost_
    currency is NULL (shipping unknown), price resolves directly. Existing
    policy of treating unknown shipping as €0 is unchanged, but the row
    must now be traceable as having used a fallback/assumption."""
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_1", canonical_inventory_id="c1",
             item_id="i1", marketplace_id="EBAY_DE", title="T", price_value=100.0,
             price_currency="USD", shipping_cost_value=None, shipping_cost_currency=None),
    ])
    clean02.clean_active_targeted(conn)
    row = conn.execute(
        "SELECT price_eur, shipping_eur, fx_rate_is_fallback FROM stg_active_targeted WHERE item_id = 'i1'"
    ).fetchone()
    price_eur, shipping_eur, is_fallback = row
    assert price_eur == pytest.approx(90.0)
    assert shipping_eur == 0  # existing €0-shipping-assumption policy, unchanged
    assert is_fallback is True, "missing shipping currency must be traceable, not silently read as False"


# ── EUR->EUR identity conversion (date-independent) ─────────────────────────
#
# Confirmed bug: a EUR-priced row could be marked fx_rate_is_fallback=True
# purely because its collection date preceded the single dated EUR->EUR
# reference row in ref_exchange_rates, even though EUR->EUR is exactly 1.0
# for any date. Fixed by resolving EUR identity directly (rate=1.0, no ASOF
# lookup, never a fallback) rather than depending on that row's own date.

def _seed_eur_identity(connection, valid_date: str = "2025-12-01") -> None:
    connection.execute(
        f"INSERT INTO ref_exchange_rates (from_currency, to_currency, rate, valid_date, source) "
        f"VALUES ('EUR', 'EUR', 1.0, DATE '{valid_date}', 'identity')"
    )


def test_eur_row_dated_before_identity_reference_row(conn):
    _seed_eur_identity(conn, "2025-12-01")
    # EUR->USD bridge coverage for the early date isolates the EUR-identity
    # question from the separate concern of bridge-rate availability.
    conn.execute("INSERT INTO ref_exchange_rates (from_currency, to_currency, rate, valid_date, source) "
                 "VALUES ('EUR', 'USD', 1.08, DATE '2023-06-01', 'test')")
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_1", canonical_inventory_id="c1",
             item_id="i1", marketplace_id="EBAY_DE", title="T", price_value=100.0,
             price_currency="EUR", shipping_cost_currency="EUR",
             fetched_at="2024-01-01T09:00:00"),
    ])
    clean02.clean_active_targeted(conn)
    row = conn.execute(
        "SELECT price_eur, fx_to_eur_rate_used, fx_rate_is_fallback FROM stg_active_targeted WHERE item_id = 'i1'"
    ).fetchone()
    price_eur, rate_used, is_fallback = row
    assert price_eur == pytest.approx(100.0), "the original EUR amount must be preserved exactly"
    assert rate_used == pytest.approx(1.0)
    assert is_fallback is False, "EUR->EUR is date-independent — must never be flagged fallback"


def test_eur_row_dated_after_identity_reference_row(conn):
    _seed_eur_identity(conn, "2025-12-01")
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_1", canonical_inventory_id="c1",
             item_id="i1", marketplace_id="EBAY_DE", title="T", price_value=100.0,
             price_currency="EUR", shipping_cost_currency="EUR",
             fetched_at="2026-06-01T09:00:00"),
    ])
    clean02.clean_active_targeted(conn)
    row = conn.execute(
        "SELECT price_eur, fx_to_eur_rate_used, fx_rate_is_fallback FROM stg_active_targeted WHERE item_id = 'i1'"
    ).fetchone()
    price_eur, rate_used, is_fallback = row
    assert price_eur == pytest.approx(100.0)
    assert rate_used == pytest.approx(1.0)
    assert is_fallback is False


def test_usd_row_with_direct_historical_rate(conn):
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_1", canonical_inventory_id="c1",
             item_id="i1", marketplace_id="EBAY_US", title="T", price_value=100.0,
             price_currency="USD", shipping_cost_currency="USD",
             fetched_at="2026-01-05T09:00:00"),
    ])
    clean02.clean_active_targeted(conn)
    row = conn.execute(
        "SELECT price_eur, fx_rate_is_fallback FROM stg_active_targeted WHERE item_id = 'i1'"
    ).fetchone()
    assert row[0] == pytest.approx(90.0)
    assert row[1] is False


def test_usd_row_without_usable_rate_existing_fallback_behaviour_intact(conn):
    """A currency with zero rows in ref_exchange_rates at all cannot be
    resolved — existing policy (price_eur left NULL, flagged) must remain
    exactly as it was, unaffected by the EUR fix."""
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_1", canonical_inventory_id="c1",
             item_id="i1", marketplace_id="EBAY_US", title="T", price_value=100.0,
             price_currency="ZZZ", shipping_cost_currency="EUR",
             fetched_at="2026-01-05T09:00:00"),
    ])
    clean02.clean_active_targeted(conn)
    row = conn.execute(
        "SELECT price_eur, fx_rate_is_fallback FROM stg_active_targeted WHERE item_id = 'i1'"
    ).fetchone()
    assert row[0] is None, "unresolvable currency must not silently guess a price_eur value"
    assert row[1] is True, "an unresolvable currency must still be flagged — EUR fix must not suppress this"


def test_null_shipping_currency_not_silently_reclassified_as_eur_targeted(conn):
    """A NULL shipping_cost_currency must retain the existing missing-
    currency/fallback policy — never silently treated as EUR (there is no
    documented rule that shipping inherits the item's price currency)."""
    _insert_raw_targeted(conn, [
        dict(id=1, inventory_uid="iuid_1", canonical_inventory_id="c1",
             item_id="i1", marketplace_id="EBAY_DE", title="T", price_value=100.0,
             price_currency="EUR", shipping_cost_value=None, shipping_cost_currency=None,
             fetched_at="2024-01-01T09:00:00"),
    ])
    clean02.clean_active_targeted(conn)
    row = conn.execute(
        "SELECT price_eur, shipping_eur, fx_rate_is_fallback FROM stg_active_targeted WHERE item_id = 'i1'"
    ).fetchone()
    price_eur, shipping_eur, is_fallback = row
    assert price_eur == pytest.approx(100.0), "price (currency=EUR) resolves via identity, unaffected"
    assert shipping_eur == 0, "documented policy: unknown shipping assumed 0, unchanged by this fix"
    assert is_fallback is True, "missing shipping currency must still be traceable as a fallback — not silently EUR"

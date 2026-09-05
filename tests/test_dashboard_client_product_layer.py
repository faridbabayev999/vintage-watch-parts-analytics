"""
Client product layer tests (2026-07-31): portfolio overview, search,
distributions, top brands, inventory management writes. Every function
tested here is either a read query or a pure aggregation of values
load_items() already produces -- none compute a new price/confidence/
turnover value. Disposable in-tmp DuckDB and tmp CSV paths only, never the
live database or the real data/raw/inventory.csv.
"""
import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
SCHEMA = SCRIPTS / "schema.sql"


def _load(name="dd_cp"):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / "16_dashboard_data.py")
    m = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPTS))
    spec.loader.exec_module(m)
    return m


def _seeded_db(tmp_path, name="cp.duckdb"):
    db = tmp_path / name
    conn = duckdb.connect(str(db))
    conn.execute(SCHEMA.read_text())
    conn.execute("""INSERT INTO staging_inventory
        (inventory_uid, canonical_inventory_id, brand, caliber, part_number, stock, validation_status)
        VALUES ('inv1','rolex_3135_x','Rolex','3135','4419', 5, 'PASS')""")
    conn.execute("""INSERT INTO tmv_results (canonical_inventory_id, tmv_eur, tmv_low_eur, tmv_high_eur, confidence_tier)
        VALUES ('rolex_3135_x', 200.0, 170.0, 230.0, 'MEDIUM')""")
    conn.execute("""INSERT INTO feat_pricing (canonical_inventory_id, brand, caliber, part_number, historical_value_eur, current_value_eur, scarcity_score)
        VALUES ('rolex_3135_x', 'Rolex', '3135', '4419', 195.0, 210.0, 0.6)""")
    conn.execute("""INSERT INTO turnover_survival (canonical_inventory_id, median_days_to_sell, probability_sell_30d, probability_sell_90d)
        VALUES ('rolex_3135_x', 20.0, 0.5, 0.8)""")
    conn.execute("""INSERT INTO staging_inventory
        (inventory_uid, canonical_inventory_id, brand, caliber, part_number, stock, validation_status)
        VALUES ('inv2','omega_1120_y','Omega','1120','5501', 3, 'PASS')""")
    conn.execute("""INSERT INTO tmv_results_algorithmic
        (canonical_inventory_id, tmv_eur, tmv_low_eur, tmv_high_eur, confidence_tier, evidence_basis_type)
        VALUES ('omega_1120_y', 100.0, 80.0, 120.0, 'AUTO_CONFIRMED', 'ALGORITHMIC')""")
    conn.execute("""INSERT INTO turnover_survival_algorithmic (canonical_inventory_id, median_days_to_sell, probability_sell_30d, probability_sell_90d)
        VALUES ('omega_1120_y', 200.0, 0.1, 0.3)""")
    conn.close()
    return db


def test_portfolio_overview_sums_real_priced_items(tmp_path):
    dd = _load()
    db = _seeded_db(tmp_path)
    conn = duckdb.connect(str(db), read_only=True)
    dd.assert_db_target(conn, db)
    ov = dd.portfolio_overview(conn)
    assert ov["priced_n"] == 2
    assert ov["portfolio_value_eur"] == pytest.approx(300.0)
    assert ("Rolex", 1) in ov["top_brands"] or any(b == "Rolex" for b, _ in ov["top_brands"])
    conn.close()


def test_search_priced_items_matches_brand_and_part_number(tmp_path):
    dd = _load()
    db = _seeded_db(tmp_path)
    conn = duckdb.connect(str(db), read_only=True)
    dd.assert_db_target(conn, db)
    assert len(dd.search_priced_items(conn, "rolex")) == 1
    assert len(dd.search_priced_items(conn, "4419")) == 1
    assert len(dd.search_priced_items(conn, "nonexistent")) == 0
    assert len(dd.search_priced_items(conn, "")) == 2
    conn.close()


def test_get_item_by_id_returns_none_when_missing(tmp_path):
    dd = _load()
    db = _seeded_db(tmp_path)
    conn = duckdb.connect(str(db), read_only=True)
    dd.assert_db_target(conn, db)
    assert dd.get_item_by_id(conn, "rolex_3135_x") is not None
    assert dd.get_item_by_id(conn, "does_not_exist") is None
    conn.close()


def test_price_distribution_bins_never_fabricates_from_empty_list():
    dd = _load()
    assert dd.price_distribution_bins([]) == []


def test_price_distribution_bins_buckets_correctly():
    dd = _load()
    items = [{"tmv_eur": 10.0}, {"tmv_eur": 60.0}, {"tmv_eur": 65.0}]
    bins = dd.price_distribution_bins(items, bin_width=50, max_bins=12)
    counts = {b["bucket"]: b["count"] for b in bins}
    assert counts["€0-50"] == 1
    assert counts["€50-100"] == 2


def test_sell_time_distribution_buckets_fast_medium_slow():
    dd = _load()
    items = [{"median_days_to_sell": 10}, {"median_days_to_sell": 100}, {"median_days_to_sell": 400}, {"median_days_to_sell": None}]
    dist = dd.sell_time_distribution(items)
    counts = {d["bucket"]: d["count"] for d in dist}
    assert counts["Fast"] == 1 and counts["Medium"] == 1 and counts["Slow"] == 1


def test_top_brands_computes_share_correctly():
    dd = _load()
    items = [{"brand": "Rolex"}, {"brand": "Rolex"}, {"brand": "Omega"}]
    tb = dd.top_brands(items)
    by_brand = {t["brand"]: t for t in tb}
    assert by_brand["Rolex"]["count"] == 2
    assert by_brand["Rolex"]["pct"] == pytest.approx(66.7, abs=0.1)


def test_export_recommendations_rows_reads_verbatim_no_computation():
    dd = _load()
    items = [{
        "part_number": "4419", "brand": "Rolex", "caliber": "3135", "stock": 2,
        "tmv_eur": 100.0, "pricing_state_label": "Pricing Ready", "median_days_to_sell": 30.0,
    }]
    rows = dd.export_recommendations_rows(items)
    assert rows[0]["Recommended Price (EUR)"] == 100.0
    assert rows[0]["Potential Revenue (EUR)"] == 200.0  # 100*2, verbatim multiplication of existing values


# ── Inventory management writes (tmp CSV path only, never the real file) ──

def test_append_inventory_item_writes_expected_row(tmp_path):
    dd = _load()
    path = tmp_path / "inventory.csv"
    result = dd.append_inventory_item("Rolex", "3135", "9999", 5, path=path)
    assert result["brand"] == "Rolex"
    assert result["action"] == "created"
    content = path.read_text()
    assert "Rolex/Tudor,Calibre,P-number,Stock" in content
    assert "Rolex,3135,9999,5" in content


def test_append_inventory_item_existing_only_updates_stock(tmp_path):
    dd = _load()
    path = tmp_path / "inventory.csv"
    first = dd.append_inventory_item("Rolex", "3135", "9999", 5, path=path)
    second = dd.append_inventory_item("Rolex", "3135", "9999", 7, path=path)
    assert first["action"] == "created"
    assert second["action"] == "stock_updated"
    lines = path.read_text().strip().splitlines()
    assert lines == ["Rolex/Tudor,Calibre,P-number,Stock", "Rolex,3135,9999,7"]


def test_append_inventory_item_rejects_missing_part_number(tmp_path):
    dd = _load()
    path = tmp_path / "inventory.csv"
    with pytest.raises(ValueError):
        dd.append_inventory_item("Rolex", "3135", "", 5, path=path)
    assert not path.exists()


def test_append_inventory_item_rejects_negative_stock(tmp_path):
    dd = _load()
    path = tmp_path / "inventory.csv"
    with pytest.raises(ValueError):
        dd.append_inventory_item("Rolex", "3135", "9999", -1, path=path)


def test_append_inventory_rows_refuses_whole_batch_on_one_bad_row(tmp_path):
    dd = _load()
    path = tmp_path / "inventory.csv"
    rows = [
        {"brand": "Rolex", "caliber": "3135", "part_number": "1111", "stock": 1},
        {"brand": "", "caliber": "3135", "part_number": "2222", "stock": 1},  # invalid
    ]
    with pytest.raises(ValueError):
        dd.append_inventory_rows(rows, path=path)
    assert not path.exists()  # nothing written -- refused before any row landed


def test_append_inventory_rows_refuses_duplicate_within_upload(tmp_path):
    dd = _load()
    path = tmp_path / "inventory.csv"
    rows = [
        {"brand": "Rolex", "caliber": "3135", "part_number": "1111", "stock": 1},
        {"brand": "Rolex", "caliber": "3135", "part_number": "1111", "stock": 2},
    ]
    with pytest.raises(ValueError):
        dd.append_inventory_rows(rows, path=path)
    assert not path.exists()


def test_validate_inventory_upload_rows_reports_without_writing(tmp_path):
    dd = _load()
    path = tmp_path / "inventory.csv"
    dd.append_inventory_item("Rolex", "3135", "1111", 3, path=path)
    rows = [
        {"brand": "Rolex", "caliber": "3135", "part_number": "1111", "stock": 4},
        {"brand": "Rolex", "caliber": "3135", "part_number": "2222", "stock": 1},
    ]
    result = dd.validate_inventory_upload_rows(rows, path=path)
    assert result["ok"] is True
    assert result["stock_updates"] == 1
    assert result["new_rows"] == 1
    assert path.read_text().strip().endswith("Rolex,3135,1111,3")


def test_validate_inventory_upload_rows_reports_duplicate_errors(tmp_path):
    dd = _load()
    path = tmp_path / "inventory.csv"
    rows = [
        {"brand": "Rolex", "caliber": "3135", "part_number": "2222", "stock": 1},
        {"brand": "Rolex", "caliber": "3135", "part_number": "2222", "stock": 2},
    ]
    result = dd.validate_inventory_upload_rows(rows, path=path)
    assert result["ok"] is False
    assert "duplicate item" in result["errors"][0]
    assert not path.exists()


def test_pipeline_job_helpers_return_events_and_contract_row(tmp_path):
    dd = _load()
    db = tmp_path / "jobs.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute(SCHEMA.read_text())
    conn.execute("""
        INSERT INTO dashboard_pipeline_jobs
        (job_id, trigger_source, job_type, status, brand, caliber, part_number, canonical_inventory_id)
        VALUES ('job1', 'dashboard', 'NEW_ITEM', 'SUCCEEDED', 'Rolex', '3135', '510', 'rolex_3135_510')
    """)
    conn.execute("""
        INSERT INTO dashboard_pipeline_job_events (event_id, job_id, event_type, message)
        VALUES (1, 'job1', 'STEP_OK:01_ingest', '0.4s rc=0')
    """)
    conn.execute("""
        INSERT INTO dashboard_inventory_pricing
        (inventory_uid, canonical_inventory_id, brand, caliber, part_number, pricing_status,
         pricing_confidence, recommended_price_eur, sell_time_display, turnover_confidence,
         active_evidence_count, historical_evidence_count, total_unique_evidence_count)
        VALUES ('iuid1', 'rolex_3135_510', 'Rolex', '3135', '510', 'PRICED',
                'HIGH', 43.55, '0-30 days', 'HIGH', 85, 11, 96)
    """)
    jobs = dd.latest_pipeline_jobs(conn)
    events = dd.pipeline_job_events(conn, "job1")
    row = dd.dashboard_contract_row(conn, "rolex_3135_510")
    conn.close()
    assert jobs[0]["canonical_inventory_id"] == "rolex_3135_510"
    assert events[0]["event_type"] == "STEP_OK:01_ingest"
    assert row["recommended_price_eur"] == 43.55


def test_append_inventory_item_never_touches_real_source_file(tmp_path):
    """Explicit safety check: the real data/raw/inventory.csv must be
    untouched by a test run using the injectable path."""
    dd = _load()
    real_path = dd.inventory_csv_path()
    before = real_path.read_bytes() if real_path.exists() else None
    dd.append_inventory_item("Rolex", "3135", "TEST_ROW_SHOULD_NOT_PERSIST", 1, path=tmp_path / "inventory.csv")
    after = real_path.read_bytes() if real_path.exists() else None
    assert before == after

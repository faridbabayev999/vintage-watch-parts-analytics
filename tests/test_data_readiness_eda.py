"""
tests/test_data_readiness_eda.py
==================================
Targeted tests for analysis/run_data_readiness_eda.py — reusable and
safety-critical logic only, not a full descriptive-profiling suite:

  - the live database is opened strictly read-only
  - running the EDA never mutates the live database or any source file
  - deterministic readiness-status output (acceptance-gate metrics)
  - acceptance-gate calculations never invent an approval threshold
  - missing-source handling (accessible-path check, script-level assessment)
  - the price-agreement reproduction (the one number this task explicitly
    requires to be reconciled, not silently replaced) is correct against a
    small, hand-verified synthetic case
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
ANALYSIS_DIR = BASE_DIR / "analysis"


def _load_eda_module():
    sys.path.insert(0, str(BASE_DIR))
    sys.path.insert(0, str(BASE_DIR / "scripts"))
    spec = importlib.util.spec_from_file_location("run_data_readiness_eda", ANALYSIS_DIR / "run_data_readiness_eda.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


eda = _load_eda_module()


def _file_digest(path: Path) -> str | None:
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ── Live database: read-only, never mutated ───────────────────────────────────

def test_readonly_connection_rejects_writes():
    conn = eda.get_readonly_connection()
    try:
        with pytest.raises(duckdb.Error):
            conn.execute("CREATE TABLE eda_test_should_never_exist (x INTEGER)")
    finally:
        conn.close()


def test_running_the_full_eda_never_mutates_the_live_database_or_source_files():
    watched_paths = [
        eda.DB_PATH,
        eda.INVENTORY_CSV,
        eda.TERAPEAK_CSV,
        eda.EBAY_SOLD_ITEMS_CSV,
        eda.LATEST_CSV,
    ]
    before = {p: _file_digest(p) for p in watched_paths}

    conn = eda.get_readonly_connection()
    try:
        inv = eda.inventory_eda(conn)
        inv.pop("_by_caliber_df", None)
        active = eda.active_listing_eda(conn)
        active.pop("_targeted_listing_availability_by_item_df", None)
        hist = eda.reproduce_historical_findings()
        eda.cross_source_eda(conn)
        eda.acceptance_gate_metrics(conn, hist)
    finally:
        conn.close()

    after = {p: _file_digest(p) for p in watched_paths}
    assert before == after, "running the EDA must never change the live database or any real source file"


# ── Missing-source handling ────────────────────────────────────────────────────

def test_check_accessible_paths_reports_missing_paths_without_crashing(monkeypatch, tmp_path):
    monkeypatch.setattr(eda, "INVENTORY_CSV", tmp_path / "does_not_exist.csv")
    result = eda.check_accessible_paths()
    assert result["inventory_source"]["exists"] is False
    # other real paths must still be reported normally
    assert result["live_database"]["exists"] is True


def test_script_level_assessment_marks_not_enough_information_when_scripts_missing(monkeypatch, tmp_path):
    missing = tmp_path / "nope.py"
    monkeypatch.setattr(eda, "EBAY_SOLD_FETCH_PY", missing)
    monkeypatch.setattr(eda, "EBAY_SOLD_PARSE_PY", missing)
    monkeypatch.setattr(eda, "TERAPEAK_FETCH_PY", missing)
    result = eda.check_accessible_paths()
    assert result["_script_level_assessment_status"] == "NOT_ENOUGH_INFORMATION"


def test_script_level_assessment_performed_when_all_three_scripts_present():
    result = eda.check_accessible_paths()
    assert result["_script_level_assessment_status"] == "PERFORMED"


# ── Acceptance-gate: deterministic, never invents a threshold ─────────────────

def test_acceptance_gate_metrics_are_deterministic():
    conn = eda.get_readonly_connection()
    try:
        hist = eda.reproduce_historical_findings()
        run1 = eda.acceptance_gate_metrics(conn, hist)
        run2 = eda.acceptance_gate_metrics(conn, hist)
    finally:
        conn.close()
    assert run1 == run2


def test_acceptance_gate_metrics_never_invent_an_approval_threshold():
    conn = eda.get_readonly_connection()
    try:
        hist = eda.reproduce_historical_findings()
        metrics = eda.acceptance_gate_metrics(conn, hist)
    finally:
        conn.close()
    assert len(metrics) > 0
    for m in metrics:
        assert m["approval_threshold"] == "TO_BE_APPROVED", (
            f"metric {m['metric']!r} must not carry an invented approval threshold"
        )


# ── Price-agreement reproduction: correctness on a small, hand-verified case ──

def test_compute_price_agreement_matches_hand_computed_synthetic_case():
    """
    Two shared titles by construction:
      - "part a": same day, price within 10% -> counts toward both same_day and price_within_tolerance
      - "part b": different days, price far apart -> counts toward neither
    One non-shared title in each dataset, which must not affect the denominator.
    """
    terapeak = pd.DataFrame([
        {"title": "Part A", "avg_price_eur": 100.0, "last_sold": "1. Jan 2024"},
        {"title": "Part B", "avg_price_eur": 50.0, "last_sold": "1. Jan 2024"},
        {"title": "Only In Terapeak", "avg_price_eur": 10.0, "last_sold": "1. Jan 2024"},
    ])
    ebay = pd.DataFrame([
        {"title": "Part A", "price_eur": 105.0, "sold_date_iso": "2024-01-01"},
        {"title": "Part B", "price_eur": 20.0, "sold_date_iso": "2024-02-15"},
        {"title": "Only In Ebay", "price_eur": 10.0, "sold_date_iso": "2024-01-01"},
    ])

    result = eda.compute_price_agreement(terapeak, ebay, tolerance=0.10)

    assert result["price_agreement_denominator_exact_title_overlap"] == 2
    assert result["price_agreement_reproduced_same_day_count"] == 1
    assert result["price_agreement_reproduced_same_day_pct"] == 50.0
    assert result["price_agreement_reproduced_price_within_tolerance_count"] == 1
    assert result["price_agreement_reproduced_price_within_tolerance_pct"] == 50.0


def test_compute_price_agreement_reproduces_previously_published_figures_from_real_files():
    """
    The safety-critical reconciliation this task exists for: recomputing the
    cross-source price-agreement statistic from the real files must match
    (or, if it ever doesn't, this test is exactly what would catch that and
    force an explicit explanation rather than a silent replacement).
    """
    if not eda.TERAPEAK_CSV.exists() or not eda.EBAY_SOLD_ITEMS_CSV.exists():
        pytest.skip("real historical files not present in this environment")

    terapeak = pd.read_csv(eda.TERAPEAK_CSV)
    ebay = pd.read_csv(eda.EBAY_SOLD_ITEMS_CSV)
    result = eda.compute_price_agreement(terapeak, ebay, tolerance=0.10)

    prev = eda.PREVIOUSLY_PUBLISHED_OVERLAP
    assert result["price_agreement_denominator_exact_title_overlap"] == prev["exact_title_overlap_count"]
    assert result["price_agreement_reproduced_same_day_count"] == prev["same_day_match_count"]
    assert result["price_agreement_reproduced_price_within_tolerance_count"] == prev["price_within_10pct_count"]
    assert result["price_agreement_matches_previously_published_report"] is True


# ── _jsonable: must not crash on numpy/pandas scalar types ────────────────────

def test_jsonable_handles_numpy_and_pandas_types():
    import numpy as np

    payload = {
        "a": np.int64(5),
        "b": np.float64(1.5),
        "c": np.bool_(True),
        "d": pd.Timestamp("2024-01-01"),
        "e": pd.NaT,
        "f": float("nan"),
        "g": [np.int64(1), {"h": np.float64(2.0)}],
    }
    import json
    dumped = json.dumps(eda._jsonable(payload))
    reloaded = json.loads(dumped)
    assert reloaded["a"] == 5
    assert reloaded["b"] == 1.5
    assert reloaded["c"] is True
    assert reloaded["e"] is None
    assert reloaded["f"] is None

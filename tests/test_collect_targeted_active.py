"""
tests/test_collect_targeted_active.py
======================================
Pytest tests for scripts/04_collect_targeted_active.py and the shared
ebay_api_common.py primitives.

Every Browse API request is mocked. These tests never use the production
DuckDB database, never call the live eBay API, never consume API quota, and
never read real credentials from .env — an autouse fixture asserts this
explicitly rather than assuming it.

Two mock layers, matching what each test needs:
  - Most tests mock search_items_single_marketplace directly (as bound on
    the loaded 04_collect_targeted_active module) — fast, isolates network
    I/O, exercises all of Module 3's own escalation/dedup/resumability logic.
  - Pagination and retry-logic tests mock one level deeper, at
    ebay_api_common.request_json_with_retry's dependency (urlopen), to
    exercise the real shared bounded-pagination and backoff code.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import re
import subprocess
import sys
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

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


collect04 = _load_module("collect04", SCRIPTS_DIR / "04_collect_targeted_active.py")
ingest01 = _load_module("ingest01", SCRIPTS_DIR / "01_ingest.py")
ebay_api_common = collect04.sys.modules.get("ebay_api_common") or __import__("ebay_api_common")


# ── Fixtures / seed data ───────────────────────────────────────────────────────

SEED_INVENTORY = [
    # PASS item: tiers 1, 2 (x4), 4
    dict(canonical_inventory_id="rolex_1030_6941", inventory_uid="iuid_pass1",
         brand="Rolex", caliber="1030", part_number="6941", stock=5,
         validation_status="PASS", part_number_is_distinctive=False),
    # WARNING-distinctive item: tier 5 only
    dict(canonical_inventory_id="rolex_unknown_24_26812_8", inventory_uid="iuid_warn1",
         brand="Rolex", caliber=None, part_number="24-26812-8", stock=2,
         validation_status="WARNING", part_number_is_distinctive=True),
    # FAIL item: excluded before this module entirely
    dict(canonical_inventory_id="rolex_1120_unknown", inventory_uid="iuid_fail1",
         brand="Rolex", caliber="1120", part_number=None, stock=2,
         validation_status="FAIL", part_number_is_distinctive=False),
    # WARNING-non-distinctive item with NO query rows at all
    dict(canonical_inventory_id="rolex_unknown_zz_1", inventory_uid="iuid_zero1",
         brand="Rolex", caliber=None, part_number="12", stock=1,
         validation_status="WARNING", part_number_is_distinctive=False),
]


def _seed_db(connection) -> None:
    df = pd.DataFrame(SEED_INVENTORY)
    df["upload_batch_id"] = "batch_test1"
    df["condition"] = None
    df["source_filename"] = "inventory.csv"
    df["ingested_at"] = "2026-01-01T00:00:00"
    connection.register("tmp_seed", df)
    cols = [
        "canonical_inventory_id", "upload_batch_id", "brand", "caliber", "part_number",
        "stock", "condition", "source_filename", "ingested_at",
        "inventory_uid", "validation_status", "part_number_is_distinctive",
    ]
    connection.execute(f"INSERT INTO staging_inventory ({','.join(cols)}) SELECT {','.join(cols)} FROM tmp_seed")
    connection.unregister("tmp_seed")

    query_rows = [
        ("iuid_pass1", "rolex_1030_6941", 1, "Rolex 1030 6941", False),
        ("iuid_pass1", "rolex_1030_6941", 2, "Rolex Cal 1030 6941", False),
        ("iuid_pass1", "rolex_1030_6941", 2, "Rolex Caliber 1030 6941", False),
        ("iuid_pass1", "rolex_1030_6941", 2, "Rolex Calibre 1030 6941", False),
        ("iuid_pass1", "rolex_1030_6941", 2, "Rolex Kaliber 1030 6941", False),
        ("iuid_pass1", "rolex_1030_6941", 4, "Rolex 1030", False),
        ("iuid_warn1", "rolex_unknown_24_26812_8", 5, "24-26812-8", False),
        # iuid_fail1 and iuid_zero1 intentionally have no search_queries rows
    ]
    qdf = pd.DataFrame(query_rows, columns=["inventory_uid", "canonical_inventory_id", "tier", "query_text", "uses_lexicon"])
    qdf["query_template_version"] = "v1"
    connection.register("tmp_q", qdf)
    connection.execute(
        "INSERT INTO search_queries (inventory_uid, canonical_inventory_id, tier, query_text, uses_lexicon, query_template_version) "
        "SELECT inventory_uid, canonical_inventory_id, tier, query_text, uses_lexicon, query_template_version FROM tmp_q"
    )
    connection.unregister("tmp_q")


def _fake_item(item_id: str, marketplace_id: str, title: str = "Rolex part") -> dict:
    return {
        "itemId": item_id,
        "title": title,
        "price": {"value": "50.00", "currency": "EUR"},
        "condition": "Used",
        "conditionId": "3000",
        "buyingOptions": ["FIXED_PRICE"],
        "itemWebUrl": f"https://ebay.example/{item_id}",
        "image": {"imageUrl": "https://ebay.example/img.jpg"},
        "seller": {"username": "seller1", "feedbackScore": 100, "feedbackPercentage": "99.0"},
        "shippingOptions": [{"shippingCost": {"value": "5.00", "currency": "EUR"}}],
        "itemLocation": {"country": "DE", "city": "Berlin"},
        "itemCreationDate": "2026-01-01T00:00:00Z",
        "source_marketplace_id": marketplace_id,
        "source_country": "Germany" if marketplace_id == "EBAY_DE" else "US",
    }


def _file_digest(path: Path) -> str | None:
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _flat_listing(item_id: str, marketplace_id: str, title: str = "T", tier: int = 1) -> dict:
    """A single already-flattened listing dict, matching what flatten_item
    produces — the shape expected inside per_marketplace[...]["listings"]."""
    return {
        "query_text": "Rolex 1030 6941", "query_tier": tier, "marketplace_id": marketplace_id,
        "fetched_at": "2026-01-01T00:00:00", "item_id": item_id, "title": title,
        "price_value": "10", "price_currency": "EUR", "condition": "", "condition_id": "",
        "buying_options": "", "item_web_url": "", "image_url": "", "seller_username": "",
        "seller_feedback_score": "", "seller_feedback_percentage": "", "shipping_cost_value": "",
        "shipping_cost_currency": "", "item_location_country": "", "item_location_city": "",
        "item_creation_date": "",
    }


def _make_fake_ingest(output_dir: Path, db_path: Path):
    """A run_targeted_ingestion() replacement that performs real ingestion
    against the test's isolated db/dir instead of shelling out to a
    subprocess that would touch the production database — needed so
    ingestion_log/collection_chunks.ingested_at genuinely reflect success,
    which batch_fully_ingested now depends on.

    Takes db_path, not a live conn: run_chunked_collection now closes its
    own connection before calling run_targeted_ingestion (see
    _run_ingestion_with_connection_released), exactly mirroring what a real
    subprocess does — opens its own connection, does the insert, closes.
    Accepting an already-open conn here would paper over that contract by
    reusing a connection the production code no longer guarantees is open
    at this point."""
    def _fake_ingest(**kwargs):
        ingest01.TARGETED_ACTIVE_DIR = output_dir
        fresh_conn = duckdb.connect(str(db_path))
        try:
            ingest01.insert_targeted_listings(fresh_conn)
        finally:
            fresh_conn.close()
    return _fake_ingest


def _durably_write_chunk(
    conn, output_dir: Path, batch_id: str, item_results: list[dict], chunk_id: str | None = None
) -> tuple[str, Path]:
    """
    Test helper mirroring exactly what run_chunked_collection does for one
    chunk: atomic CSV write, then record_chunk_written, then
    record_chunk_progress — in that order. Tests that want a combination to
    be genuinely resumable-skip-safe must go through this (not call
    write_batch_csv alone), since already_processed now validates durable
    chunk backing, not just a bare collection_progress row.
    """
    chunk_id = chunk_id or f"{batch_id}_chunk_{uuid.uuid4().hex[:12]}"
    csv_path = collect04.write_batch_csv(batch_id, chunk_id, item_results, "v1", output_dir=output_dir)
    collect04.record_chunk_written(
        conn, chunk_id=chunk_id, batch_id=batch_id, source_filename=csv_path.name,
        csv_sha256=collect04._sha256_file(csv_path),
        started_at=datetime.now(timezone.utc), items_attempted=len(item_results), calls_made=0,
    )
    collect04.record_chunk_progress(conn, batch_id=batch_id, chunk_id=chunk_id, item_results=item_results)
    return chunk_id, csv_path


# ── Isolation guard ────────────────────────────────────────────────────────────

@pytest.fixture(scope="function", autouse=True)
def guard_production_untouched():
    real_db = collect04.DB_PATH
    real_manifest = collect04.REPORTS_DIR / "targeted_collection_manifest.json"
    real_log = collect04.LOG_DIR / "04_collect_targeted_active.log"
    real_env_token_cache = BASE_DIR / ".ebay_token_cache.json"
    # Directory listings, not just one fixed filename — a test that forgets
    # to pass an isolated output_dir/reports_dir (e.g. to
    # run_chunked_collection) would otherwise silently write real files into
    # these two directories without tripping the single-file checks above.
    real_reports_dir = collect04.REPORTS_DIR
    real_targeted_active_dir = collect04.TARGETED_ACTIVE_DIR

    def _listing(directory: Path) -> set[str]:
        return set(p.name for p in directory.iterdir()) if directory.exists() else set()

    before = {
        "db": _file_digest(real_db),
        "manifest": _file_digest(real_manifest),
        "log_mtime": real_log.stat().st_mtime if real_log.exists() else None,
        "token_cache": _file_digest(real_env_token_cache),
        "reports_dir_listing": _listing(real_reports_dir),
        "targeted_active_dir_listing": _listing(real_targeted_active_dir),
    }
    yield
    after = {
        "db": _file_digest(real_db),
        "manifest": _file_digest(real_manifest),
        "log_mtime": real_log.stat().st_mtime if real_log.exists() else None,
        "token_cache": _file_digest(real_env_token_cache),
        "reports_dir_listing": _listing(real_reports_dir),
        "targeted_active_dir_listing": _listing(real_targeted_active_dir),
    }
    assert before == after, "A test touched production database/manifest/log/token-cache/report/CSV files — isolation is broken"


@pytest.fixture()
def db_path(tmp_path):
    path = tmp_path / "test.duckdb"
    assert path.resolve() != collect04.DB_PATH.resolve()
    return path


@pytest.fixture()
def conn(db_path):
    connection = duckdb.connect(str(db_path))
    connection.execute(SCHEMA_PATH.read_text())
    _seed_db(connection)
    yield connection
    connection.close()


@pytest.fixture(autouse=True)
def no_real_network(monkeypatch):
    """Belt-and-suspenders: fail loudly if any test accidentally reaches
    urlopen, rather than silently hitting the real network."""
    def _forbidden(*args, **kwargs):
        raise AssertionError("A test attempted a real network call via urlopen — must be mocked")
    monkeypatch.setattr(ebay_api_common, "urlopen", _forbidden, raising=False)


# ── Escalation behavior (mocking search_items_single_marketplace) ─────────────

def test_tier1_success_stops_escalation(conn):
    """If Tier 1 alone reaches MIN_UNIQUE_RESULTS, no Tier 2/4 query runs."""
    calls = []

    def fake_search(*, marketplace_id, keyword, **kwargs):
        calls.append((marketplace_id, keyword))
        if keyword == "Rolex 1030 6941":
            return [_fake_item(f"item{i}", marketplace_id) for i in range(collect04.MIN_UNIQUE_RESULTS)]
        return []

    with patch.object(collect04, "search_items_single_marketplace", side_effect=fake_search):
        result = collect04.escalate_for_marketplace(
            token="fake", marketplace_id="EBAY_DE",
            queries=collect04.get_queries_for_item(conn, "iuid_pass1"),
            call_budget=collect04.CallBudget(100), log_prefix="test:",
        )

    assert result["outcome_reason"] == "success"
    assert result["resolved_tier"] == 1
    assert len(result["listings"]) == collect04.MIN_UNIQUE_RESULTS
    tier2_calls = [k for _, k in calls if "Cal" in k]
    assert tier2_calls == [], f"Tier 2 should never have been queried, but got {tier2_calls}"


def test_tier_escalation_when_tier1_insufficient(conn):
    """Tier 1 alone returns too few results; escalation must continue to Tier 2."""
    def fake_search(*, marketplace_id, keyword, **kwargs):
        if keyword == "Rolex 1030 6941":
            return [_fake_item("item1", marketplace_id)]  # only 1, below threshold
        if keyword == "Rolex Cal 1030 6941":
            return [_fake_item(f"item{i}", marketplace_id) for i in range(2, 2 + collect04.MIN_UNIQUE_RESULTS)]
        return []

    with patch.object(collect04, "search_items_single_marketplace", side_effect=fake_search):
        result = collect04.escalate_for_marketplace(
            token="fake", marketplace_id="EBAY_DE",
            queries=collect04.get_queries_for_item(conn, "iuid_pass1"),
            call_budget=collect04.CallBudget(100), log_prefix="test:",
        )

    assert result["outcome_reason"] == "success"
    assert result["resolved_tier"] == 2
    assert len(result["listings"]) >= collect04.MIN_UNIQUE_RESULTS


def test_stops_after_successful_tier2_variant_skips_remaining_variants(conn):
    """Once one Tier 2 variant satisfies the threshold, remaining Tier 2
    variants (Calibre, Caliber, Kaliber) must not be queried."""
    queried_tier2_variants = []

    def fake_search(*, marketplace_id, keyword, **kwargs):
        if keyword == "Rolex 1030 6941":
            return []
        if keyword.startswith("Rolex Cal ") or keyword.startswith("Rolex Calibre") or \
           keyword.startswith("Rolex Caliber") or keyword.startswith("Rolex Kaliber"):
            queried_tier2_variants.append(keyword)
            if keyword == "Rolex Cal 1030 6941":
                return [_fake_item(f"item{i}", marketplace_id) for i in range(collect04.MIN_UNIQUE_RESULTS)]
            return []
        return []

    with patch.object(collect04, "search_items_single_marketplace", side_effect=fake_search):
        result = collect04.escalate_for_marketplace(
            token="fake", marketplace_id="EBAY_DE",
            queries=collect04.get_queries_for_item(conn, "iuid_pass1"),
            call_budget=collect04.CallBudget(100), log_prefix="test:",
        )

    assert result["resolved_tier"] == 2
    assert queried_tier2_variants == ["Rolex Cal 1030 6941"], (
        f"Expected only the first (alphabetical) Tier 2 variant to be queried, got {queried_tier2_variants}"
    )


def test_tier4_fallback(conn):
    """Tier 1 and all Tier 2 variants insufficient — falls through to Tier 4."""
    def fake_search(*, marketplace_id, keyword, **kwargs):
        if keyword == "Rolex 1030":
            return [_fake_item(f"item{i}", marketplace_id) for i in range(collect04.MIN_UNIQUE_RESULTS)]
        return []

    with patch.object(collect04, "search_items_single_marketplace", side_effect=fake_search):
        result = collect04.escalate_for_marketplace(
            token="fake", marketplace_id="EBAY_DE",
            queries=collect04.get_queries_for_item(conn, "iuid_pass1"),
            call_budget=collect04.CallBudget(100), log_prefix="test:",
        )

    assert result["outcome_reason"] == "success"
    assert result["resolved_tier"] == 4
    assert result["highest_tier_attempted"] == 4


def test_tier5_fallback_for_warning_distinctive_item(conn):
    def fake_search(*, marketplace_id, keyword, **kwargs):
        assert keyword == "24-26812-8"
        return [_fake_item(f"item{i}", marketplace_id) for i in range(collect04.MIN_UNIQUE_RESULTS)]

    with patch.object(collect04, "search_items_single_marketplace", side_effect=fake_search):
        result = collect04.escalate_for_marketplace(
            token="fake", marketplace_id="EBAY_DE",
            queries=collect04.get_queries_for_item(conn, "iuid_warn1"),
            call_budget=collect04.CallBudget(100), log_prefix="test:",
        )

    assert result["resolved_tier"] == 5
    assert result["outcome_reason"] == "success"


def test_zero_executable_queries_is_not_an_error(conn):
    """An item with no search_queries rows logs as no_executable_queries and
    is skipped without raising — this is a legitimate outcome."""
    with patch.object(collect04, "search_items_single_marketplace") as mock_search:
        result = collect04.escalate_for_marketplace(
            token="fake", marketplace_id="EBAY_DE",
            queries=collect04.get_queries_for_item(conn, "iuid_zero1"),
            call_budget=collect04.CallBudget(100), log_prefix="test:",
        )
        mock_search.assert_not_called()

    assert result["outcome_reason"] == "no_executable_queries"
    assert result["resolved_tier"] is None
    assert result["listings"] == {}


def test_fail_items_never_appear_in_eligible_inventory(conn):
    df = collect04.get_eligible_inventory(conn)
    assert "iuid_fail1" not in df["inventory_uid"].tolist()


# ── --inventory-manifest: reproducible, stratified manifest-based selection ────

def _write_manifest(tmp_path, rows: list[dict], filename: str = "manifest.csv") -> Path:
    path = tmp_path / filename
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def test_manifest_valid_returns_items_in_manifest_order(conn, tmp_path):
    """A well-formed manifest listing eligible UIDs (PASS + WARNING, never
    FAIL) must be accepted exactly, in the manifest's own order — not
    database/inventory_uid order."""
    manifest_path = _write_manifest(tmp_path, [
        {"sample_position": 1, "inventory_uid": "iuid_warn1"},
        {"sample_position": 2, "inventory_uid": "iuid_pass1"},
    ])
    df = collect04.get_inventory_from_manifest(conn, manifest_path)
    assert df["inventory_uid"].tolist() == ["iuid_warn1", "iuid_pass1"]
    assert len(df) == 2


def test_manifest_duplicate_uid_rejected(conn, tmp_path):
    manifest_path = _write_manifest(tmp_path, [
        {"sample_position": 1, "inventory_uid": "iuid_pass1"},
        {"sample_position": 2, "inventory_uid": "iuid_pass1"},
    ])
    with pytest.raises(ValueError, match="duplicate"):
        collect04.get_inventory_from_manifest(conn, manifest_path)


def test_manifest_missing_uid_rejected(conn, tmp_path):
    """An inventory_uid that doesn't exist in staging_inventory at all must
    be rejected with a clear report, never silently dropped."""
    manifest_path = _write_manifest(tmp_path, [
        {"sample_position": 1, "inventory_uid": "iuid_pass1"},
        {"sample_position": 2, "inventory_uid": "iuid_does_not_exist"},
    ])
    with pytest.raises(ValueError, match="iuid_does_not_exist"):
        collect04.get_inventory_from_manifest(conn, manifest_path)


def test_manifest_ineligible_fail_uid_rejected(conn, tmp_path):
    """A FAIL-status UID must be rejected, not silently included or
    silently swapped for a different item."""
    manifest_path = _write_manifest(tmp_path, [
        {"sample_position": 1, "inventory_uid": "iuid_pass1"},
        {"sample_position": 2, "inventory_uid": "iuid_fail1"},
    ])
    with pytest.raises(ValueError, match="iuid_fail1"):
        collect04.get_inventory_from_manifest(conn, manifest_path)


def test_manifest_deterministic_ordering_uses_sample_position_not_row_order(conn, tmp_path):
    """If sample_position is present, it governs the order — even when raw
    CSV row order disagrees (e.g. a spreadsheet tool re-saved the file)."""
    manifest_path = _write_manifest(tmp_path, [
        {"sample_position": 2, "inventory_uid": "iuid_pass1"},
        {"sample_position": 1, "inventory_uid": "iuid_warn1"},
    ])
    df = collect04.get_inventory_from_manifest(conn, manifest_path)
    assert df["inventory_uid"].tolist() == ["iuid_warn1", "iuid_pass1"]

    # Re-running against the same file is byte-for-byte reproducible.
    df2 = collect04.get_inventory_from_manifest(conn, manifest_path)
    assert df["inventory_uid"].tolist() == df2["inventory_uid"].tolist()


def test_manifest_dry_run_reports_accepted_count(conn, tmp_path, capsys):
    manifest_path = _write_manifest(tmp_path, [
        {"sample_position": 1, "inventory_uid": "iuid_pass1"},
        {"sample_position": 2, "inventory_uid": "iuid_warn1"},
    ])
    with patch.object(collect04, "search_items_single_marketplace") as mock_search:
        collect04.dry_run(conn, None, None, inventory_manifest=str(manifest_path))
        mock_search.assert_not_called()  # dry-run must never call eBay

    out = capsys.readouterr().out
    assert "2 accepted, 0 rejected" in out
    assert "Eligible inventory items: 2" in out


def test_manifest_dry_run_propagates_rejection_without_calling_ebay(conn, tmp_path):
    manifest_path = _write_manifest(tmp_path, [
        {"sample_position": 1, "inventory_uid": "iuid_fail1"},
    ])
    with patch.object(collect04, "search_items_single_marketplace") as mock_search:
        with pytest.raises(ValueError, match="iuid_fail1"):
            collect04.dry_run(conn, None, None, inventory_manifest=str(manifest_path))
        mock_search.assert_not_called()


def test_parse_args_rejects_manifest_with_inventory_uid(monkeypatch, tmp_path, capsys):
    manifest_path = tmp_path / "m.csv"
    manifest_path.write_text("inventory_uid\niuid_pass1\n")
    monkeypatch.setattr(sys, "argv", [
        "04_collect_targeted_active.py",
        "--inventory-manifest", str(manifest_path),
        "--inventory-uid", "iuid_pass1",
    ])
    with pytest.raises(SystemExit):
        collect04.parse_args()
    assert "cannot be combined" in capsys.readouterr().err


def test_parse_args_rejects_manifest_with_limit_items(monkeypatch, tmp_path, capsys):
    manifest_path = tmp_path / "m.csv"
    manifest_path.write_text("inventory_uid\niuid_pass1\n")
    monkeypatch.setattr(sys, "argv", [
        "04_collect_targeted_active.py",
        "--inventory-manifest", str(manifest_path),
        "--limit-items", "5",
    ])
    with pytest.raises(SystemExit):
        collect04.parse_args()
    assert "cannot be combined" in capsys.readouterr().err


def test_parse_args_accepts_manifest_alone_and_with_resume_or_dry_run(monkeypatch, tmp_path):
    manifest_path = tmp_path / "m.csv"
    manifest_path.write_text("inventory_uid\niuid_pass1\n")

    monkeypatch.setattr(sys, "argv", ["04_collect_targeted_active.py", "--inventory-manifest", str(manifest_path)])
    args = collect04.parse_args()
    assert args.inventory_manifest == str(manifest_path)

    monkeypatch.setattr(sys, "argv", [
        "04_collect_targeted_active.py", "--inventory-manifest", str(manifest_path), "--dry-run",
    ])
    args = collect04.parse_args()
    assert args.inventory_manifest == str(manifest_path) and args.dry_run is True

    monkeypatch.setattr(sys, "argv", [
        "04_collect_targeted_active.py", "--inventory-manifest", str(manifest_path), "--resume",
    ])
    args = collect04.parse_args()
    assert args.inventory_manifest == str(manifest_path) and args.resume is True


# ── Max-call limit ─────────────────────────────────────────────────────────────

def test_max_calls_per_chunk_stops_escalation(conn):
    def fake_search(*, marketplace_id, keyword, on_call=None, **kwargs):
        if on_call:
            on_call()  # simulate the real function reporting a call was made
        return []  # never satisfies threshold, forces continued escalation

    budget = collect04.CallBudget(1)
    with patch.object(collect04, "search_items_single_marketplace", side_effect=fake_search):
        result = collect04.escalate_for_marketplace(
            token="fake", marketplace_id="EBAY_DE",
            queries=collect04.get_queries_for_item(conn, "iuid_pass1"),
            call_budget=budget, log_prefix="test:",
        )

    assert budget.calls_made == 1
    assert result["outcome_reason"] == "max_calls_reached"


# ── Deduplication across queries/marketplaces ─────────────────────────────────

def test_dedup_same_item_across_tiers(conn):
    """The same item_id returned by two different queries in the same
    marketplace escalation must be counted once, not twice."""
    def fake_search(*, marketplace_id, keyword, **kwargs):
        if keyword == "Rolex 1030 6941":
            return [_fake_item("dup_item", marketplace_id), _fake_item("unique1", marketplace_id)]
        if keyword == "Rolex Cal 1030 6941":
            return [_fake_item("dup_item", marketplace_id)] + [
                _fake_item(f"item{i}", marketplace_id) for i in range(collect04.MIN_UNIQUE_RESULTS)
            ]
        return []

    with patch.object(collect04, "search_items_single_marketplace", side_effect=fake_search):
        result = collect04.escalate_for_marketplace(
            token="fake", marketplace_id="EBAY_DE",
            queries=collect04.get_queries_for_item(conn, "iuid_pass1"),
            call_budget=collect04.CallBudget(100), log_prefix="test:",
        )

    ids = list(result["listings"].keys())
    assert len(ids) == len(set(ids)), "item_id must not appear more than once in merged listings"
    assert ids.count("dup_item") <= 1
    assert result["listings"]["dup_item"]["query_tier"] == 1, (
        "dup_item was first found at Tier 1 and must retain that tier, not be overwritten by Tier 2"
    )


def test_tier1_and_tier4_same_item_retains_tier1_provenance(conn):
    """The same item_id can be returned by both Tier 1 and Tier 4 within one
    escalation run (e.g. a broad Tier 4 caliber query recovers a listing a
    specific Tier 1 query already found). Tier 1 is the more specific
    retrieval evidence and must survive even though Tier 4 is processed
    later and is what ultimately pushes the result over MIN_UNIQUE_RESULTS."""
    def fake_search(*, marketplace_id, keyword, **kwargs):
        if keyword == "Rolex 1030 6941":
            return [_fake_item("shared_item", marketplace_id, title="Tier1 title")]
        if keyword.startswith("Rolex Cal") or keyword.startswith("Rolex Calibre") or \
           keyword.startswith("Rolex Caliber") or keyword.startswith("Rolex Kaliber"):
            return []
        if keyword == "Rolex 1030":
            items = [_fake_item("shared_item", marketplace_id, title="Tier4 title")]
            items += [_fake_item(f"new_item{i}", marketplace_id) for i in range(4)]
            return items
        return []

    with patch.object(collect04, "search_items_single_marketplace", side_effect=fake_search):
        result = collect04.escalate_for_marketplace(
            token="fake", marketplace_id="EBAY_DE",
            queries=collect04.get_queries_for_item(conn, "iuid_pass1"),
            call_budget=collect04.CallBudget(100), log_prefix="test:",
        )

    assert result["resolved_tier"] == 4  # threshold only reached once Tier 4's new items are added
    assert len(result["listings"]) == 5
    shared = result["listings"]["shared_item"]
    assert shared["query_tier"] == 1, f"expected Tier 1 provenance retained, got tier {shared['query_tier']}"
    assert shared["title"] == "Tier1 title"
    assert shared["query_text"] == "Rolex 1030 6941"


def test_merge_listing_by_lowest_tier_retains_lowest_regardless_of_order():
    """Direct unit test of the merge primitive itself, independent of
    escalate_for_marketplace's fixed ascending tier processing order — the
    real function can never present tier 4 before tier 1 to this helper,
    but the helper's own guarantee must hold for any call order."""
    # Tier 4 arrives first, Tier 1 second -> Tier 1 must still win.
    merged = {}
    collect04.merge_listing_by_lowest_tier(merged, "item1", 4, {"query_tier": 4, "title": "T4"})
    collect04.merge_listing_by_lowest_tier(merged, "item1", 1, {"query_tier": 1, "title": "T1"})
    assert merged["item1"] == {"query_tier": 1, "title": "T1"}

    # Tier 1 arrives first, Tier 4 second -> Tier 1 must still win (no overwrite).
    merged = {}
    collect04.merge_listing_by_lowest_tier(merged, "item1", 1, {"query_tier": 1, "title": "T1"})
    collect04.merge_listing_by_lowest_tier(merged, "item1", 4, {"query_tier": 4, "title": "T4"})
    assert merged["item1"] == {"query_tier": 1, "title": "T1"}

    # Same tier seen twice -> first-seen record kept, no thrashing.
    merged = {}
    collect04.merge_listing_by_lowest_tier(merged, "item1", 2, {"query_tier": 2, "title": "first"})
    collect04.merge_listing_by_lowest_tier(merged, "item1", 2, {"query_tier": 2, "title": "second"})
    assert merged["item1"]["title"] == "first"

    # Fully scrambled arrival order across four tiers -> lowest still wins.
    merged = {}
    for tier, title in [(4, "t4"), (2, "t2"), (5, "t5"), (1, "t1")]:
        collect04.merge_listing_by_lowest_tier(merged, "item1", tier, {"query_tier": tier, "title": title})
    assert merged["item1"] == {"query_tier": 1, "title": "t1"}


def test_cross_marketplace_evidence_preserved_not_collapsed(tmp_path):
    """The same item_id found via two marketplaces must produce TWO CSV
    rows (one per marketplace) — marketplace evidence must never be
    silently discarded. Within a single marketplace, the same item_id must
    still only appear once."""
    item_results = [{
        "inventory_uid": "iuid_pass1",
        "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {
            "EBAY_DE": {"listings": {"shared_item": {
                "query_text": "Rolex 1030 6941", "query_tier": 1, "marketplace_id": "EBAY_DE",
                "fetched_at": "2026-01-01T00:00:00", "item_id": "shared_item", "title": "DE version",
                "price_value": "50", "price_currency": "EUR", "condition": "", "condition_id": "",
                "buying_options": "", "item_web_url": "", "image_url": "", "seller_username": "",
                "seller_feedback_score": "", "seller_feedback_percentage": "", "shipping_cost_value": "",
                "shipping_cost_currency": "", "item_location_country": "", "item_location_city": "",
                "item_creation_date": "",
            }}, "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success"},
            "EBAY_US": {"listings": {"shared_item": {
                "query_text": "Rolex 1030 6941", "query_tier": 1, "marketplace_id": "EBAY_US",
                "fetched_at": "2026-01-01T00:00:00", "item_id": "shared_item", "title": "US version",
                "price_value": "60", "price_currency": "USD", "condition": "", "condition_id": "",
                "buying_options": "", "item_web_url": "", "image_url": "", "seller_username": "",
                "seller_feedback_score": "", "seller_feedback_percentage": "", "shipping_cost_value": "",
                "shipping_cost_currency": "", "item_location_country": "", "item_location_city": "",
                "item_creation_date": "",
            }}, "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success"},
        },
    }]

    csv_path = collect04.write_batch_csv("batch_test", "batch_test_chunk_1", item_results, "v1", output_dir=tmp_path)
    df = pd.read_csv(csv_path)
    assert len(df) == 2, "both marketplaces' observations of the same item_id must be preserved as separate rows"
    assert set(df["marketplace_id"]) == {"EBAY_DE", "EBAY_US"}
    de_row = df[df["marketplace_id"] == "EBAY_DE"].iloc[0]
    us_row = df[df["marketplace_id"] == "EBAY_US"].iloc[0]
    assert de_row["title"] == "DE version"
    assert us_row["title"] == "US version"
    assert de_row["item_id"] == us_row["item_id"] == "shared_item"
    assert df.iloc[0]["title"] == "DE version"


# ── Pagination and retry (mocking one level deeper: urlopen via request_json) ─

class _FakeResponse:
    def __init__(self, payload: dict):
        self._payload = json.dumps(payload).encode("utf-8")

    def read(self):
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def test_bounded_pagination_stops_at_max_pages(monkeypatch):
    call_count = {"n": 0}

    def fake_urlopen(request, timeout=45, context=None):
        call_count["n"] += 1
        return _FakeResponse({
            "itemSummaries": [{"itemId": f"p{call_count['n']}"}],
            "next": "https://more",  # always claims more pages exist
        })

    monkeypatch.setattr(ebay_api_common, "urlopen", fake_urlopen)

    items = ebay_api_common.search_items_single_marketplace(
        token="fake", marketplace_id="EBAY_DE", country_name="Germany",
        keyword="test", limit=10, max_items=None, sort="newlyListed", filters=[],
        max_pages=3,
    )
    assert call_count["n"] == 3, "must stop exactly at max_pages even though eBay keeps signaling 'next'"
    assert len(items) == 3


def test_retry_on_throttling_then_success(monkeypatch):
    import io
    from urllib.error import HTTPError

    attempts = {"n": 0}

    def fake_urlopen(request, timeout=45, context=None):
        attempts["n"] += 1
        if attempts["n"] == 1:
            body = io.BytesIO(b'{"errorMessage": "rate limit exceeded"}')
            raise HTTPError(request.full_url, 429, "Too Many Requests", {}, body)
        return _FakeResponse({"itemSummaries": [{"itemId": "ok"}]})

    monkeypatch.setattr(ebay_api_common, "urlopen", fake_urlopen)

    result = ebay_api_common.request_json_with_retry(
        "https://example/search", headers={}, retry_count=2, initial_backoff_seconds=0.01, backoff_multiplier=1.0,
    )
    assert attempts["n"] == 2
    assert result["itemSummaries"][0]["itemId"] == "ok"


def test_retry_exhausted_raises(monkeypatch):
    import io
    from urllib.error import HTTPError

    def fake_urlopen(request, timeout=45, context=None):
        raise HTTPError(request.full_url, 503, "Service Unavailable", {}, io.BytesIO(b"server error"))

    monkeypatch.setattr(ebay_api_common, "urlopen", fake_urlopen)

    with pytest.raises(RuntimeError):
        ebay_api_common.request_json_with_retry(
            "https://example/search", headers={}, retry_count=2, initial_backoff_seconds=0.01, backoff_multiplier=1.0,
        )


# ── Query length validation ────────────────────────────────────────────────────

def test_query_length_validation():
    ok, _ = ebay_api_common.validate_query_length("Rolex 1030 6941")
    assert ok
    too_long, reason = ebay_api_common.validate_query_length("x" * 400)
    assert not too_long
    assert "350" in reason


# ── Resumability ───────────────────────────────────────────────────────────────

def test_resumability_skips_already_processed_items(conn, tmp_path):
    collect04.start_batch(conn, "batch_resume_test", {})
    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {"prior_item": {
                "query_text": "Rolex 1030 6941", "query_tier": 1, "marketplace_id": "EBAY_DE",
                "fetched_at": "2026-01-01T00:00:00", "item_id": "prior_item", "title": "T",
                "price_value": "10", "price_currency": "EUR", "condition": "", "condition_id": "",
                "buying_options": "", "item_web_url": "", "image_url": "", "seller_username": "",
                "seller_feedback_score": "", "seller_feedback_percentage": "", "shipping_cost_value": "",
                "shipping_cost_currency": "", "item_location_country": "", "item_location_city": "",
                "item_creation_date": "",
            }},
            "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]
    _durably_write_chunk(conn, tmp_path, "batch_resume_test", item_results)

    with patch.object(collect04, "search_items_single_marketplace") as mock_search:
        row = collect04.get_eligible_inventory(conn, "iuid_pass1").iloc[0]
        result = collect04.process_item(
            conn, item_row=row, token="fake", batch_id="batch_resume_test",
            call_budget=collect04.CallBudget(100), marketplaces=["EBAY_DE"], output_dir=tmp_path,
        )
        mock_search.assert_not_called()

    assert "EBAY_DE" not in result["per_marketplace"], "already-processed marketplace must be skipped, not re-run"


def test_find_resumable_batch_returns_unfinished_batch(conn):
    collect04.start_batch(conn, "batch_unfinished", {})
    assert collect04.find_resumable_batch(conn) == "batch_unfinished"
    collect04.finish_batch(conn, "batch_unfinished")
    assert collect04.find_resumable_batch(conn) is None


def test_end_to_end_resume_partial_item_skips_completed_marketplace_and_finishes_batch(conn, tmp_path):
    """
    Explicit walkthrough of the full resume lifecycle at (inventory_uid,
    marketplace_id) granularity, simulating a run interrupted after EBAY_DE's
    chunk was durably written but before EBAY_US started:
      1. Durable progress exists for EBAY_DE only (via a real chunk write,
         not a bare progress row — that's the whole point of the fix).
      2. find_resumable_batch locates the unfinished batch.
      3. Resuming calls process_item again with the same batch_id: EBAY_DE
         must NOT be re-called, EBAY_US must be called since outstanding.
      4. process_item alone does NOT persist EBAY_US's result — only after
         the chunk-level durable write (write_batch_csv + record_chunk_written
         + record_chunk_progress) does its progress row appear.
      5. The batch is only marked finished after that remaining work
         completes and is durably recorded — not before.
      6. Once finished, find_resumable_batch no longer returns it.
    """
    batch_id = "batch_e2e_resume"
    collect04.start_batch(conn, batch_id, {})
    de_only_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {f"item{i}": _flat_listing(f"item{i}", "EBAY_DE") for i in range(collect04.MIN_UNIQUE_RESULTS)},
            "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]
    _durably_write_chunk(conn, tmp_path, batch_id, de_only_results, chunk_id="batch_e2e_resume_chunk_1")

    # Simulate a process crash/restart: a fresh call to find_resumable_batch
    # must recover the same batch rather than starting a new one.
    assert collect04.find_resumable_batch(conn) == batch_id

    calls_made = {"EBAY_DE": 0, "EBAY_US": 0}

    def fake_search(*, marketplace_id, keyword, on_call=None, **kwargs):
        calls_made[marketplace_id] += 1
        if marketplace_id == "EBAY_DE":
            raise AssertionError("EBAY_DE was already durably processed in this batch and must not be re-called")
        if on_call:
            on_call()
        return [_fake_item(f"item{i}", marketplace_id) for i in range(collect04.MIN_UNIQUE_RESULTS)]

    with patch.object(collect04, "search_items_single_marketplace", side_effect=fake_search):
        row = collect04.get_eligible_inventory(conn, "iuid_pass1").iloc[0]
        call_budget = collect04.CallBudget(100)
        result = collect04.process_item(
            conn, item_row=row, token="fake", batch_id=batch_id,
            call_budget=call_budget, marketplaces=["EBAY_DE", "EBAY_US"], output_dir=tmp_path,
        )

    assert calls_made["EBAY_DE"] == 0, "durably completed marketplace must not be re-called"
    assert calls_made["EBAY_US"] == 1, "outstanding marketplace must be called exactly once"
    assert "EBAY_DE" not in result["per_marketplace"], "skipped marketplace produces no new result this run"
    assert result["per_marketplace"]["EBAY_US"]["outcome_reason"] == "success"

    # process_item alone must NOT have persisted EBAY_US's progress yet —
    # only chunk-level durability (below) does that.
    progress_rows = conn.execute(
        "SELECT marketplace_id FROM collection_progress WHERE collection_batch_id = ? ORDER BY marketplace_id",
        [batch_id],
    ).fetchall()
    assert [r[0] for r in progress_rows] == ["EBAY_DE"], "EBAY_US must not be marked done until durably written"

    # Now simulate the chunk loop's durable-write step for this new result.
    chunk_id, csv_path = _durably_write_chunk(conn, tmp_path, batch_id, [result], chunk_id="batch_e2e_resume_chunk_2")

    progress_rows = conn.execute(
        "SELECT marketplace_id FROM collection_progress WHERE collection_batch_id = ? ORDER BY marketplace_id",
        [batch_id],
    ).fetchall()
    assert [r[0] for r in progress_rows] == ["EBAY_DE", "EBAY_US"], "both marketplaces now durably recorded, none duplicated"

    # Batch must still be unfinished until the caller (run_chunked_collection)
    # confirms all selected inventory was iterated with the call budget intact.
    unfinished = conn.execute(
        "SELECT finished_at FROM collection_batches WHERE collection_batch_id = ?", [batch_id]
    ).fetchone()
    assert unfinished[0] is None
    assert collect04.find_resumable_batch(conn) == batch_id

    # Only one eligible item existed and it's now fully processed.
    inventory_df = collect04.get_eligible_inventory(conn, "iuid_pass1")
    fully_processed = collect04.batch_fully_processed(conn, batch_id, inventory_df, ["EBAY_DE", "EBAY_US"])
    assert fully_processed
    collect04.record_batch_stop_state(
        conn, batch_id, stop_reason="batch_fully_processed", chunks_completed=2,
        fully_processed=True, last_chunk_id=chunk_id,
    )

    finished = conn.execute(
        "SELECT finished_at, stop_reason, chunks_completed, fully_processed, last_chunk_id "
        "FROM collection_batches WHERE collection_batch_id = ?", [batch_id]
    ).fetchone()
    assert finished[0] is not None
    assert finished[1] == "batch_fully_processed"
    assert finished[2] == 2
    assert finished[3] is True
    assert finished[4] == chunk_id
    assert collect04.find_resumable_batch(conn) is None, "finished batch must no longer be offered for resume"


# ── Idempotency (through 01_ingest.py --targeted) ─────────────────────────────

def test_same_item_id_different_marketplace_both_kept_in_db(conn, tmp_path):
    """The database-level dedup key is (inventory_uid, item_id,
    marketplace_id), not item_id alone — a listing legitimately found via
    two marketplaces (same inventory item here) must produce two
    raw_active_targeted rows, not one. See
    test_same_listing_different_inventory_item_both_kept_in_db below for
    the inventory_uid dimension of this same key."""
    def listing_for(marketplace_id, title):
        return {
            "query_text": "Rolex 1030 6941", "query_tier": 1, "marketplace_id": marketplace_id,
            "fetched_at": "2026-01-01T00:00:00", "item_id": "cross_mp_item", "title": title,
            "price_value": "10", "price_currency": "EUR", "condition": "", "condition_id": "",
            "buying_options": "", "item_web_url": "", "image_url": "", "seller_username": "",
            "seller_feedback_score": "", "seller_feedback_percentage": "", "shipping_cost_value": "",
            "shipping_cost_currency": "", "item_location_country": "", "item_location_city": "",
            "item_creation_date": "",
        }

    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {
            "EBAY_DE": {"listings": {"cross_mp_item": listing_for("EBAY_DE", "DE version")},
                        "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success"},
            "EBAY_US": {"listings": {"cross_mp_item": listing_for("EBAY_US", "US version")},
                        "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success"},
        },
    }]

    targeted_dir = tmp_path / "targeted_active"
    collect04.write_batch_csv("batch_crossmp", "batch_crossmp_chunk_1", item_results, "v1", output_dir=targeted_dir)
    ingest01.TARGETED_ACTIVE_DIR = targeted_dir
    n_inserted = ingest01.insert_targeted_listings(conn)

    assert n_inserted == 2, "both marketplaces' rows for the same item_id must be inserted, not deduped away"
    rows = conn.execute(
        "SELECT marketplace_id, title FROM raw_active_targeted WHERE item_id = 'cross_mp_item' ORDER BY marketplace_id"
    ).fetchall()
    assert rows == [("EBAY_DE", "DE version"), ("EBAY_US", "US version")]

    # Re-running ingestion must still insert 0 new rows (idempotent), and
    # must not collapse the two marketplace rows into one on a repeat run.
    n_second = ingest01.insert_targeted_listings(conn)
    assert n_second == 0
    total = conn.execute("SELECT COUNT(*) FROM raw_active_targeted WHERE item_id = 'cross_mp_item'").fetchone()[0]
    assert total == 2


def test_write_batch_csv_preserves_same_listing_for_two_different_inventory_items(tmp_path):
    """
    Regression test for the confirmed evidence-loss bug: two different
    inventory items whose queries both legitimately surface the SAME real
    eBay listing (item_id + marketplace_id) must both get a CSV row for it
    — this is two distinct evidence relationships ("this listing was a
    candidate for item A" / "...for item B"), not one duplicated
    observation. Before the fix, write_batch_csv's seen_keys was keyed on
    (item_id, marketplace_id) alone, and the second item's row was
    silently dropped at CSV-write time — unrecoverable from any later
    ingestion fix, since it was never written to disk at all.
    """
    item_results = [
        {
            "inventory_uid": "iuid_item_a", "canonical_inventory_id": "rolex_32_557b",
            "per_marketplace": {
                "EBAY_DE": {
                    "listings": {"shared_listing": _flat_listing("shared_listing", "EBAY_DE", "Item A's view of it")},
                    "highest_tier_attempted": 4, "resolved_tier": 4, "api_calls": 1, "outcome_reason": "success",
                },
            },
        },
        {
            "inventory_uid": "iuid_item_b", "canonical_inventory_id": "rolex_32_20764",
            "per_marketplace": {
                "EBAY_DE": {
                    "listings": {"shared_listing": _flat_listing("shared_listing", "EBAY_DE", "Item B's view of it")},
                    "highest_tier_attempted": 4, "resolved_tier": 4, "api_calls": 1, "outcome_reason": "success",
                },
            },
        },
    ]

    output_dir = tmp_path / "targeted_active"
    csv_path = collect04.write_batch_csv("batch_collision", "batch_collision_chunk_1", item_results, "v1", output_dir=output_dir)

    df = pd.read_csv(csv_path)
    rows = df[df["item_id"] == "shared_listing"]
    assert len(rows) == 2, "both inventory items' rows for the shared listing must survive in the CSV, not just one"
    assert set(rows["inventory_uid"]) == {"iuid_item_a", "iuid_item_b"}
    assert set(rows["canonical_inventory_id"]) == {"rolex_32_557b", "rolex_32_20764"}


def test_same_listing_different_inventory_item_both_kept_in_db(conn, tmp_path):
    """
    End-to-end version of the collision regression, through real ingestion:
    the same (item_id, marketplace_id) surfacing for two different
    inventory items must produce TWO raw_active_targeted rows, one per
    item — proving the (inventory_uid, item_id, marketplace_id) dedup key
    holds through insert_targeted_listings, not only through
    write_batch_csv.
    """
    item_results = [
        {
            "inventory_uid": "iuid_item_a", "canonical_inventory_id": "rolex_32_557b",
            "per_marketplace": {
                "EBAY_DE": {
                    "listings": {"shared_listing": _flat_listing("shared_listing", "EBAY_DE", "Item A's view of it")},
                    "highest_tier_attempted": 4, "resolved_tier": 4, "api_calls": 1, "outcome_reason": "success",
                },
            },
        },
        {
            "inventory_uid": "iuid_item_b", "canonical_inventory_id": "rolex_32_20764",
            "per_marketplace": {
                "EBAY_DE": {
                    "listings": {"shared_listing": _flat_listing("shared_listing", "EBAY_DE", "Item B's view of it")},
                    "highest_tier_attempted": 4, "resolved_tier": 4, "api_calls": 1, "outcome_reason": "success",
                },
            },
        },
    ]

    targeted_dir = tmp_path / "targeted_active"
    collect04.write_batch_csv("batch_collision2", "batch_collision2_chunk_1", item_results, "v1", output_dir=targeted_dir)
    ingest01.TARGETED_ACTIVE_DIR = targeted_dir
    n_inserted = ingest01.insert_targeted_listings(conn)

    assert n_inserted == 2, "both items' rows for the shared listing must be inserted, not deduped away against each other"
    rows = conn.execute(
        "SELECT inventory_uid, canonical_inventory_id, title FROM raw_active_targeted "
        "WHERE item_id = 'shared_listing' ORDER BY inventory_uid"
    ).fetchall()
    assert rows == [
        ("iuid_item_a", "rolex_32_557b", "Item A's view of it"),
        ("iuid_item_b", "rolex_32_20764", "Item B's view of it"),
    ]

    # Idempotency must still hold under the new key: re-running ingestion
    # inserts zero further rows, and does not collapse the two items' rows.
    n_second = ingest01.insert_targeted_listings(conn)
    assert n_second == 0
    total = conn.execute("SELECT COUNT(*) FROM raw_active_targeted WHERE item_id = 'shared_listing'").fetchone()[0]
    assert total == 2


def test_same_inventory_item_same_listing_still_deduped_across_reingestion(conn, tmp_path):
    """
    Negative case, to prove the new key isn't over-broad: the SAME
    inventory item's own repeat observation of the SAME listing (e.g. a
    later chunk re-fetching it) must still be deduped as before — the
    inventory_uid dimension only prevents cross-ITEM collisions, it must
    not weaken the existing same-item duplicate suppression.
    """
    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {
            "EBAY_DE": {
                "listings": {"repeat_listing": _flat_listing("repeat_listing", "EBAY_DE", "First observation")},
                "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
            },
        },
    }]

    targeted_dir = tmp_path / "targeted_active"
    ingest01.TARGETED_ACTIVE_DIR = targeted_dir

    collect04.write_batch_csv("batch_dup1", "batch_dup1_chunk_1", item_results, "v1", output_dir=targeted_dir)
    n_first = ingest01.insert_targeted_listings(conn)
    assert n_first == 1

    # Same inventory_uid, same item_id, same marketplace, a separate later chunk.
    collect04.write_batch_csv("batch_dup1", "batch_dup1_chunk_2", item_results, "v1", output_dir=targeted_dir)
    n_second = ingest01.insert_targeted_listings(conn)
    assert n_second == 0, "the same item re-observing the same listing must still be deduped, not double-counted"

    total = conn.execute("SELECT COUNT(*) FROM raw_active_targeted WHERE item_id = 'repeat_listing'").fetchone()[0]
    assert total == 1


def test_later_batch_with_more_specific_tier_does_not_upgrade_existing_raw_row(conn, tmp_path):
    """
    Documents a real, currently-accepted limitation (see the KNOWN
    LIMITATION note in insert_targeted_listings' docstring, 01_ingest.py):
    escalate_for_marketplace's lowest-tier retention only applies WITHIN
    one collection run. If an earlier batch already ingested a
    (inventory_uid, item_id, marketplace_id) row at a less specific tier,
    and a later batch's file resolves the SAME triple at a more specific
    (lower-numbered) tier, insert_targeted_listings' existing-triple dedup
    skips the later row entirely — it does NOT update the raw row's
    query_tier/query_text in place. That would mean mutating a raw_* row
    after the fact, which violates this table's raw-layer contract (exact
    copy of source data, never modified). Ingestion itself must still be
    idempotent (no duplicate row, no error) even though provenance is
    stale — both properties are asserted here.
    """
    targeted_dir = tmp_path / "targeted_active"
    ingest01.TARGETED_ACTIVE_DIR = targeted_dir

    less_specific = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {
            "EBAY_DE": {
                "listings": {
                    "same_listing": {
                        **_flat_listing("same_listing", "EBAY_DE", "Found via broad query"),
                        "query_tier": 4, "query_text": "Rolex 1030",
                    }
                },
                "highest_tier_attempted": 4, "resolved_tier": 4, "api_calls": 1, "outcome_reason": "success",
            },
        },
    }]
    collect04.write_batch_csv("batch_early", "batch_early_chunk_1", less_specific, "v1", output_dir=targeted_dir)
    n_first = ingest01.insert_targeted_listings(conn)
    assert n_first == 1

    more_specific = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {
            "EBAY_DE": {
                "listings": {
                    "same_listing": {
                        **_flat_listing("same_listing", "EBAY_DE", "Found via specific query"),
                        "query_tier": 1, "query_text": "Rolex 1030 6941",
                    }
                },
                "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
            },
        },
    }]
    collect04.write_batch_csv("batch_later", "batch_later_chunk_1", more_specific, "v1", output_dir=targeted_dir)

    # Ingestion stays idempotent: no error, and no duplicate row created...
    n_second = ingest01.insert_targeted_listings(conn)
    assert n_second == 0, "the triple already exists — ingestion must not raise or double-insert"

    # ...but this IS the documented limitation: the raw row keeps its
    # original, less-specific tier rather than being silently upgraded.
    row = conn.execute(
        "SELECT query_tier, query_text, title FROM raw_active_targeted WHERE item_id = 'same_listing'"
    ).fetchall()
    assert len(row) == 1
    assert row[0] == (4, "Rolex 1030", "Found via broad query"), (
        "raw_active_targeted must not be mutated by a later, more-specific-tier observation — "
        f"got {row[0]}"
    )


def test_cross_chunk_idempotency_across_separate_files_and_ingestion_calls(conn, tmp_path):
    """
    Chunking means multiple separate CSVs and multiple separate
    `01_ingest.py --targeted` invocations over time, not one file/one call.
    This proves dedup holds across that boundary, not only within a single
    file/call:
      1. Chunk A's CSV has "shared_item" on EBAY_DE plus a chunk-A-only item.
      2. Chunk A is ingested on its own (chunk B does not exist yet).
      3. Chunk B's CSV — a separate file, separate write_batch_csv call —
         re-includes "shared_item" on EBAY_DE (a genuine re-fetch across
         chunks), adds "shared_item" on EBAY_US (a new marketplace
         observation, must be preserved, not deduped away), and a
         chunk-B-only item.
      4. Chunk B is ingested via a *separate* insert_targeted_listings()
         call, with both files now present in the directory.
    Expected: the repeated (shared_item, EBAY_DE) row from chunk B inserts
    zero new rows (file-level skip for chunk A's file, row-level dedup for
    the repeat inside chunk B's file); the new (shared_item, EBAY_US) row
    and the two chunk-only items all insert exactly once; a third ingestion
    call (both files already ingested) inserts zero further rows.
    """
    def listing_for(item_id, marketplace_id, title):
        return {
            "query_text": "Rolex 1030 6941", "query_tier": 1, "marketplace_id": marketplace_id,
            "fetched_at": "2026-01-01T00:00:00", "item_id": item_id, "title": title,
            "price_value": "10", "price_currency": "EUR", "condition": "", "condition_id": "",
            "buying_options": "", "item_web_url": "", "image_url": "", "seller_username": "",
            "seller_feedback_score": "", "seller_feedback_percentage": "", "shipping_cost_value": "",
            "shipping_cost_currency": "", "item_location_country": "", "item_location_city": "",
            "item_creation_date": "",
        }

    chunk_a_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {
            "EBAY_DE": {
                "listings": {
                    "shared_item": listing_for("shared_item", "EBAY_DE", "Shared, chunk A observation"),
                    "chunkA_only_item": listing_for("chunkA_only_item", "EBAY_DE", "Chunk A exclusive"),
                },
                "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
            },
        },
    }]
    chunk_b_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {
            "EBAY_DE": {
                "listings": {
                    "shared_item": listing_for("shared_item", "EBAY_DE", "Shared, re-fetched in chunk B"),
                    "chunkB_only_item": listing_for("chunkB_only_item", "EBAY_DE", "Chunk B exclusive"),
                },
                "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
            },
            "EBAY_US": {
                "listings": {"shared_item": listing_for("shared_item", "EBAY_US", "Shared, US marketplace")},
                "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
            },
        },
    }]

    targeted_dir = tmp_path / "targeted_active"
    ingest01.TARGETED_ACTIVE_DIR = targeted_dir

    # Step 1-2: chunk A written and ingested on its own.
    collect04.write_batch_csv("batch_x", "batch_x_chunk_a", chunk_a_results, "v1", output_dir=targeted_dir)
    n_a = ingest01.insert_targeted_listings(conn)
    assert n_a == 2, "chunk A: shared_item/DE + chunkA_only_item/DE"

    # Step 3-4: chunk B written as a separate file; ingested via a separate call.
    collect04.write_batch_csv("batch_x", "batch_x_chunk_b", chunk_b_results, "v1", output_dir=targeted_dir)
    csv_files_present = sorted(p.name for p in targeted_dir.glob("targeted_active_*.csv"))
    assert csv_files_present == ["targeted_active_batch_x_chunk_a.csv", "targeted_active_batch_x_chunk_b.csv"]
    n_b = ingest01.insert_targeted_listings(conn)

    assert n_b == 2, (
        "chunk B has 3 rows, but shared_item/EBAY_DE is a duplicate of chunk A's row — "
        "only shared_item/EBAY_US (new marketplace) and chunkB_only_item/EBAY_DE (new item) should insert"
    )

    total = conn.execute("SELECT COUNT(*) FROM raw_active_targeted").fetchone()[0]
    assert total == 4, "2 from chunk A + 2 genuinely new from chunk B = 4, not 5"

    shared_rows = conn.execute(
        "SELECT marketplace_id, title FROM raw_active_targeted WHERE item_id = 'shared_item' ORDER BY marketplace_id"
    ).fetchall()
    assert shared_rows == [
        ("EBAY_DE", "Shared, chunk A observation"),  # chunk A's row wins; chunk B's re-fetch was the duplicate
        ("EBAY_US", "Shared, US marketplace"),        # new marketplace observation, preserved
    ], "shared_item must end up with exactly one row per marketplace, never collapsed, never duplicated"

    # A third ingestion call: both files already recorded in ingestion_log —
    # zero further rows, proving stability beyond just "twice".
    n_third = ingest01.insert_targeted_listings(conn)
    assert n_third == 0
    total_after_third = conn.execute("SELECT COUNT(*) FROM raw_active_targeted").fetchone()[0]
    assert total_after_third == 4


def test_idempotent_ingestion_zero_duplicates(conn, tmp_path):
    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {"idem_item": {
                "query_text": "Rolex 1030 6941", "query_tier": 1, "marketplace_id": "EBAY_DE",
                "fetched_at": "2026-01-01T00:00:00", "item_id": "idem_item", "title": "T",
                "price_value": "10", "price_currency": "EUR", "condition": "", "condition_id": "",
                "buying_options": "", "item_web_url": "", "image_url": "", "seller_username": "",
                "seller_feedback_score": "", "seller_feedback_percentage": "", "shipping_cost_value": "",
                "shipping_cost_currency": "", "item_location_country": "", "item_location_city": "",
                "item_creation_date": "",
            }}, "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]

    targeted_dir = tmp_path / "targeted_active"
    collect04.write_batch_csv("batch_idem", "batch_idem_chunk_1", item_results, "v1", output_dir=targeted_dir)

    ingest01.TARGETED_ACTIVE_DIR = targeted_dir
    n1 = ingest01.insert_targeted_listings(conn)
    n2 = ingest01.insert_targeted_listings(conn)  # re-run: same file, already ingested

    assert n1 == 1
    assert n2 == 0
    total = conn.execute("SELECT COUNT(*) FROM raw_active_targeted").fetchone()[0]
    assert total == 1, "running ingestion twice must not create duplicate raw rows"


def test_inventory_uid_lineage_preserved(conn, tmp_path):
    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {"lineage_item": {
                "query_text": "Rolex 1030 6941", "query_tier": 1, "marketplace_id": "EBAY_DE",
                "fetched_at": "2026-01-01T00:00:00", "item_id": "lineage_item", "title": "T",
                "price_value": "10", "price_currency": "EUR", "condition": "", "condition_id": "",
                "buying_options": "", "item_web_url": "", "image_url": "", "seller_username": "",
                "seller_feedback_score": "", "seller_feedback_percentage": "", "shipping_cost_value": "",
                "shipping_cost_currency": "", "item_location_country": "", "item_location_city": "",
                "item_creation_date": "",
            }}, "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]
    targeted_dir = tmp_path / "targeted_active"
    collect04.write_batch_csv("batch_lineage", "batch_lineage_chunk_1", item_results, "v1", output_dir=targeted_dir)
    ingest01.TARGETED_ACTIVE_DIR = targeted_dir
    ingest01.insert_targeted_listings(conn)

    row = conn.execute(
        "SELECT inventory_uid, canonical_inventory_id, query_tier, query_template_version, marketplace_id, row_hash "
        "FROM raw_active_targeted WHERE item_id = 'lineage_item'"
    ).fetchone()
    assert row[0] == "iuid_pass1"
    assert row[1] == "rolex_1030_6941"
    assert row[2] == 1
    assert row[3] == "v1"
    assert row[4] == "EBAY_DE"
    assert row[5]  # row_hash populated


# ── Tier 2 consumption order ───────────────────────────────────────────────────

def test_tier2_order_matches_generator_prefixes_not_alphabetical(conn):
    """search_queries has no sequence column, so alphabetical ORDER BY would
    silently swap Caliber/Calibre relative to 03_generate_queries.py's actual
    CALIBER_PREFIXES = ["Cal", "Calibre", "Caliber", "Kaliber"] list (since
    "Caliber" < "Calibre" alphabetically). Insert Tier 2 rows out of order and
    confirm get_queries_for_item still reconstructs the generator's order."""
    conn.execute(
        "INSERT INTO search_queries (inventory_uid, canonical_inventory_id, tier, query_text, uses_lexicon, query_template_version) VALUES "
        "('iuid_tier2order', 'x', 2, 'Rolex Kaliber 99 1', False, 'v1'), "
        "('iuid_tier2order', 'x', 2, 'Rolex Caliber 99 1', False, 'v1'), "
        "('iuid_tier2order', 'x', 2, 'Rolex Calibre 99 1', False, 'v1'), "
        "('iuid_tier2order', 'x', 2, 'Rolex Cal 99 1', False, 'v1'), "
        "('iuid_tier2order', 'x', 1, 'Rolex 99 1', False, 'v1')"
    )
    queries = collect04.get_queries_for_item(conn, "iuid_tier2order")
    tier2_texts = [text for tier, text in queries if tier == 2]
    assert tier2_texts == [
        "Rolex Cal 99 1",
        "Rolex Calibre 99 1",
        "Rolex Caliber 99 1",
        "Rolex Kaliber 99 1",
    ]
    assert queries[0] == (1, "Rolex 99 1")


# ── Secret redaction ───────────────────────────────────────────────────────────

def test_redact_sensitive_scrubs_bearer_and_basic_tokens():
    text = 'Authorization: Bearer abc123.XYZ_-secret\nBasic dGVzdDpzZWNyZXQ='
    redacted = ebay_api_common.redact_sensitive(text)
    assert "abc123" not in redacted
    assert "dGVzdDpzZWNyZXQ" not in redacted
    assert "[REDACTED]" in redacted


def test_log_and_print_redacts(tmp_path):
    # Isolated log_dir — never touches the real logs/ directory.
    collect04.setup_logging(log_dir=tmp_path)
    collect04.log_and_print("token leaked: Bearer super-secret-token-value")

    log_path = tmp_path / "04_collect_targeted_active.log"
    assert log_path.exists()
    content = log_path.read_text()
    assert "super-secret-token-value" not in content


# ── Quota safety margin ────────────────────────────────────────────────────────

def test_compute_safe_call_budget_applies_margin():
    assert collect04.compute_safe_call_budget(1200, margin=1.2) == 1000
    assert collect04.compute_safe_call_budget(0, margin=1.2) == 0
    assert collect04.compute_safe_call_budget(-5, margin=1.2) == 0, "never negative"


def test_batch_fully_processed_requires_all_expected_pairs(conn):
    inventory_df = collect04.get_eligible_inventory(conn, "iuid_pass1")
    assert collect04.batch_fully_processed(conn, "batch_x", inventory_df, ["EBAY_DE", "EBAY_US"]) is False

    collect04.start_batch(conn, "batch_x", {})
    collect04.record_progress(
        conn, batch_id="batch_x", chunk_id="batch_x_chunk_1", inventory_uid="iuid_pass1", marketplace_id="EBAY_DE",
        highest_tier_attempted=1, resolved_tier=1, api_calls=1, listings_found=5, outcome_reason="success",
    )
    assert collect04.batch_fully_processed(conn, "batch_x", inventory_df, ["EBAY_DE", "EBAY_US"]) is False

    collect04.record_progress(
        conn, batch_id="batch_x", chunk_id="batch_x_chunk_1", inventory_uid="iuid_pass1", marketplace_id="EBAY_US",
        highest_tier_attempted=1, resolved_tier=1, api_calls=1, listings_found=5, outcome_reason="success",
    )
    assert collect04.batch_fully_processed(conn, "batch_x", inventory_df, ["EBAY_DE", "EBAY_US"]) is True


# ── record_progress: conflict-detecting, not ON CONFLICT DO NOTHING (integrity correction pass) ──

def test_record_progress_identical_reregistration_is_idempotent(conn):
    collect04.start_batch(conn, "batch_rp1", {})
    kwargs = dict(
        batch_id="batch_rp1", chunk_id="batch_rp1_chunk_1",
        inventory_uid="iuid_pass1", marketplace_id="EBAY_DE",
        highest_tier_attempted=1, resolved_tier=1, api_calls=1, listings_found=5, outcome_reason="success",
    )
    collect04.record_progress(conn, **kwargs)
    collect04.record_progress(conn, **kwargs)  # must not raise, must not duplicate

    count = conn.execute(
        "SELECT COUNT(*) FROM collection_progress WHERE collection_batch_id = 'batch_rp1'"
    ).fetchone()[0]
    assert count == 1


def test_record_progress_same_combination_different_chunk_raises(conn):
    collect04.start_batch(conn, "batch_rp2", {})
    collect04.record_progress(
        conn, batch_id="batch_rp2", chunk_id="batch_rp2_chunk_1",
        inventory_uid="iuid_pass1", marketplace_id="EBAY_DE",
        highest_tier_attempted=1, resolved_tier=1, api_calls=1, listings_found=5, outcome_reason="success",
    )
    with pytest.raises(collect04.ProgressIntegrityError):
        collect04.record_progress(
            conn, batch_id="batch_rp2", chunk_id="batch_rp2_chunk_2",
            inventory_uid="iuid_pass1", marketplace_id="EBAY_DE",
            highest_tier_attempted=1, resolved_tier=1, api_calls=1, listings_found=5, outcome_reason="success",
        )


def test_record_progress_same_chunk_contradictory_fields_raises(conn):
    collect04.start_batch(conn, "batch_rp3", {})
    collect04.record_progress(
        conn, batch_id="batch_rp3", chunk_id="batch_rp3_chunk_1",
        inventory_uid="iuid_pass1", marketplace_id="EBAY_DE",
        highest_tier_attempted=1, resolved_tier=1, api_calls=1, listings_found=5, outcome_reason="success",
    )
    with pytest.raises(collect04.ProgressIntegrityError):
        collect04.record_progress(
            conn, batch_id="batch_rp3", chunk_id="batch_rp3_chunk_1",  # same chunk_id
            inventory_uid="iuid_pass1", marketplace_id="EBAY_DE",
            highest_tier_attempted=1, resolved_tier=1, api_calls=1, listings_found=999, outcome_reason="success",
        )


def test_progress_conflict_inside_chunk_transaction_rolls_back_new_chunk(conn, tmp_path):
    """Mirrors run_chunked_collection's real per-chunk transaction: if
    record_progress raises ProgressIntegrityError because a prior,
    already-committed chunk recorded a conflicting result for the same
    combination, the WHOLE transaction — including the new chunk's
    record_chunk_written insert — must roll back, never leaving an
    orphaned collection_chunks row for a chunk whose progress was never
    actually recorded."""
    batch_id = "batch_rollback_test"
    collect04.start_batch(conn, batch_id, {})

    first_chunk_id = f"{batch_id}_chunk_first"
    collect04.record_progress(
        conn, batch_id=batch_id, chunk_id=first_chunk_id,
        inventory_uid="iuid_pass1", marketplace_id="EBAY_DE",
        highest_tier_attempted=1, resolved_tier=1, api_calls=1, listings_found=5, outcome_reason="success",
    )

    new_chunk_id = f"{batch_id}_chunk_second"
    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {"conflicting_item": _flat_listing("conflicting_item", "EBAY_DE")},
            "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]
    csv_path = collect04.write_batch_csv(batch_id, new_chunk_id, item_results, "v1", output_dir=tmp_path)
    csv_sha256 = collect04._sha256_file(csv_path)

    conn.execute("BEGIN TRANSACTION")
    try:
        collect04.record_chunk_written(
            conn, chunk_id=new_chunk_id, batch_id=batch_id, source_filename=csv_path.name,
            csv_sha256=csv_sha256, started_at=datetime.now(timezone.utc),
            items_attempted=1, calls_made=1,
        )
        collect04.record_chunk_progress(conn, batch_id=batch_id, chunk_id=new_chunk_id, item_results=item_results)
        conn.execute("COMMIT")
        raise AssertionError("expected ProgressIntegrityError to be raised before COMMIT")
    except collect04.ProgressIntegrityError:
        conn.execute("ROLLBACK")

    chunk_row = conn.execute(
        "SELECT chunk_id FROM collection_chunks WHERE chunk_id = ?", [new_chunk_id]
    ).fetchone()
    assert chunk_row is None, "the new chunk's collection_chunks row must be rolled back, not left orphaned"

    surviving = conn.execute(
        "SELECT chunk_id FROM collection_progress WHERE collection_batch_id = ? AND inventory_uid = 'iuid_pass1' "
        "AND marketplace_id = 'EBAY_DE'",
        [batch_id],
    ).fetchone()
    assert surviving[0] == first_chunk_id, "the original, correctly-committed progress row must be unaffected"


# ── batch_fully_processed: exact expected-set verification (integrity correction pass) ──

def test_batch_fully_processed_exact_expected_set_true(conn):
    """Every expected (inventory_uid, marketplace_id) pair has a row -> True."""
    inventory_df = collect04.get_eligible_inventory(conn, "iuid_pass1")
    collect04.start_batch(conn, "batch_exact", {})
    for marketplace_id in ("EBAY_DE", "EBAY_US"):
        collect04.record_progress(
            conn, batch_id="batch_exact", chunk_id="batch_exact_chunk_1",
            inventory_uid="iuid_pass1", marketplace_id=marketplace_id,
            highest_tier_attempted=1, resolved_tier=1, api_calls=1, listings_found=5, outcome_reason="success",
        )
    assert collect04.batch_fully_processed(conn, "batch_exact", inventory_df, ["EBAY_DE", "EBAY_US"]) is True


def test_batch_fully_processed_missing_pair_with_extra_stale_pair_is_false(conn):
    """Correct COUNT(*) but the wrong SET must be False: one required pair
    is missing while an unrelated extra/stale pair makes the raw count
    equal to expected — extra rows must never compensate for a missing
    required pair."""
    inventory_df = collect04.get_eligible_inventory(conn, "iuid_pass1")
    collect04.start_batch(conn, "batch_stale", {})
    # Only ONE of the two expected pairs for iuid_pass1 is recorded.
    collect04.record_progress(
        conn, batch_id="batch_stale", chunk_id="batch_stale_chunk_1",
        inventory_uid="iuid_pass1", marketplace_id="EBAY_DE",
        highest_tier_attempted=1, resolved_tier=1, api_calls=1, listings_found=5, outcome_reason="success",
    )
    # An extra, unrelated/stale pair for a DIFFERENT inventory_uid not in
    # scope — brings COUNT(*) up to 2 (== expected), but the real expected
    # set still has a hole.
    collect04.record_progress(
        conn, batch_id="batch_stale", chunk_id="batch_stale_chunk_1",
        inventory_uid="iuid_warn1", marketplace_id="EBAY_DE",
        highest_tier_attempted=5, resolved_tier=None, api_calls=1, listings_found=0, outcome_reason="tier_exhaustion",
    )
    done_count = conn.execute(
        "SELECT COUNT(*) FROM collection_progress WHERE collection_batch_id = 'batch_stale'"
    ).fetchone()[0]
    assert done_count == 2  # equals expected = len(inventory_df)*len(marketplaces) = 1*2
    assert collect04.batch_fully_processed(conn, "batch_stale", inventory_df, ["EBAY_DE", "EBAY_US"]) is False, \
        "raw COUNT(*) matching expected must not substitute for verifying the exact expected pairs"


def test_batch_fully_processed_extra_rows_plus_complete_set_is_true(conn):
    """A fully-satisfied expected set plus additional unrelated/extra rows
    must still report True — extras must not cause a false negative
    either."""
    inventory_df = collect04.get_eligible_inventory(conn, "iuid_pass1")
    collect04.start_batch(conn, "batch_extra_ok", {})
    for marketplace_id in ("EBAY_DE", "EBAY_US"):
        collect04.record_progress(
            conn, batch_id="batch_extra_ok", chunk_id="batch_extra_ok_chunk_1",
            inventory_uid="iuid_pass1", marketplace_id=marketplace_id,
            highest_tier_attempted=1, resolved_tier=1, api_calls=1, listings_found=5, outcome_reason="success",
        )
    # Extra, unrelated row for a different item not in this batch's scope.
    collect04.record_progress(
        conn, batch_id="batch_extra_ok", chunk_id="batch_extra_ok_chunk_1",
        inventory_uid="iuid_warn1", marketplace_id="EBAY_DE",
        highest_tier_attempted=5, resolved_tier=None, api_calls=1, listings_found=0, outcome_reason="tier_exhaustion",
    )
    assert collect04.batch_fully_processed(conn, "batch_extra_ok", inventory_df, ["EBAY_DE", "EBAY_US"]) is True


def test_batch_fully_processed_empty_scope_is_true(conn):
    """Empty inventory_df or empty marketplaces list -> vacuously True."""
    empty_df = collect04.get_eligible_inventory(conn, "iuid_does_not_exist")
    assert len(empty_df) == 0
    assert collect04.batch_fully_processed(conn, "batch_empty", empty_df, ["EBAY_DE", "EBAY_US"]) is True

    inventory_df = collect04.get_eligible_inventory(conn, "iuid_pass1")
    assert collect04.batch_fully_processed(conn, "batch_empty2", inventory_df, []) is True


# ── Checkpointed chunked collection (item 6) ──────────────────────────────────

def _tier1_resolving_search(*, marketplace_id, keyword, on_call=None, **kwargs):
    if on_call:
        on_call()
    return [_fake_item(f"item{i}", marketplace_id) for i in range(collect04.MIN_UNIQUE_RESULTS)]


def test_chunked_collection_single_chunk_when_quota_ample(conn, db_path, tmp_path):
    """Plenty of quota available: everything finishes in chunk 1, ingestion
    runs once, batch is marked finished."""
    collect04.start_batch(conn, "batch_ample", {})
    inventory_df = collect04.get_eligible_inventory(conn, "iuid_pass1")
    fake_quota = {"available": True, "limit": 5000, "used": 0, "remaining": 5000, "reset": "later"}
    output_dir = tmp_path / "targeted_active"

    with patch.object(collect04, "search_items_single_marketplace", side_effect=_tier1_resolving_search), \
         patch.object(collect04, "check_quota", return_value=fake_quota), \
         patch.object(collect04, "run_targeted_ingestion", side_effect=_make_fake_ingest(output_dir, db_path)) as mock_ingest:
        summary = collect04.run_chunked_collection(
            conn, inventory_df=inventory_df, token="fake", batch_id="batch_ample",
            marketplaces=["EBAY_DE", "EBAY_US"],
            output_dir=output_dir, reports_dir=tmp_path / "reports", db_path=db_path,
        )
    conn = summary["conn"]  # run_chunked_collection closes/reopens conn around ingestion — see its docstring

    assert summary["stop_reason"] == "batch_fully_processed"
    assert summary["chunks_executed"] == 1
    assert summary["fully_processed"] is True
    assert mock_ingest.call_count == 1
    finished = conn.execute(
        "SELECT finished_at FROM collection_batches WHERE collection_batch_id = ?", ["batch_ample"]
    ).fetchone()
    assert finished[0] is not None


def test_chunked_collection_spans_multiple_chunks_as_quota_recovers(conn, db_path, tmp_path):
    """First chunk's quota only covers one (item, marketplace) combo; the
    second (simulating a later re-check with more headroom) covers the
    rest. Confirms: no combo is skipped, none is double-counted, ingestion
    runs once per chunk, and the batch finishes only once everything is
    done."""
    collect04.start_batch(conn, "batch_recovers", {})
    inventory_df = collect04.get_eligible_inventory(conn, "iuid_pass1")
    quota_calls = {"n": 0}
    output_dir = tmp_path / "targeted_active"

    def fake_check_quota(token):
        quota_calls["n"] += 1
        if quota_calls["n"] == 1:
            return {"available": True, "limit": 5000, "used": 4998, "remaining": 2, "reset": "later"}
        return {"available": True, "limit": 5000, "used": 100, "remaining": 4900, "reset": "later"}

    with patch.object(collect04, "search_items_single_marketplace", side_effect=_tier1_resolving_search), \
         patch.object(collect04, "check_quota", side_effect=fake_check_quota), \
         patch.object(collect04, "run_targeted_ingestion", side_effect=_make_fake_ingest(output_dir, db_path)) as mock_ingest:
        summary = collect04.run_chunked_collection(
            conn, inventory_df=inventory_df, token="fake", batch_id="batch_recovers",
            marketplaces=["EBAY_DE", "EBAY_US"],
            output_dir=output_dir, reports_dir=tmp_path / "reports", db_path=db_path,
        )
    conn = summary["conn"]

    assert summary["chunks_executed"] == 2, "must take a second chunk once quota allows the rest"
    assert summary["stop_reason"] == "batch_fully_processed"
    assert summary["fully_processed"] is True
    assert mock_ingest.call_count == 2, "ingestion must run once per chunk, not once for the whole batch"

    progress_rows = conn.execute(
        "SELECT marketplace_id FROM collection_progress WHERE collection_batch_id = ? ORDER BY marketplace_id",
        ["batch_recovers"],
    ).fetchall()
    assert [r[0] for r in progress_rows] == ["EBAY_DE", "EBAY_US"], "both combos done exactly once, none skipped"


def test_chunked_collection_stops_when_margin_exhausted_without_progress(conn, tmp_path):
    """If even chunk 1's safety margin can't be met, no work is attempted,
    no ingestion happens, and the batch is left unfinished/resumable —
    never silently attempted anyway."""
    collect04.start_batch(conn, "batch_exhausted", {})
    inventory_df = collect04.get_eligible_inventory(conn, "iuid_pass1")
    fake_quota = {"available": True, "limit": 5000, "used": 4999, "remaining": 1, "reset": "later"}

    def fail_if_called(*args, **kwargs):
        raise AssertionError("must not attempt any API call when the safety margin is already exhausted")

    with patch.object(collect04, "search_items_single_marketplace", side_effect=fail_if_called), \
         patch.object(collect04, "check_quota", return_value=fake_quota), \
         patch.object(collect04, "run_targeted_ingestion") as mock_ingest:
        summary = collect04.run_chunked_collection(
            conn, inventory_df=inventory_df, token="fake", batch_id="batch_exhausted",
            marketplaces=["EBAY_DE", "EBAY_US"],
            output_dir=tmp_path / "targeted_active", reports_dir=tmp_path / "reports",
        )

    assert summary["stop_reason"] == "quota_safety_margin_exhausted"
    assert summary["chunks_executed"] == 0
    assert summary["fully_processed"] is False
    assert mock_ingest.call_count == 0
    unfinished = conn.execute(
        "SELECT finished_at FROM collection_batches WHERE collection_batch_id = ?", ["batch_exhausted"]
    ).fetchone()
    assert unfinished is None or unfinished[0] is None


def test_chunked_collection_falls_back_when_quota_check_unavailable(conn, db_path, tmp_path):
    """If getRateLimits itself fails, the safety margin can't be verified —
    fall back to MAX_CALLS_PER_CHUNK alone rather than guessing a number, and
    still make progress instead of refusing to run at all."""
    collect04.start_batch(conn, "batch_no_quota_info", {})
    inventory_df = collect04.get_eligible_inventory(conn, "iuid_pass1")
    fake_quota = {"available": False, "error": "getRateLimits not permitted for this application"}
    output_dir = tmp_path / "targeted_active"

    with patch.object(collect04, "search_items_single_marketplace", side_effect=_tier1_resolving_search), \
         patch.object(collect04, "check_quota", return_value=fake_quota), \
         patch.object(collect04, "run_targeted_ingestion", side_effect=_make_fake_ingest(output_dir, db_path)) as mock_ingest:
        summary = collect04.run_chunked_collection(
            conn, inventory_df=inventory_df, token="fake", batch_id="batch_no_quota_info",
            marketplaces=["EBAY_DE", "EBAY_US"],
            output_dir=tmp_path / "targeted_active", reports_dir=tmp_path / "reports", db_path=db_path,
        )
    conn = summary["conn"]

    assert summary["stop_reason"] == "batch_fully_processed"
    assert summary["fully_processed"] is True
    assert mock_ingest.call_count == 1


def test_chunked_collection_max_chunk_iterations_circuit_breaker(conn, db_path, monkeypatch, tmp_path):
    """Defensive-only: real, ongoing progress (not a stall) that would still
    take more chunks than MAX_CHUNK_ITERATIONS to finish must stop instead
    of running unbounded. Uses all seeded eligible items (iuid_pass1,
    iuid_warn1, iuid_zero1) with a safety margin tight enough to afford
    exactly 1 call per chunk, so finishing every real (item, marketplace)
    combo takes more than 3 chunks — capping MAX_CHUNK_ITERATIONS at 3 must
    cut it off with real work still pending, not because progress stalled."""
    collect04.start_batch(conn, "batch_never_finishes", {})
    inventory_df = collect04.get_eligible_inventory(conn)
    assert len(inventory_df) == 3
    monkeypatch.setattr(collect04, "MAX_CHUNK_ITERATIONS", 3)
    fake_quota = {"available": True, "limit": 5000, "used": 4998, "remaining": 2, "reset": "later"}

    with patch.object(collect04, "search_items_single_marketplace", side_effect=_tier1_resolving_search), \
         patch.object(collect04, "check_quota", return_value=fake_quota), \
         patch.object(collect04, "run_targeted_ingestion"):
        summary = collect04.run_chunked_collection(
            conn, inventory_df=inventory_df, token="fake", batch_id="batch_never_finishes",
            marketplaces=["EBAY_DE", "EBAY_US"],
            output_dir=tmp_path / "targeted_active", reports_dir=tmp_path / "reports", db_path=db_path,
        )

    assert summary["stop_reason"] == "max_chunk_iterations_reached"
    assert summary["chunks_executed"] == 3
    assert summary["fully_processed"] is False, "3 chunks at 1 call each cannot finish all 4 real combos"


def test_run_targeted_ingestion_invokes_subprocess_and_raises_on_failure(monkeypatch):
    calls = []

    class _FakeResult:
        def __init__(self, returncode, stdout="", stderr=""):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    def fake_run(cmd, capture_output=None, text=None, env=None):
        calls.append(cmd)
        return _FakeResult(0, stdout="Targeted active listing rows imported: 5\n")

    monkeypatch.setattr(collect04.subprocess, "run", fake_run)
    collect04.run_targeted_ingestion()
    assert len(calls) == 1
    assert calls[0][-1] == "--targeted"

    def fake_run_failure(cmd, capture_output=None, text=None, env=None):
        return _FakeResult(1, stderr="boom")

    monkeypatch.setattr(collect04.subprocess, "run", fake_run_failure)
    with pytest.raises(RuntimeError, match="exit code 1"):
        collect04.run_targeted_ingestion()


# ── Progress/write ordering durability fix ────────────────────────────────────

def test_crash_before_csv_write_leaves_nothing_durably_marked_and_resume_retries_fully(conn, tmp_path):
    """
    Simulates several (item, marketplace) combinations finishing their API
    escalation in memory — process_item collecting results across a whole
    chunk's item loop — but the process crashing before write_batch_csv (and
    therefore before record_chunk_written/record_chunk_progress) ever runs.
    This is the deeper bug found in verification: previously record_progress
    ran immediately per combo, so ALL combinations collected so far in the
    chunk (not just the one in flight) would have been wrongly marked done.
    Confirms zero progress rows exist after the simulated crash, and that a
    fresh attempt retries every combination rather than skipping any.
    """
    batch_id = "batch_crash_before_csv"
    collect04.start_batch(conn, batch_id, {})

    call_log = []

    def fake_search(*, marketplace_id, keyword, on_call=None, **kwargs):
        call_log.append((marketplace_id, keyword))
        if on_call:
            on_call()
        return [_fake_item(f"item{i}", marketplace_id) for i in range(collect04.MIN_UNIQUE_RESULTS)]

    inventory_df = collect04.get_eligible_inventory(conn)  # iuid_pass1, iuid_warn1, iuid_zero1
    with patch.object(collect04, "search_items_single_marketplace", side_effect=fake_search):
        call_budget = collect04.CallBudget(100)
        for _, row in inventory_df.iterrows():
            collect04.process_item(
                conn, item_row=row, token="fake", batch_id=batch_id,
                call_budget=call_budget, marketplaces=["EBAY_DE", "EBAY_US"], output_dir=tmp_path,
            )
    # >>> Simulated crash here — before write_batch_csv/record_chunk_written/record_chunk_progress <<<

    assert len(call_log) >= 4, "sanity check: multiple real combinations were attempted this simulated chunk"
    progress_count = conn.execute(
        "SELECT COUNT(*) FROM collection_progress WHERE collection_batch_id = ?", [batch_id]
    ).fetchone()[0]
    assert progress_count == 0, "nothing may be marked done until the chunk's CSV is durably written"

    # A fresh attempt (simulating the next --resume invocation) must retry
    # every combination — none of them were durably skip-safe.
    call_log.clear()
    with patch.object(collect04, "search_items_single_marketplace", side_effect=fake_search):
        for _, row in inventory_df.iterrows():
            collect04.process_item(
                conn, item_row=row, token="fake", batch_id=batch_id,
                call_budget=collect04.CallBudget(100), marketplaces=["EBAY_DE", "EBAY_US"], output_dir=tmp_path,
            )
    assert len(call_log) >= 4, "resume must retry all previously in-memory-only work, not skip any of it"


def test_resume_ingests_pending_durable_chunk_before_new_collection(conn, db_path, tmp_path):
    """
    A chunk's CSV can be durably written and its progress recorded, yet
    ingestion never completed (process died right after, or the ingestion
    subprocess itself failed) — collection_chunks shows csv_written_at set,
    ingested_at NULL. run_chunked_collection's startup step must discover
    and ingest this pending file before/without needing any new collection.
    """
    batch_id = "batch_pending_ingest"
    collect04.start_batch(conn, batch_id, {})
    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {"pending_item": _flat_listing("pending_item", "EBAY_DE")},
            "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]
    chunk_id, csv_path = _durably_write_chunk(conn, tmp_path, batch_id, item_results)

    pending_before = collect04.get_pending_ingestion_chunks(conn, batch_id)
    assert pending_before == [(chunk_id, csv_path.name)]

    def fake_ingest(**kwargs):
        # Simulates the real subprocess by performing real ingestion against
        # our isolated test db/dir via a fresh connection, instead of
        # shelling out to a process that would touch the production
        # database — matches what run_chunked_collection now guarantees
        # (its own conn is closed before this runs).
        ingest01.TARGETED_ACTIVE_DIR = tmp_path
        fresh_conn = duckdb.connect(str(db_path))
        try:
            ingest01.insert_targeted_listings(fresh_conn)
        finally:
            fresh_conn.close()

    inventory_df = collect04.get_eligible_inventory(conn, "iuid_pass1")
    fake_quota = {"available": True, "limit": 5000, "used": 0, "remaining": 5000, "reset": "later"}

    with patch.object(collect04, "search_items_single_marketplace") as mock_search, \
         patch.object(collect04, "check_quota", return_value=fake_quota), \
         patch.object(collect04, "run_targeted_ingestion", side_effect=fake_ingest):
        mock_search.side_effect = AssertionError("should not need to search — EBAY_DE is already durable")
        summary = collect04.run_chunked_collection(
            conn, inventory_df=inventory_df, token="fake", batch_id=batch_id,
            marketplaces=["EBAY_DE"], output_dir=tmp_path, reports_dir=tmp_path / "reports", db_path=db_path,
        )
    conn = summary["conn"]

    pending_after = collect04.get_pending_ingestion_chunks(conn, batch_id)
    assert pending_after == [], "pending chunk must be ingested/reconciled at the start of the next invocation"
    count = conn.execute(
        "SELECT COUNT(*) FROM raw_active_targeted WHERE item_id = 'pending_item'"
    ).fetchone()[0]
    assert count == 1


def test_unique_chunk_ids_across_separate_invocations_no_overwrite(conn, db_path, tmp_path):
    """
    Two separate calls to run_chunked_collection for the SAME batch_id
    (simulating two separate process invocations — e.g. today's run and a
    --resume run after tomorrow's quota reset) must never produce colliding
    chunk_ids, and must never overwrite each other's CSV/manifest files —
    the root cause of the filename-collision data-loss bug found in
    verification.
    """
    batch_id = "batch_two_invocations"
    collect04.start_batch(conn, batch_id, {})
    inventory_df = collect04.get_eligible_inventory(conn, "iuid_pass1")
    output_dir = tmp_path / "targeted_active"
    reports_dir = tmp_path / "reports"
    # remaining=2 -> safe_budget=int(2/1.2)=1 call per chunk, forcing this
    # single item's two marketplaces to span two chunks within invocation 1.
    fake_quota = {"available": True, "limit": 5000, "used": 0, "remaining": 2, "reset": "later"}

    with patch.object(collect04, "search_items_single_marketplace", side_effect=_tier1_resolving_search), \
         patch.object(collect04, "check_quota", return_value=fake_quota), \
         patch.object(collect04, "run_targeted_ingestion"):
        summary1 = collect04.run_chunked_collection(
            conn, inventory_df=inventory_df, token="fake", batch_id=batch_id,
            marketplaces=["EBAY_DE", "EBAY_US"], output_dir=output_dir, reports_dir=reports_dir, db_path=db_path,
        )
    conn = summary1["conn"]

    files_after_1 = sorted(output_dir.glob("targeted_active_*.csv"))
    contents_after_1 = {p.name: p.read_bytes() for p in files_after_1}
    assert len(files_after_1) >= 1
    manifests_after_1 = sorted(reports_dir.glob("targeted_collection_manifest_*.json"))
    manifest_contents_after_1 = {p.name: p.read_bytes() for p in manifests_after_1}

    # Invocation 2: a brand-new call for the SAME batch_id.
    with patch.object(collect04, "search_items_single_marketplace", side_effect=_tier1_resolving_search), \
         patch.object(collect04, "check_quota", return_value=fake_quota), \
         patch.object(collect04, "run_targeted_ingestion"):
        summary2 = collect04.run_chunked_collection(
            conn, inventory_df=inventory_df, token="fake", batch_id=batch_id,
            marketplaces=["EBAY_DE", "EBAY_US"], output_dir=output_dir, reports_dir=reports_dir, db_path=db_path,
        )
    conn = summary2["conn"]

    for name, content in contents_after_1.items():
        assert (output_dir / name).read_bytes() == content, f"{name} was overwritten by a later invocation"
    for name, content in manifest_contents_after_1.items():
        assert (reports_dir / name).read_bytes() == content, f"{name} was overwritten by a later invocation"

    chunk_ids_1 = {c["chunk_id"] for c in summary1["chunks"]}
    chunk_ids_2 = {c["chunk_id"] for c in summary2["chunks"]}
    assert chunk_ids_1, "invocation 1 must have executed at least one chunk"
    assert chunk_ids_2, "invocation 2 must have executed at least one chunk"
    assert chunk_ids_1.isdisjoint(chunk_ids_2), "chunk_ids must never collide across separate invocations"


def test_ingestion_failure_leaves_chunk_in_retryable_state_not_lost(conn, db_path, tmp_path):
    """
    If scripts/01_ingest.py --targeted itself fails (subprocess exits
    non-zero), the chunk's CSV and progress are already durable — nothing is
    lost. The loop stops with stop_reason="ingestion_failed" instead of
    crashing uncleanly, leaving collection_chunks.ingested_at NULL (a safe,
    retryable state) and the batch resumable, not finished.
    """
    batch_id = "batch_ingest_fails"
    collect04.start_batch(conn, batch_id, {})
    inventory_df = collect04.get_eligible_inventory(conn, "iuid_pass1")
    output_dir = tmp_path / "targeted_active"
    reports_dir = tmp_path / "reports"
    fake_quota = {"available": True, "limit": 5000, "used": 0, "remaining": 5000, "reset": "later"}

    with patch.object(collect04, "search_items_single_marketplace", side_effect=_tier1_resolving_search), \
         patch.object(collect04, "check_quota", return_value=fake_quota), \
         patch.object(collect04, "run_targeted_ingestion", side_effect=RuntimeError("exit code 1")):
        summary = collect04.run_chunked_collection(
            conn, inventory_df=inventory_df, token="fake", batch_id=batch_id,
            marketplaces=["EBAY_DE", "EBAY_US"], output_dir=output_dir, reports_dir=reports_dir, db_path=db_path,
        )
    conn = summary["conn"]

    assert summary["stop_reason"] == "ingestion_failed"
    assert summary["fully_processed"] is False
    chunk_id = summary["last_chunk_id"]
    assert chunk_id is not None

    chunk_row = conn.execute(
        "SELECT csv_written_at, ingested_at FROM collection_chunks WHERE chunk_id = ?", [chunk_id]
    ).fetchone()
    assert chunk_row[0] is not None, "CSV must already be durable even though ingestion failed"
    assert chunk_row[1] is None, "ingestion failure must leave ingested_at NULL — retryable, not lost"

    progress_count = conn.execute(
        "SELECT COUNT(*) FROM collection_progress WHERE collection_batch_id = ?", [batch_id]
    ).fetchone()[0]
    assert progress_count == 2, "collected results remain durably recorded even though ingestion hasn't happened"

    batch_row = conn.execute(
        "SELECT stop_reason, fully_processed, finished_at FROM collection_batches WHERE collection_batch_id = ?",
        [batch_id],
    ).fetchone()
    assert batch_row[0] == "ingestion_failed"
    assert batch_row[1] is False
    assert batch_row[2] is None, "batch must not be marked finished when ingestion failed"


def test_ingestion_failed_batch_reconciled_to_success_once_fixed(conn, db_path, tmp_path):
    """
    Task 2 requirement: after a genuinely-durable batch's ingestion failure
    is later fixed (e.g. via --reconcile-only running in a separate
    invocation), the batch's OWN bookkeeping must be corrected too — not
    left permanently reading stop_reason='ingestion_failed'/
    status='INCOMPLETE' forever, which would be a false failure once every
    expected combination has actually been collected and ingested.
    """
    batch_id = "batch_ingest_then_reconciled"
    inventory_df = collect04.get_eligible_inventory(conn, "iuid_pass1")
    expected_pairs = [(uid, mp) for uid in inventory_df["inventory_uid"] for mp in ["EBAY_DE", "EBAY_US"]]
    collect04.start_batch(conn, batch_id, {}, expected_pairs=expected_pairs)
    output_dir = tmp_path / "targeted_active"
    reports_dir = tmp_path / "reports"
    fake_quota = {"available": True, "limit": 5000, "used": 0, "remaining": 5000, "reset": "later"}

    # First invocation: collection succeeds fully, but ingestion fails.
    with patch.object(collect04, "search_items_single_marketplace", side_effect=_tier1_resolving_search), \
         patch.object(collect04, "check_quota", return_value=fake_quota), \
         patch.object(collect04, "run_targeted_ingestion", side_effect=RuntimeError("exit code 1")):
        summary = collect04.run_chunked_collection(
            conn, inventory_df=inventory_df, token="fake", batch_id=batch_id,
            marketplaces=["EBAY_DE", "EBAY_US"], output_dir=output_dir, reports_dir=reports_dir, db_path=db_path,
        )
    conn = summary["conn"]

    pre_row = conn.execute(
        "SELECT status, stop_reason, fully_processed, finished_at FROM collection_batches "
        "WHERE collection_batch_id = ?",
        [batch_id],
    ).fetchone()
    assert pre_row == ("INCOMPLETE", "ingestion_failed", False, None)

    # Fix ingestion for real this time (mirrors what --reconcile-only does
    # in a separate invocation), then reconcile the batch's own state.
    ingest01.TARGETED_ACTIVE_DIR = output_dir
    fresh_conn = duckdb.connect(str(db_path))
    try:
        ingest01.insert_targeted_listings(fresh_conn)
    finally:
        fresh_conn.close()
    collect04.reconcile_chunk_ingestion_state(conn)
    changes = collect04.reconcile_batch_state(conn, batch_id)

    assert changes == [{"batch_id": batch_id, "action": "marked_success"}]
    post_row = conn.execute(
        "SELECT status, stop_reason, fully_processed, finished_at FROM collection_batches "
        "WHERE collection_batch_id = ?",
        [batch_id],
    ).fetchone()
    assert post_row[0] == "SUCCESS"
    assert post_row[1] == "reconciled_success"
    assert post_row[2] is True
    assert post_row[3] is not None

    # Idempotent: reconciling an already-SUCCESS batch again is a no-op.
    changes2 = collect04.reconcile_batch_state(conn, batch_id)
    assert changes2 == []


def test_reconcile_batch_state_never_guesses_without_expected_pairs(conn, db_path, tmp_path):
    """A batch started the old way (no expected_pairs) must be left
    completely untouched by reconcile_batch_state — never upgraded to
    SUCCESS and never downgraded/relabeled, since there is no way to know
    what 'complete' means for it without the original expected set."""
    batch_id = "batch_no_expected_pairs"
    collect04.start_batch(conn, batch_id, {})  # no expected_pairs — legacy-style call
    before = conn.execute(
        "SELECT status, stop_reason, fully_processed, finished_at FROM collection_batches "
        "WHERE collection_batch_id = ?",
        [batch_id],
    ).fetchone()
    assert before[0] == "INCOMPLETE"  # the default set at start_batch time

    changes = collect04.reconcile_batch_state(conn, batch_id)
    assert changes == [{"batch_id": batch_id, "action": "skipped_no_expected_pairs_recorded"}]

    after = conn.execute(
        "SELECT status, stop_reason, fully_processed, finished_at FROM collection_batches "
        "WHERE collection_batch_id = ?",
        [batch_id],
    ).fetchone()
    assert after == before, "a batch with no recorded expected_pairs must never be reinterpreted"


def test_ingestion_success_then_crash_before_local_sync_remains_idempotent(conn, tmp_path):
    """
    If run_targeted_ingestion() succeeds (the file IS ingested into
    raw_active_targeted / ingestion_log) but the process crashes before
    reconcile_chunk_ingestion_state runs locally, collection_chunks still
    shows ingested_at NULL. The next invocation's startup reconciliation
    must detect the already-successful ingestion_log entry and mark it
    ingested without re-ingesting or creating duplicate raw rows.
    """
    batch_id = "batch_ingest_then_crash"
    collect04.start_batch(conn, batch_id, {})
    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {"crash_item": _flat_listing("crash_item", "EBAY_DE")},
            "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]
    chunk_id, csv_path = _durably_write_chunk(conn, tmp_path, batch_id, item_results)

    # Simulate ingestion succeeding for real, but the local sync step
    # (reconcile_chunk_ingestion_state) never running.
    ingest01.TARGETED_ACTIVE_DIR = tmp_path
    n_inserted = ingest01.insert_targeted_listings(conn)
    assert n_inserted == 1
    row = conn.execute(
        "SELECT ingested_at FROM collection_chunks WHERE chunk_id = ?", [chunk_id]
    ).fetchone()
    assert row[0] is None, "simulated crash: local sync never ran despite real ingestion succeeding"

    # Next invocation's startup step: reconcile pending chunks against
    # ingestion_log's authoritative state.
    reconciled = collect04.reconcile_chunk_ingestion_state(conn)
    assert reconciled == 1
    row_after = conn.execute(
        "SELECT ingested_at FROM collection_chunks WHERE chunk_id = ?", [chunk_id]
    ).fetchone()
    assert row_after[0] is not None

    # Even if ingestion were attempted again (belt-and-braces), zero
    # duplicate raw rows result.
    n_second = ingest01.insert_targeted_listings(conn)
    assert n_second == 0
    total = conn.execute("SELECT COUNT(*) FROM raw_active_targeted WHERE item_id = 'crash_item'").fetchone()[0]
    assert total == 1


# ── Recovery / backward-compatibility audit ───────────────────────────────────
# Bullets 7 (ingestion failure leaves CSV intact/state incomplete), 8 (retry is
# idempotent, no duplicate raw rows), and 9 (same item_id across marketplaces
# preserved) are already covered by
# test_ingestion_failure_leaves_chunk_in_retryable_state_not_lost,
# test_cross_chunk_idempotency_across_separate_files_and_ingestion_calls /
# test_idempotent_ingestion_zero_duplicates, and
# test_same_item_id_different_marketplace_both_kept_in_db /
# test_cross_marketplace_evidence_preserved_not_collapsed above — re-confirmed
# passing under the already_processed rewrite in this run, not duplicated here.

def test_ingested_chunk_remains_trusted_after_csv_archived(conn, tmp_path):
    """Case A: once a chunk is authoritatively ingested (ingested_at set,
    confirmed via ingestion_log), its CSV becoming unavailable — archived,
    moved to cold storage, deleted — must NOT cause it to be re-collected.
    raw_active_targeted is the durable store once ingestion succeeds; the
    CSV was only ever the intermediate handoff artifact."""
    batch_id = "batch_archive_test"
    collect04.start_batch(conn, batch_id, {})
    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {"archived_item": _flat_listing("archived_item", "EBAY_DE")},
            "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]
    chunk_id, csv_path = _durably_write_chunk(conn, tmp_path, batch_id, item_results)

    ingest01.TARGETED_ACTIVE_DIR = tmp_path
    ingest01.insert_targeted_listings(conn)
    collect04.reconcile_chunk_ingestion_state(conn)

    assert collect04.already_processed(conn, batch_id, "iuid_pass1", "EBAY_DE", output_dir=tmp_path) is True

    csv_path.unlink()  # simulate archival/deletion of the intermediate CSV
    assert collect04.already_processed(conn, batch_id, "iuid_pass1", "EBAY_DE", output_dir=tmp_path) is True, \
        "authoritatively ingested chunk must remain trusted even once its CSV is gone"


# ── Hash-aware, deterministic ingestion reconciliation (integrity correction pass) ──

def test_reconcile_matching_filename_and_hash_marks_ingested(conn, tmp_path):
    """A collection_chunks row is marked ingested when an authoritative
    successful ingestion_log row matches BOTH source_filename AND
    file_hash == csv_sha256 — and the copied ingested_at is the
    ingestion_log row's own value, never current_timestamp."""
    batch_id = "batch_hash_match"
    collect04.start_batch(conn, batch_id, {})
    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {"hash_match_item": _flat_listing("hash_match_item", "EBAY_DE")},
            "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]
    chunk_id, csv_path = _durably_write_chunk(conn, tmp_path, batch_id, item_results)
    actual_hash = collect04._sha256_file(csv_path)
    authoritative_time = datetime.now(timezone.utc) - timedelta(days=3)

    conn.execute(
        "INSERT INTO ingestion_log (source_type, source_filename, file_hash, upload_batch_id, ingested_at, "
        "rows_inserted, status) VALUES ('targeted_active', ?, ?, '', ?, 1, 'success')",
        [csv_path.name, actual_hash, authoritative_time],
    )

    reconciled = collect04.reconcile_chunk_ingestion_state(conn)
    assert reconciled == 1

    chunk_ingested_at = conn.execute(
        "SELECT ingested_at FROM collection_chunks WHERE chunk_id = ?", [chunk_id]
    ).fetchone()[0]
    log_ingested_at = conn.execute(
        "SELECT ingested_at FROM ingestion_log WHERE source_type='targeted_active' AND source_filename=?",
        [csv_path.name],
    ).fetchone()[0]
    assert chunk_ingested_at is not None
    assert chunk_ingested_at == log_ingested_at, (
        "must copy the authoritative ingestion_log.ingested_at value, not current_timestamp"
    )


def test_reconcile_matching_filename_different_hash_not_marked_ingested(conn, tmp_path):
    """A filename match alone must never be sufficient: an ingestion_log
    success row for the same source_filename but a DIFFERENT file_hash
    (e.g. a same-named but distinct/corrupted file) must leave the chunk
    un-ingested."""
    batch_id = "batch_hash_mismatch"
    collect04.start_batch(conn, batch_id, {})
    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {"hash_mismatch_item": _flat_listing("hash_mismatch_item", "EBAY_DE")},
            "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]
    chunk_id, csv_path = _durably_write_chunk(conn, tmp_path, batch_id, item_results)

    conn.execute(
        "INSERT INTO ingestion_log (source_type, source_filename, file_hash, upload_batch_id, ingested_at, "
        "rows_inserted, status) VALUES ('targeted_active', ?, ?, '', ?, 1, 'success')",
        [csv_path.name, "a_completely_different_hash", datetime.now(timezone.utc)],
    )

    reconciled = collect04.reconcile_chunk_ingestion_state(conn)
    assert reconciled == 0
    row = conn.execute(
        "SELECT ingested_at FROM collection_chunks WHERE chunk_id = ?", [chunk_id]
    ).fetchone()
    assert row[0] is None, "filename match with a different hash must leave the chunk un-ingested"


def test_reconcile_selects_matching_hash_row_and_ignores_non_matching_hash_row(conn, tmp_path):
    """The ingestion_log PK is (source_type, source_filename, file_hash), so
    two successful rows for the same source_filename must have different
    file_hash values — there is no way for two rows to both match
    csv_sha256 at once. What this proves is narrower than "latest wins
    among several matches": with one non-matching-hash success row (older)
    and one matching-hash success row (newer) both present for the same
    filename, reconcile_chunk_ingestion_state's hash-aware WHERE clause
    selects only the matching-hash row and ignores the non-matching one —
    it is not a real multi-candidate tie-break, since the non-matching row
    was never eligible to match in the first place.
    """
    batch_id = "batch_multi_success_rows"
    collect04.start_batch(conn, batch_id, {})
    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {"multi_row_item": _flat_listing("multi_row_item", "EBAY_DE")},
            "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]
    chunk_id, csv_path = _durably_write_chunk(conn, tmp_path, batch_id, item_results)
    actual_hash = collect04._sha256_file(csv_path)

    older = datetime.now(timezone.utc) - timedelta(hours=2)
    newer = datetime.now(timezone.utc) - timedelta(minutes=1)
    # An older, non-matching-hash success row and a newer, matching-hash
    # success row for the same filename — since a matching-hash row is
    # never ignored in favor of a non-matching one, this confirms the
    # matching row is what gets selected, not merely "whichever is latest".
    conn.execute(
        "INSERT INTO ingestion_log (source_type, source_filename, file_hash, upload_batch_id, ingested_at, "
        "rows_inserted, status) VALUES ('targeted_active', ?, ?, '', ?, 1, 'success')",
        [csv_path.name, "stale_nonmatching_hash", older],
    )
    conn.execute(
        "INSERT INTO ingestion_log (source_type, source_filename, file_hash, upload_batch_id, ingested_at, "
        "rows_inserted, status) VALUES ('targeted_active', ?, ?, '', ?, 1, 'success')",
        [csv_path.name, actual_hash, newer],
    )

    reconciled = collect04.reconcile_chunk_ingestion_state(conn)
    assert reconciled == 1
    chunk_ingested_at = conn.execute(
        "SELECT ingested_at FROM collection_chunks WHERE chunk_id = ?", [chunk_id]
    ).fetchone()[0]
    newer_entry_ingested_at = conn.execute(
        "SELECT ingested_at FROM ingestion_log WHERE source_type='targeted_active' AND source_filename=? AND file_hash=?",
        [csv_path.name, actual_hash],
    ).fetchone()[0]
    assert chunk_ingested_at == newer_entry_ingested_at, (
        "must select the matching-hash entry's ingested_at and ignore the non-matching-hash entry"
    )


def test_reconcile_archived_csv_after_correct_hash_match_remains_trusted(conn, tmp_path):
    """Once reconcile_chunk_ingestion_state has hash-verified a match and
    marked the chunk ingested, the CSV becoming unavailable afterwards
    (archived/deleted) must not cause the chunk to lose trust — ingestion_log
    is the durable record once ingestion is confirmed."""
    batch_id = "batch_hash_match_archived"
    collect04.start_batch(conn, batch_id, {})
    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {"archived_hash_item": _flat_listing("archived_hash_item", "EBAY_DE")},
            "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]
    chunk_id, csv_path = _durably_write_chunk(conn, tmp_path, batch_id, item_results)
    actual_hash = collect04._sha256_file(csv_path)
    conn.execute(
        "INSERT INTO ingestion_log (source_type, source_filename, file_hash, upload_batch_id, ingested_at, "
        "rows_inserted, status) VALUES ('targeted_active', ?, ?, '', ?, 1, 'success')",
        [csv_path.name, actual_hash, datetime.now(timezone.utc)],
    )
    assert collect04.reconcile_chunk_ingestion_state(conn) == 1

    csv_path.unlink()
    assert collect04.already_processed(conn, batch_id, "iuid_pass1", "EBAY_DE", output_dir=tmp_path) is True, \
        "hash-verified ingested chunk must remain trusted even after its CSV is archived/deleted"


def test_uningested_chunk_with_missing_csv_is_retryable(conn, tmp_path):
    """Case B: a chunk durably written but never confirmed ingested, whose
    CSV then goes missing before ingestion could run, has nothing durable
    representing it anywhere — must be retried, not trusted."""
    batch_id = "batch_missing_csv_test"
    collect04.start_batch(conn, batch_id, {})
    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {"orphaned_item": _flat_listing("orphaned_item", "EBAY_DE")},
            "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]
    chunk_id, csv_path = _durably_write_chunk(conn, tmp_path, batch_id, item_results)
    # never ingested
    csv_path.unlink()

    assert collect04.already_processed(conn, batch_id, "iuid_pass1", "EBAY_DE", output_dir=tmp_path) is False


def test_uningested_chunk_with_tampered_csv_content_is_retryable(conn, tmp_path):
    """
    Strengthened already_processed (item 4): file existence ALONE is not
    sufficient for an un-ingested chunk. A same-named file that has been
    modified/truncated/corrupted since it was written — present on disk,
    but no longer matching its recorded csv_sha256 — must not be trusted
    either. This is distinct from the missing-file case above: here the
    file exists, but its content integrity is what fails.
    """
    batch_id = "batch_tampered_csv_test"
    collect04.start_batch(conn, batch_id, {})
    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {"tampered_item": _flat_listing("tampered_item", "EBAY_DE")},
            "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]
    chunk_id, csv_path = _durably_write_chunk(conn, tmp_path, batch_id, item_results)
    assert collect04.already_processed(conn, batch_id, "iuid_pass1", "EBAY_DE", output_dir=tmp_path) is True

    # Tamper with the file's content in place — it still exists, same name,
    # but its bytes no longer match csv_sha256 recorded at write time.
    with csv_path.open("a") as fh:
        fh.write("tampered,extra,row\n")

    assert collect04.already_processed(conn, batch_id, "iuid_pass1", "EBAY_DE", output_dir=tmp_path) is False, \
        "file existence alone must not be trusted — content must match the recorded hash"


def test_null_chunk_id_legacy_row_never_trusted_and_does_not_crash(conn):
    """Case C: a collection_progress row with chunk_id IS NULL (predating
    chunk-durability tracking) must never be silently trusted, and the
    LEFT JOIN against collection_chunks must not raise for a NULL key."""
    batch_id = "batch_legacy_null_chunk"
    collect04.start_batch(conn, batch_id, {})
    conn.execute(
        "INSERT INTO collection_progress (collection_batch_id, chunk_id, inventory_uid, marketplace_id, "
        "highest_tier_attempted, resolved_tier, api_calls, listings_found, outcome_reason) "
        "VALUES (?, NULL, ?, ?, 1, 1, 1, 5, 'success')",
        [batch_id, "iuid_pass1", "EBAY_DE"],
    )
    result = collect04.already_processed(conn, batch_id, "iuid_pass1", "EBAY_DE")
    assert result is False


def test_orphan_csv_discovered_and_reconciled_before_new_api_collection(conn, db_path, tmp_path):
    """Case D: a chunk's CSV was atomically renamed to its final path, but
    the process crashed before record_chunk_written/record_chunk_progress
    ever ran. run_chunked_collection's startup step must discover and
    reconcile this orphan file BEFORE making any new API call for the
    combinations it already contains."""
    batch_id = "batch_orphan_csv"
    collect04.start_batch(conn, batch_id, {})
    output_dir = tmp_path / "targeted_active"

    orphan_chunk_id = f"{batch_id}_chunk_orphan123"
    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {"orphan_discovered_item": _flat_listing("orphan_discovered_item", "EBAY_DE")},
            "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]
    csv_path = collect04.write_batch_csv(batch_id, orphan_chunk_id, item_results, "v1", output_dir=output_dir)
    # Deliberately do NOT call record_chunk_written/record_chunk_progress —
    # this is the exact "crash between rename and DB record" scenario.

    assert collect04.get_pending_ingestion_chunks(conn, batch_id) == []
    assert collect04.already_processed(conn, batch_id, "iuid_pass1", "EBAY_DE", output_dir=output_dir) is False

    inventory_df = collect04.get_eligible_inventory(conn, "iuid_pass1")
    fake_quota = {"available": True, "limit": 5000, "used": 0, "remaining": 5000, "reset": "later"}

    with patch.object(collect04, "search_items_single_marketplace") as mock_search, \
         patch.object(collect04, "check_quota", return_value=fake_quota), \
         patch.object(collect04, "run_targeted_ingestion"):
        mock_search.side_effect = AssertionError(
            "must not re-collect EBAY_DE for this item — its data is already in the orphan CSV"
        )
        summary = collect04.run_chunked_collection(
            conn, inventory_df=inventory_df, token="fake", batch_id=batch_id,
            marketplaces=["EBAY_DE"], output_dir=output_dir, reports_dir=tmp_path / "reports", db_path=db_path,
        )
    conn = summary["conn"]

    pending_chunk_ids = {c for c, _ in collect04.get_pending_ingestion_chunks(conn, batch_id)}
    assert orphan_chunk_id in pending_chunk_ids, "orphan chunk must now be tracked and pending ingestion"
    assert collect04.already_processed(conn, batch_id, "iuid_pass1", "EBAY_DE", output_dir=output_dir) is True
    progress_row = conn.execute(
        "SELECT chunk_id, outcome_reason, listings_found FROM collection_progress "
        "WHERE collection_batch_id = ? AND inventory_uid = ? AND marketplace_id = ?",
        [batch_id, "iuid_pass1", "EBAY_DE"],
    ).fetchone()
    assert progress_row == (orphan_chunk_id, "reconciled_from_orphan_csv", 1)


def _orphan_item_results_with_one_zero_result_combo():
    """Two items: iuid_pass1/EBAY_DE has real listings; iuid_warn1/EBAY_DE
    genuinely found zero listings (tier_exhaustion) — the CSV will have
    rows for the first and structurally none for the second."""
    return [
        {
            "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
            "per_marketplace": {"EBAY_DE": {
                "listings": {"has_listings_item": _flat_listing("has_listings_item", "EBAY_DE")},
                "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
            }},
        },
        {
            "inventory_uid": "iuid_warn1", "canonical_inventory_id": "rolex_unknown_24_26812_8",
            "per_marketplace": {"EBAY_DE": {
                "listings": {},
                "highest_tier_attempted": 5, "resolved_tier": None, "api_calls": 1,
                "outcome_reason": "tier_exhaustion",
            }},
        },
    ]


def test_orphan_chunk_with_manifest_reconstructs_zero_result_combo_too(conn, tmp_path):
    """Case A: CSV and manifest both survive. The CSV alone cannot prove
    iuid_warn1/EBAY_DE returned zero results (no rows either way could mean
    'never attempted' or 'attempted, found nothing') — only the manifest's
    attempted_combinations distinguishes them. With the manifest present,
    BOTH combinations must be reconstructed: the real one cross-validated
    against CSV rows, the zero-result one trusted directly since there is
    no data at stake."""
    batch_id = "batch_orphan_with_manifest"
    collect04.start_batch(conn, batch_id, {})
    output_dir = tmp_path / "targeted_active"
    reports_dir = tmp_path / "reports"
    chunk_id = f"{batch_id}_chunk_manifest123"
    item_results = _orphan_item_results_with_one_zero_result_combo()

    csv_path = collect04.write_batch_csv(batch_id, chunk_id, item_results, "v1", output_dir=output_dir)
    now = datetime.now(timezone.utc)
    manifest_path = collect04.write_manifest(
        batch_id=batch_id, chunk_id=chunk_id, started_at=now, finished_at=now, source_csv_path=csv_path,
        item_results=item_results, call_budget=collect04.CallBudget(100), marketplaces=["EBAY_DE"],
        source_filename=csv_path.name, reports_dir=reports_dir,
        manifest_filename=f"targeted_collection_manifest_{chunk_id}.json",
    )
    assert manifest_path.exists()
    # Deliberately do NOT call record_chunk_written/record_chunk_progress.

    reconciled = collect04.discover_orphan_chunk_csvs(conn, batch_id, output_dir=output_dir, reports_dir=reports_dir)
    assert reconciled == 1

    real_row = conn.execute(
        "SELECT chunk_id, listings_found FROM collection_progress "
        "WHERE collection_batch_id = ? AND inventory_uid = ? AND marketplace_id = ?",
        [batch_id, "iuid_pass1", "EBAY_DE"],
    ).fetchone()
    assert real_row == (chunk_id, 1)

    zero_row = conn.execute(
        "SELECT chunk_id, listings_found, outcome_reason FROM collection_progress "
        "WHERE collection_batch_id = ? AND inventory_uid = ? AND marketplace_id = ?",
        [batch_id, "iuid_warn1", "EBAY_DE"],
    ).fetchone()
    assert zero_row is not None, "the zero-result combo must be reconstructed when the manifest survives"
    assert zero_row[0] == chunk_id
    assert zero_row[1] == 0
    assert "reconciled_from_orphan_manifest" in zero_row[2]

    assert collect04.already_processed(conn, batch_id, "iuid_pass1", "EBAY_DE", output_dir=output_dir) is True
    assert collect04.already_processed(conn, batch_id, "iuid_warn1", "EBAY_DE", output_dir=output_dir) is True


def test_orphan_chunk_without_manifest_leaves_zero_result_combo_retryable(conn, tmp_path):
    """Case B: CSV survives, manifest does not. The CSV cannot prove
    iuid_warn1/EBAY_DE returned zero results — the real combo (which has
    actual rows) is still safely reconstructed, but the zero-result combo
    must be left with NO progress row, so it gets retried on resume rather
    than silently assumed complete from CSV absence alone."""
    batch_id = "batch_orphan_no_manifest"
    collect04.start_batch(conn, batch_id, {})
    output_dir = tmp_path / "targeted_active"
    chunk_id = f"{batch_id}_chunk_nomanifest123"
    item_results = _orphan_item_results_with_one_zero_result_combo()

    collect04.write_batch_csv(batch_id, chunk_id, item_results, "v1", output_dir=output_dir)
    # No manifest written at all, and no record_chunk_written/record_chunk_progress.

    reconciled = collect04.discover_orphan_chunk_csvs(
        conn, batch_id, output_dir=output_dir, reports_dir=tmp_path / "reports"
    )
    assert reconciled == 1

    real_row = conn.execute(
        "SELECT chunk_id, listings_found FROM collection_progress "
        "WHERE collection_batch_id = ? AND inventory_uid = ? AND marketplace_id = ?",
        [batch_id, "iuid_pass1", "EBAY_DE"],
    ).fetchone()
    assert real_row == (chunk_id, 1), "the real combo, backed by actual CSV rows, must still be reconstructed"

    zero_row = conn.execute(
        "SELECT 1 FROM collection_progress WHERE collection_batch_id = ? AND inventory_uid = ? AND marketplace_id = ?",
        [batch_id, "iuid_warn1", "EBAY_DE"],
    ).fetchone()
    assert zero_row is None, "without a manifest, the zero-result combo has no evidence and must not be fabricated"
    assert collect04.already_processed(conn, batch_id, "iuid_warn1", "EBAY_DE", output_dir=output_dir) is False, \
        "must be retried, not silently trusted, since the CSV alone cannot prove zero-result work"


# ── Manifest-to-CSV integrity (item 3) ────────────────────────────────────────

def _write_orphan_chunk_with_manifest(output_dir, reports_dir, batch_id, chunk_id, item_results):
    """Writes a CSV + its correctly hash-linked manifest, mimicking exactly
    what run_chunked_collection produces for one chunk."""
    csv_path = collect04.write_batch_csv(batch_id, chunk_id, item_results, "v1", output_dir=output_dir)
    now = datetime.now(timezone.utc)
    manifest_path = collect04.write_manifest(
        batch_id=batch_id, chunk_id=chunk_id, started_at=now, finished_at=now, source_csv_path=csv_path,
        item_results=item_results, call_budget=collect04.CallBudget(100), marketplaces=["EBAY_DE"],
        source_filename=csv_path.name, reports_dir=reports_dir,
        manifest_filename=f"targeted_collection_manifest_{chunk_id}.json",
    )
    return csv_path, manifest_path


def test_manifest_with_matching_csv_hash_is_trusted(conn, tmp_path):
    """Correct manifest + correct (unmodified) CSV hash: full reconstruction,
    including the zero-result combination the CSV alone cannot prove."""
    batch_id = "batch_manifest_hash_ok"
    collect04.start_batch(conn, batch_id, {})
    output_dir, reports_dir = tmp_path / "targeted_active", tmp_path / "reports"
    chunk_id = f"{batch_id}_chunk_hashok"
    item_results = _orphan_item_results_with_one_zero_result_combo()
    _write_orphan_chunk_with_manifest(output_dir, reports_dir, batch_id, chunk_id, item_results)

    reconciled = collect04.discover_orphan_chunk_csvs(conn, batch_id, output_dir=output_dir, reports_dir=reports_dir)
    assert reconciled == 1
    assert collect04.already_processed(conn, batch_id, "iuid_pass1", "EBAY_DE", output_dir=output_dir) is True
    assert collect04.already_processed(conn, batch_id, "iuid_warn1", "EBAY_DE", output_dir=output_dir) is True


def test_manifest_paired_with_modified_csv_is_rejected(conn, tmp_path):
    """The manifest's recorded hash no longer matches the CSV's actual
    content (e.g. the file was edited/corrupted/truncated after the
    manifest was written) — the manifest must be refused, and the
    zero-result combination it would have proven must NOT be fabricated."""
    batch_id = "batch_manifest_modified_csv"
    collect04.start_batch(conn, batch_id, {})
    output_dir, reports_dir = tmp_path / "targeted_active", tmp_path / "reports"
    chunk_id = f"{batch_id}_chunk_modified"
    item_results = _orphan_item_results_with_one_zero_result_combo()
    csv_path, manifest_path = _write_orphan_chunk_with_manifest(output_dir, reports_dir, batch_id, chunk_id, item_results)

    # Modify the CSV after the manifest was written — its hash no longer matches.
    with csv_path.open("a") as fh:
        fh.write("extra,garbage,row\n")

    reconciled = collect04.discover_orphan_chunk_csvs(conn, batch_id, output_dir=output_dir, reports_dir=reports_dir)
    assert reconciled == 1
    # The real combo is still recoverable directly from CSV content (its
    # own rows are untouched by the appended garbage line at the end).
    assert collect04.already_processed(conn, batch_id, "iuid_pass1", "EBAY_DE", output_dir=output_dir) is True
    # But the zero-result combo's only evidence was the now-untrusted manifest.
    assert collect04.already_processed(conn, batch_id, "iuid_warn1", "EBAY_DE", output_dir=output_dir) is False, \
        "a manifest whose hash no longer matches the CSV must not be trusted for the zero-result claim"


def test_manifest_paired_with_wrong_csv_is_rejected(conn, tmp_path):
    """A manifest for one chunk_id ends up sitting alongside a completely
    different chunk's CSV (e.g. copy/paste error, wrong file restored from
    backup) — collection_batch_id alone is not enough; the hash must not
    match, and the manifest must be refused."""
    batch_id = "batch_manifest_wrong_csv"
    collect04.start_batch(conn, batch_id, {})
    output_dir, reports_dir = tmp_path / "targeted_active", tmp_path / "reports"
    chunk_id = f"{batch_id}_chunk_wrongfile"
    item_results = _orphan_item_results_with_one_zero_result_combo()
    csv_path, manifest_path = _write_orphan_chunk_with_manifest(output_dir, reports_dir, batch_id, chunk_id, item_results)

    # Replace the CSV's content entirely with different (but plausible) data
    # — the manifest still claims to describe the ORIGINAL content's hash.
    other_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {"totally_different_item": _flat_listing("totally_different_item", "EBAY_DE")},
            "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]
    collect04.write_batch_csv(batch_id, chunk_id, other_results, "v1", output_dir=output_dir)  # overwrites via same chunk_id path

    reconciled = collect04.discover_orphan_chunk_csvs(conn, batch_id, output_dir=output_dir, reports_dir=reports_dir)
    assert reconciled == 1
    # The zero-result combo's manifest evidence must be refused — the file
    # it actually points at is not the one the manifest describes.
    assert collect04.already_processed(conn, batch_id, "iuid_warn1", "EBAY_DE", output_dir=output_dir) is False


def test_zero_result_combination_in_valid_manifest_is_reconstructed(conn, tmp_path):
    """Explicit, isolated check that a zero-result combination inside an
    otherwise-valid, hash-verified manifest is reconstructed with the
    correct fields (listings_found=0, distinct auditable outcome_reason)."""
    batch_id = "batch_manifest_zero_result_explicit"
    collect04.start_batch(conn, batch_id, {})
    output_dir, reports_dir = tmp_path / "targeted_active", tmp_path / "reports"
    chunk_id = f"{batch_id}_chunk_zero_explicit"
    item_results = _orphan_item_results_with_one_zero_result_combo()
    _write_orphan_chunk_with_manifest(output_dir, reports_dir, batch_id, chunk_id, item_results)

    collect04.discover_orphan_chunk_csvs(conn, batch_id, output_dir=output_dir, reports_dir=reports_dir)

    row = conn.execute(
        "SELECT chunk_id, listings_found, resolved_tier, outcome_reason FROM collection_progress "
        "WHERE collection_batch_id = ? AND inventory_uid = ? AND marketplace_id = ?",
        [batch_id, "iuid_warn1", "EBAY_DE"],
    ).fetchone()
    assert row is not None
    assert row[0] == chunk_id
    assert row[1] == 0
    assert row[2] is None
    assert row[3] == "reconciled_from_orphan_manifest:tier_exhaustion"


def test_invalid_mismatched_manifest_leaves_combination_retryable(conn, tmp_path):
    """Restating the mismatch case from the resumability angle: after a
    rejected manifest, the zero-result combination must actually be
    retried on the next real collection pass, not merely absent from
    collection_progress — already_processed's False must translate into
    process_item actually attempting it again."""
    batch_id = "batch_manifest_mismatch_retry"
    collect04.start_batch(conn, batch_id, {})
    output_dir, reports_dir = tmp_path / "targeted_active", tmp_path / "reports"
    chunk_id = f"{batch_id}_chunk_mismatch_retry"
    item_results = _orphan_item_results_with_one_zero_result_combo()
    csv_path, _ = _write_orphan_chunk_with_manifest(output_dir, reports_dir, batch_id, chunk_id, item_results)
    with csv_path.open("a") as fh:
        fh.write("tampered,row\n")

    collect04.discover_orphan_chunk_csvs(conn, batch_id, output_dir=output_dir, reports_dir=reports_dir)

    call_log = []

    def fake_search(*, marketplace_id, keyword, on_call=None, **kwargs):
        call_log.append((marketplace_id, keyword))
        if on_call:
            on_call()
        return []

    with patch.object(collect04, "search_items_single_marketplace", side_effect=fake_search):
        row = collect04.get_eligible_inventory(conn, "iuid_warn1").iloc[0]
        collect04.process_item(
            conn, item_row=row, token="fake", batch_id=batch_id,
            call_budget=collect04.CallBudget(100), marketplaces=["EBAY_DE"], output_dir=output_dir,
        )
    assert len(call_log) >= 1, "the zero-result combo with a rejected manifest must actually be retried, not just marked pending"


class _CrashAfterNCalls:
    """Wraps a real DuckDB connection and raises on the Nth call to
    .execute(), forwarding every other call through unchanged — used to
    simulate a crash landing at an exact point inside migrate_legacy_progress_to_chunks'
    per-batch transaction."""
    def __init__(self, real_conn, crash_on_call: int):
        self._real = real_conn
        self._count = 0
        self._crash_on_call = crash_on_call

    def execute(self, *args, **kwargs):
        self._count += 1
        if self._count == self._crash_on_call:
            raise RuntimeError(f"simulated crash on execute() call #{self._crash_on_call}")
        return self._real.execute(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._real, name)


def _seed_two_row_verifiable_legacy_batch(conn, output_dir, batch_id):
    """A legacy batch with exactly 2 non-zero, CSV-backed, verifiable
    progress rows and a matching ingested CSV — precise enough that
    execute() call counts inside the migration's transaction are
    predictable: call 1=BEGIN, 2=INSERT collection_chunks, 3=UPDATE row A,
    4=UPDATE row B, 5=COMMIT."""
    collect04.start_batch(conn, batch_id, {})
    conn.execute(
        "INSERT INTO collection_progress (collection_batch_id, chunk_id, inventory_uid, marketplace_id, "
        "highest_tier_attempted, resolved_tier, api_calls, listings_found, outcome_reason) VALUES "
        "(?, NULL, 'iuid_pass1', 'EBAY_DE', 1, 1, 1, 1, 'success'), "
        "(?, NULL, 'iuid_warn1', 'EBAY_DE', 1, 1, 1, 1, 'success')",
        [batch_id, batch_id],
    )
    csv_path = output_dir / f"targeted_active_{batch_id}.csv"
    csv_path.write_text(
        "collection_batch_id,inventory_uid,canonical_inventory_id,query_text,query_tier,"
        "query_template_version,marketplace_id,fetched_at,item_id,title,price_value,price_currency,"
        "condition,condition_id,buying_options,item_web_url,image_url,seller_username,"
        "seller_feedback_score,seller_feedback_percentage,shipping_cost_value,shipping_cost_currency,"
        "item_location_country,item_location_city,item_creation_date\n"
        f"{batch_id},iuid_pass1,rolex_1030_6941,Rolex 1030 6941,1,v1,EBAY_DE,2026-01-01T00:00:00,itemA,T,10,EUR,,,,,,,,,,,,,\n"
        f"{batch_id},iuid_warn1,rolex_unknown_24_26812_8,24-26812-8,5,v1,EBAY_DE,2026-01-01T00:00:00,itemB,T,10,EUR,,,,,,,,,,,,,\n"
    )
    return csv_path


@pytest.mark.parametrize("crash_on_call,label", [
    (3, "after legacy chunk insertion, before any progress linking"),
    (4, "after partial progress linking (row A linked, row B not yet)"),
    (5, "right before commit, after all linking succeeded internally"),
])
def test_migration_interruption_rolls_back_and_rerun_recovers(conn, tmp_path, crash_on_call, label):
    """
    Item 5: migration atomicity, precisely characterized. Interrupts the
    per-batch transaction at 3 distinct points and proves: (a) nothing
    from that batch's transaction survives the crash — chunk row absent,
    progress rows still chunk_id NULL; (b) a subsequent real (uncrashed)
    call to migrate_legacy_progress_to_chunks recovers to the exact
    correct final state, with no duplicate chunk row and both progress
    rows linked exactly once.

    The guarantee this proves is precise, not the vaguer "no partial
    state": each batch's collection_chunks insert + all its progress-row
    links commit as ONE transaction. A crash anywhere inside it rolls back
    that whole batch's transaction (DuckDB's real BEGIN/COMMIT/ROLLBACK
    semantics, not application-level bookkeeping) — never a half-linked
    row, never a chunk record with no matching links. It does NOT mean no
    interruption is possible at all; it means each interruption's blast
    radius is exactly one batch's transaction, cleanly reversible.
    """
    output_dir = tmp_path / "targeted_active"
    output_dir.mkdir(parents=True)
    batch_id = f"batch_migration_crash_{crash_on_call}"
    _seed_two_row_verifiable_legacy_batch(conn, output_dir, batch_id)

    crashy_conn = _CrashAfterNCalls(conn, crash_on_call=crash_on_call)
    with pytest.raises(RuntimeError, match="simulated crash"):
        collect04.migrate_legacy_progress_to_chunks(crashy_conn, output_dir=output_dir, reports_dir=tmp_path / "reports_unused")

    # Nothing from this interrupted batch's transaction survived.
    chunk_id = f"{batch_id}_legacy_chunk"
    assert conn.execute("SELECT COUNT(*) FROM collection_chunks WHERE chunk_id = ?", [chunk_id]).fetchone()[0] == 0, \
        f"rollback failed for case: {label}"
    null_chunk_count = conn.execute(
        "SELECT COUNT(*) FROM collection_progress WHERE collection_batch_id = ? AND chunk_id IS NULL", [batch_id]
    ).fetchone()[0]
    assert null_chunk_count == 2, f"both rows must still be unlinked after rollback — case: {label}"

    # A real, uncrashed rerun recovers to the correct final state.
    report = collect04.migrate_legacy_progress_to_chunks(conn, output_dir=output_dir, reports_dir=tmp_path / "reports_unused")
    assert len(report) == 1
    assert report[0]["rows_backfilled"] == 2
    assert report[0]["rows_left_unverifiable"] == 0

    chunk_count = conn.execute("SELECT COUNT(*) FROM collection_chunks WHERE chunk_id = ?", [chunk_id]).fetchone()[0]
    assert chunk_count == 1, "exactly one chunk record — never duplicated by the earlier interrupted attempt"
    linked_count = conn.execute(
        "SELECT COUNT(*) FROM collection_progress WHERE collection_batch_id = ? AND chunk_id = ?", [batch_id, chunk_id]
    ).fetchone()[0]
    assert linked_count == 2, "both rows linked exactly once"


def test_verifiably_ingested_legacy_row_backfilled_on_copied_db(conn, tmp_path):
    """Case: a legacy collection_progress row (chunk_id IS NULL) whose
    matching CSV exists on disk AND has a successful ingestion_log entry
    with a hash matching the current file content — migrate_legacy_progress_to_chunks
    must deterministically backfill it (never guess from timestamps/naming),
    and running the migration twice must be a no-op the second time."""
    batch_id = "batch_legacy_verified"
    collect04.start_batch(conn, batch_id, {})
    conn.execute(
        "INSERT INTO collection_progress (collection_batch_id, chunk_id, inventory_uid, marketplace_id, "
        "highest_tier_attempted, resolved_tier, api_calls, listings_found, outcome_reason) "
        "VALUES (?, NULL, ?, ?, 1, 1, 1, 1, 'success')",
        [batch_id, "iuid_pass1", "EBAY_DE"],
    )
    output_dir = tmp_path / "targeted_active"
    output_dir.mkdir(parents=True)
    csv_path = output_dir / f"targeted_active_{batch_id}.csv"
    csv_path.write_text(
        "collection_batch_id,inventory_uid,canonical_inventory_id,query_text,query_tier,"
        "query_template_version,marketplace_id,fetched_at,item_id,title,price_value,price_currency,"
        "condition,condition_id,buying_options,item_web_url,image_url,seller_username,"
        "seller_feedback_score,seller_feedback_percentage,shipping_cost_value,shipping_cost_currency,"
        "item_location_country,item_location_city,item_creation_date\n"
        f"{batch_id},iuid_pass1,rolex_1030_6941,Rolex 1030 6941,1,v1,EBAY_DE,2026-01-01T00:00:00,"
        "legacy_item,T,10,EUR,,,,,,,,,,,,,\n"
    )
    file_hash = ingest01.file_sha256(csv_path)
    ingest01.log_ingestion(
        conn, source_type="targeted_active", source_filename=csv_path.name, file_hash=file_hash,
        upload_batch_id="", rows_inserted=1, status="success",
    )

    report1 = collect04.migrate_legacy_progress_to_chunks(conn, output_dir=output_dir, reports_dir=tmp_path / 'reports_unused')
    assert len(report1) == 1
    assert report1[0]["batch_id"] == batch_id
    assert report1[0]["ingested"] is True
    assert report1[0]["rows_backfilled"] == 1

    row = conn.execute(
        "SELECT chunk_id FROM collection_progress WHERE collection_batch_id = ?", [batch_id]
    ).fetchone()
    assert row[0] == report1[0]["chunk_id"]
    chunk_row = conn.execute(
        "SELECT csv_written_at, ingested_at FROM collection_chunks WHERE chunk_id = ?", [row[0]]
    ).fetchone()
    assert chunk_row[0] is not None
    assert chunk_row[1] is not None

    # Now trusted even without the file present (authoritatively ingested).
    csv_path.unlink()
    assert collect04.already_processed(conn, batch_id, "iuid_pass1", "EBAY_DE", output_dir=output_dir) is True

    # Idempotency: running the migration again must be a no-op.
    report2 = collect04.migrate_legacy_progress_to_chunks(conn, output_dir=output_dir, reports_dir=tmp_path / 'reports_unused')
    assert report2 == []


def test_legacy_zero_result_row_backfilled_only_with_surviving_manifest_evidence(conn, tmp_path):
    """
    The core distinction from item 2 of the recovery audit: a legacy
    batch's CSV can deterministically prove its non-zero rows (their rows
    are physically present), but can NEVER prove a zero-result row on its
    own. migrate_legacy_progress_to_chunks must therefore treat these two
    rows from the SAME batch differently — backfilling the real one from
    the CSV, and the zero-result one ONLY because a surviving manifest's
    call_reconciliation proves that exact query was actually executed.
    """
    batch_id = "batch_legacy_mixed"
    collect04.start_batch(conn, batch_id, {})
    conn.execute(
        "INSERT INTO collection_progress (collection_batch_id, chunk_id, inventory_uid, marketplace_id, "
        "highest_tier_attempted, resolved_tier, api_calls, listings_found, outcome_reason) VALUES "
        "(?, NULL, 'iuid_pass1', 'EBAY_DE', 1, 1, 1, 1, 'success'), "
        "(?, NULL, 'iuid_warn1', 'EBAY_DE', 5, NULL, 1, 0, 'tier_exhaustion')",
        [batch_id, batch_id],
    )
    output_dir = tmp_path / "targeted_active"
    output_dir.mkdir(parents=True)
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True)

    csv_path = output_dir / f"targeted_active_{batch_id}.csv"
    csv_path.write_text(
        "collection_batch_id,inventory_uid,canonical_inventory_id,query_text,query_tier,"
        "query_template_version,marketplace_id,fetched_at,item_id,title,price_value,price_currency,"
        "condition,condition_id,buying_options,item_web_url,image_url,seller_username,"
        "seller_feedback_score,seller_feedback_percentage,shipping_cost_value,shipping_cost_currency,"
        "item_location_country,item_location_city,item_creation_date\n"
        f"{batch_id},iuid_pass1,rolex_1030_6941,Rolex 1030 6941,1,v1,EBAY_DE,2026-01-01T00:00:00,"
        "legacy_item,T,10,EUR,,,,,,,,,,,,,\n"
        # iuid_warn1/EBAY_DE contributes NO row — a zero-result combo cannot appear here at all.
    )

    # No ingestion_log entry for this batch (durable-but-not-ingested), and
    # a surviving legacy-format manifest whose call_reconciliation proves
    # iuid_warn1/EBAY_DE's tier-5 query really was executed, found nothing.
    (reports_dir / "targeted_collection_manifest.json").write_text(json.dumps({
        "collection_batch_id": batch_id,
        "inventory_items_processed": 2,
        "unresolved_inventory_items": ["rolex_unknown_24_26812_8"],
        "call_reconciliation": [
            {"canonical_inventory_id": "rolex_1030_6941", "inventory_uid": "iuid_pass1", "marketplace_id": "EBAY_DE",
             "tier": 1, "query_text": "Rolex 1030 6941", "calls": 1, "retries": 0, "results_returned": 1},
            {"canonical_inventory_id": "rolex_unknown_24_26812_8", "inventory_uid": "iuid_warn1", "marketplace_id": "EBAY_DE",
             "tier": 5, "query_text": "24-26812-8", "calls": 1, "retries": 0, "results_returned": 0},
        ],
    }))

    report = collect04.migrate_legacy_progress_to_chunks(conn, output_dir=output_dir, reports_dir=reports_dir)
    assert len(report) == 1
    assert report[0]["rows_backfilled"] == 2
    assert report[0]["rows_left_unverifiable"] == 0

    real_chunk_id = conn.execute(
        "SELECT chunk_id FROM collection_progress WHERE collection_batch_id = ? AND inventory_uid = 'iuid_pass1'",
        [batch_id],
    ).fetchone()[0]
    zero_chunk_id = conn.execute(
        "SELECT chunk_id FROM collection_progress WHERE collection_batch_id = ? AND inventory_uid = 'iuid_warn1'",
        [batch_id],
    ).fetchone()[0]
    assert real_chunk_id is not None
    assert zero_chunk_id == real_chunk_id, "the zero-result row must be backfilled to the same chunk, proven by the manifest"


def test_legacy_zero_result_row_left_unverifiable_without_surviving_manifest(conn, tmp_path):
    """Same mixed batch as above, but with NO surviving manifest (the
    realistic state for two of the three real legacy batches, whose
    manifests were overwritten by the old fixed-path design). The real row
    must still be backfilled from the CSV; the zero-result row must be
    left with chunk_id NULL — unverifiable, not guessed at — even though
    it belongs to the same otherwise-verified batch."""
    batch_id = "batch_legacy_mixed_no_manifest"
    collect04.start_batch(conn, batch_id, {})
    conn.execute(
        "INSERT INTO collection_progress (collection_batch_id, chunk_id, inventory_uid, marketplace_id, "
        "highest_tier_attempted, resolved_tier, api_calls, listings_found, outcome_reason) VALUES "
        "(?, NULL, 'iuid_pass1', 'EBAY_DE', 1, 1, 1, 1, 'success'), "
        "(?, NULL, 'iuid_warn1', 'EBAY_DE', 5, NULL, 1, 0, 'tier_exhaustion')",
        [batch_id, batch_id],
    )
    output_dir = tmp_path / "targeted_active"
    output_dir.mkdir(parents=True)
    csv_path = output_dir / f"targeted_active_{batch_id}.csv"
    csv_path.write_text(
        "collection_batch_id,inventory_uid,canonical_inventory_id,query_text,query_tier,"
        "query_template_version,marketplace_id,fetched_at,item_id,title,price_value,price_currency,"
        "condition,condition_id,buying_options,item_web_url,image_url,seller_username,"
        "seller_feedback_score,seller_feedback_percentage,shipping_cost_value,shipping_cost_currency,"
        "item_location_country,item_location_city,item_creation_date\n"
        f"{batch_id},iuid_pass1,rolex_1030_6941,Rolex 1030 6941,1,v1,EBAY_DE,2026-01-01T00:00:00,"
        "legacy_item,T,10,EUR,,,,,,,,,,,,,\n"
    )
    # No manifest anywhere.

    report = collect04.migrate_legacy_progress_to_chunks(
        conn, output_dir=output_dir, reports_dir=tmp_path / "reports_empty"
    )
    assert len(report) == 1
    assert report[0]["rows_backfilled"] == 1
    assert report[0]["rows_left_unverifiable"] == 1

    real_chunk_id = conn.execute(
        "SELECT chunk_id FROM collection_progress WHERE collection_batch_id = ? AND inventory_uid = 'iuid_pass1'",
        [batch_id],
    ).fetchone()[0]
    zero_chunk_id = conn.execute(
        "SELECT chunk_id FROM collection_progress WHERE collection_batch_id = ? AND inventory_uid = 'iuid_warn1'",
        [batch_id],
    ).fetchone()[0]
    assert real_chunk_id is not None
    assert zero_chunk_id is None, "without a surviving manifest, the zero-result row must remain unverifiable"
    assert collect04.already_processed(conn, batch_id, "iuid_warn1", "EBAY_DE", output_dir=output_dir) is False
    total_chunks = conn.execute("SELECT COUNT(*) FROM collection_chunks").fetchone()[0]
    assert total_chunks == 1, "second migration run must not create a duplicate chunk record"


def test_unverifiable_legacy_row_remains_retryable(conn, tmp_path):
    """A legacy row (chunk_id IS NULL) with no matching CSV file on disk at
    all has no evidence to backfill from — the migration must leave it
    exactly as-is (never guessing), and it must remain untrusted/retryable."""
    batch_id = "batch_legacy_no_evidence"
    collect04.start_batch(conn, batch_id, {})
    conn.execute(
        "INSERT INTO collection_progress (collection_batch_id, chunk_id, inventory_uid, marketplace_id, "
        "highest_tier_attempted, resolved_tier, api_calls, listings_found, outcome_reason) "
        "VALUES (?, NULL, ?, ?, 1, 1, 1, 1, 'success')",
        [batch_id, "iuid_pass1", "EBAY_DE"],
    )
    output_dir = tmp_path / "targeted_active"
    output_dir.mkdir(parents=True)
    # deliberately no CSV file written for this batch

    report = collect04.migrate_legacy_progress_to_chunks(conn, output_dir=output_dir, reports_dir=tmp_path / 'reports_unused')
    assert len(report) == 1
    assert report[0]["chunk_id"] is None
    assert "no matching CSV" in report[0]["status"]

    row = conn.execute(
        "SELECT chunk_id FROM collection_progress WHERE collection_batch_id = ?", [batch_id]
    ).fetchone()
    assert row[0] is None, "unverifiable row must be left untouched, not guessed at"
    assert collect04.already_processed(conn, batch_id, "iuid_pass1", "EBAY_DE", output_dir=output_dir) is False


def test_exact_pending_legacy_state_reconciled_by_resume_idempotently(conn, db_path, tmp_path):
    """
    Reproduces the EXACT live transitional state (item 3 of the recovery
    audit) end to end: an existing collection_batches row, legacy
    collection_progress rows (chunk_id NULL), a matching pre-chunk CSV, and
    NO successful ingestion_log entry — then runs the real resume path
    (run_chunked_collection) and proves:
      1. the legacy file is reconciled (ingested) before any new API call
         is made for its already-covered combinations;
      2. exactly one deterministic legacy chunk_id is created (never a
         second one on repeat);
      3. the existing progress rows are LINKED to it, never duplicated;
      4. ingestion succeeds and is idempotent;
      5. a second resume performs no further ingestion or collection.

    Sequencing matches the approved design: migrate_legacy_progress_to_chunks
    is the deliberate, separately-invoked one-time step (modeling the
    approved migration having been applied) — it is never auto-invoked by
    run_chunked_collection itself. Once applied, the ordinary resume path's
    existing pending-ingestion reconciliation (already exercised by
    test_resume_ingests_pending_durable_chunk_before_new_collection) is
    what actually completes the job, with no new code required for that half.
    """
    batch_id = "batch_20260710_legacy_repro"
    output_dir = tmp_path / "targeted_active"
    output_dir.mkdir(parents=True)

    # 1. Existing collection_batches row (unfinished — this is what makes
    #    it discoverable via find_resumable_batch / --resume).
    collect04.start_batch(conn, batch_id, {})

    # 2. Legacy collection_progress rows, chunk_id NULL, for both
    #    marketplaces of the one eligible item.
    conn.execute(
        "INSERT INTO collection_progress (collection_batch_id, chunk_id, inventory_uid, marketplace_id, "
        "highest_tier_attempted, resolved_tier, api_calls, listings_found, outcome_reason) VALUES "
        "(?, NULL, 'iuid_pass1', 'EBAY_DE', 1, 1, 1, 1, 'success'), "
        "(?, NULL, 'iuid_pass1', 'EBAY_US', 1, 1, 1, 1, 'success')",
        [batch_id, batch_id],
    )

    # 3. Matching pre-chunk targeted CSV (old naming: batch_id used
    #    directly, no chunk suffix) with rows for both marketplaces.
    csv_path = output_dir / f"targeted_active_{batch_id}.csv"
    csv_path.write_text(
        "collection_batch_id,inventory_uid,canonical_inventory_id,query_text,query_tier,"
        "query_template_version,marketplace_id,fetched_at,item_id,title,price_value,price_currency,"
        "condition,condition_id,buying_options,item_web_url,image_url,seller_username,"
        "seller_feedback_score,seller_feedback_percentage,shipping_cost_value,shipping_cost_currency,"
        "item_location_country,item_location_city,item_creation_date\n"
        f"{batch_id},iuid_pass1,rolex_1030_6941,Rolex 1030 6941,1,v1,EBAY_DE,2026-01-01T00:00:00,"
        "legacy_de_item,T,10,EUR,,,,,,,,,,,,,\n"
        f"{batch_id},iuid_pass1,rolex_1030_6941,Rolex 1030 6941,1,v1,EBAY_US,2026-01-01T00:00:00,"
        "legacy_us_item,T,12,USD,,,,,,,,,,,,,\n"
    )
    # 4. No ingestion_log entry — matches the real live state exactly.
    assert conn.execute(
        "SELECT COUNT(*) FROM ingestion_log WHERE source_type='targeted_active' AND source_filename = ?",
        [csv_path.name],
    ).fetchone()[0] == 0

    # Step: apply the (already-approved-in-principle) one-time legacy
    # migration — this is the deliberate, separate step, not something
    # run_chunked_collection does silently on its own.
    migration_report = collect04.migrate_legacy_progress_to_chunks(
        conn, output_dir=output_dir, reports_dir=tmp_path / "reports_empty"
    )
    assert len(migration_report) == 1
    legacy_chunk_id = migration_report[0]["chunk_id"]
    assert migration_report[0]["rows_backfilled"] == 2
    assert migration_report[0]["ingested"] is False  # no ingestion_log entry existed

    def fake_ingest(**kwargs):
        ingest01.TARGETED_ACTIVE_DIR = output_dir
        fresh_conn = duckdb.connect(str(db_path))
        try:
            ingest01.insert_targeted_listings(fresh_conn)
        finally:
            fresh_conn.close()

    inventory_df = collect04.get_eligible_inventory(conn, "iuid_pass1")
    fake_quota = {"available": True, "limit": 5000, "used": 0, "remaining": 5000, "reset": "later"}

    # Resume #1: must reconcile the pending legacy chunk (ingest it) BEFORE
    # any new API call — mock_search raising proves no new call happens for
    # the already-covered combinations.
    with patch.object(collect04, "search_items_single_marketplace") as mock_search, \
         patch.object(collect04, "check_quota", return_value=fake_quota), \
         patch.object(collect04, "run_targeted_ingestion", side_effect=fake_ingest):
        mock_search.side_effect = AssertionError("must not re-collect combinations already covered by the legacy chunk")
        summary = collect04.run_chunked_collection(
            conn, inventory_df=inventory_df, token="fake", batch_id=batch_id,
            marketplaces=["EBAY_DE", "EBAY_US"], output_dir=output_dir, reports_dir=tmp_path / "reports", db_path=db_path,
        )
    conn = summary["conn"]

    # 4. Ingestion succeeded, exactly once, for the legacy file.
    ingestion_rows = conn.execute(
        "SELECT status, rows_inserted FROM ingestion_log WHERE source_type='targeted_active' AND source_filename = ?",
        [csv_path.name],
    ).fetchall()
    assert ingestion_rows == [("success", 2)]
    assert conn.execute(
        "SELECT COUNT(*) FROM raw_active_targeted WHERE item_id IN ('legacy_de_item','legacy_us_item')"
    ).fetchone()[0] == 2

    # 2/3. Exactly one deterministic legacy chunk exists; both original
    # progress rows are linked to it, not duplicated.
    chunk_count = conn.execute(
        "SELECT COUNT(*) FROM collection_chunks WHERE chunk_id = ?", [legacy_chunk_id]
    ).fetchone()[0]
    assert chunk_count == 1
    progress_rows = conn.execute(
        "SELECT COUNT(*) FROM collection_progress WHERE collection_batch_id = ? AND chunk_id = ?",
        [batch_id, legacy_chunk_id],
    ).fetchone()[0]
    assert progress_rows == 2, "no duplicate progress rows — the original two rows were linked, not re-inserted"

    ingestion_log_count = conn.execute(
        "SELECT COUNT(*) FROM ingestion_log WHERE source_type='targeted_active' AND source_filename = ?",
        [csv_path.name],
    ).fetchone()[0]
    assert ingestion_log_count == 1

    # 5. Second resume: no further ingestion or collection — everything is
    # already durably represented and confirmed ingested.
    with patch.object(collect04, "search_items_single_marketplace") as mock_search2, \
         patch.object(collect04, "check_quota", return_value=fake_quota), \
         patch.object(collect04, "run_targeted_ingestion", side_effect=fake_ingest) as mock_ingest2:
        mock_search2.side_effect = AssertionError("second resume must not re-collect anything")
        summary2 = collect04.run_chunked_collection(
            conn, inventory_df=inventory_df, token="fake", batch_id=batch_id,
            marketplaces=["EBAY_DE", "EBAY_US"], output_dir=output_dir, reports_dir=tmp_path / "reports", db_path=db_path,
        )
    conn = summary2["conn"]

    assert conn.execute(
        "SELECT COUNT(*) FROM ingestion_log WHERE source_type='targeted_active' AND source_filename = ?",
        [csv_path.name],
    ).fetchone()[0] == 1, "second resume must not create a second ingestion_log entry for the same file"
    assert conn.execute(
        "SELECT COUNT(*) FROM raw_active_targeted WHERE item_id IN ('legacy_de_item','legacy_us_item')"
    ).fetchone()[0] == 2, "second resume must not duplicate raw rows"


def test_ingestion_crash_between_raw_insert_and_log_commit_is_idempotent_on_retry(conn, tmp_path):
    """
    Precise ingestion semantics: at-least-once execution, idempotent final
    effects — NOT exactly-once, NOT fully transactional. INSERT INTO
    raw_active_targeted and log_ingestion(status="success") are two
    separate, independently auto-committed DuckDB statements (verified:
    no explicit BEGIN/COMMIT wraps them together in insert_targeted_listings).
    This simulates a crash landing exactly between those two commits — the
    real rows ARE durable, but ingestion_log does NOT yet show success —
    and proves a retry produces zero duplicate raw rows, then correctly
    completes the missed log entry.
    """
    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {"crash_between_commits_item": _flat_listing("crash_between_commits_item", "EBAY_DE")},
            "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]
    targeted_dir = tmp_path / "targeted_active"
    collect04.write_batch_csv("batch_crash_commit", "batch_crash_commit_chunk_1", item_results, "v1", output_dir=targeted_dir)
    ingest01.TARGETED_ACTIVE_DIR = targeted_dir

    real_log_ingestion = ingest01.log_ingestion
    with patch.object(ingest01, "log_ingestion", side_effect=RuntimeError("simulated crash before log_ingestion commits")):
        with pytest.raises(RuntimeError, match="simulated crash"):
            ingest01.insert_targeted_listings(conn)

    # The raw INSERT already committed (it ran and auto-committed BEFORE
    # the patched log_ingestion call raised) — the row is durably present
    # despite the "crash".
    assert conn.execute(
        "SELECT COUNT(*) FROM raw_active_targeted WHERE item_id = 'crash_between_commits_item'"
    ).fetchone()[0] == 1
    # ingestion_log shows NO success entry for this file — the crash
    # genuinely landed before that commit.
    assert conn.execute(
        "SELECT COUNT(*) FROM ingestion_log WHERE source_type='targeted_active' AND status='success' "
        "AND source_filename LIKE 'targeted_active_batch_crash_commit%'"
    ).fetchone()[0] == 0

    # Retry (at-least-once: the file gets attempted again since no success
    # log entry exists for it) — row-level dedup must produce ZERO
    # duplicate raw rows, and the missed log entry finally gets written.
    n_retry = ingest01.insert_targeted_listings(conn)
    assert n_retry == 0, "retry must insert zero new rows — the data was already durably there from before the crash"
    assert conn.execute(
        "SELECT COUNT(*) FROM raw_active_targeted WHERE item_id = 'crash_between_commits_item'"
    ).fetchone()[0] == 1, "still exactly one row — no duplicate created by the retry"
    assert conn.execute(
        "SELECT status, rows_inserted FROM ingestion_log WHERE source_type='targeted_active' "
        "AND source_filename LIKE 'targeted_active_batch_crash_commit%'"
    ).fetchall() == [("success", 0)], "the missed log entry is now correctly recorded, with 0 new rows this time"


# ── Fix-round tests: duplicate-definition audit, aggregated zero-result ──────
# evidence, full manifest-object validation, conflict-detecting chunk
# registration, atomic orphan reconciliation, deterministic ingestion_log
# selection, correct partial-rerun reporting, atomic manifest writes, and
# schema-upgrade compatibility.

def test_find_surviving_legacy_manifest_has_exactly_one_production_definition():
    """Source-level audit: _find_surviving_legacy_manifest must be defined
    exactly once in the production module — no duplicate definition."""
    source = (SCRIPTS_DIR / "04_collect_targeted_active.py").read_text()
    matches = re.findall(r"^def _find_surviving_legacy_manifest\(", source, flags=re.MULTILINE)
    assert len(matches) == 1


def _manifest_with_call_reconciliation(entries: list[dict]) -> dict:
    return {"call_reconciliation": entries}


def test_zero_result_evidence_with_zero_calls_rejected():
    """An entry claiming a combination was checked but with calls=0 is not
    genuine evidence of an executed query — the whole key must be excluded."""
    manifest = _manifest_with_call_reconciliation([
        {"inventory_uid": "iuid_x", "marketplace_id": "EBAY_DE", "calls": 0, "results_returned": 0},
    ])
    assert collect04._verified_zero_result_keys_from_manifest(manifest) == set()


def test_zero_result_evidence_with_positive_results_rejected():
    """A contradiction: an entry for a supposedly zero-result combination
    that actually reports results_returned > 0 must invalidate the key
    entirely, not be averaged away or ignored."""
    manifest = _manifest_with_call_reconciliation([
        {"inventory_uid": "iuid_x", "marketplace_id": "EBAY_DE", "calls": 1, "results_returned": 3},
    ])
    assert collect04._verified_zero_result_keys_from_manifest(manifest) == set()


def test_valid_zero_result_evidence_accepted():
    """At least one call, aggregated results_returned == 0, no contradiction,
    all fields valid — this is genuine, certifiable zero-result evidence."""
    manifest = _manifest_with_call_reconciliation([
        {"inventory_uid": "iuid_x", "marketplace_id": "EBAY_DE", "calls": 1, "results_returned": 0},
        {"inventory_uid": "iuid_x", "marketplace_id": "EBAY_DE", "calls": 1, "results_returned": 0},
    ])
    assert collect04._verified_zero_result_keys_from_manifest(manifest) == {("iuid_x", "EBAY_DE")}


def test_zero_result_evidence_with_malformed_fields_rejected():
    """Missing/non-numeric calls or results_returned fields must exclude
    the key entirely — never partially trusted."""
    manifest = _manifest_with_call_reconciliation([
        {"inventory_uid": "iuid_x", "marketplace_id": "EBAY_DE", "calls": "one", "results_returned": 0},
    ])
    assert collect04._verified_zero_result_keys_from_manifest(manifest) == set()

    manifest2 = _manifest_with_call_reconciliation([
        {"inventory_uid": "iuid_y", "marketplace_id": "EBAY_DE", "calls": 1},  # results_returned missing entirely
    ])
    assert collect04._verified_zero_result_keys_from_manifest(manifest2) == set()


def _base_valid_manifest(*, batch_id="batch_x", chunk_id="batch_x_chunk_abc", source_filename="targeted_active_batch_x_chunk_abc.csv", csv_hash="abc123"):
    return {
        "schema_version": collect04.MANIFEST_SCHEMA_VERSION,
        "collection_batch_id": batch_id,
        "chunk_id": chunk_id,
        "source_filename": source_filename,
        "source_csv_sha256": csv_hash,
        "attempted_combinations": [
            {"inventory_uid": "iuid_x", "marketplace_id": "EBAY_DE", "listings_found": 0},
        ],
    }


def test_orphan_manifest_wrong_batch_id_rejected():
    manifest = _base_valid_manifest()
    manifest["collection_batch_id"] = "batch_other"
    result = collect04._validate_orphan_manifest(
        manifest, batch_id="batch_x", chunk_id="batch_x_chunk_abc",
        source_filename="targeted_active_batch_x_chunk_abc.csv", actual_hash="abc123",
    )
    assert result is None


def test_orphan_manifest_wrong_chunk_id_rejected():
    manifest = _base_valid_manifest()
    manifest["chunk_id"] = "batch_x_chunk_different"
    result = collect04._validate_orphan_manifest(
        manifest, batch_id="batch_x", chunk_id="batch_x_chunk_abc",
        source_filename="targeted_active_batch_x_chunk_abc.csv", actual_hash="abc123",
    )
    assert result is None


def test_orphan_manifest_wrong_source_filename_rejected():
    manifest = _base_valid_manifest()
    manifest["source_filename"] = "targeted_active_batch_x_chunk_OTHER.csv"
    result = collect04._validate_orphan_manifest(
        manifest, batch_id="batch_x", chunk_id="batch_x_chunk_abc",
        source_filename="targeted_active_batch_x_chunk_abc.csv", actual_hash="abc123",
    )
    assert result is None


def test_orphan_manifest_wrong_csv_hash_rejected():
    manifest = _base_valid_manifest()
    manifest["source_csv_sha256"] = "deadbeef"
    result = collect04._validate_orphan_manifest(
        manifest, batch_id="batch_x", chunk_id="batch_x_chunk_abc",
        source_filename="targeted_active_batch_x_chunk_abc.csv", actual_hash="abc123",
    )
    assert result is None


def test_orphan_manifest_unsupported_schema_version_rejected():
    manifest = _base_valid_manifest()
    manifest["schema_version"] = 1
    result = collect04._validate_orphan_manifest(
        manifest, batch_id="batch_x", chunk_id="batch_x_chunk_abc",
        source_filename="targeted_active_batch_x_chunk_abc.csv", actual_hash="abc123",
    )
    assert result is None


def test_orphan_manifest_malformed_attempted_combinations_rejected():
    manifest = _base_valid_manifest()
    manifest["attempted_combinations"] = "not a list"
    result = collect04._validate_orphan_manifest(
        manifest, batch_id="batch_x", chunk_id="batch_x_chunk_abc",
        source_filename="targeted_active_batch_x_chunk_abc.csv", actual_hash="abc123",
    )
    assert result is None

    manifest2 = _base_valid_manifest()
    manifest2["attempted_combinations"] = [{"inventory_uid": "iuid_x"}]  # missing marketplace_id/listings_found
    result2 = collect04._validate_orphan_manifest(
        manifest2, batch_id="batch_x", chunk_id="batch_x_chunk_abc",
        source_filename="targeted_active_batch_x_chunk_abc.csv", actual_hash="abc123",
    )
    assert result2 is None


def test_orphan_manifest_contradictory_duplicate_combination_rejected():
    manifest = _base_valid_manifest()
    manifest["attempted_combinations"] = [
        {"inventory_uid": "iuid_x", "marketplace_id": "EBAY_DE", "listings_found": 0},
        {"inventory_uid": "iuid_x", "marketplace_id": "EBAY_DE", "listings_found": 5},  # contradiction
    ]
    result = collect04._validate_orphan_manifest(
        manifest, batch_id="batch_x", chunk_id="batch_x_chunk_abc",
        source_filename="targeted_active_batch_x_chunk_abc.csv", actual_hash="abc123",
    )
    assert result is None


def test_orphan_manifest_valid_object_accepted():
    manifest = _base_valid_manifest()
    result = collect04._validate_orphan_manifest(
        manifest, batch_id="batch_x", chunk_id="batch_x_chunk_abc",
        source_filename="targeted_active_batch_x_chunk_abc.csv", actual_hash="abc123",
    )
    assert result == manifest["attempted_combinations"]


def test_malformed_manifest_still_allows_csv_backed_reconstruction(conn, tmp_path):
    """A malformed manifest disables ONLY the manifest-derived zero-result
    recovery path — the real, CSV-backed combination in the same orphan
    chunk must still be reconstructed independently from the CSV."""
    batch_id = "batch_malformed_manifest_partial"
    collect04.start_batch(conn, batch_id, {})
    output_dir = tmp_path / "targeted_active"
    reports_dir = tmp_path / "reports"
    chunk_id = f"{batch_id}_chunk_malformed"
    item_results = _orphan_item_results_with_one_zero_result_combo()
    csv_path, manifest_path = _write_orphan_chunk_with_manifest(output_dir, reports_dir, batch_id, chunk_id, item_results)

    # Corrupt the manifest's attempted_combinations after writing it (still
    # matches the CSV hash — the corruption is purely structural).
    manifest_data = json.loads(manifest_path.read_text())
    manifest_data["attempted_combinations"] = "not a list"
    manifest_path.write_text(json.dumps(manifest_data))

    reconciled = collect04.discover_orphan_chunk_csvs(conn, batch_id, output_dir=output_dir, reports_dir=reports_dir)
    assert reconciled == 1
    assert collect04.already_processed(conn, batch_id, "iuid_pass1", "EBAY_DE", output_dir=output_dir) is True, \
        "the CSV-backed non-zero combo must still be reconstructed despite the malformed manifest"
    assert collect04.already_processed(conn, batch_id, "iuid_warn1", "EBAY_DE", output_dir=output_dir) is False, \
        "the zero-result combo has no valid manifest evidence and must remain retryable"


def test_uningested_chunk_with_missing_csv_sha256_is_retryable(conn, tmp_path):
    """Item 4: the unsafe hashless fallback is removed. A chunk with
    ingested_at NULL and NO recorded csv_sha256 must return False from
    already_processed even though its file exists on disk — a missing hash
    is a missing precondition, never trusted via file existence alone."""
    batch_id = "batch_missing_hash_test"
    collect04.start_batch(conn, batch_id, {})
    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {"no_hash_item": _flat_listing("no_hash_item", "EBAY_DE")},
            "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]
    chunk_id = f"{batch_id}_chunk_nohash"
    csv_path = collect04.write_batch_csv(batch_id, chunk_id, item_results, "v1", output_dir=tmp_path)
    # Insert the chunk record directly with csv_sha256 left NULL — simulates
    # an older record predating hash tracking, without using
    # record_chunk_written (which would require a hash).
    conn.execute(
        "INSERT INTO collection_chunks (chunk_id, collection_batch_id, source_filename, csv_written_at, "
        "items_attempted, calls_made) VALUES (?, ?, ?, current_timestamp, 1, 1)",
        [chunk_id, batch_id, csv_path.name],
    )
    collect04.record_chunk_progress(conn, batch_id=batch_id, chunk_id=chunk_id, item_results=item_results)

    assert collect04.already_processed(conn, batch_id, "iuid_pass1", "EBAY_DE", output_dir=tmp_path) is False


def test_record_chunk_written_conflicting_chunk_id_raises(conn, tmp_path):
    chunk_id = "batch_conflict_chunk_1"
    collect04.record_chunk_written(
        conn, chunk_id=chunk_id, batch_id="batch_a", source_filename="targeted_active_batch_conflict_chunk_1.csv",
        csv_sha256="hash_a", started_at=datetime.now(timezone.utc), items_attempted=1, calls_made=1,
    )
    with pytest.raises(collect04.ChunkIntegrityError):
        collect04.record_chunk_written(
            conn, chunk_id=chunk_id, batch_id="batch_b",  # different batch_id — conflicting identity
            source_filename="targeted_active_batch_conflict_chunk_1.csv", csv_sha256="hash_a",
            started_at=datetime.now(timezone.utc), items_attempted=1, calls_made=1,
        )
    with pytest.raises(collect04.ChunkIntegrityError):
        collect04.record_chunk_written(
            conn, chunk_id=chunk_id, batch_id="batch_a", source_filename="targeted_active_batch_conflict_chunk_1.csv",
            csv_sha256="hash_DIFFERENT",  # different hash — conflicting identity
            started_at=datetime.now(timezone.utc), items_attempted=1, calls_made=1,
        )


def test_record_chunk_written_identical_existing_chunk_id_is_idempotent(conn):
    chunk_id = "batch_idempotent_chunk_1"
    kwargs = dict(
        chunk_id=chunk_id, batch_id="batch_a", source_filename="targeted_active_batch_idempotent_chunk_1.csv",
        csv_sha256="hash_a", started_at=datetime.now(timezone.utc), items_attempted=1, calls_made=1,
    )
    collect04.record_chunk_written(conn, **kwargs)
    collect04.record_chunk_written(conn, **kwargs)  # identical — must not raise, must not duplicate
    count = conn.execute("SELECT COUNT(*) FROM collection_chunks WHERE chunk_id = ?", [chunk_id]).fetchone()[0]
    assert count == 1


def test_orphan_reconciliation_transaction_interruption_rolls_back_fully(conn, tmp_path):
    """Item 6: a crash partway through orphan reconciliation (after the
    chunk registration, mid-way through reconstructing combinations) must
    roll back the ENTIRE orphan chunk — no chunk row, no partial progress
    rows — and a subsequent real call must reconcile it fully."""
    batch_id = "batch_orphan_txn_crash"
    collect04.start_batch(conn, batch_id, {})
    output_dir = tmp_path / "targeted_active"
    reports_dir = tmp_path / "reports"
    chunk_id = f"{batch_id}_chunk_txncrash"
    item_results = _orphan_item_results_with_one_zero_result_combo()
    _write_orphan_chunk_with_manifest(output_dir, reports_dir, batch_id, chunk_id, item_results)

    # Crash on the 3rd execute() call inside the transaction: call 1=BEGIN,
    # 2=record_chunk_written's SELECT existing, 3=its INSERT — interrupts
    # right after chunk registration's own lookup, before anything commits.
    crashy_conn = _CrashAfterNCalls(conn, crash_on_call=3)
    with pytest.raises(RuntimeError, match="simulated crash"):
        collect04.discover_orphan_chunk_csvs(crashy_conn, batch_id, output_dir=output_dir, reports_dir=reports_dir)

    assert conn.execute("SELECT COUNT(*) FROM collection_chunks WHERE chunk_id = ?", [chunk_id]).fetchone()[0] == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM collection_progress WHERE collection_batch_id = ?", [batch_id]
    ).fetchone()[0] == 0

    # A real, uncrashed rerun reconciles the same orphan CSV fully.
    reconciled = collect04.discover_orphan_chunk_csvs(conn, batch_id, output_dir=output_dir, reports_dir=reports_dir)
    assert reconciled == 1
    assert conn.execute("SELECT COUNT(*) FROM collection_chunks WHERE chunk_id = ?", [chunk_id]).fetchone()[0] == 1
    assert collect04.already_processed(conn, batch_id, "iuid_pass1", "EBAY_DE", output_dir=output_dir) is True
    assert collect04.already_processed(conn, batch_id, "iuid_warn1", "EBAY_DE", output_dir=output_dir) is True


def test_deterministic_ingestion_log_selection_among_multiple_success_rows(conn, tmp_path):
    """
    Item 7: multiple successful ingestion_log rows can exist for the same
    source_filename only with DIFFERENT file_hash values (the table's PK is
    (source_type, source_filename, file_hash) — an identical hash would be
    a duplicate key, not a second real record). This models a file whose
    name was reused with different content over time (an older, stale
    success entry with a non-matching hash, and a newer one whose hash
    matches the file's CURRENT actual content). migrate_legacy_progress_to_chunks
    must deterministically select the LATEST entry by ingested_at — an
    unordered/arbitrary fetchone() could pick the stale, non-matching one
    and wrongly refuse to backfill data that is, in fact, correctly ingested.
    """
    batch_id = "batch_multi_ingestion_log"
    collect04.start_batch(conn, batch_id, {})
    conn.execute(
        "INSERT INTO collection_progress (collection_batch_id, chunk_id, inventory_uid, marketplace_id, "
        "highest_tier_attempted, resolved_tier, api_calls, listings_found, outcome_reason) "
        "VALUES (?, NULL, 'iuid_pass1', 'EBAY_DE', 1, 1, 1, 1, 'success')",
        [batch_id],
    )
    output_dir = tmp_path / "targeted_active"
    output_dir.mkdir(parents=True)
    csv_path = output_dir / f"targeted_active_{batch_id}.csv"
    csv_path.write_text(
        "collection_batch_id,inventory_uid,canonical_inventory_id,query_text,query_tier,"
        "query_template_version,marketplace_id,fetched_at,item_id,title,price_value,price_currency,"
        "condition,condition_id,buying_options,item_web_url,image_url,seller_username,"
        "seller_feedback_score,seller_feedback_percentage,shipping_cost_value,shipping_cost_currency,"
        "item_location_country,item_location_city,item_creation_date\n"
        f"{batch_id},iuid_pass1,rolex_1030_6941,Rolex 1030 6941,1,v1,EBAY_DE,2026-01-01T00:00:00,mi_item,T,10,EUR,,,,,,,,,,,,,\n"
    )
    current_hash = ingest01.file_sha256(csv_path)
    now = datetime.now(timezone.utc)

    # Older, STALE success entry with a non-matching hash (an earlier
    # version of this filename's content) — must be ignored in favor of...
    conn.execute(
        "INSERT INTO ingestion_log (source_type, source_filename, file_hash, upload_batch_id, ingested_at, "
        "rows_inserted, status) VALUES ('targeted_active', ?, ?, '', ?, 1, 'success')",
        [csv_path.name, "stale_nonmatching_hash", now - timedelta(hours=1)],
    )
    # ...the NEWER success entry whose hash matches the file's actual
    # current content.
    conn.execute(
        "INSERT INTO ingestion_log (source_type, source_filename, file_hash, upload_batch_id, ingested_at, "
        "rows_inserted, status) VALUES ('targeted_active', ?, ?, '', ?, 1, 'success')",
        [csv_path.name, current_hash, now],
    )

    report = collect04.migrate_legacy_progress_to_chunks(conn, output_dir=output_dir, reports_dir=tmp_path / "reports_unused")
    assert len(report) == 1
    assert report[0]["ingested"] is True, (
        "must select the latest (matching-hash) entry — an unordered/arbitrary selection risks "
        "picking the stale non-matching one and wrongly refusing to backfill"
    )
    chunk_ingested_at = conn.execute(
        "SELECT ingested_at FROM collection_chunks WHERE chunk_id = ?", [report[0]["chunk_id"]]
    ).fetchone()[0]
    newer_entry_ingested_at, stale_entry_ingested_at = conn.execute(
        "SELECT ingested_at FROM ingestion_log WHERE source_type='targeted_active' AND source_filename=? "
        "ORDER BY ingested_at DESC",
        [csv_path.name],
    ).fetchall()
    assert chunk_ingested_at == newer_entry_ingested_at[0], "must use the LATEST success entry's ingested_at"
    assert chunk_ingested_at != stale_entry_ingested_at[0], "must not use the stale entry's ingested_at"


def test_partial_legacy_rerun_reports_true_remaining_null_rows(conn, tmp_path):
    """Item 8: a batch left with some rows verified and some unverifiable
    after run 1 must NOT be skipped wholesale on rerun just because its
    synthetic chunk_id already exists. Rerun must reconsider the actual
    remaining chunk_id IS NULL rows and report the true remaining
    unverifiable count — never silently reporting zero when rows remain."""
    batch_id = "batch_partial_rerun_test"
    collect04.start_batch(conn, batch_id, {})
    conn.execute(
        "INSERT INTO collection_progress (collection_batch_id, chunk_id, inventory_uid, marketplace_id, "
        "highest_tier_attempted, resolved_tier, api_calls, listings_found, outcome_reason) VALUES "
        "(?, NULL, 'iuid_pass1', 'EBAY_DE', 1, 1, 1, 1, 'success'), "
        "(?, NULL, 'iuid_warn1', 'EBAY_DE', 5, NULL, 1, 0, 'tier_exhaustion')",
        [batch_id, batch_id],
    )
    output_dir = tmp_path / "targeted_active"
    output_dir.mkdir(parents=True)
    csv_path = output_dir / f"targeted_active_{batch_id}.csv"
    csv_path.write_text(
        "collection_batch_id,inventory_uid,canonical_inventory_id,query_text,query_tier,"
        "query_template_version,marketplace_id,fetched_at,item_id,title,price_value,price_currency,"
        "condition,condition_id,buying_options,item_web_url,image_url,seller_username,"
        "seller_feedback_score,seller_feedback_percentage,shipping_cost_value,shipping_cost_currency,"
        "item_location_country,item_location_city,item_creation_date\n"
        f"{batch_id},iuid_pass1,rolex_1030_6941,Rolex 1030 6941,1,v1,EBAY_DE,2026-01-01T00:00:00,real_item,T,10,EUR,,,,,,,,,,,,,\n"
        # iuid_warn1/EBAY_DE has no CSV row and no manifest — genuinely unverifiable.
    )

    # Run 1: real row backfilled, zero-result row left unverifiable (no manifest).
    report1 = collect04.migrate_legacy_progress_to_chunks(conn, output_dir=output_dir, reports_dir=tmp_path / "reports_empty")
    assert len(report1) == 1
    assert report1[0]["rows_backfilled"] == 1
    assert report1[0]["rows_left_unverifiable"] == 1
    assert report1[0]["status"] == "partially_backfilled (some rows left unverifiable)"

    # Run 2: the synthetic chunk_id now exists, but ONE row is still
    # chunk_id NULL — must be reported truthfully, not as "0 remaining".
    report2 = collect04.migrate_legacy_progress_to_chunks(conn, output_dir=output_dir, reports_dir=tmp_path / "reports_empty")
    assert len(report2) == 1
    assert report2[0]["rows_backfilled"] == 0
    assert report2[0]["rows_left_unverifiable"] == 1, "must truthfully report 1 remaining, never silently 0"

    still_null = conn.execute(
        "SELECT COUNT(*) FROM collection_progress WHERE collection_batch_id = ? AND chunk_id IS NULL", [batch_id]
    ).fetchone()[0]
    assert still_null == 1
    chunk_count = conn.execute(
        "SELECT COUNT(*) FROM collection_chunks WHERE chunk_id = ?", [f"{batch_id}_legacy_chunk"]
    ).fetchone()[0]
    assert chunk_count == 1, "the chunk record itself must not be duplicated across reruns"


def test_manifest_temp_file_interruption_does_not_create_trusted_final_manifest(conn, tmp_path):
    """Item 9: an interrupted manifest write (crash after the temp file is
    created but before os.replace()) must leave no final manifest at the
    expected path — discover_orphan_chunk_csvs must correctly report 'no
    manifest found' and fall back to CSV-only (non-zero-only) reconstruction,
    never treating a leftover temp file as a trusted final manifest."""
    batch_id = "batch_manifest_temp_crash"
    collect04.start_batch(conn, batch_id, {})
    output_dir = tmp_path / "targeted_active"
    reports_dir = tmp_path / "reports"
    chunk_id = f"{batch_id}_chunk_tempcrash"
    item_results = _orphan_item_results_with_one_zero_result_combo()
    csv_path = collect04.write_batch_csv(batch_id, chunk_id, item_results, "v1", output_dir=output_dir)

    real_mkstemp = tempfile.mkstemp

    def crashing_mkstemp(*args, **kwargs):
        fd, name = real_mkstemp(*args, **kwargs)
        import os as _os
        _os.write(fd, b'{"incomplete": true')  # partial, invalid JSON content
        _os.close(fd)
        raise RuntimeError("simulated crash after temp file created, before os.replace()")

    reports_dir.mkdir(parents=True, exist_ok=True)
    with patch.object(collect04.tempfile, "mkstemp", side_effect=crashing_mkstemp):
        with pytest.raises(RuntimeError, match="simulated crash"):
            collect04.write_manifest(
                batch_id=batch_id, chunk_id=chunk_id, started_at=datetime.now(timezone.utc),
                finished_at=datetime.now(timezone.utc), source_csv_path=csv_path,
                item_results=item_results, call_budget=collect04.CallBudget(100), marketplaces=["EBAY_DE"],
                source_filename=csv_path.name, reports_dir=reports_dir,
                manifest_filename=f"targeted_collection_manifest_{chunk_id}.json",
            )

    final_manifest_path = reports_dir / f"targeted_collection_manifest_{chunk_id}.json"
    assert not final_manifest_path.exists(), "an interrupted write must never leave a file at the final trusted path"

    # discover_orphan_chunk_csvs must not be fooled by any leftover temp file.
    reconciled = collect04.discover_orphan_chunk_csvs(conn, batch_id, output_dir=output_dir, reports_dir=reports_dir)
    assert reconciled == 1
    assert collect04.already_processed(conn, batch_id, "iuid_pass1", "EBAY_DE", output_dir=output_dir) is True
    assert collect04.already_processed(conn, batch_id, "iuid_warn1", "EBAY_DE", output_dir=output_dir) is False, \
        "no valid manifest survived, so the zero-result combo must remain retryable, not fabricated from a temp file"


def test_schema_upgrade_adds_csv_sha256_idempotently_to_existing_db(tmp_path):
    """
    Item 10: exercises an UPGRADED existing schema, not only a fresh one.
    Creates a test database with collection_chunks in its OLD shape
    (missing csv_sha256 — as if created before this column existed),
    seeds real rows, then runs migrate_collection_tables_schema and proves:
    the column is added, running it again is idempotent (no error, no
    duplicate columns), and existing row counts are completely unchanged.
    """
    db_path = tmp_path / "old_shape.duckdb"
    old_conn = duckdb.connect(str(db_path))
    old_conn.execute("""
        CREATE TABLE collection_chunks (
            chunk_id              VARCHAR PRIMARY KEY,
            collection_batch_id   VARCHAR,
            source_filename       VARCHAR,
            started_at            TIMESTAMP,
            csv_written_at        TIMESTAMP,
            ingested_at           TIMESTAMP,
            items_attempted       INTEGER,
            calls_made            INTEGER
        )
    """)
    old_conn.execute("""
        CREATE TABLE collection_progress (
            collection_batch_id VARCHAR, inventory_uid VARCHAR, marketplace_id VARCHAR,
            highest_tier_attempted INTEGER, resolved_tier INTEGER, api_calls INTEGER,
            listings_found INTEGER, outcome_reason VARCHAR, processed_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (collection_batch_id, inventory_uid, marketplace_id)
        )
    """)
    old_conn.execute("""
        CREATE TABLE collection_batches (
            collection_batch_id VARCHAR PRIMARY KEY, started_at TIMESTAMP, finished_at TIMESTAMP,
            config_snapshot VARCHAR
        )
    """)
    old_conn.execute(
        "INSERT INTO collection_chunks (chunk_id, collection_batch_id, source_filename, started_at, "
        "csv_written_at, items_attempted, calls_made) VALUES ('c1', 'b1', 'f1.csv', current_timestamp, "
        "current_timestamp, 1, 1)"
    )
    old_conn.execute(
        "INSERT INTO collection_progress (collection_batch_id, inventory_uid, marketplace_id, "
        "highest_tier_attempted, resolved_tier, api_calls, listings_found, outcome_reason) "
        "VALUES ('b1', 'iuid_1', 'EBAY_DE', 1, 1, 1, 5, 'success')"
    )
    old_conn.execute("INSERT INTO collection_batches (collection_batch_id, started_at) VALUES ('b1', current_timestamp)")

    def counts():
        return (
            old_conn.execute("SELECT COUNT(*) FROM collection_chunks").fetchone()[0],
            old_conn.execute("SELECT COUNT(*) FROM collection_progress").fetchone()[0],
            old_conn.execute("SELECT COUNT(*) FROM collection_batches").fetchone()[0],
        )

    before = counts()
    assert "csv_sha256" not in {r[0] for r in old_conn.execute("DESCRIBE collection_chunks").fetchall()}

    collect04.migrate_collection_tables_schema(old_conn)
    after_first = counts()
    columns_after = {r[0] for r in old_conn.execute("DESCRIBE collection_chunks").fetchall()}
    assert "csv_sha256" in columns_after
    assert after_first == before

    # Idempotent: running it again must not error or change anything.
    collect04.migrate_collection_tables_schema(old_conn)
    after_second = counts()
    assert after_second == before
    columns_after_second = {r[0] for r in old_conn.execute("DESCRIBE collection_chunks").fetchall()}
    assert columns_after_second == columns_after

    old_conn.close()


# ── --reconcile-only: ingestion-only path, never touches quota/token/eBay (integrity correction pass) ──

def test_parse_args_recognizes_reconcile_only_flag(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["04_collect_targeted_active.py", "--reconcile-only"])
    args = collect04.parse_args()
    assert args.reconcile_only is True
    assert args.resume is False
    assert args.dry_run is False


def test_run_reconcile_only_ingests_pending_chunk_and_reports_before_after_counts(conn, db_path, tmp_path):
    """Unit-level: run_reconcile_only ingests an already-durable,
    not-yet-ingested chunk CSV and reconciles collection_chunks.ingested_at
    — pending count must drop from 1 to 0, and the underlying chunk must
    end up marked ingested. Takes db_path, not the open `conn` fixture —
    same-process multiple connections to the same file are fine (DuckDB
    shares one underlying instance), so run_reconcile_only opening/closing
    its own internal connections against the same db_path as the `conn`
    fixture does not conflict."""
    batch_id = "batch_reconcile_only_unit"
    collect04.start_batch(conn, batch_id, {})
    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {"reconcile_only_item": _flat_listing("reconcile_only_item", "EBAY_DE")},
            "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]
    chunk_id, csv_path = _durably_write_chunk(conn, tmp_path, batch_id, item_results)

    with patch.object(collect04, "run_targeted_ingestion", side_effect=_make_fake_ingest(tmp_path, db_path)):
        summary = collect04.run_reconcile_only(db_path=db_path)

    assert summary["pending_before"] == 1
    assert summary["pending_after"] == 0
    assert summary["newly_marked_ingested"] == 1
    # This batch was started without expected_pairs (old-style call), so
    # reconcile_batch_state must not guess its completeness either way.
    assert summary["batches_reconciled"] == [
        {"batch_id": batch_id, "action": "skipped_no_expected_pairs_recorded"}
    ]
    row = conn.execute(
        "SELECT ingested_at FROM collection_chunks WHERE chunk_id = ?", [chunk_id]
    ).fetchone()
    assert row[0] is not None


def test_run_reconcile_only_with_nothing_pending_is_a_clean_no_op(conn, db_path):
    summary = collect04.run_reconcile_only(db_path=db_path)
    assert summary == {
        "pending_before": 0, "pending_after": 0, "newly_marked_ingested": 0, "batches_reconciled": [],
    }


def test_reconcile_only_cli_never_touches_quota_token_or_ebay_and_exits_before_new_collection(tmp_path, monkeypatch):
    """
    End-to-end through main(): --reconcile-only must ingest a pending
    durable chunk and reconcile it, but must never call get_access_token,
    never call check_quota, never call the Browse API
    (search_items_single_marketplace), and must exit before any new
    collection_batches/collection_progress activity — proving it is a
    genuinely separate path from --resume, which would fall through into
    quota checks and new collection after ingesting pending chunks.

    Uses its own throwaway on-disk database (not the shared `conn`
    fixture) because main() unconditionally closes the connection it gets
    from get_connection() in its `finally` block; connections here are
    opened and closed strictly sequentially against the same file, so nothing
    is ever accessed concurrently.
    """
    db_path = tmp_path / "reconcile_only_cli.duckdb"
    output_dir = tmp_path / "targeted_active"

    setup_conn = duckdb.connect(str(db_path))
    setup_conn.execute(SCHEMA_PATH.read_text())
    _seed_db(setup_conn)
    batch_id = "batch_reconcile_only_cli"
    collect04.start_batch(setup_conn, batch_id, {})
    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {"cli_reconcile_item": _flat_listing("cli_reconcile_item", "EBAY_DE")},
            "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]
    chunk_id, csv_path = _durably_write_chunk(setup_conn, output_dir, batch_id, item_results)
    setup_conn.close()

    def _forbidden(name):
        def _f(*args, **kwargs):
            raise AssertionError(f"--reconcile-only must never call {name}")
        return _f

    monkeypatch.setattr(sys, "argv", ["04_collect_targeted_active.py", "--reconcile-only"])

    # get_connection is called TWICE by the fixed run_reconcile_only (open ->
    # count -> close, then reopen -> reconcile -> close) — a fixed
    # return_value connection would be reused after being closed the first
    # time, so this side_effect returns a genuinely fresh connection (real
    # schema + migration applied, exactly like the real get_connection) on
    # every call, always redirected to this test's db_path regardless of
    # whatever db_path argument main()/run_reconcile_only passes.
    def _fake_get_connection(*_args, **_kwargs):
        fresh = duckdb.connect(str(db_path))  # always this test's db_path, never whatever was passed in
        fresh.execute(SCHEMA_PATH.read_text())
        collect04.migrate_collection_tables_schema(fresh)
        return fresh

    def _fake_ingest(**kwargs):
        ingest_conn = duckdb.connect(str(db_path))
        ingest01.TARGETED_ACTIVE_DIR = output_dir
        ingest01.insert_targeted_listings(ingest_conn)
        ingest_conn.close()

    with patch.object(collect04, "setup_logging"), \
         patch.object(collect04, "get_connection", side_effect=_fake_get_connection), \
         patch.object(collect04, "get_access_token", side_effect=_forbidden("get_access_token")), \
         patch.object(collect04, "check_quota", side_effect=_forbidden("check_quota")), \
         patch.object(collect04, "search_items_single_marketplace", side_effect=_forbidden("the Browse API")), \
         patch.object(collect04, "run_targeted_ingestion", side_effect=_fake_ingest):
        collect04.main()

    verify_conn = duckdb.connect(str(db_path))
    row = verify_conn.execute(
        "SELECT ingested_at FROM collection_chunks WHERE chunk_id = ?", [chunk_id]
    ).fetchone()
    assert row[0] is not None, "the pending durable chunk must be ingested and reconciled"

    batch_row = verify_conn.execute(
        "SELECT chunks_completed, stop_reason FROM collection_batches WHERE collection_batch_id = ?", [batch_id]
    ).fetchone()
    assert batch_row == (None, None), "--reconcile-only must never start new collection for the batch"
    verify_conn.close()


def test_reconcile_only_closes_connection_before_ingestion_no_self_lock(tmp_path):
    """
    Directly reproduces the real production bug and proves the fix: DuckDB
    refuses a second writable connection to the same file from a genuinely
    SEPARATE process while the first connection is still open
    (_duckdb.IOException: "Could not set lock on file ... Conflicting lock
    is held ..."), confirmed empirically (same-process multiple connections
    to the same file are fine — DuckDB shares one underlying instance; only
    a different OS process conflicts). This is exactly what crashed a real
    --reconcile-only run in production: main() held its own connection open
    while scripts/01_ingest.py --targeted ran as a subprocess.

    Uses two real, sequential DuckDB connections across a genuine process
    boundary: the fake "ingestion" step below launches an actual separate
    Python subprocess that tries to open the SAME database file for
    writing. If run_reconcile_only still held its own connection open at
    that point, this probe subprocess would fail with that same
    IOException (nonzero exit code); it must succeed, proving the parent
    connection was fully closed first. The probe subprocess then performs
    the real ingestion itself (via a fresh in-process connection, exactly
    like the real scripts/01_ingest.py subprocess would with its own
    connection), so pending_before -> pending_after and the chunk's
    ingested_at are also proven for real, not just assumed.
    """
    db_path = tmp_path / "reconcile_lock_test.duckdb"
    output_dir = tmp_path / "targeted_active"

    setup_conn = duckdb.connect(str(db_path))
    setup_conn.execute(SCHEMA_PATH.read_text())
    _seed_db(setup_conn)
    batch_id = "batch_lock_test"
    collect04.start_batch(setup_conn, batch_id, {})
    item_results = [{
        "inventory_uid": "iuid_pass1", "canonical_inventory_id": "rolex_1030_6941",
        "per_marketplace": {"EBAY_DE": {
            "listings": {"lock_test_item": _flat_listing("lock_test_item", "EBAY_DE")},
            "highest_tier_attempted": 1, "resolved_tier": 1, "api_calls": 1, "outcome_reason": "success",
        }},
    }]
    chunk_id, csv_path = _durably_write_chunk(setup_conn, output_dir, batch_id, item_results)
    setup_conn.close()  # nothing left open in this process before run_reconcile_only starts

    def _forbidden(name):
        def _f(*args, **kwargs):
            raise AssertionError(f"--reconcile-only must never call {name}")
        return _f

    def _fake_ingest_with_real_process_boundary_probe(**kwargs):
        probe = subprocess.run(
            [sys.executable, "-c",
             "import duckdb, sys\n"
             "c = duckdb.connect(sys.argv[1])\n"
             "c.execute('SELECT 1')\n"
             "c.close()\n",
             str(db_path)],
            capture_output=True, text=True,
        )
        assert probe.returncode == 0, (
            "a genuinely separate process could not open the database file — this process's own "
            f"connection was still held open (self-lock reproduced): {probe.stderr}"
        )
        ingest_conn = duckdb.connect(str(db_path))
        ingest01.TARGETED_ACTIVE_DIR = output_dir
        ingest01.insert_targeted_listings(ingest_conn)
        ingest_conn.close()

    with patch.object(collect04, "run_targeted_ingestion", side_effect=_fake_ingest_with_real_process_boundary_probe), \
         patch.object(collect04, "get_access_token", side_effect=_forbidden("get_access_token")), \
         patch.object(collect04, "check_quota", side_effect=_forbidden("check_quota")), \
         patch.object(collect04, "search_items_single_marketplace", side_effect=_forbidden("the Browse API")):
        summary = collect04.run_reconcile_only(db_path=db_path)

    assert summary["pending_before"] == 1
    assert summary["pending_after"] == 0
    assert summary["newly_marked_ingested"] == 1

    verify_conn = duckdb.connect(str(db_path), read_only=True)
    ingested_at = verify_conn.execute(
        "SELECT ingested_at FROM collection_chunks WHERE chunk_id = ?", [chunk_id]
    ).fetchone()[0]
    verify_conn.close()
    assert ingested_at is not None, "the chunk must be marked ingested after reconciliation"


def test_chunked_collection_closes_connection_before_ingestion_no_self_lock(tmp_path):
    """
    Directly reproduces the real production bug from the 40-item bounded
    validation collection: run_chunked_collection's own chunk-loop
    ingestion call used to hold `conn` open across the subprocess call to
    scripts/01_ingest.py --targeted, causing that subprocess to fail with
    _duckdb.IOException: "Could not set lock on file ... Conflicting lock
    is held ..." — collection completed and the chunk CSV was durably
    written, but ingestion crashed immediately after, requiring a manual
    --reconcile-only recovery step in production.

    Same technique as test_reconcile_only_closes_connection_before_ingestion_no_self_lock:
    the fake "ingestion" step launches a genuinely separate Python
    subprocess that tries to open the SAME database file for writing. If
    run_chunked_collection still held its own connection open at that
    point, this probe would fail (nonzero exit code); it must succeed,
    proving _run_ingestion_with_connection_released actually closed the
    parent connection before calling run_targeted_ingestion, not just in
    the --reconcile-only path.
    """
    db_path = tmp_path / "chunked_lock_test.duckdb"
    output_dir = tmp_path / "targeted_active"

    setup_conn = duckdb.connect(str(db_path))
    setup_conn.execute(SCHEMA_PATH.read_text())
    _seed_db(setup_conn)
    batch_id = "batch_chunked_lock_test"
    collect04.start_batch(setup_conn, batch_id, {})

    def _fake_ingest_with_real_process_boundary_probe(**kwargs):
        probe = subprocess.run(
            [sys.executable, "-c",
             "import duckdb, sys\n"
             "c = duckdb.connect(sys.argv[1])\n"
             "c.execute('SELECT 1')\n"
             "c.close()\n",
             str(db_path)],
            capture_output=True, text=True,
        )
        assert probe.returncode == 0, (
            "a genuinely separate process could not open the database file — run_chunked_collection's "
            f"own connection was still held open (self-lock reproduced): {probe.stderr}"
        )
        ingest_conn = duckdb.connect(str(db_path))
        ingest01.TARGETED_ACTIVE_DIR = output_dir
        ingest01.insert_targeted_listings(ingest_conn)
        ingest_conn.close()

    inventory_df = collect04.get_eligible_inventory(setup_conn, "iuid_pass1")
    fake_quota = {"available": True, "limit": 5000, "used": 0, "remaining": 5000, "reset": "later"}

    with patch.object(collect04, "search_items_single_marketplace", side_effect=_tier1_resolving_search), \
         patch.object(collect04, "check_quota", return_value=fake_quota), \
         patch.object(collect04, "run_targeted_ingestion", side_effect=_fake_ingest_with_real_process_boundary_probe):
        summary = collect04.run_chunked_collection(
            setup_conn, inventory_df=inventory_df, token="fake", batch_id=batch_id,
            marketplaces=["EBAY_DE", "EBAY_US"], output_dir=output_dir,
            reports_dir=tmp_path / "reports", db_path=db_path,
        )

    assert summary["stop_reason"] == "batch_fully_processed"
    assert summary["fully_processed"] is True
    summary["conn"].close()

    verify_conn = duckdb.connect(str(db_path), read_only=True)
    finished_at = verify_conn.execute(
        "SELECT finished_at FROM collection_batches WHERE collection_batch_id = ?", [batch_id]
    ).fetchone()[0]
    verify_conn.close()
    assert finished_at is not None

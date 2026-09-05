"""
Tests for scripts/15_historical_drop_ingest.py (historical drop-folder
automation). Covers discovery, source-type detection, schema validation,
idempotency (hash-based skip), routing to the correct per-source ingestion,
and rejection of unrecognized files — using a fake ingest01 so no real
ingestion or live data is touched.
"""
import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"


def _load():
    spec = importlib.util.spec_from_file_location("drop15", SCRIPTS / "15_historical_drop_ingest.py")
    m = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPTS))
    spec.loader.exec_module(m)
    return m


EBAY_COLS = ["item_number", "title", "price_eur", "currency", "sold_date_iso", "is_sold"]
VCP_COLS = ["caliber", "avg_price_eur", "total_sold", "last_sold_date"]


def _write_csv(path, cols, rows=1):
    pd.DataFrame({c: [f"v{i}" for i in range(rows)] for c in cols}).to_csv(path, index=False)


# ---- pure classification -----------------------------------------------------

def test_detect_source_type():
    m = _load()
    assert m.detect_source_type(EBAY_COLS) == "ebay_sold"
    assert m.detect_source_type(VCP_COLS) == "vcp"
    assert m.detect_source_type(["foo", "bar"]) is None


def test_validate_columns_reports_missing():
    m = _load()
    assert m.validate_columns(EBAY_COLS, "ebay_sold") == []
    missing = m.validate_columns(["item_number", "title"], "ebay_sold")
    assert "price_eur" in missing and "sold_date_iso" in missing


def test_plan_drop_folder_classifies(tmp_path):
    m = _load()
    _write_csv(tmp_path / "sold.csv", EBAY_COLS)
    _write_csv(tmp_path / "vcp.csv", VCP_COLS)
    _write_csv(tmp_path / "junk.csv", ["a", "b"])
    plan = {p["path"].name: p for p in m.plan_drop_folder(tmp_path)}
    assert plan["sold.csv"]["source_type"] == "ebay_sold" and plan["sold.csv"]["error"] is None
    assert plan["vcp.csv"]["source_type"] == "vcp"
    assert plan["junk.csv"]["source_type"] is None and plan["junk.csv"]["error"]


# ---- orchestration with a fake ingest01 --------------------------------------

class _FakeIngest01:
    def __init__(self, tmp_path, already=()):
        self.EBAY_SOLD_EXPORTS_DIR = tmp_path / "exports_ebay"
        self.HISTORICAL_EXPORTS_DIR = tmp_path / "exports_vcp"
        self.EBAY_SOLD_EXPORTS_DIR.mkdir(); self.HISTORICAL_EXPORTS_DIR.mkdir()
        self._already = set(already)
        self.ebay_calls = 0
        self.vcp_calls = 0

    def file_sha256(self, path):
        import hashlib
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()

    def successful_file_ingested(self, conn, *, source_type, source_filename, file_hash):
        return source_filename in self._already

    def insert_historical_ebay_sold_exports(self, conn):
        self.ebay_calls += 1

    def insert_historical_exports(self, conn):
        self.vcp_calls += 1


def _patch_dirs(m, tmp_path, monkeypatch):
    monkeypatch.setattr(m, "PROCESSED_DIR", tmp_path / "processed")
    monkeypatch.setattr(m, "REJECTED_DIR", tmp_path / "rejected")


def test_process_routes_and_archives(tmp_path, monkeypatch):
    m = _load(); _patch_dirs(m, tmp_path, monkeypatch)
    drop = tmp_path / "drop"; drop.mkdir()
    _write_csv(drop / "sold.csv", EBAY_COLS)
    _write_csv(drop / "vcp.csv", VCP_COLS)
    fake = _FakeIngest01(tmp_path)
    summary = m.process_drop_folder(fake, fake, drop_dir=drop)  # conn unused by fake
    assert {e["file"] for e in summary["ingested"]} == {"sold.csv", "vcp.csv"}
    assert fake.ebay_calls == 1 and fake.vcp_calls == 1
    # copied into the right per-source dirs
    assert (fake.EBAY_SOLD_EXPORTS_DIR / "sold.csv").exists()
    assert (fake.HISTORICAL_EXPORTS_DIR / "vcp.csv").exists()
    # archived out of the drop folder
    assert not (drop / "sold.csv").exists()
    assert (tmp_path / "processed" / "sold.csv").exists()


def test_process_idempotent_skip(tmp_path, monkeypatch):
    m = _load(); _patch_dirs(m, tmp_path, monkeypatch)
    drop = tmp_path / "drop"; drop.mkdir()
    _write_csv(drop / "sold.csv", EBAY_COLS)
    fake = _FakeIngest01(tmp_path, already={"sold.csv"})  # hash already ingested
    summary = m.process_drop_folder(fake, fake, drop_dir=drop)
    assert summary["ingested"] == []
    assert summary["skipped"] and summary["skipped"][0]["file"] == "sold.csv"
    assert fake.ebay_calls == 0                      # no re-ingestion
    assert (drop / "sold.csv").exists()              # left in place, not archived


def test_process_rejects_unrecognized(tmp_path, monkeypatch):
    m = _load(); _patch_dirs(m, tmp_path, monkeypatch)
    drop = tmp_path / "drop"; drop.mkdir()
    _write_csv(drop / "junk.csv", ["a", "b"])
    fake = _FakeIngest01(tmp_path)
    summary = m.process_drop_folder(fake, fake, drop_dir=drop)
    assert summary["ingested"] == [] and fake.ebay_calls == 0 and fake.vcp_calls == 0
    assert summary["rejected"] and summary["rejected"][0]["file"] == "junk.csv"
    assert (tmp_path / "rejected" / "junk.csv").exists()

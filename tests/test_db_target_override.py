"""
tests/test_db_target_override.py
================================
Phase 3 (Option A) — proves the extraction writers can be pointed at a
disposable database and never touch the default/live DB.

Covers:
  * WATCHPARTS_DB env var resolution at import (01_ingest.py, 04_collect...).
  * --db CLI arg redirects the run to a disposable DB.
  * --dry-run is read-only: it does not modify the DB it opens (this is the
    exact regression that changed the live DB's checksum).
  * the default/live DB is never touched when --db points elsewhere.
  * the 01_ingest.py ingestion subprocess inherits the same DB target.

No live eBay API call, no real credentials, no production DB write.
"""

from __future__ import annotations

import hashlib
import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

TESTS_DIR = Path(__file__).parent
BASE_DIR = TESTS_DIR.parent
SCRIPTS_DIR = BASE_DIR / "scripts"
SCHEMA_PATH = SCRIPTS_DIR / "schema.sql"
DEFAULT_LIVE_DB = BASE_DIR / "database" / "watchparts.duckdb"


def _load_module(name: str, path: Path):
    sys.path.insert(0, str(SCRIPTS_DIR))
    sys.path.insert(0, str(BASE_DIR))
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sha256(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _fresh_db(path: Path) -> None:
    """A disposable DB with the project schema applied."""
    conn = duckdb.connect(str(path))
    conn.execute(SCHEMA_PATH.read_text())
    conn.close()


# ── env-var resolution at import ────────────────────────────────────────────

def test_env_var_overrides_default_db_path_in_both_scripts(tmp_path, monkeypatch):
    target = tmp_path / "env_target.duckdb"
    monkeypatch.setenv("WATCHPARTS_DB", str(target))
    ingest01 = _load_module("ingest01_env", SCRIPTS_DIR / "01_ingest.py")
    collect04 = _load_module("collect04_env", SCRIPTS_DIR / "04_collect_targeted_active.py")
    assert ingest01.DB_PATH == target
    assert collect04.DB_PATH == target


def test_default_db_path_unchanged_without_env(monkeypatch):
    monkeypatch.delenv("WATCHPARTS_DB", raising=False)
    ingest01 = _load_module("ingest01_def", SCRIPTS_DIR / "01_ingest.py")
    collect04 = _load_module("collect04_def", SCRIPTS_DIR / "04_collect_targeted_active.py")
    assert ingest01.DB_PATH == ingest01.DEFAULT_DB_PATH == DEFAULT_LIVE_DB
    assert collect04.DB_PATH == collect04.DEFAULT_DB_PATH == DEFAULT_LIVE_DB


def test_env_var_overrides_matching_scripts(tmp_path, monkeypatch):
    """05/06 (matching rebuild) must also honour WATCHPARTS_DB so the rebuild
    can run on a disposable copy, never the live DB."""
    target = tmp_path / "match_target.duckdb"
    monkeypatch.setenv("WATCHPARTS_DB", str(target))
    gen05 = _load_module("gen05_env", SCRIPTS_DIR / "05_generate_match_candidates.py")
    dec06 = _load_module("dec06_env", SCRIPTS_DIR / "06_decide_matches.py")
    assert gen05.DB_PATH == target
    assert dec06.DB_PATH == target


def test_matching_scripts_default_unchanged_without_env(monkeypatch):
    monkeypatch.delenv("WATCHPARTS_DB", raising=False)
    gen05 = _load_module("gen05_def", SCRIPTS_DIR / "05_generate_match_candidates.py")
    dec06 = _load_module("dec06_def", SCRIPTS_DIR / "06_decide_matches.py")
    assert gen05.DB_PATH == gen05.DEFAULT_DB_PATH == DEFAULT_LIVE_DB
    assert dec06.DB_PATH == dec06.DEFAULT_DB_PATH == DEFAULT_LIVE_DB


# ── --dry-run is read-only and isolated to the target ───────────────────────

def test_dry_run_with_db_does_not_modify_target_or_default(tmp_path):
    target = tmp_path / "dry_target.duckdb"
    _fresh_db(target)
    before = _sha256(target)
    live_before = _sha256(DEFAULT_LIVE_DB) if DEFAULT_LIVE_DB.exists() else None

    r = subprocess.run(
        [sys.executable, str(SCRIPTS_DIR / "04_collect_targeted_active.py"),
         "--dry-run", "--db", str(target)],
        capture_output=True, text=True, cwd=str(BASE_DIR),
    )
    assert r.returncode == 0, r.stderr
    # read-only dry-run must not change the DB it opened...
    assert _sha256(target) == before, "dry-run mutated its target DB"
    # ...and must never touch the default/live DB
    if live_before is not None:
        assert _sha256(DEFAULT_LIVE_DB) == live_before, "dry-run touched the live DB"


def test_dry_run_connection_is_read_only(tmp_path):
    """A read-only connection must reject writes — proves dry-run cannot mutate."""
    collect04 = _load_module("collect04_ro", SCRIPTS_DIR / "04_collect_targeted_active.py")
    target = tmp_path / "ro.duckdb"
    _fresh_db(target)
    conn = collect04.get_connection(db_path=target, read_only=True)
    try:
        with pytest.raises(Exception):
            conn.execute("CREATE TABLE _should_fail (x INTEGER)")
    finally:
        conn.close()


# ── ingestion subprocess targets the override DB ────────────────────────────

def test_ingestion_subprocess_targets_override_db(tmp_path, monkeypatch):
    """run_targeted_ingestion(db_path=...) must direct 01_ingest.py at the
    override DB (via --db + WATCHPARTS_DB), never the default live DB."""
    collect04 = _load_module("collect04_sub", SCRIPTS_DIR / "04_collect_targeted_active.py")
    target = tmp_path / "sub_target.duckdb"
    captured = {}

    def fake_run(cmd, *args, **kwargs):
        captured["cmd"] = cmd
        captured["env"] = kwargs.get("env")
        class R:
            returncode = 0
            stdout = ""
            stderr = ""
        return R()

    monkeypatch.setattr(collect04.subprocess, "run", fake_run)
    collect04.run_targeted_ingestion(db_path=target)
    assert "--db" in captured["cmd"]
    assert str(target) in captured["cmd"]
    assert captured["env"]["WATCHPARTS_DB"] == str(target)


def test_default_ingestion_subprocess_unchanged(tmp_path, monkeypatch):
    """Without db_path the subprocess command is the original (no --db, no env
    override) — default behaviour preserved."""
    collect04 = _load_module("collect04_sub2", SCRIPTS_DIR / "04_collect_targeted_active.py")
    captured = {}

    def fake_run(cmd, *args, **kwargs):
        captured["cmd"] = cmd
        captured["env"] = kwargs.get("env")
        class R:
            returncode = 0
            stdout = ""
            stderr = ""
        return R()

    monkeypatch.setattr(collect04.subprocess, "run", fake_run)
    collect04.run_targeted_ingestion()
    assert "--db" not in captured["cmd"]
    assert captured["env"] is None

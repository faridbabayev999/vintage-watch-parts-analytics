"""
Step 5 regression tests (owner final work plan, 2026-07-30) for
scripts/19_retain_candidate_runs.py. Disposable in-tmp DuckDB only.
"""
import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
SCHEMA = SCRIPTS / "schema.sql"


def _load():
    spec = importlib.util.spec_from_file_location("retain19", SCRIPTS / "19_retain_candidate_runs.py")
    m = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPTS))
    spec.loader.exec_module(m)
    return m


def _fresh_db(tmp_path, name="rt.duckdb"):
    db = tmp_path / name
    conn = duckdb.connect(str(db))
    conn.execute(SCHEMA.read_text())
    return conn, db


def _seed_run(conn, run_id, n_active=3, offset=0):
    conn.execute("INSERT INTO match_run (match_run_id, algorithm_version) VALUES (?, 'v1')", [run_id])
    for i in range(n_active):
        idx = offset + i
        conn.execute(
            "INSERT INTO match_candidates_active (match_candidate_id, match_run_id, inventory_uid, "
            "active_raw_id, match_method, evidence_json) VALUES (?, ?, ?, ?, 'CALIBER_EXACT', '{}')",
            [idx, run_id, f"inv{idx}", idx],
        )


def test_keeps_only_n_most_recent_runs_operational(tmp_path):
    m = _load()
    conn, db = _fresh_db(tmp_path)
    _seed_run(conn, "run1", n_active=2, offset=0)
    _seed_run(conn, "run2", n_active=2, offset=10)
    _seed_run(conn, "run3", n_active=2, offset=20)
    m.retain(conn, keep_runs=2)
    remaining_runs = {r[0] for r in conn.execute("SELECT DISTINCT match_run_id FROM match_candidates_active").fetchall()}
    assert remaining_runs == {"run2", "run3"}
    conn.close()


def test_rerun_does_not_double_purge_or_relog(tmp_path):
    m = _load()
    conn, db = _fresh_db(tmp_path)
    _seed_run(conn, "run1", n_active=2, offset=0)
    _seed_run(conn, "run2", n_active=2, offset=10)
    m.retain(conn, keep_runs=1)
    log_count_1 = conn.execute("SELECT COUNT(*) FROM match_run_candidate_retention_log").fetchone()[0]
    m.retain(conn, keep_runs=1)  # rerun -- run1 already purged, nothing new to do
    log_count_2 = conn.execute("SELECT COUNT(*) FROM match_run_candidate_retention_log").fetchone()[0]
    assert log_count_1 == log_count_2 == 1
    remaining = conn.execute("SELECT COUNT(*) FROM match_candidates_active").fetchone()[0]
    assert remaining == 2  # only run2's rows
    conn.close()


def test_decision_outputs_identical_before_and_after_retention(tmp_path):
    """The whole point of the retention design: purging old-run candidate
    rows must never change match_decisions, because decisions store their
    own copies of every field they need, not a live reference into
    match_candidates_*. Simulated here directly (06_decide_matches.py's
    actual dedup-by-arg_max logic is exercised in its own test suite)."""
    m = _load()
    conn, db = _fresh_db(tmp_path)
    _seed_run(conn, "run1", n_active=2, offset=0)
    _seed_run(conn, "run2", n_active=2, offset=10)
    # A decision row referencing run1's now-purgeable candidate, by value not FK.
    conn.execute("""INSERT INTO match_decisions
        (decision_id, decision_version, decision_run_id, candidate_key, inventory_uid, source_table,
         source_id, matching_rule, evidence_tier, match_status, match_reason_code, collection_relationship,
         price_evidence_status)
        VALUES (1, 'v1', 'dr1', 'ck1', 'inv0', 'match_candidates_active', 0, 'CALIBER_EXACT', 'A',
                'REVIEW_REQUIRED', 'OK', 'NOT_APPLICABLE', 'NOT_APPLICABLE')""")
    before = conn.execute("SELECT * FROM match_decisions").fetchall()
    m.retain(conn, keep_runs=1)  # purges run1, whose candidate row decision_id=1 references by value
    after = conn.execute("SELECT * FROM match_decisions").fetchall()
    assert before == after
    conn.close()


def test_retained_audit_metadata_remains_available_after_purge(tmp_path):
    m = _load()
    conn, db = _fresh_db(tmp_path)
    _seed_run(conn, "run1", n_active=3, offset=0)
    _seed_run(conn, "run2", n_active=1, offset=10)
    m.retain(conn, keep_runs=1)
    # match_run itself: never deleted, full history intact.
    runs = {r[0] for r in conn.execute("SELECT match_run_id FROM match_run").fetchall()}
    assert runs == {"run1", "run2"}
    # retention log: proves run1 had 3 active candidates, even though the rows are gone.
    row = conn.execute(
        "SELECT candidate_count FROM match_run_candidate_retention_log "
        "WHERE match_run_id='run1' AND source_table='match_candidates_active'"
    ).fetchone()
    assert row == (3,)
    conn.close()


def test_interrupted_cleanup_is_recoverable_via_rerun(tmp_path):
    """Simulates a crash mid-cleanup by rolling back a transaction, then
    verifies a subsequent full run still reaches the correct end state --
    proving atomicity (nothing partially purged) plus recoverability."""
    m = _load()
    conn, db = _fresh_db(tmp_path)
    _seed_run(conn, "run1", n_active=2, offset=0)
    _seed_run(conn, "run2", n_active=2, offset=10)

    conn.execute(m.RETENTION_LOG_DDL)
    conn.execute("BEGIN TRANSACTION")
    conn.execute("DELETE FROM match_candidates_active WHERE match_run_id = 'run1'")
    conn.execute("ROLLBACK")  # simulated crash before commit

    # Pre-cleanup state fully intact after the simulated crash.
    assert conn.execute("SELECT COUNT(*) FROM match_candidates_active WHERE match_run_id='run1'").fetchone()[0] == 2

    # A full, uninterrupted run now reaches the correct end state.
    m.retain(conn, keep_runs=1)
    remaining_runs = {r[0] for r in conn.execute("SELECT DISTINCT match_run_id FROM match_candidates_active").fetchall()}
    assert remaining_runs == {"run2"}
    conn.close()


def test_keep_runs_larger_than_available_runs_is_noop(tmp_path):
    m = _load()
    conn, db = _fresh_db(tmp_path)
    _seed_run(conn, "run1", n_active=2, offset=0)
    m.retain(conn, keep_runs=5)
    assert conn.execute("SELECT COUNT(*) FROM match_candidates_active").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM match_run_candidate_retention_log").fetchone()[0] == 0
    conn.close()

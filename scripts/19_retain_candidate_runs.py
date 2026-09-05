"""
19_retain_candidate_runs.py
============================
Module 5: production-safe retention for match_candidates_active/ebay_sold/vcp.

Problem (confirmed, docs/FINAL_PRODUCTION_AUDIT_REPORT.md and this session's
own live run): scripts/05_generate_match_candidates.py never deletes a prior
run's candidate rows -- every rerun appends a complete new set under a fresh
match_run_id, so the operational candidate tables grow unbounded (live DB
went from ~51MB to ~721MB in one rerun this session).

06_decide_matches.py already dedupes correctly across runs via
arg_max(match_run_id, created_at) per (inventory_uid, source_id, method), so
decision correctness never depended on old runs' candidate rows still
existing -- match_decisions stores its own copies of every field it needs
(candidate_key, source_table, source_id, matching_rule, evidence_tier, ...),
not a live foreign key into match_candidates_*. Purging old candidate rows is
therefore safe for decision correctness by construction, not by assumption --
verified by tests/test_retain_candidate_runs.py::test_decision_outputs_
identical_before_and_after_retention.

Design (per owner's Step 5 spec):
  - Keep the N most recent full runs' candidate rows in the operational
    tables (--keep-runs, default 2: current + one prior for a quick rollback
    window).
  - Before deleting anything, write a permanent per-run, per-source summary
    row (run_id, source_table, candidate_count, min/max created_at,
    purged_at) to match_run_candidate_retention_log -- an audit trail that
    survives the purge, so "how many candidates did run X generate" remains
    answerable forever even after the raw rows are gone.
  - match_run itself (run_id, created_at, algorithm_version) is NEVER
    deleted -- full run history stays intact regardless of retention.
  - Everything in ONE transaction: summary insert + all three deletes.
    DuckDB transactions are atomic, so an interrupted process leaves the
    database in its PRE-cleanup state (nothing partially purged) -- recovery
    is simply "run it again."
  - Idempotent: a run_id already summarized (present in the retention log)
    is skipped on rerun, never re-summarized or double-counted; a rerun with
    nothing new to purge is a no-op.

Usage:
    python scripts/19_retain_candidate_runs.py --keep-runs 2 [--db PATH]
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb

BASE_DIR = Path(__file__).parent.parent
SCHEMA_PATH = BASE_DIR / "scripts" / "schema.sql"
DEFAULT_DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"

CANDIDATE_TABLES = [
    ("match_candidates_active", "active_raw_id"),
    ("match_candidates_ebay_sold", "ebay_sold_raw_id"),
    ("match_candidates_vcp", "vcp_raw_id"),
]

RETENTION_LOG_DDL = """
CREATE TABLE IF NOT EXISTS match_run_candidate_retention_log (
    retention_log_id   INTEGER PRIMARY KEY,
    match_run_id          VARCHAR NOT NULL,
    source_table             VARCHAR NOT NULL,
    candidate_count            INTEGER NOT NULL,
    min_created_at                TIMESTAMP,
    max_created_at                TIMESTAMP,
    purged_at                        TIMESTAMP DEFAULT current_timestamp,
    UNIQUE (match_run_id, source_table)
)
"""


def retain(conn: duckdb.DuckDBPyConnection, keep_runs: int) -> dict:
    conn.execute(SCHEMA_PATH.read_text())
    conn.execute(RETENTION_LOG_DDL)

    runs = conn.execute(
        "SELECT match_run_id FROM match_run ORDER BY created_at DESC"
    ).fetchall()
    run_ids = [r[0] for r in runs]
    keep = set(run_ids[:keep_runs])
    purge = [r for r in run_ids if r not in keep]

    summary = {"kept_runs": len(keep), "purge_candidate_runs": len(purge), "per_table": {}}
    if not purge:
        return summary

    conn.execute("BEGIN TRANSACTION")
    try:
        already_logged = {
            row[0] for row in conn.execute(
                "SELECT match_run_id || '|' || source_table FROM match_run_candidate_retention_log"
            ).fetchall()
        }
        for table, _ in CANDIDATE_TABLES:
            deleted_total = 0
            for run_id in purge:
                key = f"{run_id}|{table}"
                stats = conn.execute(
                    f"SELECT COUNT(*), MIN(created_at), MAX(created_at) FROM {table} WHERE match_run_id = ?",
                    [run_id],
                ).fetchone()
                n, min_c, max_c = stats
                if n == 0:
                    continue
                if key not in already_logged:
                    next_id = conn.execute(
                        "SELECT COALESCE(MAX(retention_log_id), 0) + 1 FROM match_run_candidate_retention_log"
                    ).fetchone()[0]
                    conn.execute(
                        "INSERT INTO match_run_candidate_retention_log "
                        "(retention_log_id, match_run_id, source_table, candidate_count, min_created_at, max_created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        [next_id, run_id, table, n, min_c, max_c],
                    )
                conn.execute(f"DELETE FROM {table} WHERE match_run_id = ?", [run_id])
                deleted_total += n
            summary["per_table"][table] = deleted_total
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return summary


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep-runs", type=int, default=2)
    ap.add_argument("--db", default=str(os.environ.get("WATCHPARTS_DB", DEFAULT_DB_PATH)))
    args = ap.parse_args()

    conn = duckdb.connect(args.db)
    result = retain(conn, args.keep_runs)
    conn.close()
    print(f"Kept {result['kept_runs']} most recent run(s); purged candidates from {result['purge_candidate_runs']} older run(s).")
    for table, n in result.get("per_table", {}).items():
        print(f"  {table}: {n:,} candidate rows purged (summary preserved in match_run_candidate_retention_log)")


if __name__ == "__main__":
    main()

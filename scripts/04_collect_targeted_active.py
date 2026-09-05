"""
04_collect_targeted_active.py
==============================
Module 3: automated targeted active-listing collection via eBay's Browse API.

Consumes scripts/03_generate_queries.py's output (search_queries) to fetch
current active listings for each eligible inventory item, escalating through
query tiers only as far as needed.

This script orchestrates two independently-separated operations, not one:
  1. It collects listings and writes them to a CSV (its own responsibility —
     it never writes to raw_active_targeted directly and never talks to
     eBay from within 01_ingest.py).
  2. It then launches scripts/01_ingest.py --targeted as a subprocess to
     ingest that CSV. 01_ingest.py alone owns all raw_active_targeted writes
     and idempotency; this script only triggers it, exactly as a human
     previously ran it as a second manual command — it does not perform the
     ingestion itself.
A combination (inventory item, marketplace) is only ever treated as
resumable-skip-safe once its results are durably written to an atomically-
renamed CSV file — never merely because an API call succeeded in memory.
See collection_chunks / collection_progress in schema.sql.

Does NOT: historical extraction, historical orchestration, coverage
evaluation, matching, feature engineering, TMV, turnover, or dashboard logic.

Query generation is the single source of truth for search construction —
this module only ever consumes query_text rows already produced by
03_generate_queries.py. It never constructs, modifies, normalizes, or
invents a search string itself.

Usage:
    python scripts/04_collect_targeted_active.py --dry-run
    python scripts/04_collect_targeted_active.py --limit-items 10
    python scripts/04_collect_targeted_active.py --resume
    python scripts/04_collect_targeted_active.py --inventory-uid iuid_xxx
    python scripts/04_collect_targeted_active.py --reconcile-only

--reconcile-only is the documented, safe way to ingest an already-durable,
not-yet-ingested targeted-active CSV (e.g. a pending legacy chunk) without
continuing into quota checks or new eBay collection. Plain --resume is NOT
the right tool for that: it ingests pending chunks too, but then continues
straight into collecting new chunks for the same batch. See
run_reconcile_only.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import os
import json
import logging
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))
from ebay_api_common import (  # noqa: E402
    DEFAULT_SCOPE,
    MARKETPLACE_ID_TO_COUNTRY,
    first_value,
    get_access_token,
    get_rate_limits,
    load_dotenv,
    redact_sensitive,
    search_items_single_marketplace,
    validate_query_length,
)


def _load_caliber_prefixes() -> list[str]:
    """
    Imports CALIBER_PREFIXES from 03_generate_queries.py (the query-generation
    source of truth) purely to sort already-generated Tier 2 rows in the same
    order they were created. Does not reconstruct or duplicate query text.
    03_generate_queries.py's filename starts with a digit, so it can't be
    imported with a normal `import` statement — importlib is required.
    """
    spec = importlib.util.spec_from_file_location(
        "_gen03_for_tier2_order", BASE_DIR / "scripts" / "03_generate_queries.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return list(module.CALIBER_PREFIXES)


CALIBER_PREFIXES = _load_caliber_prefixes()

# DB target resolution: --db CLI arg (applied in main(), and exported to the
# 01_ingest.py subprocess via WATCHPARTS_DB) > WATCHPARTS_DB env var (read here
# at import, so default-bound db_path parameters pick it up) > default live DB.
# Default behaviour is unchanged when neither is set.
DEFAULT_DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"
DB_PATH = Path(os.environ["WATCHPARTS_DB"]) if os.environ.get("WATCHPARTS_DB") else DEFAULT_DB_PATH
SCHEMA_PATH = BASE_DIR / "scripts" / "schema.sql"
LOG_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"
TARGETED_ACTIVE_DIR = BASE_DIR / "data" / "raw" / "targeted_active"

# ── Configurable constants — no operational value is hardcoded inline. ────────
MIN_UNIQUE_RESULTS = 5          # escalation stops once this many unique item_ids are found
MAX_PAGES_PER_QUERY = 3         # bounded pagination per single query execution
MAX_RESULTS_PER_ITEM = 50       # hard ceiling per (inventory item, marketplace)
# Applies PER CHUNK, not per whole process invocation — a single `main()`
# run can execute up to MAX_CHUNK_ITERATIONS chunks, each individually
# capped at MAX_CALLS_PER_CHUNK calls. There is deliberately no separate
# whole-invocation cap: each chunk is already bounded by live remaining
# quota (QUOTA_SAFETY_MARGIN), so an additional invocation-wide limit would
# just duplicate that same protection under a different name.
MAX_CALLS_PER_CHUNK = 500
RETRY_COUNT = 3
INITIAL_BACKOFF_SECONDS = 1.0
BACKOFF_MULTIPLIER = 2.0
PAGE_LIMIT = 50                 # eBay page size per request (<=200 per documented limit)
SORT_ORDER = "newlyListed"
SEARCH_FILTERS = ["buyingOptions:{FIXED_PRICE|AUCTION}"]

# A full-inventory run is executed as a series of checkpointed chunks, never
# as one opaque pass. Before each chunk, remaining daily quota must be at
# least QUOTA_SAFETY_MARGIN times what that chunk plans to use — e.g. 1.2
# means only up to remaining/1.2 of the quota is ever planned against,
# always keeping >=20% of the day's quota in reserve. This is a collection-
# effort safety valve, not a statement about how many calls any given item
# actually needs.
QUOTA_SAFETY_MARGIN = 1.2
# Defensive circuit breaker on chunks-per-process-invocation — not expected
# to bind in practice (each chunk consumes real quota, which is itself
# finite), but prevents an unforeseen bug from spinning indefinitely.
MAX_CHUNK_ITERATIONS = 20

# Bumped whenever the manifest's structure changes in a way orphan recovery
# depends on (e.g. attempted_combinations' fields) — lets old manifests be
# recognized as such rather than silently misparsed.
MANIFEST_SCHEMA_VERSION = 2

# Escalation runs independently per marketplace (adequate results on the
# first marketplace must not prevent searching the second) — configurable,
# opt-in beyond these two. First entry is the "primary" marketplace used for
# the alone-vs-combined comparison in the manifest.
TARGETED_MARKETPLACES = ["EBAY_DE", "EBAY_US"]


def setup_logging(log_dir: Path = LOG_DIR) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=log_dir / "04_collect_targeted_active.log",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        force=True,
    )


def log_and_print(message: str = "") -> None:
    print(message)
    logging.info(redact_sensitive(message))


def migrate_collection_tables_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Additive-only migration for a pre-existing database created before
    chunk-durability tracking existed: adds the new columns/table needed by
    the fixed progress/write ordering, without touching any existing rows.
    Safe to call on every connection — every statement is idempotent.
    """
    conn.execute("ALTER TABLE collection_progress ADD COLUMN IF NOT EXISTS chunk_id VARCHAR")
    conn.execute("ALTER TABLE collection_batches ADD COLUMN IF NOT EXISTS stop_reason VARCHAR")
    conn.execute("ALTER TABLE collection_batches ADD COLUMN IF NOT EXISTS chunks_completed INTEGER")
    conn.execute("ALTER TABLE collection_batches ADD COLUMN IF NOT EXISTS fully_processed BOOLEAN")
    conn.execute("ALTER TABLE collection_batches ADD COLUMN IF NOT EXISTS last_chunk_id VARCHAR")
    conn.execute("ALTER TABLE collection_chunks ADD COLUMN IF NOT EXISTS csv_sha256 VARCHAR")


def _sha256_file(path: Path) -> str:
    import hashlib
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _find_surviving_legacy_manifest(reports_dir: Path, batch_id: str, csv_path: Path) -> dict | None:
    """
    Scans reports_dir for any manifest JSON whose collection_batch_id
    matches — the pre-chunking design used a single fixed-path manifest
    file that got overwritten by each new run, so at most one legacy
    manifest can ever survive per batch, and usually only the most recent
    batch's manifest survives at all.

    If the manifest carries a source_csv_sha256 (schema_version >= 2), it
    is verified against csv_path's actual current content before being
    trusted — same integrity check as discover_orphan_chunk_csvs. Legacy
    manifests written before that field existed have no hash to check;
    for those, collection_batch_id match is the only verification
    available and is used as-is, since it is the best (and only) evidence
    that survives for this historical period — documented explicitly as a
    weaker guarantee than the hash-verified path, not silently equated
    with it.

    Returns the parsed manifest dict, or None if none is found / unreadable
    / hash-mismatched.
    """
    for candidate in sorted(reports_dir.glob("*.json")):
        try:
            data = json.loads(candidate.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        if data.get("collection_batch_id") != batch_id:
            continue
        recorded_hash = data.get("source_csv_sha256")
        if recorded_hash is not None:
            if recorded_hash != _sha256_file(csv_path):
                log_and_print(
                    f"  ⚠ manifest {candidate.name}'s recorded CSV hash does not match {csv_path.name} — "
                    "refusing to trust it."
                )
                continue
        else:
            log_and_print(
                f"  ⚠ manifest {candidate.name} predates hash tracking (legacy schema) — verified only by "
                "collection_batch_id match, not CSV hash. Used as the best available evidence for this "
                "historical period."
            )
        return data
    return None


def _verified_zero_result_keys_from_manifest(manifest: dict) -> set[tuple[str, str]]:
    """
    A (inventory_uid, marketplace_id) key merely APPEARING in
    call_reconciliation is not sufficient evidence of a genuine zero-result
    outcome — that only proves a query was logged for that combination, not
    that it actually returned nothing. Evidence is aggregated per key across
    ALL of that key's call_reconciliation entries and only certified when:
      - at least one entry exists for the key;
      - every entry has valid, present, non-negative integer "calls" and
        "results_returned" fields (malformed/missing fields invalidate the
        WHOLE key, never partially trusted);
      - no single entry reports results_returned > 0 for this key (a
        contradiction — the combination cannot be both "zero result" and
        have a call that actually returned something);
      - the aggregated total calls > 0 (something was genuinely attempted)
        and aggregated total results_returned == 0.
    Returns the set of keys that pass ALL of the above.
    """
    entries_by_key: dict[tuple[str, str], list[dict]] = {}
    for entry in manifest.get("call_reconciliation", []):
        uid, mp = entry.get("inventory_uid"), entry.get("marketplace_id")
        if uid is None or mp is None:
            continue
        entries_by_key.setdefault((uid, mp), []).append(entry)

    verified: set[tuple[str, str]] = set()
    for key, entries in entries_by_key.items():
        total_calls = 0
        total_results = 0
        valid = True
        for entry in entries:
            calls = entry.get("calls")
            results = entry.get("results_returned")
            if not isinstance(calls, int) or isinstance(calls, bool) or calls < 0:
                valid = False
                break
            if not isinstance(results, int) or isinstance(results, bool) or results < 0:
                valid = False
                break
            if results > 0:
                valid = False  # contradiction: a "zero-result" key with an entry that returned results
                break
            total_calls += calls
            total_results += results
        if valid and total_calls > 0 and total_results == 0:
            verified.add(key)
    return verified


def migrate_legacy_progress_to_chunks(
    conn: duckdb.DuckDBPyConnection, output_dir: Path = TARGETED_ACTIVE_DIR, reports_dir: Path = REPORTS_DIR
) -> list[dict]:
    """
    One-time backfill for collection_progress rows with chunk_id IS NULL —
    rows written before chunk-durability tracking existed. NOT called
    automatically by get_connection() or run_chunked_collection(): this is
    a deliberate, separately-invoked, one-time operation, run manually
    after reviewing its report, never silently as part of normal startup.

    Verification happens at PER-ROW granularity, not per-batch, because a
    batch's file-level evidence (CSV hash matches ingestion_log) proves the
    batch was durably collected as a whole, but does NOT by itself prove
    every individual zero-result row within it — a CSV structurally cannot
    represent a combination that returned zero listings, so that specific
    claim needs its own evidence (a surviving manifest's call_reconciliation
    showing the query was actually executed for that exact combination).
    Under the pre-fix code, batch_id was used directly (1:1) as both
    collection_progress.collection_batch_id and the CSV filename
    (targeted_active_<batch_id>.csv, with no chunk suffix), and manifests
    used one shared fixed path that got overwritten by each new run — so at
    most one legacy manifest can possibly survive.

    listings_found's semantics, proven (not assumed) by direct
    reconstruction against the real legacy data: it is the count of unique
    item_ids escalate_for_marketplace merged for THAT combo, in isolation,
    BEFORE write_batch_csv's write-time dedup. It is NOT a guarantee of
    that many surviving CSV rows. write_batch_csv dedupes (item_id,
    marketplace_id) GLOBALLY across every item in the file/chunk — when
    two different inventory items share an identical tier-4/5 query (same
    brand+caliber, different part number, e.g. "Rolex 32"), their real
    eBay results legitimately overlap, and whichever item is processed
    first (ascending inventory_uid order) claims the shared item_ids;
    a later item's listings_found can therefore correctly exceed its own
    surviving CSV row count. Confirmed directly: iuid_00ea45cd6bd84fd2
    (rolex_32_20764) recorded listings_found=50 with 0 surviving CSV rows
    in two of the three real legacy batches, because iuid_0036028630784b76
    (rolex_32_557b) — same tier-4 query "Rolex 32", processed first —
    already claimed the shared pool. Exact-count equality is therefore an
    INVALID cross-validation invariant; only presence is meaningful.

    For each distinct legacy batch_id with a matching CSV on disk (hash
    verified against the LATEST successful ingestion_log entry, deterministically
    selected — ORDER BY ingested_at DESC LIMIT 1, never an unordered
    fetchone() — if one exists; a mismatch is refused rather than trusted):
      - Rows with listings_found > 0 are backfilled if the CSV contains AT
        LEAST ONE row for that exact (inventory_uid, marketplace_id) —
        proving this combo's own identity has some durable representation,
        which is the property already_processed actually needs. NOT
        cross-validated against the exact recorded count (see above).
      - Rows with listings_found == 0 are backfilled only if
        _verified_zero_result_keys_from_manifest certifies that exact
        combination from a surviving manifest — aggregated, validated
        evidence (calls>0, results_returned==0, no contradiction, no
        malformed fields), never merely "the key appears somewhere in
        call_reconciliation." No surviving/valid evidence means the row is
        left untouched (chunk_id stays NULL), genuinely unverifiable,
        safely retryable.
      - ingested_at is copied from that ingestion_log entry's own
        ingested_at (an existing authoritative fact) when found; otherwise
        NULL.
      - csv_written_at is set to the file's own OS mtime — a real,
        verifiable fact, not fabricated.

    A batch with no matching CSV on disk at all gets NO backfill whatsoever
    and is left exactly as-is — genuinely unverifiable, never guessed at.

    Reruns ALWAYS re-examine the batch's actual current chunk_id IS NULL
    rows — never skipped merely because collection_chunks already has a
    row for this batch's synthetic chunk_id (a batch can be genuinely
    partially backfilled: some rows verified, some still unverifiable, and
    a later rerun — e.g. after a manifest becomes available — may verify
    more of them). Idempotent at the database-state level: the chunk
    record itself is inserted via record_chunk_written's conflict-detecting
    insert/no-op (same identity -> no-op; different identity -> raises),
    and already-linked progress rows are never re-selected (the query is
    scoped to chunk_id IS NULL). A batch whose progress rows are ALL
    already linked is reported as fully resolved with 0 remaining, and is
    the only case treated as a pure no-op.

    Each batch's chunk registration and all its progress-row links commit
    as one transaction (see the loop body) — a crash anywhere inside rolls
    back that whole batch's transaction cleanly; other, already-committed
    batches from earlier in this same call are unaffected.

    Returns a list of {batch_id, chunk_id, source_filename, ingested,
    rows_backfilled, rows_left_unverifiable, status} dicts.
    """
    legacy_batches = conn.execute(
        "SELECT DISTINCT collection_batch_id FROM collection_progress WHERE chunk_id IS NULL ORDER BY 1"
    ).fetchall()

    report = []
    for (batch_id,) in legacy_batches:
        chunk_id = f"{batch_id}_legacy_chunk"

        csv_path = output_dir / f"targeted_active_{batch_id}.csv"
        if not csv_path.exists():
            report.append({
                "batch_id": batch_id, "chunk_id": None, "source_filename": None,
                "ingested": None, "rows_backfilled": 0, "rows_left_unverifiable": 0,
                "status": "no matching CSV on disk — left retryable, unverifiable",
            })
            continue

        actual_hash = _sha256_file(csv_path)
        # Deterministic: the latest successful ingestion_log entry for this
        # exact file, explicitly ordered — never an unordered fetchone()
        # over however many rows (success or otherwise) happen to exist.
        ingestion_row = conn.execute(
            """
            SELECT file_hash, ingested_at FROM ingestion_log
            WHERE source_type = 'targeted_active' AND source_filename = ? AND status = 'success'
            ORDER BY ingested_at DESC LIMIT 1
            """,
            [csv_path.name],
        ).fetchone()

        ingested_at = None
        if ingestion_row is not None:
            logged_hash, logged_ingested_at = ingestion_row
            if logged_hash != actual_hash:
                report.append({
                    "batch_id": batch_id, "chunk_id": None, "source_filename": csv_path.name,
                    "ingested": None, "rows_backfilled": 0, "rows_left_unverifiable": 0,
                    "status": "on-disk file hash no longer matches ingestion_log — refusing to backfill",
                })
                continue
            ingested_at = logged_ingested_at

        # ALWAYS re-fetch the batch's actual current chunk_id IS NULL rows —
        # never skip based on whether collection_chunks already has a row
        # for this batch's chunk_id (item 8: a batch can be genuinely
        # partially backfilled, and a rerun must reconsider exactly what
        # still remains, not assume "chunk exists" means "nothing left").
        progress_rows = conn.execute(
            "SELECT inventory_uid, marketplace_id, listings_found FROM collection_progress "
            "WHERE collection_batch_id = ? AND chunk_id IS NULL",
            [batch_id],
        ).fetchall()

        if not progress_rows:
            existing_chunk = conn.execute(
                "SELECT ingested_at IS NOT NULL FROM collection_chunks WHERE chunk_id = ?", [chunk_id]
            ).fetchone()
            report.append({
                "batch_id": batch_id, "chunk_id": chunk_id, "source_filename": csv_path.name,
                "ingested": existing_chunk[0] if existing_chunk else None,
                "rows_backfilled": 0, "rows_left_unverifiable": 0,
                "status": "already_backfilled (idempotent no-op)",
            })
            continue

        df = pd.read_csv(csv_path)
        csv_groups: dict[tuple[str, str], int] = {}
        if not df.empty:
            for key, group in df.groupby(["inventory_uid", "marketplace_id"]):
                csv_groups[key] = len(group)

        manifest = _find_surviving_legacy_manifest(reports_dir, batch_id, csv_path)
        verified_zero_keys = _verified_zero_result_keys_from_manifest(manifest) if manifest is not None else set()

        mtime = datetime.fromtimestamp(csv_path.stat().st_mtime, tz=timezone.utc)

        # Everything below — the collection_chunks registration AND every
        # progress-row link for this ONE batch — commits as a single
        # transaction. If interrupted anywhere inside, DuckDB rolls the
        # whole block back: no chunk row without its links, no half-linked
        # set of rows. The batch is left exactly as it was before this
        # iteration started, safely retried in full by simply calling this
        # function again — specifically: partial writes within one batch's
        # transaction cannot survive a crash, and nothing is ever partially
        # linked across a restart. Other, already-committed batches earlier
        # in this same call are unaffected, since each batch gets its own
        # transaction, not one for the whole run.
        conn.execute("BEGIN TRANSACTION")
        try:
            record_chunk_written(
                conn, chunk_id=chunk_id, batch_id=batch_id, source_filename=csv_path.name, csv_sha256=actual_hash,
                csv_written_at=mtime, started_at=mtime, items_attempted=0, calls_made=0,
            )
            if ingested_at is not None:
                conn.execute(
                    "UPDATE collection_chunks SET ingested_at = ? WHERE chunk_id = ? AND ingested_at IS NULL",
                    [ingested_at, chunk_id],
                )

            verified_rows = 0
            unverifiable_rows = 0
            for inventory_uid, marketplace_id, listings_found in progress_rows:
                key = (inventory_uid, marketplace_id)
                if listings_found and listings_found > 0:
                    # >=1, NOT >=listings_found: proven (not assumed) that
                    # exact equality is an invalid invariant for this LEGACY
                    # data. At the time these CSVs were written, write_batch_csv
                    # deduped (item_id, marketplace_id) GLOBALLY across the
                    # whole file, with no inventory_uid in the key — this has
                    # since been fixed (write_batch_csv now keys on
                    # (inventory_uid, item_id, marketplace_id), so this
                    # collision cannot recur in newly-collected chunks), but
                    # legacy CSVs predating the fix must still be reconciled
                    # with this looser check, not a tightened one — when two
                    # different inventory items share
                    # an identical tier-4/5 query (same brand+caliber,
                    # different part number), their real eBay results
                    # legitimately overlap, and whichever item is processed
                    # first (ascending inventory_uid order) claims the
                    # shared item_ids in the CSV. A later item's
                    # listings_found can therefore correctly exceed its own
                    # surviving CSV row count — this is real collision
                    # behavior, not corruption. Confirmed by direct
                    # reconstruction: iuid_00ea45cd6bd84fd2
                    # (rolex_32_20764) and iuid_0036028630784b76
                    # (rolex_32_557b) both resolve their tier-4 query to the
                    # literal text "Rolex 32" (see search_queries), and
                    # 0036...'s earlier processing claims the shared pool,
                    # leaving 00ea...'s own CSV rows for that combo at 0
                    # despite listings_found=50. >=1 verifies "this combo's
                    # own identity has at least some durable
                    # representation," which is the property
                    # already_processed actually needs.
                    verified = csv_groups.get(key, 0) >= 1
                else:
                    verified = key in verified_zero_keys
                if not verified:
                    unverifiable_rows += 1
                    continue
                conn.execute(
                    "UPDATE collection_progress SET chunk_id = ? "
                    "WHERE collection_batch_id = ? AND inventory_uid = ? AND marketplace_id = ? AND chunk_id IS NULL",
                    [chunk_id, batch_id, inventory_uid, marketplace_id],
                )
                verified_rows += 1
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        report.append({
            "batch_id": batch_id, "chunk_id": chunk_id, "source_filename": csv_path.name,
            "ingested": ingested_at is not None, "rows_backfilled": verified_rows,
            "rows_left_unverifiable": unverifiable_rows,
            "status": "backfilled" if unverifiable_rows == 0 else "partially_backfilled (some rows left unverifiable)",
        })

    return report


def get_connection(db_path: Path = DB_PATH, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """
    Read staging_inventory/search_queries; read/write collection_batches,
    collection_chunks, and collection_progress (this module's own
    operational bookkeeping — not the raw listings table). Never touches
    raw_active_targeted; that stays 01_ingest.py's exclusive responsibility
    via the CSV hand-off.

    read_only=True opens the file read-only and skips the schema/migration
    DDL entirely — so a read-only caller (e.g. --dry-run, which only reads
    inventory to estimate calls) physically cannot modify the database. This
    is what makes a dry-run against the live DB safe: DuckDB rejects any
    write on a read-only connection.
    """
    if read_only:
        return duckdb.connect(str(db_path), read_only=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    last_exc = None
    for attempt in range(31):
        try:
            conn = duckdb.connect(str(db_path))
            break
        except duckdb.IOException as exc:
            last_exc = exc
            if "lock" not in str(exc).lower() or attempt == 30:
                raise
            time.sleep(0.5)
    else:
        raise last_exc
    conn.execute(SCHEMA_PATH.read_text())
    migrate_collection_tables_schema(conn)
    return conn


class CallBudget:
    """Tracks API calls made this run against MAX_CALLS_PER_CHUNK."""

    def __init__(self, max_calls: int):
        self.max_calls = max_calls
        self.calls_made = 0

    def increment(self) -> None:
        self.calls_made += 1

    def exhausted(self) -> bool:
        return self.calls_made >= self.max_calls

    def remaining(self) -> int:
        return max(0, self.max_calls - self.calls_made)


# ══════════════════════════════════════════════════════════════════════════════
# INVENTORY / QUERY LOOKUP
# ══════════════════════════════════════════════════════════════════════════════

def get_eligible_inventory(
    conn: duckdb.DuckDBPyConnection,
    inventory_uid_filter: str | None = None,
    limit_items: int | None = None,
) -> pd.DataFrame:
    query = """
        SELECT canonical_inventory_id, inventory_uid, brand, caliber, part_number, stock
        FROM staging_inventory
        WHERE validation_status <> 'FAIL'
    """
    params: list[str] = []
    if inventory_uid_filter:
        query += " AND inventory_uid = ?"
        params.append(inventory_uid_filter)
    query += " ORDER BY inventory_uid"
    df = conn.execute(query, params).df()
    if limit_items is not None:
        df = df.head(limit_items)
    return df.reset_index(drop=True)


def get_inventory_from_manifest(conn: duckdb.DuckDBPyConnection, manifest_path: Path | str) -> pd.DataFrame:
    """
    Load an exact, ordered set of inventory_uid values from a CSV manifest
    (see reports/validation/targeted_active_validation_manifest_40.csv for
    the canonical example) and resolve them against staging_inventory —
    the manifest-based alternative to get_eligible_inventory's
    filter/limit selection, for reproducible bounded validation runs where
    "the next N rows in database order" would be an unstratified, biased
    sample.

    Guarantees, all enforced before any query touches eBay:
      - manifest order is preserved in the returned DataFrame. If the
        manifest has a `sample_position` column, rows are sorted by it
        first (the documented deterministic order for manifests produced
        with an explicit position, robust to the file being re-saved or
        re-sorted by a spreadsheet tool); otherwise raw CSV row order is
        used;
      - duplicate inventory_uid values raise ValueError, listing every
        duplicate found — never silently deduplicated;
      - any inventory_uid absent from staging_inventory, or present but
        validation_status = 'FAIL' (never eligible for collection,
        matching get_eligible_inventory's own WHERE clause), raises
        ValueError listing every such UID — never silently dropped or
        substituted with a different item.
    """
    manifest_path = Path(manifest_path)
    if not manifest_path.exists():
        raise ValueError(f"Inventory manifest not found: {manifest_path}")

    manifest_df = pd.read_csv(manifest_path, dtype=str)
    if "inventory_uid" not in manifest_df.columns:
        raise ValueError(f"Inventory manifest {manifest_path} has no 'inventory_uid' column.")

    if "sample_position" in manifest_df.columns:
        manifest_df = manifest_df.copy()
        manifest_df["sample_position"] = manifest_df["sample_position"].astype(int)
        manifest_df = manifest_df.sort_values("sample_position").reset_index(drop=True)

    uids = manifest_df["inventory_uid"].tolist()

    seen: set[str] = set()
    duplicates: list[str] = []
    for uid in uids:
        if uid in seen:
            duplicates.append(uid)
        seen.add(uid)
    if duplicates:
        raise ValueError(
            f"Inventory manifest {manifest_path} contains duplicate inventory_uid value(s): "
            f"{sorted(set(duplicates))}"
        )

    known = conn.execute(
        "SELECT canonical_inventory_id, inventory_uid, brand, caliber, part_number, stock, validation_status "
        "FROM staging_inventory"
    ).df().set_index("inventory_uid", drop=False)

    missing = [uid for uid in uids if uid not in known.index]
    if missing:
        raise ValueError(
            f"Inventory manifest {manifest_path} references inventory_uid value(s) not found "
            f"in staging_inventory: {missing}"
        )

    ineligible = [uid for uid in uids if known.loc[uid, "validation_status"] == "FAIL"]
    if ineligible:
        raise ValueError(
            f"Inventory manifest {manifest_path} references inventory_uid value(s) with "
            f"validation_status = FAIL, never eligible for collection: {ineligible}"
        )

    ordered = known.loc[uids, ["canonical_inventory_id", "inventory_uid", "brand", "caliber", "part_number", "stock"]]
    return ordered.reset_index(drop=True)


def _tier2_prefix_rank(query_text: str) -> int:
    """
    search_queries has no sequence column (its PK is inventory_uid, tier,
    query_text), so alphabetical ORDER BY would silently reorder Tier 2
    variants relative to how 03_generate_queries.py generated them (e.g. it
    swaps "Caliber" and "Calibre" since "Caliber" < "Calibre" alphabetically
    but CALIBER_PREFIXES lists Calibre before Caliber). This reconstructs the
    generator's intended order from the existing row text, without
    constructing or modifying any query.
    """
    for rank, prefix in enumerate(CALIBER_PREFIXES):
        if f" {prefix} " in query_text:
            return rank
    return len(CALIBER_PREFIXES)


def get_queries_for_item(conn: duckdb.DuckDBPyConnection, inventory_uid: str) -> list[tuple[int, str]]:
    """Query generation is the single source of truth — this only ever reads
    rows 03_generate_queries.py already produced, ordered ascending by tier.
    Within Tier 2, rows are ordered to match CALIBER_PREFIXES generation
    order rather than alphabetically (see _tier2_prefix_rank)."""
    rows = conn.execute(
        "SELECT tier, query_text FROM search_queries WHERE inventory_uid = ? ORDER BY tier, query_text",
        [inventory_uid],
    ).fetchall()
    pairs = [(int(tier), text) for tier, text in rows]
    pairs.sort(key=lambda pair: (pair[0], _tier2_prefix_rank(pair[1]) if pair[0] == 2 else 0))
    return pairs


# ══════════════════════════════════════════════════════════════════════════════
# BATCH LIFECYCLE / RESUMABILITY
# ══════════════════════════════════════════════════════════════════════════════

def find_resumable_batch(conn: duckdb.DuckDBPyConnection) -> str | None:
    row = conn.execute(
        "SELECT collection_batch_id FROM collection_batches WHERE finished_at IS NULL "
        "ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def start_batch(
    conn: duckdb.DuckDBPyConnection,
    batch_id: str,
    config_snapshot: dict,
    expected_pairs: list[tuple[str, str]] | None = None,
) -> None:
    """
    expected_pairs — the exact (inventory_uid, marketplace_id) set this
    batch is responsible for, captured once here so a LATER, separate
    invocation (--reconcile-only, or a --resume long after) can correctly
    tell whether collection was ever complete, without needing to
    re-derive the original inventory_df/manifest selection (which is not
    otherwise persisted). Optional and defaults to None for callers that
    don't have it yet — reconcile_batch_state() never guesses completeness
    for a batch where this is NULL, it just leaves that batch's recorded
    state untouched rather than risk a false SUCCESS or a false FAILED.
    """
    conn.execute(
        "INSERT INTO collection_batches (collection_batch_id, started_at, config_snapshot, status, expected_pairs_json) "
        "VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
        [
            batch_id, datetime.now(timezone.utc), json.dumps(config_snapshot), "INCOMPLETE",
            json.dumps(sorted(set(expected_pairs))) if expected_pairs is not None else None,
        ],
    )


def finish_batch(conn: duckdb.DuckDBPyConnection, batch_id: str) -> None:
    """Bare completion marker — kept for direct/manual use. The richer,
    always-called version is record_batch_stop_state, which also persists
    why/where a run stopped even when it did NOT finish."""
    conn.execute(
        "UPDATE collection_batches SET finished_at = ? WHERE collection_batch_id = ?",
        [datetime.now(timezone.utc), batch_id],
    )


def record_batch_stop_state(
    conn: duckdb.DuckDBPyConnection,
    batch_id: str,
    *,
    stop_reason: str,
    chunks_completed: int,
    fully_processed: bool,
    last_chunk_id: str | None,
) -> None:
    """
    Persists a durable record of why/where the most recent invocation of
    this batch stopped — called at every stop point, not only on successful
    completion, so auditability never depends on ephemeral log output.
    Sets finished_at only when fully_processed is True.

    status is set to SUCCESS or INCOMPLETE here — never FAILED. Every
    stop_reason this function is ever called with from this module
    (ingestion_failed, quota_safety_margin_exhausted,
    max_chunk_iterations_reached, no_progress_possible) is retryable via
    --resume or --reconcile-only, not a terminal failure — labeling any of
    them FAILED would be exactly the false-failure reporting this status
    column exists to avoid. FAILED is reserved for a genuinely
    non-retryable state, which no code path here currently produces.
    """
    status = "SUCCESS" if fully_processed else "INCOMPLETE"
    conn.execute(
        """
        UPDATE collection_batches
        SET stop_reason = ?, chunks_completed = ?, fully_processed = ?, last_chunk_id = ?, status = ?,
            finished_at = CASE WHEN ? THEN current_timestamp ELSE finished_at END
        WHERE collection_batch_id = ?
        """,
        [stop_reason, chunks_completed, fully_processed, last_chunk_id, status, fully_processed, batch_id],
    )


def reconcile_batch_state(conn: duckdb.DuckDBPyConnection, batch_id: str | None = None) -> list[dict]:
    """
    Re-evaluates every unfinished batch (finished_at IS NULL) — or just
    `batch_id` if given — against the CURRENT database state, and corrects
    any stale bookkeeping left over from an earlier stop.

    This exists because record_batch_stop_state only ever runs INSIDE the
    same run_chunked_collection invocation that stopped — if that stop was
    stop_reason='ingestion_failed' and a LATER, separate invocation
    (--reconcile-only, or a plain retry) fixes the ingestion without ever
    calling run_chunked_collection again for that batch, the batch row was
    previously left permanently reading 'ingestion_failed'/fully_processed
    False forever, even once every combination was genuinely collected
    AND ingested. That is a false failure reading — this function is the
    fix, and is safe to call as often as wanted (idempotent: a batch
    already SUCCESS or already correctly INCOMPLETE is left unchanged).

    Never invents completeness: for a batch with expected_pairs_json NULL
    (created before that column existed, or otherwise never given
    expected_pairs at start_batch time), this function makes NO changes —
    it does not know the original expected set and must not guess either
    SUCCESS or FAILED for it. Returns a list of {"batch_id", "action"} for
    every batch actually reconsidered, for logging/tests.
    """
    where = "WHERE finished_at IS NULL"
    params: list[str] = []
    if batch_id is not None:
        where += " AND collection_batch_id = ?"
        params.append(batch_id)

    rows = conn.execute(
        f"SELECT collection_batch_id, expected_pairs_json, stop_reason FROM collection_batches {where}",
        params,
    ).fetchall()

    changes: list[dict] = []
    for bid, expected_pairs_json, current_stop_reason in rows:
        if expected_pairs_json is None:
            changes.append({"batch_id": bid, "action": "skipped_no_expected_pairs_recorded"})
            continue

        expected_pairs = {tuple(pair) for pair in json.loads(expected_pairs_json)}
        done_pairs = {
            (row[0], row[1])
            for row in conn.execute(
                "SELECT DISTINCT inventory_uid, marketplace_id FROM collection_progress "
                "WHERE collection_batch_id = ?",
                [bid],
            ).fetchall()
        }
        collection_complete = expected_pairs.issubset(done_pairs)
        ingestion_complete = len(get_pending_ingestion_chunks(conn, bid)) == 0

        if collection_complete and ingestion_complete:
            conn.execute(
                """
                UPDATE collection_batches
                SET status = 'SUCCESS', fully_processed = TRUE, stop_reason = 'reconciled_success',
                    finished_at = current_timestamp
                WHERE collection_batch_id = ?
                """,
                [bid],
            )
            changes.append({"batch_id": bid, "action": "marked_success"})
        elif current_stop_reason == "ingestion_failed" and ingestion_complete:
            # The ONE thing that was blocking this batch (ingestion) is now
            # fixed, but collection itself genuinely isn't done — correct
            # the misleading "ingestion_failed" label (which reads like a
            # permanent failure) to an honest, still-INCOMPLETE reason.
            conn.execute(
                """
                UPDATE collection_batches
                SET status = 'INCOMPLETE', stop_reason = 'incomplete_pending_more_collection'
                WHERE collection_batch_id = ?
                """,
                [bid],
            )
            changes.append({"batch_id": bid, "action": "corrected_misleading_ingestion_failed_label"})
        # Otherwise: state already accurately reflects reality — nothing to change.

    return changes


class ChunkIntegrityError(RuntimeError):
    """Raised by record_chunk_written when a chunk_id already exists with a
    conflicting identity (different batch_id, source_filename, or
    csv_sha256) — never silently overwritten, never silently ignored."""


def record_chunk_written(
    conn: duckdb.DuckDBPyConnection,
    *,
    chunk_id: str,
    batch_id: str,
    source_filename: str,
    csv_sha256: str,
    started_at: datetime,
    items_attempted: int,
    calls_made: int,
    csv_written_at: datetime | None = None,
) -> None:
    """
    Inserts the collection_chunks row ONLY after this chunk's CSV has been
    atomically renamed to its final path — there is no earlier "started"
    row, since nothing should exist for a chunk whose CSV never finished
    writing. csv_written_at defaults to now (the normal, live-collection
    case); callers reconstructing a chunk from a real historical file (e.g.
    migrate_legacy_progress_to_chunks) may pass the file's own OS mtime
    instead, so a real, verifiable fact is recorded rather than the
    migration's run time. ingested_at stays NULL until
    reconcile_chunk_ingestion_state confirms it via ingestion_log. csv_sha256 is recorded so
    already_processed can re-verify the file's integrity later — file
    existence alone is not sufficient once a chunk is un-ingested.

    Conflict-detecting, not a silent ON CONFLICT DO NOTHING: if chunk_id
    doesn't exist yet, inserts it. If it already exists with an IDENTICAL
    (collection_batch_id, source_filename, csv_sha256), this is an
    idempotent no-op (safe to call again for the same real chunk). If it
    already exists with ANY of those three different, raises
    ChunkIntegrityError rather than silently keeping the old row or
    silently overwriting it — a chunk_id collision with different
    identity is a bug (e.g. chunk_id reuse) that must never pass silently.
    """
    existing = conn.execute(
        "SELECT collection_batch_id, source_filename, csv_sha256 FROM collection_chunks WHERE chunk_id = ?",
        [chunk_id],
    ).fetchone()

    if existing is None:
        written_at = csv_written_at if csv_written_at is not None else datetime.now(timezone.utc)
        conn.execute(
            """
            INSERT INTO collection_chunks
                (chunk_id, collection_batch_id, source_filename, csv_sha256, started_at, csv_written_at,
                 items_attempted, calls_made)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [chunk_id, batch_id, source_filename, csv_sha256, started_at, written_at, items_attempted, calls_made],
        )
        return

    existing_batch_id, existing_filename, existing_hash = existing
    if existing_batch_id == batch_id and existing_filename == source_filename and existing_hash == csv_sha256:
        return  # idempotent no-op — an identical chunk record already exists

    raise ChunkIntegrityError(
        f"chunk_id {chunk_id!r} already exists with a conflicting identity: "
        f"existing=(batch_id={existing_batch_id!r}, filename={existing_filename!r}, hash={existing_hash!r}) "
        f"vs new=(batch_id={batch_id!r}, filename={source_filename!r}, hash={csv_sha256!r})"
    )


def reconcile_chunk_ingestion_state(conn: duckdb.DuckDBPyConnection) -> int:
    """
    ingestion_log (owned by 01_ingest.py) is the single source of truth for
    whether a file was actually ingested. This mirrors that fact onto
    collection_chunks.ingested_at for convenient reporting/resumability
    checks — it never invents ingestion state, only copies it.

    Hash-aware and deterministic: a chunk is only marked ingested when an
    authoritative ingestion_log row matches ALL of source_type =
    'targeted_active', source_filename = collection_chunks.source_filename,
    file_hash = collection_chunks.csv_sha256, and status = 'success' — a
    filename match alone is never sufficient, since a same-named file could
    be a different, unrelated, or corrupted file. If csv_sha256 is NULL
    (legacy row predating hash tracking), no ingestion_log row can ever
    match by construction, so the chunk correctly stays un-ingested here.
    When more than one successful ingestion_log row matches (should not
    normally happen, but the table's PK allows different file_hash values
    for the same filename), the latest ingested_at is selected
    deterministically, and that authoritative ingested_at value is copied
    onto collection_chunks — never current_timestamp — so the recorded
    time reflects when ingestion actually happened, not when this
    reconciliation happened to run.

    Naturally idempotent: re-running this after ingestion already succeeded
    is a no-op. Returns the number of chunks newly marked ingested.
    """
    candidates = conn.execute(
        """
        SELECT
            cc.chunk_id,
            (
                SELECT il.ingested_at
                FROM ingestion_log il
                WHERE il.source_type = 'targeted_active'
                  AND il.source_filename = cc.source_filename
                  AND il.file_hash = cc.csv_sha256
                  AND il.status = 'success'
                ORDER BY il.ingested_at DESC
                LIMIT 1
            ) AS authoritative_ingested_at
        FROM collection_chunks cc
        WHERE cc.ingested_at IS NULL
        """
    ).fetchall()

    newly_marked = 0
    for chunk_id, authoritative_ingested_at in candidates:
        if authoritative_ingested_at is None:
            continue
        conn.execute(
            "UPDATE collection_chunks SET ingested_at = ? WHERE chunk_id = ?",
            [authoritative_ingested_at, chunk_id],
        )
        newly_marked += 1
    return newly_marked


def get_pending_ingestion_chunks(conn: duckdb.DuckDBPyConnection, batch_id: str) -> list[tuple[str, str]]:
    """Chunks whose CSV is durably written but not yet confirmed ingested —
    the safe, retryable state described in the module docstring. Returns
    (chunk_id, source_filename) pairs."""
    return conn.execute(
        "SELECT chunk_id, source_filename FROM collection_chunks "
        "WHERE collection_batch_id = ? AND csv_written_at IS NOT NULL AND ingested_at IS NULL",
        [batch_id],
    ).fetchall()


def _validate_orphan_manifest(
    manifest_data: dict, *, batch_id: str, chunk_id: str, source_filename: str, actual_hash: str
) -> list[dict] | None:
    """
    Validates a manifest as ONE coherent object before its
    attempted_combinations are trusted for zero-result recovery — never
    partially trusted. Requires ALL of:
      - schema_version is the currently supported MANIFEST_SCHEMA_VERSION;
      - collection_batch_id equals the requested batch_id;
      - chunk_id equals the filename-derived chunk_id;
      - source_filename equals the current CSV's actual filename;
      - source_csv_sha256 equals the current CSV's actual hash;
      - attempted_combinations is a list of dicts, each with a valid
        non-empty string inventory_uid/marketplace_id and a valid
        non-negative int listings_found;
      - no duplicate (inventory_uid, marketplace_id) key appears twice with
        different listings_found values (a contradiction).
    A single failure invalidates the WHOLE manifest for this purpose — CSV-
    backed non-zero combinations can still be reconstructed independently
    from the CSV itself by the caller; only the manifest-derived zero-
    result recovery path is disabled. Returns the validated
    attempted_combinations list, or None if any check fails.
    """
    if manifest_data.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        log_and_print(
            f"  ⚠ manifest schema_version {manifest_data.get('schema_version')!r} is not supported "
            f"(expected {MANIFEST_SCHEMA_VERSION}) — treating manifest as absent."
        )
        return None
    if manifest_data.get("collection_batch_id") != batch_id:
        log_and_print(
            f"  ⚠ manifest collection_batch_id {manifest_data.get('collection_batch_id')!r} does not match "
            f"{batch_id!r} — treating manifest as absent."
        )
        return None
    if manifest_data.get("chunk_id") != chunk_id:
        log_and_print(
            f"  ⚠ manifest chunk_id {manifest_data.get('chunk_id')!r} does not match {chunk_id!r} — "
            "treating manifest as absent."
        )
        return None
    if manifest_data.get("source_filename") != source_filename:
        log_and_print(
            f"  ⚠ manifest source_filename {manifest_data.get('source_filename')!r} does not match "
            f"{source_filename!r} — treating manifest as absent."
        )
        return None
    if manifest_data.get("source_csv_sha256") != actual_hash:
        log_and_print(
            f"  ⚠ manifest source_csv_sha256 does not match the actual CSV's current hash — "
            "treating manifest as absent."
        )
        return None

    combos = manifest_data.get("attempted_combinations")
    if not isinstance(combos, list):
        log_and_print("  ⚠ manifest attempted_combinations is not a list — treating manifest as absent.")
        return None

    seen: dict[tuple[str, str], int] = {}
    for entry in combos:
        if not isinstance(entry, dict):
            log_and_print("  ⚠ manifest attempted_combinations contains a non-dict entry — treating manifest as absent.")
            return None
        uid, mp = entry.get("inventory_uid"), entry.get("marketplace_id")
        listings_found = entry.get("listings_found")
        if not isinstance(uid, str) or not uid or not isinstance(mp, str) or not mp:
            log_and_print(
                "  ⚠ manifest attempted_combinations entry is missing a valid inventory_uid/marketplace_id — "
                "treating manifest as absent."
            )
            return None
        if not isinstance(listings_found, int) or isinstance(listings_found, bool) or listings_found < 0:
            log_and_print(
                f"  ⚠ manifest attempted_combinations entry for ({uid}, {mp}) has an invalid listings_found — "
                "treating manifest as absent."
            )
            return None
        key = (uid, mp)
        if key in seen and seen[key] != listings_found:
            log_and_print(
                f"  ⚠ manifest attempted_combinations has contradictory duplicate entries for ({uid}, {mp}) — "
                "treating manifest as absent."
            )
            return None
        seen[key] = listings_found

    return combos


def discover_orphan_chunk_csvs(
    conn: duckdb.DuckDBPyConnection,
    batch_id: str,
    output_dir: Path = TARGETED_ACTIVE_DIR,
    reports_dir: Path = REPORTS_DIR,
) -> int:
    """
    Recovers from the one remaining gap in the write-then-checkpoint
    ordering: a chunk's CSV can be atomically renamed to its final path,
    and then the process can crash before record_chunk_written/
    record_chunk_progress ever run. Never ingested by mistake either: the
    glob only matches finished "targeted_active_<chunk_id>.csv" names,
    never the "."-prefixed ".<name>.<random>.tmp" temp files
    write_batch_csv uses before its os.replace() — those don't match this
    pattern at all, by construction.

    A CSV file structurally CANNOT represent a combination that returned
    zero listings — zero listings means zero rows, so nothing in the file
    distinguishes "never attempted" from "attempted, found nothing." The
    only durable record of that fact is the chunk's own manifest
    (attempted_combinations, written unconditionally for every combination
    regardless of result count — see write_manifest). This function
    therefore only ever reconstructs a zero-result combination when that
    manifest survives AND passes full validation as one coherent object
    (see _validate_orphan_manifest: supported schema_version, matching
    batch_id/chunk_id/source_filename/CSV hash, attempted_combinations
    structurally valid with no contradictory duplicates) — a manifest that
    fails any single check is treated as entirely absent, never partially
    trusted. It never infers a zero-result outcome from CSV absence alone.
    Four cases, handled distinctly:

      A. CSV and manifest both exist — full reconstruction. Every
         combination in attempted_combinations is backfilled: non-zero
         ones are cross-validated against the CSV's own row count for that
         combination (the manifest's claim alone is never trusted for data
         that would need to be re-ingested); zero-result ones are trusted
         directly from the manifest, since there is no data at stake to
         get wrong.
      B. CSV exists, manifest missing — only combinations with actual CSV
         rows can be reconstructed (via groupby, as before). Zero-result
         combinations have no evidence anywhere and are deliberately left
         unreconstructed — they will be retried on resume, which is safe
         since there is nothing to lose by re-attempting them.
      C. Manifest exists, CSV missing — NOT reconciled by this function at
         all. A manifest's claim that a combination returned listings is
         not sufficient on its own: the actual listing rows needed for
         ingestion are gone once the CSV is gone, so there is nothing safe
         to reconstruct for non-zero combinations. This chunk is
         intentionally left with no collection_chunks row; its
         combinations simply have no progress row and are retried on
         resume like any other never-attempted combination. (This branch
         is unreachable via this function's glob, which only iterates CSV
         files — a manifest-only chunk is never visited here at all.)
      D. Zero-result combinations within an otherwise-recoverable chunk —
         handled per-combination inside case A/B above, never as a whole-
         chunk decision.

    Backfills strictly from file contents, never from timestamps or
    filename guessing. outcome_reason is suffixed to make reconstructed
    rows always auditable as such, never confused with an originally-
    recorded outcome.

    Returns the number of orphan files discovered and reconciled.
    """
    if not output_dir.exists():
        return 0

    known_filenames = set(
        row[0] for row in conn.execute(
            "SELECT source_filename FROM collection_chunks WHERE collection_batch_id = ?", [batch_id]
        ).fetchall()
    )

    reconciled = 0
    for csv_path in sorted(output_dir.glob(f"targeted_active_{batch_id}_chunk_*.csv")):
        if csv_path.name in known_filenames:
            continue

        chunk_id = csv_path.stem.removeprefix("targeted_active_")
        df = pd.read_csv(csv_path)
        csv_groups: dict[tuple[str, str], object] = {}
        if not df.empty:
            for key, group in df.groupby(["inventory_uid", "marketplace_id"]):
                csv_groups[key] = group
        actual_hash = _sha256_file(csv_path)

        manifest_path = reports_dir / f"targeted_collection_manifest_{chunk_id}.json"
        manifest_combos: list[dict] | None = None
        if manifest_path.exists():
            try:
                manifest_data = json.loads(manifest_path.read_text())
            except (json.JSONDecodeError, OSError) as exc:
                log_and_print(f"  ⚠ manifest {manifest_path.name} for orphan chunk {chunk_id} is unreadable ({exc}) — treating as absent.")
                manifest_data = None
            if manifest_data is not None:
                # The manifest is validated as ONE coherent object — schema
                # version, batch/chunk/filename identity, CSV hash, and the
                # internal structure of attempted_combinations — before any
                # of it is trusted for zero-result recovery. A single
                # failure disables the whole manifest for this purpose;
                # CSV-backed non-zero combinations are still reconstructed
                # independently from the CSV in the Case B branch below.
                manifest_combos = _validate_orphan_manifest(
                    manifest_data, batch_id=batch_id, chunk_id=chunk_id,
                    source_filename=csv_path.name, actual_hash=actual_hash,
                )

        log_and_print(
            f"  ⚠ found orphan chunk CSV {csv_path.name} with no collection_chunks record "
            f"(a crash between the atomic rename and recording chunk state) — "
            f"reconciling from its contents{' and its manifest' if manifest_combos is not None else ' (no manifest found)'}."
        )

        # The chunk registration AND every combination it reconstructs
        # commit as one transaction — a crash anywhere inside rolls the
        # whole orphan chunk back: no chunk row without its progress rows,
        # no half-reconstructed set of combinations. A subsequent call to
        # discover_orphan_chunk_csvs simply re-discovers the same CSV as an
        # orphan (nothing was left registered) and reconciles it fully.
        conn.execute("BEGIN TRANSACTION")
        try:
            record_chunk_written(
                conn, chunk_id=chunk_id, batch_id=batch_id, source_filename=csv_path.name, csv_sha256=actual_hash,
                started_at=datetime.now(timezone.utc),
                items_attempted=int(df["inventory_uid"].nunique()) if not df.empty else 0,
                calls_made=0,  # genuinely unrecoverable from the CSV alone — see docstring
            )

            if manifest_combos is not None:
                # Case A: full manifest available — reconstruct every attempted
                # combination, cross-validating non-zero claims against the CSV.
                for combo in manifest_combos:
                    key = (combo["inventory_uid"], combo["marketplace_id"])
                    listings_found = combo["listings_found"]
                    if listings_found > 0:
                        # >=1, not >=listings_found — see migrate_legacy_progress_to_chunks'
                        # matching comment: write_batch_csv dedupes (item_id,
                        # marketplace_id) GLOBALLY across every item in the
                        # chunk, so two items sharing an identical tier-4/5
                        # query (same brand+caliber) can legitimately overlap,
                        # and the item processed later ends up with fewer
                        # surviving CSV rows than its own listings_found —
                        # proven, not assumed, by direct reconstruction against
                        # real collected data.
                        group = csv_groups.get(key)
                        if group is None or len(group) < 1:
                            log_and_print(
                                f"  ⚠ manifest claims {listings_found} listing(s) for {key} but the CSV has "
                                f"{0 if group is None else len(group)} matching row(s) — not trusting the manifest "
                                "claim alone; leaving retryable."
                            )
                            continue
                    record_progress(
                        conn, batch_id=batch_id, chunk_id=chunk_id,
                        inventory_uid=combo["inventory_uid"], marketplace_id=combo["marketplace_id"],
                        highest_tier_attempted=combo.get("highest_tier_attempted"),
                        resolved_tier=combo.get("resolved_tier"),
                        api_calls=combo.get("api_calls"),
                        listings_found=listings_found,
                        outcome_reason=f"reconciled_from_orphan_manifest:{combo.get('outcome_reason')}",
                    )
            else:
                # Case B: no manifest — only CSV-backed (non-zero) combinations
                # can be reconstructed. Zero-result combinations are left
                # unreconstructed and will be retried.
                for (inventory_uid, marketplace_id), group in csv_groups.items():
                    max_tier = int(group["query_tier"].max())
                    listings_found = len(group)
                    resolved = listings_found >= MIN_UNIQUE_RESULTS
                    record_progress(
                        conn, batch_id=batch_id, chunk_id=chunk_id,
                        inventory_uid=inventory_uid, marketplace_id=marketplace_id,
                        highest_tier_attempted=max_tier,
                        resolved_tier=max_tier if resolved else None,
                        api_calls=None,
                        listings_found=listings_found,
                        outcome_reason="reconciled_from_orphan_csv",
                    )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        reconciled += 1

    return reconciled


def already_processed(
    conn: duckdb.DuckDBPyConnection,
    batch_id: str,
    inventory_uid: str,
    marketplace_id: str,
    output_dir: Path = TARGETED_ACTIVE_DIR,
) -> bool:
    """
    A combination is only skip-safe if its durability can be proven — never
    trust a bare progress row. Four distinct cases, each handled on its own
    terms rather than one generic fallback:

      (no row at all)  -> never processed. Retry.
      chunk_id IS NULL  -> legacy row predating chunk-durability tracking
                           (see the migration audit). Never auto-trusted,
                           regardless of anything else. Retry.
      chunk_id set but
      no matching
      collection_chunks
      row / csv_written_at
      IS NULL           -> broken/incomplete chunk reference (e.g. the
                           process crashed after the atomic CSV rename but
                           before collection_chunks was written — see
                           discover_orphan_chunk_csvs for how that specific
                           gap gets reconciled at startup instead of here).
                           Never auto-trusted. Retry.
      ingested_at IS NOT NULL
                        -> authoritatively ingested. ingestion_log (via
                           reconcile_chunk_ingestion_state) is the single
                           source of truth here, and raw_active_targeted
                           itself is now the durable store — the CSV file
                           may legitimately be archived or deleted after a
                           successful ingest without affecting safety. Skip
                           unconditionally; do NOT check file existence.
      ingested_at IS NULL
                        -> not yet confirmed ingested. The CSV is the only
                           durable copy that exists, so file existence
                           ALONE is not sufficient — a same-named file could
                           be a different, unrelated, or corrupted file.
                           Verified durable backing requires ALL of:
                             (a) the file exists,
                             (b) its current sha256 matches csv_sha256
                                 recorded in collection_chunks at write
                                 time, and
                             (c) chunk_id/filename are linked consistently
                                 (already guaranteed by the join above).
                           Skip only if all three hold; retry otherwise. A
                           missing recorded csv_sha256 fails requirement
                           (b) outright — there is no hashless fallback.
    """
    row = conn.execute(
        """
        SELECT p.chunk_id, c.source_filename, c.csv_written_at, c.ingested_at, c.csv_sha256
        FROM collection_progress p
        LEFT JOIN collection_chunks c ON p.chunk_id = c.chunk_id
        WHERE p.collection_batch_id = ? AND p.inventory_uid = ? AND p.marketplace_id = ?
        """,
        [batch_id, inventory_uid, marketplace_id],
    ).fetchone()
    if row is None:
        return False

    chunk_id, source_filename, csv_written_at, ingested_at, recorded_hash = row

    if chunk_id is None:
        log_and_print(
            f"  ⚠ legacy progress row for ({inventory_uid}, {marketplace_id}) has no chunk_id — "
            "predates durability tracking, never auto-trusted. Not treating as already-processed."
        )
        return False

    if csv_written_at is None:
        log_and_print(
            f"  ⚠ progress row for ({inventory_uid}, {marketplace_id}) references chunk {chunk_id!r} "
            "with no durable collection_chunks record — not trusting it as already-processed."
        )
        return False

    if ingested_at is not None:
        # Authoritatively ingested — raw_active_targeted is now the durable
        # store, independent of whether the intermediate CSV still exists.
        return True

    if not source_filename:
        return False
    file_path = output_dir / source_filename
    if not file_path.exists():
        log_and_print(
            f"  ⚠ progress row for ({inventory_uid}, {marketplace_id}) is not yet confirmed ingested and its "
            f"file {source_filename!r} is missing — not trusting it as already-processed."
        )
        return False
    if recorded_hash is None:
        # No csv_sha256 recorded for this chunk — file existence alone is
        # NEVER skip-safe (a same-named file could be a different,
        # unrelated, or corrupted file). This is not a weaker fallback to
        # accept; it is a missing precondition. Legacy records that
        # predate hash tracking must be handled explicitly through
        # migrate_legacy_progress_to_chunks/discover_orphan_chunk_csvs
        # reconciliation, never trusted here by omission.
        log_and_print(
            f"  ⚠ chunk {chunk_id!r} has no recorded csv_sha256 — cannot verify durability for "
            f"({inventory_uid}, {marketplace_id}); not trusting it as already-processed."
        )
        return False
    if _sha256_file(file_path) != recorded_hash:
        log_and_print(
            f"  ⚠ file {source_filename!r} for chunk {chunk_id!r} no longer matches its recorded hash "
            f"(modified, truncated, or replaced) — not trusting it as already-processed for "
            f"({inventory_uid}, {marketplace_id})."
        )
        return False
    return True


class ProgressIntegrityError(RuntimeError):
    """Raised by record_progress when a (collection_batch_id, inventory_uid,
    marketplace_id) row already exists with a conflicting identity — either
    a different chunk_id, or the same chunk_id but a contradictory
    substantive progress value (highest_tier_attempted, resolved_tier,
    api_calls, listings_found, outcome_reason). Never silently overwritten,
    never silently kept — a genuine conflict here means two different
    collection/reconciliation attempts disagree about what happened for the
    same combination, which must surface rather than pass silently."""


def record_progress(
    conn: duckdb.DuckDBPyConnection,
    *,
    batch_id: str,
    chunk_id: str,
    inventory_uid: str,
    marketplace_id: str,
    highest_tier_attempted: int | None,
    resolved_tier: int | None,
    api_calls: int,
    listings_found: int,
    outcome_reason: str,
) -> None:
    """
    Must only be called AFTER record_chunk_written has confirmed chunk_id's
    CSV is durably on disk — never immediately after an API call succeeds.
    See the module docstring and already_processed above.

    Conflict-detecting, not a silent ON CONFLICT DO NOTHING: if no row
    exists yet for (collection_batch_id, inventory_uid, marketplace_id),
    inserts it. If a row already exists with an IDENTICAL chunk_id and
    identical substantive progress fields, this is an idempotent no-op —
    safe to call again for the same real result (e.g. re-running orphan or
    legacy reconciliation over the same chunk/CSV). If a row already exists
    linked to a DIFFERENT chunk_id, or the SAME chunk_id but any
    substantive field differs, raises ProgressIntegrityError rather than
    silently keeping the old row or silently discarding the new one — a
    broken or contradictory old progress row must never be silently
    preserved.
    """
    existing = conn.execute(
        """
        SELECT chunk_id, highest_tier_attempted, resolved_tier, api_calls, listings_found, outcome_reason
        FROM collection_progress
        WHERE collection_batch_id = ? AND inventory_uid = ? AND marketplace_id = ?
        """,
        [batch_id, inventory_uid, marketplace_id],
    ).fetchone()

    if existing is None:
        conn.execute(
            """
            INSERT INTO collection_progress
                (collection_batch_id, chunk_id, inventory_uid, marketplace_id, highest_tier_attempted,
                 resolved_tier, api_calls, listings_found, outcome_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [batch_id, chunk_id, inventory_uid, marketplace_id, highest_tier_attempted,
             resolved_tier, api_calls, listings_found, outcome_reason],
        )
        return

    existing_fields = tuple(existing)
    new_fields = (chunk_id, highest_tier_attempted, resolved_tier, api_calls, listings_found, outcome_reason)
    if existing_fields == new_fields:
        return  # idempotent no-op — an identical progress row already exists

    raise ProgressIntegrityError(
        f"({batch_id!r}, {inventory_uid!r}, {marketplace_id!r}) already has a conflicting progress row: "
        f"existing=(chunk_id={existing_fields[0]!r}, highest_tier_attempted={existing_fields[1]!r}, "
        f"resolved_tier={existing_fields[2]!r}, api_calls={existing_fields[3]!r}, "
        f"listings_found={existing_fields[4]!r}, outcome_reason={existing_fields[5]!r}) "
        f"vs new=(chunk_id={chunk_id!r}, highest_tier_attempted={highest_tier_attempted!r}, "
        f"resolved_tier={resolved_tier!r}, api_calls={api_calls!r}, listings_found={listings_found!r}, "
        f"outcome_reason={outcome_reason!r})"
    )


# ══════════════════════════════════════════════════════════════════════════════
# ESCALATION
# ══════════════════════════════════════════════════════════════════════════════

def flatten_item(item: dict, query_text: str, query_tier: int, fetched_at: str) -> dict:
    shipping_options = item.get("shippingOptions") or []
    shipping = shipping_options[0] if shipping_options else {}
    seller = item.get("seller") or {}

    return {
        "query_text": query_text,
        "query_tier": query_tier,
        "marketplace_id": item.get("source_marketplace_id", ""),
        "fetched_at": fetched_at,
        "item_id": first_value(item, ["itemId"]),
        "title": first_value(item, ["title"]),
        "price_value": first_value(item, ["price", "value"]),
        "price_currency": first_value(item, ["price", "currency"]),
        "condition": first_value(item, ["condition"]),
        "condition_id": first_value(item, ["conditionId"]),
        "buying_options": "|".join(item.get("buyingOptions") or []),
        "item_web_url": first_value(item, ["itemWebUrl"]),
        "image_url": first_value(item, ["image", "imageUrl"]),
        "seller_username": str(seller.get("username", "")),
        "seller_feedback_score": str(seller.get("feedbackScore", "")),
        "seller_feedback_percentage": str(seller.get("feedbackPercentage", "")),
        "shipping_cost_value": first_value(shipping, ["shippingCost", "value"]),
        "shipping_cost_currency": first_value(shipping, ["shippingCost", "currency"]),
        "item_location_country": first_value(item, ["itemLocation", "country"]),
        "item_location_city": first_value(item, ["itemLocation", "city"]),
        "item_creation_date": first_value(item, ["itemCreationDate"]),
    }


def merge_listing_by_lowest_tier(merged: dict, item_id: str, tier: int, flat: dict) -> None:
    """Insert `flat` into `merged` keyed by item_id, retaining whichever
    record has the LOWEST numeric query_tier — the most specific retrieval
    evidence — regardless of the order in which tiers or duplicate item_ids
    are encountered. A higher tier never overwrites an already-recorded
    lower tier; a lower tier always overwrites an already-recorded higher
    one. Ties (same tier seen twice) keep the first-seen record."""
    existing = merged.get(item_id)
    if existing is None or tier < existing["query_tier"]:
        merged[item_id] = flat


def escalate_for_marketplace(
    *,
    token: str,
    marketplace_id: str,
    queries: list[tuple[int, str]],
    call_budget: CallBudget,
    log_prefix: str,
) -> dict:
    """
    Run the full tier ladder for ONE inventory item against ONE marketplace,
    independently of any other marketplace's results. Escalates ascending by
    tier, skipping tiers with no query row for this item; within a tier with
    multiple rows (Tier 2's four prefix variants), executes them in ascending
    query_text order and stops the moment the cumulative unique item_id count
    for THIS marketplace reaches MIN_UNIQUE_RESULTS.

    If the same item_id is returned by more than one tier before that
    cutoff is reached, the record from the LOWEST numeric query_tier is
    retained — the lowest tier is the most specific retrieval evidence, and
    a later, broader tier's occurrence of the same item must never
    overwrite it. This is enforced by an explicit tier comparison at merge
    time, not by relying on ascending processing order alone.
    """
    if not queries:
        return {
            "listings": {},
            "highest_tier_attempted": None,
            "resolved_tier": None,
            "api_calls": 0,
            "outcome_reason": "no_executable_queries",
            "call_log": [],
        }

    country_name = MARKETPLACE_ID_TO_COUNTRY.get(marketplace_id, marketplace_id)
    tiers_present = sorted(set(tier for tier, _ in queries))
    by_tier: dict[int, list[str]] = {
        tier: sorted(text for t, text in queries if t == tier) for tier in tiers_present
    }

    merged: dict[str, dict] = {}
    api_calls = 0
    highest_tier_attempted: int | None = None
    resolved_tier: int | None = None
    outcome_reason = "tier_exhaustion"
    call_log: list[dict] = []  # one entry per query executed: tier, text, pages, retries, calls

    for tier in tiers_present:
        highest_tier_attempted = tier
        for query_text in by_tier[tier]:
            if call_budget.exhausted():
                outcome_reason = "max_calls_reached"
                return {
                    "listings": merged, "highest_tier_attempted": highest_tier_attempted,
                    "resolved_tier": resolved_tier, "api_calls": api_calls, "outcome_reason": outcome_reason,
                    "call_log": call_log,
                }

            is_valid, invalid_reason = validate_query_length(query_text)
            if not is_valid:
                log_and_print(f"{log_prefix} ⚠ skipping invalid query {query_text!r}: {invalid_reason}")
                continue

            query_calls = {"n": 0}
            query_retries = {"n": 0}

            def _on_call():
                nonlocal api_calls
                api_calls += 1
                query_calls["n"] += 1
                call_budget.increment()

            def _on_retry(attempt: int, sleep_seconds: float, reason: str) -> None:
                query_retries["n"] += 1
                log_and_print(f"{log_prefix}   retry {attempt}/{RETRY_COUNT} in {sleep_seconds:.1f}s: {reason}")

            # MAX_RESULTS_PER_ITEM is a cumulative ceiling for this (item,
            # marketplace) escalation, not a per-query allowance — remaining
            # budget shrinks as earlier queries already contribute results.
            remaining_budget = MAX_RESULTS_PER_ITEM - len(merged)
            if remaining_budget <= 0:
                continue

            fetched_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            try:
                items = search_items_single_marketplace(
                    token=token,
                    marketplace_id=marketplace_id,
                    country_name=country_name,
                    keyword=query_text,
                    limit=PAGE_LIMIT,
                    max_items=remaining_budget,
                    sort=SORT_ORDER,
                    filters=SEARCH_FILTERS,
                    max_pages=MAX_PAGES_PER_QUERY,
                    on_call=_on_call,
                    retry_count=RETRY_COUNT,
                    initial_backoff_seconds=INITIAL_BACKOFF_SECONDS,
                    backoff_multiplier=BACKOFF_MULTIPLIER,
                    on_retry=_on_retry,
                )
            except RuntimeError as exc:
                log_and_print(f"{log_prefix}   ⚠ query {query_text!r} failed permanently: {redact_sensitive(str(exc))}")
                items = []

            call_log.append({
                "tier": tier, "query_text": query_text,
                "pages_requested": query_calls["n"], "retries": query_retries["n"],
                "calls": query_calls["n"], "results_returned": len(items),
            })

            for raw_item in items:
                flat = flatten_item(raw_item, query_text, tier, fetched_at)
                item_id = flat["item_id"]
                if item_id:
                    merge_listing_by_lowest_tier(merged, item_id, tier, flat)

            if len(merged) >= MIN_UNIQUE_RESULTS:
                resolved_tier = tier
                outcome_reason = "success"
                return {
                    "listings": merged, "highest_tier_attempted": highest_tier_attempted,
                    "resolved_tier": resolved_tier, "api_calls": api_calls, "outcome_reason": outcome_reason,
                    "call_log": call_log,
                }

    return {
        "listings": merged, "highest_tier_attempted": highest_tier_attempted,
        "resolved_tier": resolved_tier, "api_calls": api_calls, "outcome_reason": outcome_reason,
        "call_log": call_log,
    }


def process_item(
    conn: duckdb.DuckDBPyConnection,
    *,
    item_row: pd.Series,
    token: str,
    batch_id: str,
    call_budget: CallBudget,
    marketplaces: list[str],
    output_dir: Path = TARGETED_ACTIVE_DIR,
) -> dict:
    """
    Collects results only — does NOT call record_progress. Progress rows
    are persisted later, in bulk, by the chunk loop, only after this
    chunk's CSV has been atomically written. This is the core fix for the
    data-loss bug found in verification: previously record_progress ran
    immediately per (item, marketplace), so a crash before the chunk's CSV
    was written could mark combinations "done" whose results only ever
    existed in memory.
    """
    inventory_uid = item_row["inventory_uid"]
    canonical_id = item_row["canonical_inventory_id"]
    queries = get_queries_for_item(conn, inventory_uid)

    per_marketplace: dict[str, dict] = {}
    for marketplace_id in marketplaces:
        if already_processed(conn, batch_id, inventory_uid, marketplace_id, output_dir=output_dir):
            log_and_print(f"  [{canonical_id}] {marketplace_id}: already processed in this batch — resuming past it")
            continue
        if call_budget.exhausted():
            log_and_print(f"  [{canonical_id}] {marketplace_id}: MAX_CALLS_PER_CHUNK reached, stopping run")
            break

        result = escalate_for_marketplace(
            token=token,
            marketplace_id=marketplace_id,
            queries=queries,
            call_budget=call_budget,
            log_prefix=f"  [{canonical_id}] {marketplace_id}:",
        )
        per_marketplace[marketplace_id] = result
        log_and_print(
            f"  [{canonical_id}] {marketplace_id}: tier={result['highest_tier_attempted']} "
            f"resolved={result['resolved_tier']} calls={result['api_calls']} "
            f"listings={len(result['listings'])} outcome={result['outcome_reason']} "
            "(collected in memory — not yet durable)"
        )

    return {"inventory_uid": inventory_uid, "canonical_inventory_id": canonical_id, "per_marketplace": per_marketplace}


def record_chunk_progress(
    conn: duckdb.DuckDBPyConnection, *, batch_id: str, chunk_id: str, item_results: list[dict]
) -> None:
    """Persists collection_progress rows for every (item, marketplace) combo
    collected this chunk. Must only be called AFTER record_chunk_written has
    confirmed this chunk_id's CSV is durably on disk."""
    for item_result in item_results:
        for marketplace_id, result in item_result["per_marketplace"].items():
            record_progress(
                conn,
                batch_id=batch_id,
                chunk_id=chunk_id,
                inventory_uid=item_result["inventory_uid"],
                marketplace_id=marketplace_id,
                highest_tier_attempted=result["highest_tier_attempted"],
                resolved_tier=result["resolved_tier"],
                api_calls=result["api_calls"],
                listings_found=len(result["listings"]),
                outcome_reason=result["outcome_reason"],
            )


# ══════════════════════════════════════════════════════════════════════════════
# CSV OUTPUT
# ══════════════════════════════════════════════════════════════════════════════

TARGETED_CSV_COLUMNS = [
    "collection_batch_id", "inventory_uid", "canonical_inventory_id",
    "query_text", "query_tier", "query_template_version", "marketplace_id", "fetched_at",
    "item_id", "title", "price_value", "price_currency", "condition", "condition_id",
    "buying_options", "item_web_url", "image_url", "seller_username",
    "seller_feedback_score", "seller_feedback_percentage",
    "shipping_cost_value", "shipping_cost_currency",
    "item_location_country", "item_location_city", "item_creation_date",
]


def write_batch_csv(
    batch_id: str,
    chunk_id: str,
    item_results: list[dict],
    query_template_version: str,
    output_dir: Path = TARGETED_ACTIVE_DIR,
) -> Path:
    """
    One row per (inventory_uid, item_id, marketplace_id) — never collapsed
    across marketplaces, and never collapsed across inventory items either.
    The same real eBay listing found via both EBAY_DE and EBAY_US is
    preserved as two raw rows, each with that marketplace's own
    price/currency/shipping observation. The same real eBay listing
    surfacing as a candidate for two DIFFERENT inventory items (e.g. two
    items sharing an identical broad tier-4 query) is likewise preserved as
    two rows — one per item — because each represents a distinct evidence
    relationship ("this listing was a candidate for THIS item"), not a
    duplicate observation of the listing itself. Deduping globally by
    (item_id, marketplace_id) alone previously discarded a later item's
    entire candidate set whenever an earlier item's query happened to
    return an overlapping listing first — confirmed data loss, not a
    hypothetical: 97 of 100 candidate listings for one real inventory item
    were silently dropped this way before this fix. raw_* is an exact copy
    of source data, never modified, and evidence is never silently
    discarded here. Deduplicating the same item_id into a single valuation
    comparable is downstream matching's responsibility (04_match.py, out of
    scope for this module), not raw ingestion's.

    Written atomically: content goes to a temp file in the same directory
    first, then os.replace()'d into its final, chunk_id-derived (globally
    unique) path — never a plain overwrite of an existing filename. This is
    what makes it safe for record_chunk_written/record_progress to treat
    the final path's existence as proof of durability, and what prevents
    two separate process invocations from ever colliding on the same
    filename (chunk_id is unique per invocation by construction).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    final_path = output_dir / f"targeted_active_{chunk_id}.csv"

    seen_keys: set[tuple[str, str, str]] = set()
    rows: list[dict] = []
    for item_result in item_results:
        inventory_uid = item_result["inventory_uid"]
        for marketplace_id, result in item_result["per_marketplace"].items():
            for item_id, listing in result["listings"].items():
                key = (inventory_uid, item_id, marketplace_id)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                row = {
                    "collection_batch_id": batch_id,
                    "inventory_uid": item_result["inventory_uid"],
                    "canonical_inventory_id": item_result["canonical_inventory_id"],
                    "query_template_version": query_template_version,
                    **{k: v for k, v in listing.items() if k in TARGETED_CSV_COLUMNS},
                }
                rows.append({col: row.get(col, "") for col in TARGETED_CSV_COLUMNS})

    fd, tmp_name = tempfile.mkstemp(dir=output_dir, prefix=f".{final_path.name}.", suffix=".tmp")
    tmp_path = Path(tmp_name)
    try:
        with open(fd, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=TARGETED_CSV_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        tmp_path.replace(final_path)
    finally:
        tmp_path.unlink(missing_ok=True)  # no-op once replace() has moved it

    return final_path


# ══════════════════════════════════════════════════════════════════════════════
# MANIFEST
# ══════════════════════════════════════════════════════════════════════════════

def compute_marketplace_comparison(item_results: list[dict], marketplaces: list[str]) -> dict:
    """Per-item breakdown: did the primary marketplace alone satisfy
    MIN_UNIQUE_RESULTS, or was a second marketplace needed, or was the
    threshold not reached even combined? This is the evidence for whether a
    multi-marketplace default is justified."""
    if len(marketplaces) < 2:
        return {}

    primary, secondary = marketplaces[0], marketplaces[1]
    counts = {"primary_alone_sufficient": 0, "needed_secondary_too": 0, "insufficient_even_combined": 0}
    details = []

    for item_result in item_results:
        per_mp = item_result["per_marketplace"]
        primary_listings = set(per_mp.get(primary, {}).get("listings", {}).keys())
        secondary_listings = set(per_mp.get(secondary, {}).get("listings", {}).keys())
        combined = primary_listings | secondary_listings

        if len(primary_listings) >= MIN_UNIQUE_RESULTS:
            category = "primary_alone_sufficient"
        elif len(combined) >= MIN_UNIQUE_RESULTS:
            category = "needed_secondary_too"
        else:
            category = "insufficient_even_combined"
        counts[category] += 1
        details.append({
            "canonical_inventory_id": item_result["canonical_inventory_id"],
            f"{primary}_unique": len(primary_listings),
            f"{secondary}_unique": len(secondary_listings),
            "combined_unique": len(combined),
            "category": category,
        })

    return {"primary_marketplace": primary, "secondary_marketplace": secondary, "counts": counts, "details": details}


def write_manifest(
    *,
    batch_id: str,
    chunk_id: str | None = None,
    started_at: datetime,
    finished_at: datetime,
    item_results: list[dict],
    call_budget: CallBudget,
    marketplaces: list[str],
    quota_info: dict | None = None,
    source_filename: str | None = None,
    source_csv_path: Path | None = None,
    reports_dir: Path = REPORTS_DIR,
    manifest_filename: str = "targeted_collection_manifest.json",
) -> Path:
    """
    One immutable manifest per unique chunk — manifest_filename is derived
    from chunk_id (globally unique), so this never overwrites a prior
    invocation's manifest. chunk_id/source_filename/source_csv_sha256 are
    included in the body so a manifest can always be traced back to the
    exact durable CSV it describes — and, critically, so orphan recovery
    can verify the CSV hasn't been modified/replaced since this manifest
    was written before trusting the manifest's attempted_combinations at
    all (see discover_orphan_chunk_csvs). source_csv_path, if given, must
    point at the CSV this manifest describes — it is only ever read to
    compute its hash, never written to.
    """
    reports_dir.mkdir(parents=True, exist_ok=True)
    out_path = reports_dir / manifest_filename
    source_csv_sha256 = _sha256_file(source_csv_path) if source_csv_path is not None else None

    tier_resolution: dict[str, int] = {}
    unresolved_items = []
    total_listings_per_marketplace: dict[str, int] = {mp: 0 for mp in marketplaces}

    call_reconciliation = []
    # Authoritative per-combination ledger for THIS chunk — the only durable
    # record of a combination that returned zero listings. A CSV can never
    # represent that on its own (zero listings means zero rows), so this is
    # what discover_orphan_chunk_csvs falls back to for reconstructing
    # zero-result combinations if this manifest survives a crash.
    attempted_combinations = []
    for item_result in item_results:
        any_resolved = False
        for marketplace_id, result in item_result["per_marketplace"].items():
            total_listings_per_marketplace[marketplace_id] = (
                total_listings_per_marketplace.get(marketplace_id, 0) + len(result["listings"])
            )
            key = str(result["resolved_tier"]) if result["resolved_tier"] is not None else "unresolved"
            tier_resolution[key] = tier_resolution.get(key, 0) + 1
            if result["outcome_reason"] == "success":
                any_resolved = True
            for entry in result.get("call_log", []):
                call_reconciliation.append({
                    "canonical_inventory_id": item_result["canonical_inventory_id"],
                    "inventory_uid": item_result["inventory_uid"],
                    "marketplace_id": marketplace_id,
                    **entry,
                })
            attempted_combinations.append({
                "inventory_uid": item_result["inventory_uid"],
                "canonical_inventory_id": item_result["canonical_inventory_id"],
                "marketplace_id": marketplace_id,
                "listings_found": len(result["listings"]),
                "highest_tier_attempted": result["highest_tier_attempted"],
                "resolved_tier": result["resolved_tier"],
                "api_calls": result["api_calls"],
                "outcome_reason": result["outcome_reason"],
            })
        if not any_resolved:
            unresolved_items.append(item_result["canonical_inventory_id"])

    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "collection_batch_id": batch_id,
        "chunk_id": chunk_id,
        "source_filename": source_filename,
        "source_csv_sha256": source_csv_sha256,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "runtime_seconds": (finished_at - started_at).total_seconds(),
        "inventory_items_processed": len(item_results),
        "marketplaces": marketplaces,
        "api_calls_made": call_budget.calls_made,
        "max_calls_per_chunk": call_budget.max_calls,
        "tier_resolution_counts": tier_resolution,
        "unresolved_inventory_items": unresolved_items,
        "listings_per_marketplace": total_listings_per_marketplace,
        "marketplace_comparison": compute_marketplace_comparison(item_results, marketplaces),
        "call_reconciliation": call_reconciliation,
        "attempted_combinations": attempted_combinations,
        "quota": quota_info or {"available": False, "error": "not checked this run"},
        "config": {
            "MIN_UNIQUE_RESULTS": MIN_UNIQUE_RESULTS,
            "MAX_PAGES_PER_QUERY": MAX_PAGES_PER_QUERY,
            "MAX_RESULTS_PER_ITEM": MAX_RESULTS_PER_ITEM,
            "MAX_CALLS_PER_CHUNK": MAX_CALLS_PER_CHUNK,
            "RETRY_COUNT": RETRY_COUNT,
            "INITIAL_BACKOFF_SECONDS": INITIAL_BACKOFF_SECONDS,
            "BACKOFF_MULTIPLIER": BACKOFF_MULTIPLIER,
            "QUOTA_SAFETY_MARGIN": QUOTA_SAFETY_MARGIN,
            "MAX_CHUNK_ITERATIONS": MAX_CHUNK_ITERATIONS,
        },
    }
    # Written atomically: content goes to a temp file in the same directory
    # first, then os.replace()'d into its final path — never a plain
    # write_text() to the final name. This is what makes the manifest
    # "immutable once written" a real guarantee rather than a comment: an
    # interrupted write leaves only an incomplete ".{name}.{random}.tmp"
    # file, which never matches manifest-discovery globs (all of which
    # require the final "targeted_collection_manifest*.json" name), so it
    # can never be picked up as a trusted final manifest — the underlying
    # chunk just stays "no manifest found" until a later, complete write
    # (if any) succeeds.
    fd, tmp_name = tempfile.mkstemp(dir=reports_dir, prefix=f".{out_path.name}.", suffix=".tmp")
    tmp_path = Path(tmp_name)
    try:
        with open(fd, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(manifest, indent=2))
        tmp_path.replace(out_path)
    finally:
        tmp_path.unlink(missing_ok=True)  # no-op once replace() has moved it

    return out_path


# ══════════════════════════════════════════════════════════════════════════════
# QUOTA
# ══════════════════════════════════════════════════════════════════════════════

def check_quota(token: str) -> dict:
    """
    Read-only call to eBay's Developer Analytics getRateLimits endpoint.
    Never guesses a number: reports the exact figures if the call succeeds,
    or the exact error text if the current credentials/access don't permit
    it. One extra API call per run, not counted against MAX_CALLS_PER_CHUNK
    since it's informational, not a search.
    """
    try:
        result = get_rate_limits(token, api_name="Browse")
        rate = result["rateLimits"][0]["resources"][0]["rates"][0]
        return {
            "available": True,
            "limit": rate.get("limit"),
            "used": rate.get("count"),
            "remaining": rate.get("remaining"),
            "reset": rate.get("reset"),
            "time_window_seconds": rate.get("timeWindow"),
        }
    except Exception as exc:  # noqa: BLE001 — report the exact error, never guess a number
        return {"available": False, "error": redact_sensitive(str(exc))}


def compute_safe_call_budget(remaining_quota: int, margin: float = QUOTA_SAFETY_MARGIN) -> int:
    """How many calls may safely be planned this chunk while keeping
    `margin`x headroom in the day's remaining quota (e.g. margin=1.2 keeps
    >=~17% in reserve: only remaining/1.2 is ever budgeted against)."""
    return max(0, int(remaining_quota / margin))


def batch_fully_processed(
    conn: duckdb.DuckDBPyConnection, batch_id: str, inventory_df: pd.DataFrame, marketplaces: list[str]
) -> bool:
    """
    True only once EVERY expected (inventory_uid, marketplace_id) pair —
    every inventory_uid in inventory_df crossed with every requested
    marketplace — has its own collection_progress row for this batch.
    Verifies the exact expected SET, not a raw COUNT(*) >= expected
    comparison: a batch with stale or unrelated extra progress rows (e.g.
    left over from a differently-scoped run, or an item no longer in
    scope) could otherwise reach the expected count while a genuinely
    required pair is still missing — extra rows must never compensate for
    a missing required one. This is COLLECTION state only — see
    batch_fully_ingested for collection AND ingestion state.
    """
    expected_uids = list(inventory_df["inventory_uid"])
    if not expected_uids or not marketplaces:
        return True
    expected_pairs = {(uid, marketplace_id) for uid in expected_uids for marketplace_id in marketplaces}
    rows = conn.execute(
        "SELECT DISTINCT inventory_uid, marketplace_id FROM collection_progress WHERE collection_batch_id = ?",
        [batch_id],
    ).fetchall()
    done_pairs = {(row[0], row[1]) for row in rows}
    return expected_pairs.issubset(done_pairs)


def batch_fully_ingested(
    conn: duckdb.DuckDBPyConnection, batch_id: str, inventory_df: pd.DataFrame, marketplaces: list[str]
) -> bool:
    """
    True only when collection is complete AND every durable chunk for this
    batch has also been confirmed ingested — deliberately stricter than
    batch_fully_processed. Collection state and ingestion state are
    distinct: a batch can finish collecting everything while its last
    chunk's ingestion attempt failed, and that must NOT be reported/treated
    as fully done (finished_at must stay NULL so a future --resume retries
    the pending ingestion, per get_pending_ingestion_chunks).
    """
    if not batch_fully_processed(conn, batch_id, inventory_df, marketplaces):
        return False
    return len(get_pending_ingestion_chunks(conn, batch_id)) == 0


def run_targeted_ingestion(ingest_script: Path | None = None, db_path: Path | None = None) -> None:
    """
    Hands this chunk's just-written CSV to scripts/01_ingest.py --targeted so
    it becomes durable immediately, without a human running a second command.
    This module still never writes to raw_active_targeted itself — it only
    invokes the separate ingestion script as a subprocess after its own CSV
    is on disk, preserving the documented module boundary (04 produces CSV,
    01_ingest.py owns all raw-table writes and idempotency) while making the
    handoff automatic. No output here is trusted to be secret-free by
    construction, so stdout/stderr are redacted before logging anyway.

    When db_path is given it is passed to the subprocess both as --db and via
    the WATCHPARTS_DB env var, so the subprocess writes to the SAME target as
    this run — never the default live DB — without depending on any ambient
    global. Default behaviour (db_path=None) is unchanged.
    """
    script_path = ingest_script or (BASE_DIR / "scripts" / "01_ingest.py")
    cmd = [sys.executable, str(script_path), "--targeted"]
    env = None
    if db_path is not None:
        cmd += ["--db", str(db_path)]
        env = {**os.environ, "WATCHPARTS_DB": str(db_path)}
    result = subprocess.run(
        cmd,
        capture_output=True, text=True, env=env,
    )
    if result.stdout:
        log_and_print(redact_sensitive(result.stdout))
    if result.returncode != 0:
        if result.stderr:
            log_and_print(redact_sensitive(result.stderr))
        raise RuntimeError(f"01_ingest.py --targeted failed with exit code {result.returncode}")


# ══════════════════════════════════════════════════════════════════════════════
# DRY RUN
# ══════════════════════════════════════════════════════════════════════════════

def dry_run(
    conn: duckdb.DuckDBPyConnection,
    inventory_uid_filter: str | None,
    limit_items: int | None,
    inventory_manifest: str | None = None,
) -> None:
    if inventory_manifest is not None:
        inventory_df = get_inventory_from_manifest(conn, inventory_manifest)
        log_and_print(f"Inventory manifest: {inventory_manifest} — {len(inventory_df):,} accepted, 0 rejected")
    else:
        inventory_df = get_eligible_inventory(conn, inventory_uid_filter, limit_items)
    log_and_print(f"Eligible inventory items: {len(inventory_df):,}")

    total_queries = 0
    best_case_calls = 0
    worst_case_calls = 0
    tier_counts: dict[int, int] = {}
    zero_query_items = 0

    for _, row in inventory_df.iterrows():
        queries = get_queries_for_item(conn, row["inventory_uid"])
        if not queries:
            zero_query_items += 1
            continue
        total_queries += len(queries)
        tiers_present = sorted(set(t for t, _ in queries))
        for tier, _ in queries:
            tier_counts[tier] = tier_counts.get(tier, 0) + 1

        per_marketplace_best = len(TARGETED_MARKETPLACES)  # 1 query executed per marketplace, best case
        per_marketplace_worst = len(queries) * MAX_PAGES_PER_QUERY * len(TARGETED_MARKETPLACES)
        best_case_calls += per_marketplace_best
        worst_case_calls += per_marketplace_worst

    log_and_print(f"Total available queries across eligible items: {total_queries:,}")
    log_and_print(f"Tier distribution (query rows): {dict(sorted(tier_counts.items()))}")
    log_and_print(f"Items with zero executable queries: {zero_query_items:,}")
    log_and_print(f"Marketplaces configured: {TARGETED_MARKETPLACES}")
    log_and_print(f"Estimated best-case API calls: {best_case_calls:,}")
    log_and_print(f"Estimated worst-case API calls: {worst_case_calls:,}")
    log_and_print("(No network access performed — this is an estimate only.)")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Module 3: targeted active listing collection.")
    parser.add_argument("--inventory-uid", default=None, help="Process only this single inventory_uid.")
    parser.add_argument("--limit-items", type=int, default=None, help="Cap the number of eligible items processed.")
    parser.add_argument("--resume", action="store_true", help="Continue the latest unfinished batch instead of starting a new one.")
    parser.add_argument("--dry-run", action="store_true", help="Estimate items/queries/calls without contacting eBay.")
    parser.add_argument(
        "--db", default=None,
        help="Target DuckDB file. Overrides the WATCHPARTS_DB env var and the default "
        "live database, and is exported to the 01_ingest.py ingestion subprocess. Use a "
        "disposable copy for tests/pilots so the live database is never touched.",
    )
    parser.add_argument(
        "--reconcile-only", action="store_true",
        help="Ingest any already-durable, not-yet-ingested chunk CSVs and reconcile "
             "collection_chunks.ingested_at, then exit — never checks quota or calls eBay, "
             "never starts a new collection chunk. The documented way to ingest a pending "
             "legacy/durable CSV without --resume's fall-through into new collection.",
    )
    parser.add_argument(
        "--inventory-manifest", default=None,
        help="Path to a CSV manifest with an inventory_uid column (an optional sample_position "
             "column gives an explicit deterministic order; otherwise CSV row order is used). "
             "Processes exactly and only the listed UIDs, in that order — never the next N rows "
             "in database order. Rejects duplicate, unknown, or FAIL-status UIDs with a clear "
             "error before any eBay call. Mutually exclusive with --inventory-uid and "
             "--limit-items. Compatible with --dry-run. Compatible with --resume provided the "
             "same manifest path is passed again on the resumed invocation — inventory selection "
             "is always re-derived from args on every invocation (never persisted across a "
             "resume), so resuming with the same manifest is unambiguous.",
    )
    args = parser.parse_args()
    if args.inventory_manifest is not None:
        if args.inventory_uid is not None:
            parser.error("--inventory-manifest cannot be combined with --inventory-uid.")
        if args.limit_items is not None:
            parser.error("--inventory-manifest cannot be combined with --limit-items.")
    return args


def run_reconcile_only(db_path: Path = DB_PATH) -> dict:
    """
    Ingestion-only / reconciliation-only path across ALL batches: ingests
    any already-durable, not-yet-ingested targeted-active CSV chunk(s) via
    the normal scripts/01_ingest.py --targeted handoff, then reconciles
    collection_chunks.ingested_at against ingestion_log (hash-aware, via
    reconcile_chunk_ingestion_state) — and nothing else. Never loads
    credentials, never acquires a token, never checks quota, never calls
    eBay, never starts a new collection chunk.

    This is the documented, safe way to ingest a pending durable CSV (e.g.
    the legacy batch_20260710_094319 chunk) without accidentally
    continuing into new live collection, which plain --resume would do
    (run_chunked_collection ingests pending chunks too, but then proceeds
    straight into checking quota and collecting new chunks for that batch).

    Scans across all batches, not one — insert_targeted_listings itself
    globs every targeted_active_*.csv file in the output directory
    regardless of batch, so a single-batch scope here would be misleading.

    Takes a db_path, not an open connection, and never holds a connection
    open while scripts/01_ingest.py --targeted runs as a subprocess. DuckDB
    refuses a second writable connection to the same file from a genuinely
    separate process while the first is still open
    (_duckdb.IOException: "Could not set lock on file ... Conflicting lock
    is held ...") — confirmed by a real production run that crashed exactly
    this way when the caller kept its connection open across the subprocess
    call. The lifecycle here is strictly sequential: open -> count pending
    -> CLOSE -> (subprocess, only if pending) -> reopen -> reconcile ->
    close. Same-process multiple connections to the same file are fine
    (DuckDB shares one underlying instance); the failure mode is
    specifically a second OS process trying to open the file while this
    process still holds it.
    """
    conn = get_connection(db_path=db_path)
    try:
        pending_before = conn.execute(
            "SELECT COUNT(*) FROM collection_chunks WHERE csv_written_at IS NOT NULL AND ingested_at IS NULL"
        ).fetchone()[0]
    finally:
        conn.close()

    if pending_before > 0:
        run_targeted_ingestion(db_path=db_path)

    conn = get_connection(db_path=db_path)
    try:
        newly_marked = reconcile_chunk_ingestion_state(conn)
        pending_after = conn.execute(
            "SELECT COUNT(*) FROM collection_chunks WHERE csv_written_at IS NOT NULL AND ingested_at IS NULL"
        ).fetchone()[0]
        # Fixes the false-failure bookkeeping gap: chunk ingestion is now
        # reconciled above, but the owning batch's own stop_reason/status
        # (e.g. a stale 'ingestion_failed') is never revisited by anything
        # else once this process's collection loop has already exited —
        # this is the only place that later corrects it.
        batch_changes = reconcile_batch_state(conn)
    finally:
        conn.close()

    return {
        "pending_before": pending_before,
        "pending_after": pending_after,
        "newly_marked_ingested": newly_marked,
        "batches_reconciled": batch_changes,
    }


def _run_ingestion_with_connection_released(
    conn: duckdb.DuckDBPyConnection, db_path: Path,
) -> tuple[duckdb.DuckDBPyConnection, Exception | None]:
    """
    Closes `conn` before invoking run_targeted_ingestion() (which shells out
    to scripts/01_ingest.py --targeted as a genuinely separate OS process),
    and always reopens a fresh connection to the same db_path afterward —
    on success AND on failure — before returning control to the caller.

    Why this exists: DuckDB refuses a second writable connection to the
    same file from a separate process while the first is still open
    (_duckdb.IOException: "Could not set lock on file ... Conflicting lock
    is held ..."). run_chunked_collection used to hold its own `conn` open
    across the entire chunk loop, including across this exact subprocess
    call — confirmed by a real production run (the 40-item validation
    batch) that crashed with exactly that IOException on its first chunk's
    ingestion attempt, forcing a manual --reconcile-only recovery
    afterward. run_reconcile_only already established the correct
    close -> subprocess -> reopen sequence for its own single ingestion
    call; this helper applies the same sequence to run_chunked_collection's
    two call sites (the startup pending-chunk reconciliation, and the
    per-chunk ingestion after every new chunk) so neither one can
    self-deadlock the same way again.

    Returns (new_conn, exception_or_None) rather than raising directly —
    the caller decides how to react to a failed ingestion (e.g. set
    stop_reason and break cleanly), but must always receive a live,
    reopened connection either way, since callers still need it for
    reconcile_chunk_ingestion_state / batch_fully_processed / the next
    loop iteration's queries.
    """
    conn.close()
    ingestion_exc: Exception | None = None
    try:
        run_targeted_ingestion(db_path=db_path)
    except RuntimeError as exc:
        ingestion_exc = exc
    finally:
        conn = get_connection(db_path)
    return conn, ingestion_exc


def run_chunked_collection(
    conn: duckdb.DuckDBPyConnection,
    *,
    inventory_df: pd.DataFrame,
    token: str,
    batch_id: str,
    marketplaces: list[str] = TARGETED_MARKETPLACES,
    ingest_after_chunk: bool = True,
    output_dir: Path = TARGETED_ACTIVE_DIR,
    reports_dir: Path = REPORTS_DIR,
    db_path: Path = DB_PATH,
) -> dict:
    """
    Runs the whole selected inventory_df as a series of internally
    checkpointed chunks rather than one opaque pass. Each chunk follows a
    strict durable state transition — a combination is never treated as
    resumable-skip-safe until its results are durably stored:

        collect (in memory)
          -> atomically write chunk CSV to a unique, chunk_id-derived path
          -> record_chunk_written (collection_chunks.csv_written_at)
          -> record_chunk_progress (collection_progress, referencing chunk_id)
          -> write per-chunk manifest (also chunk_id-derived, never overwritten)
          -> ingest the CSV (scripts/01_ingest.py --targeted) — conn is
             CLOSED for the duration of this subprocess call and reopened
             immediately after, via _run_ingestion_with_connection_released
             (see its docstring — this is the fix for a real production
             self-lock crash, not defensive-only)
          -> reconcile_chunk_ingestion_state, once ingestion_log confirms success

    Before any new collection, first reconciles + retries ingestion for any
    previously-written-but-not-yet-ingested chunks belonging to this batch
    (Case C from verification: a durable CSV can legitimately outlive the
    process that wrote it). A batch-level stop record is persisted at every
    stop point via record_batch_stop_state, not only on full completion, so
    auditability never depends on ephemeral log output. No interactive
    prompt exists anywhere in this loop — the deployed product must run
    unattended.

    IMPORTANT for callers: because conn is closed and reopened internally
    around every ingestion call, the connection object passed in may not
    be the same object that's valid by the time this function returns.
    The summary dict's "conn" key always holds the live, currently-open
    connection — callers (main(), tests) must use summary["conn"] for any
    further queries, not the original `conn` argument, which may already
    be closed.
    """
    orphans_found = discover_orphan_chunk_csvs(conn, batch_id, output_dir=output_dir, reports_dir=reports_dir)
    if orphans_found:
        log_and_print(f"Reconciled {orphans_found} orphan chunk CSV(s) with no prior collection_chunks record.")
    reconcile_chunk_ingestion_state(conn)
    pending = get_pending_ingestion_chunks(conn, batch_id)
    if pending:
        log_and_print(
            f"Found {len(pending)} durably-written chunk(s) from a prior invocation not yet "
            "ingested — ingesting those before any new collection."
        )
        if ingest_after_chunk:
            conn, startup_ingestion_exc = _run_ingestion_with_connection_released(conn, db_path)
            if startup_ingestion_exc is not None:
                log_and_print(
                    f"Startup reconciliation ingestion failed: {redact_sensitive(str(startup_ingestion_exc))}"
                )
        reconcile_chunk_ingestion_state(conn)

    # Correct any stale bookkeeping left over from an earlier stop of THIS
    # batch (e.g. a prior invocation's stop_reason='ingestion_failed' that
    # the reconciliation just above has now actually resolved) before
    # deciding whether to collect anything new.
    reconcile_batch_state(conn, batch_id)

    query_template_version = None
    chunks_completed = 0
    chunks: list[dict] = []
    stop_reason = None
    last_chunk_id: str | None = None

    while True:
        if chunks_completed >= MAX_CHUNK_ITERATIONS:
            stop_reason = "max_chunk_iterations_reached"
            break

        quota_info = check_quota(token)
        if quota_info["available"]:
            log_and_print(
                f"Quota (Browse API, daily): limit={quota_info['limit']:,} used={quota_info['used']:,} "
                f"remaining={quota_info['remaining']:,} resets={quota_info['reset']}"
            )
            safe_budget = compute_safe_call_budget(quota_info["remaining"])
            log_and_print(
                f"Safety margin {QUOTA_SAFETY_MARGIN}x applied — safe call budget this chunk: {safe_budget:,}"
            )
        else:
            log_and_print(f"Quota check unavailable: {quota_info['error']}")
            log_and_print(
                "Cannot verify the safety margin without quota visibility — "
                "falling back to MAX_CALLS_PER_CHUNK alone for this chunk."
            )
            safe_budget = MAX_CALLS_PER_CHUNK

        if safe_budget <= 0:
            stop_reason = "quota_safety_margin_exhausted"
            break

        effective_max_calls = min(MAX_CALLS_PER_CHUNK, safe_budget)
        call_budget = CallBudget(effective_max_calls)
        started_at = datetime.now(timezone.utc)
        item_results = []

        for _, row in inventory_df.iterrows():
            if call_budget.exhausted():
                break
            result = process_item(
                conn, item_row=row, token=token, batch_id=batch_id,
                call_budget=call_budget, marketplaces=marketplaces, output_dir=output_dir,
            )
            item_results.append(result)
            if query_template_version is None:
                queries = get_queries_for_item(conn, row["inventory_uid"])
                if queries:
                    row_version = conn.execute(
                        "SELECT query_template_version FROM search_queries WHERE inventory_uid = ? LIMIT 1",
                        [row["inventory_uid"]],
                    ).fetchone()
                    query_template_version = row_version[0] if row_version else "v1"

        finished_at = datetime.now(timezone.utc)
        chunk_id = f"{batch_id}_chunk_{uuid.uuid4().hex[:12]}"

        # ── Durable write, then durable checkpoint — this ordering is the fix. ──
        csv_path = write_batch_csv(
            batch_id, chunk_id, item_results, query_template_version or "v1", output_dir=output_dir
        )
        # record_chunk_written + record_chunk_progress commit together as one
        # transaction — closes the narrow window between them: if the
        # process dies mid-transaction, neither lands, and
        # discover_orphan_chunk_csvs reconstructs both consistently from
        # the CSV's own content on the next invocation, rather than leaving
        # a collection_chunks row with no matching progress rows.
        csv_sha256 = _sha256_file(csv_path)
        conn.execute("BEGIN TRANSACTION")
        try:
            record_chunk_written(
                conn, chunk_id=chunk_id, batch_id=batch_id, source_filename=csv_path.name, csv_sha256=csv_sha256,
                started_at=started_at, items_attempted=len(item_results), calls_made=call_budget.calls_made,
            )
            record_chunk_progress(conn, batch_id=batch_id, chunk_id=chunk_id, item_results=item_results)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        manifest_path = write_manifest(
            batch_id=batch_id, chunk_id=chunk_id, started_at=started_at, finished_at=finished_at,
            item_results=item_results, call_budget=call_budget, marketplaces=marketplaces,
            quota_info=quota_info, source_filename=csv_path.name, source_csv_path=csv_path,
            manifest_filename=f"targeted_collection_manifest_{chunk_id}.json",
            reports_dir=reports_dir,
        )
        log_and_print(
            f"Chunk {chunk_id}: {len(item_results):,} items attempted, "
            f"{call_budget.calls_made:,} calls made, csv={csv_path.name}, manifest={manifest_path.name}"
        )
        chunks_completed += 1
        last_chunk_id = chunk_id
        chunks.append({
            "chunk_id": chunk_id, "calls_made": call_budget.calls_made,
            "items_attempted": len(item_results), "csv_path": str(csv_path), "manifest_path": str(manifest_path),
        })

        if ingest_after_chunk:
            conn, ingestion_exc = _run_ingestion_with_connection_released(conn, db_path)
            if ingestion_exc is not None:
                # The chunk's data is already durable (CSV + progress rows
                # committed above) — ingestion failing here is a safe,
                # retryable state, not data loss. Stop cleanly; the next
                # invocation's startup reconciliation step retries it.
                log_and_print(f"Ingestion failed for chunk {chunk_id}: {redact_sensitive(str(ingestion_exc))}")
                stop_reason = "ingestion_failed"
                break
            reconcile_chunk_ingestion_state(conn)

        if batch_fully_processed(conn, batch_id, inventory_df, marketplaces):
            stop_reason = "batch_fully_processed"
            break

        if call_budget.calls_made == 0:
            # Nothing runnable was left to attempt this chunk (e.g. every
            # remaining item already processed) yet the batch isn't marked
            # fully processed — stop rather than spin.
            stop_reason = "no_progress_possible"
            break
        # Otherwise: loop continues automatically to the next chunk.

    # Deliberately stricter than the mid-loop check above: a batch that
    # finished collecting everything but whose last ingestion attempt
    # failed is NOT reported as fully processed here, and finished_at stays
    # NULL — see batch_fully_ingested and record_batch_stop_state.
    fully_processed = batch_fully_ingested(conn, batch_id, inventory_df, marketplaces)
    record_batch_stop_state(
        conn, batch_id, stop_reason=stop_reason, chunks_completed=chunks_completed,
        fully_processed=fully_processed, last_chunk_id=last_chunk_id,
    )

    return {
        "batch_id": batch_id,
        "chunks_executed": chunks_completed,
        "stop_reason": stop_reason,
        "fully_processed": fully_processed,
        "last_chunk_id": last_chunk_id,
        "chunks": chunks,
        "conn": conn,
    }


def main() -> None:
    setup_logging()
    args = parse_args()

    # Resolve the DB target. --db wins; otherwise fall back to the module
    # DB_PATH (already env/default-resolved at import). Export it so the
    # 01_ingest.py subprocess (which reads WATCHPARTS_DB at its own import)
    # writes to the same target, and pass it explicitly to the call sites
    # below (their default-bound db_path parameters cannot see a late
    # reassignment of the module global).
    effective_db = Path(args.db) if args.db else DB_PATH
    os.environ["WATCHPARTS_DB"] = str(effective_db)

    log_and_print("=" * 60)
    log_and_print("WATCHPARTS — STEP 3: COLLECT TARGETED ACTIVE LISTINGS")
    log_and_print("=" * 60)
    log_and_print(f"Database target: {effective_db}")

    if args.reconcile_only:
        # run_reconcile_only manages its own connection lifecycle entirely
        # (open -> count -> CLOSE -> subprocess, if needed -> reopen ->
        # reconcile -> close) so that this process never holds a DuckDB
        # connection open while scripts/01_ingest.py --targeted runs as a
        # separate process. No connection must be opened here first — doing
        # so previously caused a real self-inflicted lock conflict.
        summary = run_reconcile_only(db_path=effective_db)
        log_and_print(f"Pending durable-but-uningested chunks before: {summary['pending_before']:,}")
        log_and_print(f"Newly marked ingested this run: {summary['newly_marked_ingested']:,}")
        log_and_print(f"Pending durable-but-uningested chunks after: {summary['pending_after']:,}")
        return

    if args.dry_run:
        # Read-only: a dry-run only reads inventory to estimate calls and must
        # never modify the target DB (this is the fix for the schema-DDL-on-
        # connect mutation that changed the live DB's checksum).
        conn = get_connection(db_path=effective_db, read_only=True)
        try:
            dry_run(conn, args.inventory_uid, args.limit_items, inventory_manifest=args.inventory_manifest)
        except ValueError as exc:
            log_and_print(f"Inventory manifest rejected: {exc}")
        finally:
            conn.close()
        return

    conn = get_connection(db_path=effective_db)
    try:
        # Resolved and validated BEFORE start_batch/token acquisition: an
        # invalid manifest must never create a stray batch record or touch
        # credentials — fail fast, with nothing durable written yet.
        if args.inventory_manifest:
            try:
                inventory_df = get_inventory_from_manifest(conn, args.inventory_manifest)
            except ValueError as exc:
                log_and_print(f"Inventory manifest rejected: {exc}")
                return
            log_and_print(f"Inventory manifest: {args.inventory_manifest} — {len(inventory_df):,} items accepted, 0 rejected")
        else:
            inventory_df = get_eligible_inventory(conn, args.inventory_uid, args.limit_items)
        log_and_print(f"Eligible inventory items to process: {len(inventory_df):,}")

        if args.resume:
            batch_id = find_resumable_batch(conn)
            if batch_id:
                log_and_print(f"Resuming batch: {batch_id}")
            else:
                log_and_print("No unfinished batch found — starting a new batch instead.")
                batch_id = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        else:
            batch_id = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

        expected_pairs = [
            (uid, marketplace_id)
            for uid in inventory_df["inventory_uid"]
            for marketplace_id in TARGETED_MARKETPLACES
        ]
        start_batch(conn, batch_id, {
            "MIN_UNIQUE_RESULTS": MIN_UNIQUE_RESULTS,
            "MAX_PAGES_PER_QUERY": MAX_PAGES_PER_QUERY,
            "MAX_RESULTS_PER_ITEM": MAX_RESULTS_PER_ITEM,
            "MAX_CALLS_PER_CHUNK": MAX_CALLS_PER_CHUNK,
            "QUOTA_SAFETY_MARGIN": QUOTA_SAFETY_MARGIN,
            "MAX_CHUNK_ITERATIONS": MAX_CHUNK_ITERATIONS,
            "marketplaces": TARGETED_MARKETPLACES,
        }, expected_pairs=expected_pairs)
        log_and_print(f"Batch ID: {batch_id}")

        load_dotenv()
        token = get_access_token(DEFAULT_SCOPE)

        summary = run_chunked_collection(conn, inventory_df=inventory_df, token=token, batch_id=batch_id, db_path=effective_db)
        conn = summary["conn"]  # may be a freshly-reopened connection — see run_chunked_collection's docstring

        log_and_print("")
        log_and_print("Run summary")
        log_and_print(f"Batch ID: {summary['batch_id']}")
        log_and_print(f"Chunks executed this invocation: {summary['chunks_executed']}")
        log_and_print(f"Stop reason: {summary['stop_reason']}")
        log_and_print(f"Last chunk ID: {summary['last_chunk_id']}")
        log_and_print(f"Batch fully processed: {summary['fully_processed']}")
        if not summary["fully_processed"]:
            log_and_print("Not all eligible work is done yet — re-run with --resume "
                           "(e.g. after the next quota reset) to continue automatically.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

"""
05b_evidence_coverage_audit.py
================================
Module 5: Historical Evidence Gap Audit.

Read-only against staging_inventory and the three match_candidates_*
tables (which must already be populated by a prior
scripts/05_generate_match_candidates.py run — this script generates NO
new candidates and modifies NO existing candidate rule). Writes only to
inventory_evidence_coverage (a computed summary, full-rebuild each run)
and, via main(), reports/unmatched_inventory_analysis.csv.

No scoring, no confidence, no accept/reject anywhere in this file.
evidence_category (A/B/C/D) is a coarse, deterministic bucket derived
from candidate PRESENCE/ABSENCE, never a numeric score.

Usage:
    python scripts/05b_evidence_coverage_audit.py
"""

import logging
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"
LOG_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"

sys.path.insert(0, str(Path(__file__).parent))
import utils  # noqa: E402

PART_NUMBER_METHODS = ("PART_NUMBER_EXACT", "BRAND_PART_NUMBER", "CALIBER_PART_NUMBER")
CALIBER_METHODS = ("CALIBER_EXACT", "BRAND_CALIBER")
COMPONENT_METHODS = ("CALIBER_COMPONENT", "BRAND_CALIBER_COMPONENT")

COVERAGE_COLUMNS = [
    "inventory_uid", "brand", "caliber", "part_number",
    "active_candidate_count", "ebay_sold_candidate_count", "vcp_candidate_count",
    "part_number_candidate_count", "caliber_candidate_count", "component_candidate_count",
    "evidence_category", "audit_run_id",
]

CANDIDATE_SOURCES = [
    ("match_candidates_active", "active_candidate_count"),
    ("match_candidates_ebay_sold", "ebay_sold_candidate_count"),
    ("match_candidates_vcp", "vcp_candidate_count"),
]


def setup_logging(log_dir: Path = LOG_DIR) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=log_dir / "05b_evidence_coverage_audit.log",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        force=True,
    )


def log_and_print(message: str = "") -> None:
    print(message)
    logging.info(message)


def get_connection(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(db_path))


def _strip_non_alnum(value) -> str:
    """Normalization-variant key: lowercase, strip everything except
    letters/digits. '13\"72' -> '1372'; '24-603-0' -> '24603'0' no —
    -> '246030'. Used ONLY for the C-category normalization-variant
    check, never for the exact-token rules in 05_generate_match_candidates.py
    (which are untouched by this file)."""
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


def get_eligible_inventory(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    df = conn.execute("""
        SELECT inventory_uid, canonical_inventory_id, brand, caliber, part_number
        FROM staging_inventory
        WHERE validation_status <> 'FAIL'
    """).df()
    return df.reset_index(drop=True)


def _in_clause(methods: tuple[str, ...]) -> str:
    return ",".join(f"'{m}'" for m in methods)


def _candidate_counts_by_method(conn: duckdb.DuckDBPyConnection, table: str) -> pd.DataFrame:
    """One row per inventory_uid with total candidate count and per-method-group counts, for one
    table. Built from the module-level *_METHODS constants (not a separately hand-maintained SQL
    literal) specifically so a new rule only ever needs to be added in one place — this query
    previously hardcoded its own copy of PART_NUMBER_METHODS, which silently drifted out of sync
    when CALIBER_PART_NUMBER (RULE 3) was added to 05_generate_match_candidates.py: its candidates
    were correctly counted in the raw per-source `total`, but invisible to every one of pn/cal/comp,
    so items whose only evidence was CALIBER_PART_NUMBER were undercounted into category B instead
    of A. Fixed by including CALIBER_PART_NUMBER in PART_NUMBER_METHODS — see
    docs/MODULE5_EVIDENCE_TIER_CONTRACT.md for why it belongs in the part-number tier, not its own."""
    return conn.execute(f"""
        SELECT
            inventory_uid,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE match_method IN ({_in_clause(PART_NUMBER_METHODS)})) AS pn,
            COUNT(*) FILTER (WHERE match_method IN ({_in_clause(CALIBER_METHODS)})) AS cal,
            COUNT(*) FILTER (WHERE match_method IN ({_in_clause(COMPONENT_METHODS)})) AS comp
        FROM {table}
        GROUP BY inventory_uid
    """).df()


def _collect_all_titles(conn: duckdb.DuckDBPyConnection) -> list[str]:
    titles: list[str] = []
    for table in ("stg_active_targeted", "stg_historical_ebay_sold", "stg_historical_vcp_aggregate"):
        rows = conn.execute(f"SELECT normalized_title FROM {table} WHERE normalized_title IS NOT NULL").fetchall()
        titles.extend(r[0] for r in rows)
    return titles


def _normalization_variant_found(identifier, titles_stripped: list[str]) -> bool:
    """True if a punctuation/space/case-insensitive-stripped version of
    `identifier` appears as a substring of a similarly-stripped title —
    i.e. the SAME identifier likely exists in the corpus, just formatted
    differently (quote marks, dashes, spaces) than the exact-token rules
    require. Requires >= 3 stripped characters to avoid trivial matches."""
    if identifier is None:
        return False
    variant = _strip_non_alnum(identifier)
    if len(variant) < 3:
        return False
    return any(variant in t for t in titles_stripped)


def build_inventory_evidence_coverage(
    conn: duckdb.DuckDBPyConnection, *, audit_run_id: str | None = None,
) -> pd.DataFrame:
    """
    Computes the inventory_evidence_coverage table. Pure read against
    staging_inventory + the three match_candidates_* tables (whatever
    they currently contain, across ALL match_run_ids — this audit does
    not filter to a single run, it reflects however many candidate-
    generation runs have accumulated). Does not call
    05_generate_match_candidates.py itself.

    evidence_category (mutually exclusive, checked in this priority order):
      A — Strong existing evidence: part_number_candidate_count > 0
          (a PART_NUMBER_EXACT, BRAND_PART_NUMBER, or CALIBER_PART_NUMBER
          candidate exists in ANY of the 3 sources — see PART_NUMBER_METHODS).
      B — Potential matching gap: no part-number candidate, but
          caliber_candidate_count > 0 or component_candidate_count > 0
          (only the weaker caliber-level signal exists).
      C — Normalization gap candidate: no candidate of any kind, but a
          punctuation/space-stripped variant of the caliber OR part
          number is found somewhere in the combined title corpus —
          i.e. the identifier likely exists in the data, just not in the
          exact token shape the current rules require.
      D — No evidence found: none of the above.
    """
    audit_run_id = audit_run_id or f"evidence_audit_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    inventory_df = get_eligible_inventory(conn)

    per_source = {}
    for table, _ in CANDIDATE_SOURCES:
        try:
            per_source[table] = _candidate_counts_by_method(conn, table)
        except duckdb.CatalogException:
            # Table doesn't exist yet in this DB (e.g. no candidate
            # generation run has happened) — treat as zero everywhere,
            # never a crash.
            per_source[table] = pd.DataFrame(columns=["inventory_uid", "total", "pn", "cal", "comp"])

    titles_stripped = [_strip_non_alnum(t) for t in _collect_all_titles(conn)]

    rows = []
    for row in inventory_df.itertuples():
        uid = row.inventory_uid
        counts = {}
        pn_total = cal_total = comp_total = 0
        for table, count_col in CANDIDATE_SOURCES:
            src = per_source[table]
            match = src[src["inventory_uid"] == uid]
            if match.empty:
                counts[count_col] = 0
            else:
                counts[count_col] = int(match["total"].iloc[0])
                pn_total += int(match["pn"].iloc[0])
                cal_total += int(match["cal"].iloc[0])
                comp_total += int(match["comp"].iloc[0])

        if pn_total > 0:
            category = "A"
        elif cal_total > 0 or comp_total > 0:
            category = "B"
        elif _normalization_variant_found(row.caliber, titles_stripped) or \
                _normalization_variant_found(row.part_number, titles_stripped):
            category = "C"
        else:
            category = "D"

        rows.append({
            "inventory_uid": uid,
            "brand": row.brand,
            "caliber": row.caliber,
            "part_number": row.part_number,
            "active_candidate_count": counts["active_candidate_count"],
            "ebay_sold_candidate_count": counts["ebay_sold_candidate_count"],
            "vcp_candidate_count": counts["vcp_candidate_count"],
            "part_number_candidate_count": pn_total,
            "caliber_candidate_count": cal_total,
            "component_candidate_count": comp_total,
            "evidence_category": category,
            "audit_run_id": audit_run_id,
        })

    return pd.DataFrame(rows, columns=COVERAGE_COLUMNS)


def write_inventory_evidence_coverage(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    """Full rebuild — deterministic given unchanged inputs, matching the
    idempotency discipline of every clean_* function in this project."""
    conn.execute("DELETE FROM inventory_evidence_coverage")
    if df.empty:
        return
    cols = COVERAGE_COLUMNS
    conn.register("tmp_coverage", df[cols])
    conn.execute(f"INSERT INTO inventory_evidence_coverage ({','.join(cols)}) SELECT {','.join(cols)} FROM tmp_coverage")
    conn.unregister("tmp_coverage")


def build_unmatched_inventory_analysis(conn: duckdb.DuckDBPyConnection, coverage_df: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 3: for every inventory item NOT in category A (i.e. B, C, or D
    — "unmatched or weakly matched"), analyze WHY, without recommending
    scraping by default.
    """
    raw_titles = _collect_all_titles(conn)
    titles_stripped = [_strip_non_alnum(t) for t in raw_titles]
    weak = coverage_df[coverage_df["evidence_category"] != "A"].copy()

    def _analyze(row) -> pd.Series:
        pn = row["part_number"]
        cal = row["caliber"]

        pn_variant_found = _normalization_variant_found(pn, titles_stripped)
        cal_variant_found = _normalization_variant_found(cal, titles_stripped)
        normalization_issue = pn_variant_found or cal_variant_found

        distinctive = utils.part_number_is_distinctive(pn)

        pn_pattern = r"\b" + re.escape(str(pn).lower()) + r"\b" if pn else None
        cal_pattern = r"\b" + re.escape(str(cal).lower()) + r"\b" if cal else None
        # NOTE: this is the exact-token search, same shape as the rule
        # engine's own matching — reused here only to COUNT mentions for
        # reporting, not to generate any candidate.
        mentions = 0
        if pn_pattern:
            mentions += sum(1 for t in raw_titles if re.search(pn_pattern, t))
        if cal_pattern:
            mentions += sum(1 for t in raw_titles if re.search(cal_pattern, t))

        if row["evidence_category"] == "B":
            if not distinctive:
                action = "PART_NUMBER_TOO_GENERIC_FOR_RELIABLE_MATCH"
            else:
                action = "REVIEW_CALIBER_LEVEL_CANDIDATES_MANUALLY"
        elif normalization_issue:
            action = "RENORMALIZE_IDENTIFIER_AND_RETRY_MATCHING"
        elif not distinctive:
            action = "PART_NUMBER_TOO_GENERIC_AND_NO_EVIDENCE_FOUND"
        elif mentions == 0:
            action = "NO_MARKET_EVIDENCE_TARGETED_EXTRACTION_CANDIDATE"
        else:
            action = "TARGETED_EXTRACTION_RECOMMENDED"

        return pd.Series({
            "possible_normalization_issue": normalization_issue,
            "historical_mentions_found": mentions,
            "recommended_action": action,
        })

    analysis = weak.apply(_analyze, axis=1)
    result = pd.concat([weak[["inventory_uid", "brand", "caliber", "part_number", "evidence_category"]], analysis], axis=1)
    return result.reset_index(drop=True)


def run_evidence_gap_audit(conn: duckdb.DuckDBPyConnection, *, reports_dir: Path = REPORTS_DIR) -> dict:
    coverage_df = build_inventory_evidence_coverage(conn)
    write_inventory_evidence_coverage(conn, coverage_df)

    unmatched_df = build_unmatched_inventory_analysis(conn, coverage_df)
    reports_dir.mkdir(parents=True, exist_ok=True)
    csv_path = reports_dir / "unmatched_inventory_analysis.csv"
    unmatched_df.to_csv(csv_path, index=False)

    counts = coverage_df["evidence_category"].value_counts().to_dict()
    return {
        "total_eligible": len(coverage_df),
        "category_counts": {c: int(counts.get(c, 0)) for c in ["A", "B", "C", "D"]},
        "unmatched_csv": str(csv_path),
        "unmatched_count": len(unmatched_df),
    }


def main() -> None:
    setup_logging()
    log_and_print("=" * 60)
    log_and_print("WATCHPARTS — STEP 5b: HISTORICAL EVIDENCE GAP AUDIT")
    log_and_print("=" * 60)

    conn = get_connection()
    try:
        summary = run_evidence_gap_audit(conn)
    finally:
        conn.close()

    log_and_print("")
    log_and_print(f"Total eligible inventory: {summary['total_eligible']:,}")
    for cat in ["A", "B", "C", "D"]:
        log_and_print(f"  Category {cat}: {summary['category_counts'][cat]:,}")
    log_and_print(f"Unmatched analysis written: {summary['unmatched_csv']} ({summary['unmatched_count']:,} rows)")
    log_and_print("✓ Audit complete. No scoring, no confidence, no new candidates generated.")


if __name__ == "__main__":
    main()

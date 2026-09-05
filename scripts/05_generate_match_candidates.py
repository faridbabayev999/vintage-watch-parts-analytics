"""
05_generate_match_candidates.py
================================
Module 5: deterministic, explainable CANDIDATE GENERATION only.

No scoring. No confidence. No accept/reject. No ML, no embeddings, no
fuzzy matching. See docs/MODULE5_MATCHING_FOUNDATION_DESIGN.md for the
full rule design and rationale.

Reads staging_inventory + the three evidence staging tables
(stg_active_targeted, stg_historical_ebay_sold, stg_historical_vcp_aggregate)
read-only. Writes only to match_run / match_candidates_active /
match_candidates_ebay_sold / match_candidates_vcp. Never touches any raw_*
table, never touches any staging table it reads from.

Usage:
    python scripts/05_generate_match_candidates.py
"""

import json
import logging
import re
import sys
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

BASE_DIR = Path(__file__).parent.parent
# DB target: WATCHPARTS_DB env var (set by the caller to point at a disposable
# copy) > default live DB. Default behaviour unchanged when unset. Lets the
# matching rebuild run against a scratch DB without touching the live one.
DEFAULT_DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"
DB_PATH = Path(os.environ["WATCHPARTS_DB"]) if os.environ.get("WATCHPARTS_DB") else DEFAULT_DB_PATH
LOG_DIR = BASE_DIR / "logs"

sys.path.insert(0, str(Path(__file__).parent))
import utils  # noqa: E402

ALGORITHM_VERSION = "v3_exact_token_rules_plus_component_vocabulary_plus_caliber_part_number"

# One entry per evidence source: (staging table, id column, candidates
# table, candidates table's source-id column, stable evidence-uid
# column). The evidence_uid column is Module 5's stable-identity join
# key (docs/MODULE5_EVIDENCE_IDENTITY_IMPLEMENTATION_CHECKLIST.md) —
# additive alongside the legacy positional id column, which is left
# untouched for backward compatibility.
SOURCES = [
    ("stg_active_targeted", "id", "match_candidates_active", "active_raw_id", "stable_evidence_uid"),
    ("stg_historical_ebay_sold", "id", "match_candidates_ebay_sold", "ebay_sold_raw_id", "stable_evidence_uid"),
    ("stg_historical_vcp_aggregate", "id", "match_candidates_vcp", "vcp_raw_id", "stable_evidence_uid"),
]

# Component vocabulary — mined from real corpus token frequency across all
# three evidence tables (7,611 titles), not hand-picked from the example
# list alone. See docs/MODULE5_COMPONENT_VOCABULARY.md for the full
# methodology and per-token frequency evidence, including the two
# categories (PLATE, JEWEL) that are explicitly flagged there as
# weakly-supported by this corpus.
COMPONENT_VOCABULARY: dict[str, list[str]] = {
    "CROWN": ["crown", "krone"],
    "LINK": ["link", "glied", "ersatzglied", "bandglied", "bracelet", "band", "strap"],
    "DIAL": ["dial", "zifferblatt"],
    "WHEEL": ["wheel", "rad", "umkehrrad", "stundenrad", "kronrad", "ankerrad", "minutenrad", "sperrrad"],
    "CRYSTAL": ["crystal", "saphirglas", "glas", "sapphire", "plexiglas", "uhrenglas"],
    "BEZEL": ["bezel", "lünette"],
    "SPRING": ["spring", "mainspring", "feder", "zugfeder", "hauptfeder"],
    "BALANCE": ["balance", "unruh", "unruhwelle"],
    "TUBE": ["tube", "tubus"],
    "HANDS": ["zeiger", "hands", "hand"],
    "LEVER": ["lever", "anker", "pallet", "fork", "winkelhebel", "hebel"],
    "SCREW": ["screw", "schraube"],
    "CLASP": ["clasp", "faltschliesse", "dornschliesse"],
    "ROTOR": ["rotor", "schwungmasse"],
    "BRIDGE": ["bridge", "brücke"],
    "BARREL": ["barrel", "federhaus"],
    "CASE": ["case", "gehäuse"],
    "RATCHET": ["ratchet", "click", "klinke"],
    "ARBOR": ["arbor", "staff", "achse", "trieb"],
    "STEM": ["stem", "welle"],
    "GASKET": ["gasket", "dichtung"],
    "PUSHER": ["pusher", "drücker"],
    "JEWEL": ["jewel", "stein", "deckstein", "lochstein"],
    "PLATE": ["plate", "platte"],
}


def setup_logging(log_dir: Path = LOG_DIR) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=log_dir / "05_generate_match_candidates.log",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        force=True,
    )


def log_and_print(message: str = "") -> None:
    print(message)
    logging.info(message)


def get_connection(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    last_exc = None
    for attempt in range(31):
        try:
            return duckdb.connect(str(db_path))
        except duckdb.IOException as exc:
            last_exc = exc
            if "lock" not in str(exc).lower() or attempt == 30:
                raise
            time.sleep(0.5)
    raise last_exc


def get_eligible_inventory_for_matching(
    conn: duckdb.DuckDBPyConnection,
    *,
    inventory_uid: str | None = None,
) -> pd.DataFrame:
    """
    validation_status <> 'FAIL' — same eligibility rule used everywhere
    else in this project (get_eligible_inventory in
    scripts/04_collect_targeted_active.py). Adds
    part_number_is_distinctive, reusing the existing utility rather than
    inventing a new distinctiveness rule for matching.
    """
    where = "WHERE validation_status <> 'FAIL'"
    params: list[str] = []
    if inventory_uid:
        where += " AND inventory_uid = ?"
        params.append(inventory_uid)
    df = conn.execute(f"""
        SELECT inventory_uid, canonical_inventory_id, brand, caliber, part_number
        FROM staging_inventory
        {where}
    """, params).df()
    df["part_number_is_distinctive"] = df["part_number"].apply(utils.part_number_is_distinctive)
    return df.reset_index(drop=True)


def _token_pattern(value) -> str:
    """Exact, word-bounded token match — never a bare substring (e.g. part
    number '123' must not match inside '12345')."""
    return r"\b" + re.escape(str(value)) + r"\b"


_COMPONENT_PATTERNS: dict[str, str] = {
    cat: r"\b(?:" + "|".join(re.escape(t) for t in toks) + r")\b"
    for cat, toks in COMPONENT_VOCABULARY.items()
}


def _annotate_component_matches(source_df: pd.DataFrame, title_col: str = "norm_title") -> pd.DataFrame:
    """
    Computed ONCE per source table (not per inventory item — the
    vocabulary doesn't depend on the item), adding:
      - any_component: True if any category's tokens matched this title
      - component_category / component_token: the FIRST matching category
        (in COMPONENT_VOCABULARY's fixed, deterministic dict order) and
        its exact matched surface form, for evidence. A title can
        genuinely match more than one category (e.g. both CROWN and
        SCREW); only the first is recorded — this is a documented
        simplification (one component field in the evidence shape, per
        spec), not an attempt to enumerate every category present.
    """
    df = source_df.copy()
    df["any_component"] = False
    df["component_category"] = None
    df["component_token"] = None
    for cat, pattern in _COMPONENT_PATTERNS.items():
        mask = df[title_col].str.contains(pattern, case=False, regex=True, na=False)
        newly = mask & ~df["any_component"]
        if newly.any():
            extracted = df.loc[newly, title_col].str.extract(f"({pattern})", flags=re.IGNORECASE)[0]
            df.loc[newly, "component_category"] = cat
            df.loc[newly, "component_token"] = extracted.str.lower()
        df["any_component"] = df["any_component"] | mask
    return df


def generate_candidates_for_source(
    conn: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    id_col: str,
    title_col: str,
    inventory_df: pd.DataFrame,
) -> list[dict]:
    """
    Applies seven deterministic rules (see design doc + component
    vocabulary doc + docs/MODULE5_RULE3_SAFEGUARD_FINAL_VALIDATION.md)
    against one evidence source table. Returns a list of {inventory_uid,
    source_id, match_method, evidence} dicts — pure computation, nothing
    written here.

    Rule scope limit (stated in the design doc, not silent): PART_NUMBER_EXACT
    and BRAND_PART_NUMBER are only evaluated when
    utils.part_number_is_distinctive(part_number) is True, to avoid a
    foreseeable noise explosion from generic part numbers. CALIBER_EXACT
    and BRAND_CALIBER have no such restriction — unchanged from before.

    CALIBER_COMPONENT and BRAND_CALIBER_COMPONENT (new, additive — the
    original four rules' logic below is untouched) additionally require a
    component-vocabulary token to be present, using the evidence shape
    specified for these two rules specifically: {rule, caliber, component,
    matched_tokens, source_id} — deliberately not the same shape as the
    original four rules' evidence.

    CALIBER_PART_NUMBER (new, additive — nothing above this point is
    touched) requires caliber AND part_number tokens both present, with
    its OWN rule-specific gate: normalized part_number length >= 3
    alphanumeric characters — deliberately a lower, separate floor from
    utils.part_number_is_distinctive's >= 5 (used only by PART_NUMBER_EXACT/
    BRAND_PART_NUMBER, unchanged). See
    docs/MODULE5_RULE3_SAFEGUARD_FINAL_VALIDATION.md for why: a full-population
    validation found the >=5 floor discards verified-genuine compound-code
    matches (e.g. "24-530-0", "25-104"), while a >=3 floor removes the
    entire confirmed false-positive pocket (measurement/quantity-prefix/
    model-edition-number token collisions, e.g. part_number "2" matching
    "GMT Master 2") at a small, bounded coverage cost. This floor is local
    to this rule only — utils.part_number_is_distinctive and every other
    rule's gating are unchanged.
    """
    # collection_inventory_uid: the inventory item whose targeted query
    # collected this evidence row. Only stg_active_targeted is collected
    # per inventory item, so only it has this column; for the historical
    # sources we select NULL so `hit.collection_inventory_uid` always
    # exists. Captured HERE, at generation time, and persisted on the
    # candidate so SELF_SOURCED/CROSS_REFERENCED never has to be
    # reconstructed from the (unstable) staging positional id at decision
    # time (docs/MODULE5_STATUS_AND_RUNBOOK.md §6).
    src_has_inventory_uid = conn.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        f"WHERE table_name = '{source_table}' AND column_name = 'inventory_uid'"
    ).fetchone()[0] > 0
    collection_select = "inventory_uid AS collection_inventory_uid" if src_has_inventory_uid else "NULL AS collection_inventory_uid"
    source_df = conn.execute(
        f"SELECT {id_col} AS src_id, stable_evidence_uid, {collection_select}, "
        f"{title_col} AS norm_title, title FROM {source_table} "
        f"WHERE {title_col} IS NOT NULL"
    ).df()
    if source_df.empty or inventory_df.empty:
        return []

    source_df = _annotate_component_matches(source_df)

    candidates: list[dict] = []

    for row in inventory_df.itertuples():
        brand, caliber, part_number = row.brand, row.caliber, row.part_number

        brand_mask = None
        if brand:
            brand_mask = source_df["norm_title"].str.contains(
                _token_pattern(str(brand).lower()), case=False, regex=True, na=False
            )

        caliber_mask = None
        if caliber:
            caliber_mask = source_df["norm_title"].str.contains(
                _token_pattern(str(caliber).lower()), case=False, regex=True, na=False
            )

        pn_mask = None
        if part_number and row.part_number_is_distinctive:
            pn_mask = source_df["norm_title"].str.contains(
                _token_pattern(str(part_number).lower()), case=False, regex=True, na=False
            )

        # RULE 3 (CALIBER_PART_NUMBER) — its own gate, separate from pn_mask
        # above: >= 3 alphanumeric characters, not utils.part_number_is_distinctive's
        # >= 5. A lower, rule-specific floor is safe here because caliber
        # co-occurrence is already required — see the function docstring and
        # docs/MODULE5_RULE3_SAFEGUARD_FINAL_VALIDATION.md for the full
        # validation this threshold is based on. pn_mask (>=5, above) is
        # untouched and still used, unchanged, by PART_NUMBER_EXACT/BRAND_PART_NUMBER.
        pn_mask_rule3 = None
        if part_number and caliber:
            pn_alnum_len = len([c for c in str(part_number) if c.isalnum()])
            if pn_alnum_len >= 3:
                pn_mask_rule3 = source_df["norm_title"].str.contains(
                    _token_pattern(str(part_number).lower()), case=False, regex=True, na=False
                )

        def _emit(mask, rule: str, inventory_value, matched_tokens: list[str]) -> None:
            if mask is None or not mask.any():
                return
            for hit in source_df[mask].itertuples():
                candidates.append({
                    "inventory_uid": row.inventory_uid,
                    "source_id": int(hit.src_id),
                    "evidence_uid": hit.stable_evidence_uid,
                    "collection_inventory_uid": hit.collection_inventory_uid,
                    "match_method": rule,
                    "evidence": {
                        "rule": rule,
                        "inventory_value": inventory_value,
                        "title": hit.title,
                        "matched_tokens": matched_tokens,
                    },
                })

        _emit(pn_mask, "PART_NUMBER_EXACT", part_number, [str(part_number)])
        _emit(caliber_mask, "CALIBER_EXACT", caliber, [str(caliber)])
        if brand_mask is not None and caliber_mask is not None:
            _emit(
                brand_mask & caliber_mask, "BRAND_CALIBER",
                {"brand": brand, "caliber": caliber}, [str(brand), str(caliber)],
            )
        if brand_mask is not None and pn_mask is not None:
            _emit(
                brand_mask & pn_mask, "BRAND_PART_NUMBER",
                {"brand": brand, "part_number": part_number}, [str(brand), str(part_number)],
            )

        # ── New, additive rule: caliber + part number (>=3 alnum floor) ─
        if caliber_mask is not None and pn_mask_rule3 is not None:
            _emit(
                caliber_mask & pn_mask_rule3, "CALIBER_PART_NUMBER",
                {"caliber": caliber, "part_number": part_number}, [str(caliber), str(part_number)],
            )

        # ── New, additive rules: caliber + component vocabulary ────────
        if caliber_mask is not None:
            caliber_component_mask = caliber_mask & source_df["any_component"]
            if caliber_component_mask.any():
                for hit in source_df[caliber_component_mask].itertuples():
                    candidates.append({
                        "inventory_uid": row.inventory_uid,
                        "source_id": int(hit.src_id),
                        "evidence_uid": hit.stable_evidence_uid,
                        "collection_inventory_uid": hit.collection_inventory_uid,
                        "match_method": "CALIBER_COMPONENT",
                        "evidence": {
                            "rule": "CALIBER_COMPONENT",
                            "caliber": caliber,
                            "component": hit.component_category,
                            "matched_tokens": [str(caliber), hit.component_token],
                            "source_id": int(hit.src_id),
                        },
                    })

            if brand_mask is not None:
                brand_caliber_component_mask = brand_mask & caliber_component_mask
                if brand_caliber_component_mask.any():
                    for hit in source_df[brand_caliber_component_mask].itertuples():
                        candidates.append({
                            "inventory_uid": row.inventory_uid,
                            "source_id": int(hit.src_id),
                            "evidence_uid": hit.stable_evidence_uid,
                            "collection_inventory_uid": hit.collection_inventory_uid,
                            "match_method": "BRAND_CALIBER_COMPONENT",
                            "evidence": {
                                "rule": "BRAND_CALIBER_COMPONENT",
                                "caliber": caliber,
                                "component": hit.component_category,
                                "matched_tokens": [str(brand), str(caliber), hit.component_token],
                                "source_id": int(hit.src_id),
                            },
                        })

    return candidates


def _next_id(conn: duckdb.DuckDBPyConnection, table: str, pk_col: str) -> int:
    row = conn.execute(f"SELECT COALESCE(MAX({pk_col}), 0) + 1 FROM {table}").fetchone()
    return row[0]


def _write_candidates(
    conn: duckdb.DuckDBPyConnection,
    *,
    candidates_table: str,
    source_id_col: str,
    match_run_id: str,
    candidates: list[dict],
) -> int:
    if not candidates:
        return 0
    df = pd.DataFrame(candidates)
    df["match_run_id"] = match_run_id
    df["evidence_json"] = df["evidence"].apply(json.dumps)
    df = df.rename(columns={"source_id": source_id_col})

    # Candidate-relationship grain fix
    # (docs/MODULE5_POST_PHASE1_ARCHITECTURE_AUDIT.md Bug 1, reproduced:
    # one real-world listing represented by multiple staging rows — e.g.
    # stg_active_targeted's (inventory_uid, item_id, marketplace) pairing
    # grain, confirmed up to 8 pairing-rows per listing — previously
    # produced one duplicate candidate PER pairing-row PER rule, because
    # dedup keyed on the positional source_id_col, which differs per
    # pairing-row even though evidence_uid is identical. Dedup on
    # evidence_uid when present (the canonical evidence identity); fall
    # back to source_id_col only for the rare row with no evidence_uid
    # (e.g. a source without a resolvable natural key).
    df["_dedup_key"] = df["evidence_uid"].fillna(df[source_id_col].astype(str))
    # Deterministic tie-break: sort by source_id_col ascending before
    # keeping "first", so which pairing-row's positional id survives in
    # the retained row is reproducible across runs, not scan-order-dependent.
    df = df.sort_values(source_id_col, kind="stable")
    df = df.drop_duplicates(subset=["inventory_uid", "_dedup_key", "match_method"], keep="first")
    df = df.drop(columns=["_dedup_key"])

    start_id = _next_id(conn, candidates_table, "match_candidate_id")
    df.insert(0, "match_candidate_id", range(start_id, start_id + len(df)))

    # evidence_uid additive alongside the legacy positional source_id_col
    # — Module 5 stable-identity join key
    # (docs/MODULE5_EVIDENCE_IDENTITY_IMPLEMENTATION_CHECKLIST.md).
    cols = ["match_candidate_id", "match_run_id", "inventory_uid", source_id_col, "evidence_uid", "match_method", "evidence_json"]
    # collection_inventory_uid only for candidate tables that have the
    # column (only match_candidates_active — the historical sources have
    # no per-inventory collection target). Additive, never dropped.
    target_cols = set(
        r[0] for r in conn.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{candidates_table}'"
        ).fetchall()
    )
    if "collection_inventory_uid" in target_cols and "collection_inventory_uid" in df.columns:
        cols.insert(cols.index("evidence_uid") + 1, "collection_inventory_uid")
    conn.register("tmp_candidates", df[cols])
    conn.execute(
        f"INSERT INTO {candidates_table} ({','.join(cols)}) "
        f"SELECT {','.join(cols)} FROM tmp_candidates "
        f"ON CONFLICT (match_run_id, inventory_uid, {source_id_col}, match_method) DO NOTHING"
    )
    conn.unregister("tmp_candidates")
    return len(df)


def run_candidate_generation(
    conn: duckdb.DuckDBPyConnection,
    *,
    match_run_id: str | None = None,
    inventory_snapshot_reference: str | None = None,
    inventory_uid: str | None = None,
) -> dict:
    """
    Orchestrates candidate generation across all three evidence sources
    for one match_run. Read-only against staging_inventory and the three
    evidence staging tables; writes only to match_run and the three
    match_candidates_* tables. Returns a summary dict.
    """
    match_run_id = match_run_id or f"match_run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    inventory_df = get_eligible_inventory_for_matching(conn, inventory_uid=inventory_uid)
    if inventory_snapshot_reference is None:
        scope = f"inventory_uid:{inventory_uid}" if inventory_uid else "all_inventory"
        inventory_snapshot_reference = f"staging_inventory:{scope}:{len(inventory_df)}_eligible_rows"

    # ON CONFLICT DO NOTHING: calling this function twice with the same
    # explicit match_run_id (e.g. a safe retry) must not raise — the
    # match_candidates_* inserts below are already safely re-runnable via
    # their own UNIQUE constraints, so this must be too.
    conn.execute(
        "INSERT INTO match_run (match_run_id, algorithm_version, inventory_snapshot_reference) VALUES (?, ?, ?) "
        "ON CONFLICT (match_run_id) DO NOTHING",
        [match_run_id, ALGORITHM_VERSION, inventory_snapshot_reference],
    )

    counts: dict[str, int] = {}
    for source_table, id_col, candidates_table, source_id_col, _evidence_uid_col in SOURCES:
        candidates = generate_candidates_for_source(
            conn, source_table=source_table, id_col=id_col, title_col="normalized_title", inventory_df=inventory_df,
        )
        n_written = _write_candidates(
            conn, candidates_table=candidates_table, source_id_col=source_id_col,
            match_run_id=match_run_id, candidates=candidates,
        )
        counts[candidates_table] = n_written
        log_and_print(f"{candidates_table}: {n_written:,} candidates generated")

    return {"match_run_id": match_run_id, "eligible_inventory_count": len(inventory_df), **counts}


def main() -> None:
    setup_logging()
    log_and_print("=" * 60)
    log_and_print("WATCHPARTS — STEP 5: GENERATE MATCH CANDIDATES")
    log_and_print("=" * 60)

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(DB_PATH))
    parser.add_argument("--inventory-uid", default=None)
    args = parser.parse_args()

    conn = get_connection(Path(args.db))
    try:
        summary = run_candidate_generation(conn, inventory_uid=args.inventory_uid)
    finally:
        conn.close()

    log_and_print("")
    log_and_print(f"Match run: {summary['match_run_id']}")
    log_and_print(f"Eligible inventory considered: {summary['eligible_inventory_count']:,}")
    log_and_print("✓ Candidate generation complete. No scoring, no confidence, no matches accepted.")


if __name__ == "__main__":
    main()

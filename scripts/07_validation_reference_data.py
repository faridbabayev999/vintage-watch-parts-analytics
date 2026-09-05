"""
07_validation_reference_data.py
=================================
Module 5: reference/documentation data for the validation-policy gate
(docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md). This file NEVER makes a
matching decision and scripts/06_decide_matches.py never imports it —
06_decide_matches.py only ever reads validation_policy,
ref_calibre_compatibility, and compatibility_policy_authorization at
decision time; this script is how (non-authoritative, non-activated)
reference rows get INTO those tables, run by a human, never automatically.

Two separate concerns, kept separate on purpose:

1. seed_threshold_policy_evaluations() — inserts the EXPLORATORY /
   INTERNAL_OPERATIONAL / COMPETITION_READY candidate standards evaluated
   in docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md Task 4, all with
   is_active=FALSE. This is a documentation record of an evaluation, not
   an activation — no code anywhere reads confirmation_threshold_policy to
   auto-approve a validation_policy segment; that would be exactly the
   "auto-approve from computed statistics" behaviour this whole gate
   exists to prevent.

2. export_validation_review_samples() — inserts the REAL, already-reviewed
   candidate rows this project's manual validation work has actually
   labelled, with their real candidate_key (computed deterministically,
   the same formula as 06_decide_matches.py's _candidate_key — no live DB
   read required), so sample membership is queryable rather than
   re-derived from a non-reproducible reseed. Two populations are
   included, both fully re-verified against the disposable copy in the
   same session that authored docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md:
     - the full 18-row (10 evidence-pair) CALIBRE_CONFLICT_EXPLICIT_LABEL_
       MISMATCH population (Task 6);
     - the full 22-row PART_NUMBER_EXACT/BRAND_PART_NUMBER reference-list-
       pattern population (Task 5), split into 8 genuine
       REFERENCE_LIST_GENUINE_AMBIGUITY rows and 14
       CALIBRE_FAMILY_LIST_NOT_REFERENCE_LIST rows (the structural
       false-positive of the simple regex check, corrected by
       distinguishing "matched token itself in the list" from "some list
       exists in the title").

   NOT included, and explicitly NOT fabricated: the ~80 remaining rows of
   the original pooled Tier A n=90 / SELF_SOURCED n=20 / CALIBER_PART_NUMBER
   n=30 samples. Their row-level identity cannot be reproduced (the
   disclosed ORDER BY-before-random.sample() gap,
   docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md Task 3) — only their
   aggregate counts survive, already recorded in
   docs/MODULE5_DECISION_LAYER_VALIDATION.md. Inventing row-level entries
   for them here would violate "do not invent labels."

Usage:
    python scripts/07_validation_reference_data.py
"""

import hashlib
import sys
from pathlib import Path

import duckdb

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"

sys.path.insert(0, str(Path(__file__).parent))


def _candidate_key(source_table: str, inventory_uid: str, source_id: int, match_method: str) -> str:
    """Identical formula to scripts/06_decide_matches.py's _candidate_key —
    duplicated deliberately (single source of truth documented, same
    convention already used for LOT_TITLE_RE/VALID_BRANDS in that file)
    so this script never needs to import decision-engine internals."""
    raw = f"{source_table}|{inventory_uid}|{source_id}|{match_method}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


# ══════════════════════════════════════════════════════════════════════════
# Phase 3 — threshold-policy CONTRACT evaluation (documented, NOT activated)
# ══════════════════════════════════════════════════════════════════════════

THRESHOLD_POLICY_EVALUATIONS = [
    {
        "threshold_policy_version": "v1_exploratory",
        "policy_purpose": "EXPLORATORY",
        "min_reviewed_sample_size": None,
        "min_observed_precision": None,
        "min_precision_lower_bound": None,
        "confidence_method": None,
        "confidence_level": None,
        "required_edge_case_representation": None,
        "max_unresolved_critical_risk_count": None,
        "notes": (
            "No gate. Point estimate + interval reported for internal analysis only, "
            "never used to justify a validation_policy APPROVED row. "
            "docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md Task 4."
        ),
    },
    {
        "threshold_policy_version": "v1_internal_operational",
        "policy_purpose": "INTERNAL_OPERATIONAL",
        "min_reviewed_sample_size": 30,
        "min_observed_precision": 0.90,
        "min_precision_lower_bound": None,
        "confidence_method": None,
        "confidence_level": None,
        "required_edge_case_representation": None,
        "max_unresolved_critical_risk_count": None,
        "notes": (
            "The historical '90% observed, n>=30' standard this project used before this "
            "task -- retained here ONLY as a labelled historical/provisional reference "
            "point, per docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md Task 2's finding that "
            "it has no statistical or stakeholder derivation. is_active=FALSE: this "
            "project's own Tier A pooled sample (76/90 strict) fails even this looser "
            "standard once ambiguous cases count as failures, so it is not proposed for "
            "use as a live gate."
        ),
    },
    {
        "threshold_policy_version": "v1_competition_ready",
        "policy_purpose": "COMPETITION_READY",
        "min_reviewed_sample_size": 50,
        "min_observed_precision": 0.95,
        "min_precision_lower_bound": 0.85,
        "confidence_method": "CLOPPER_PEARSON_EXACT",
        "confidence_level": 0.95,
        "required_edge_case_representation": (
            "SELF_SOURCED and CROSS_REFERENCED/other-source evidence in production "
            "proportion; >=1 deliberately-selected watch-reference-list case; >=1 "
            "deliberately-selected calibre-family-list case; >=1 multi-inventory-collision "
            "case; source-table breakdown proportional to real volume."
        ),
        "max_unresolved_critical_risk_count": 0,
        "notes": (
            "The candidate standard recommended in "
            "docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md Task 4, evaluated against this "
            "project's own data: no currently-measured segment clears a Clopper-Pearson "
            "95% lower bound >=85% at n>=50 with the required representation -- "
            "CALIBER_PART_NUMBER's 30/30 clears the point-estimate and lower-bound figures "
            "in isolation (88.4%) but not the n>=50 or representation requirements (its "
            "30-sample happened not to include any of the 22 caliber-family-list rows that "
            "exist in its own full population). is_active=FALSE: an evaluation record, not "
            "an activation -- no code reads this table to approve a validation_policy row."
        ),
    },
]


def seed_threshold_policy_evaluations(conn: duckdb.DuckDBPyConnection) -> int:
    existing = conn.execute("SELECT COUNT(*) FROM confirmation_threshold_policy").fetchone()[0]
    if existing:
        return 0  # idempotent: never duplicate on rerun
    start_id = 1
    for i, row in enumerate(THRESHOLD_POLICY_EVALUATIONS):
        conn.execute(
            """
            INSERT INTO confirmation_threshold_policy (
                threshold_policy_id, threshold_policy_version, policy_purpose,
                min_reviewed_sample_size, min_observed_precision, min_precision_lower_bound,
                confidence_method, confidence_level, required_edge_case_representation,
                max_unresolved_critical_risk_count, is_active, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE, ?)
            """,
            [
                start_id + i, row["threshold_policy_version"], row["policy_purpose"],
                row["min_reviewed_sample_size"], row["min_observed_precision"], row["min_precision_lower_bound"],
                row["confidence_method"], row["confidence_level"], row["required_edge_case_representation"],
                row["max_unresolved_critical_risk_count"], row["notes"],
            ],
        )
    return len(THRESHOLD_POLICY_EVALUATIONS)


# ══════════════════════════════════════════════════════════════════════════
# Phase 9 — review dataset lineage (REAL labels only, never invented)
# ══════════════════════════════════════════════════════════════════════════

CALIBRE_CONFLICT_REASONS = {
    "CONFLICT_CORRECT": (
        "Explicitly labelled evidence caliber genuinely differs from the inventory item's "
        "own caliber; no compatibility signal present in the same title. Correct NO_MATCH."
    ),
    "CONFLICT_AMBIGUOUS_CROSS_COMPATIBLE": (
        "Evidence caliber differs from the inventory item's own labelled caliber, but the "
        "title also carries either explicit compatibility language ('kompatibel mit') or a "
        "multi-caliber list -- Rolex 3130/3135-family relationship asserted by the reviewer "
        "from general domain familiarity, REVIEWER_INFERENCE_ONLY, not a cited project "
        "source. docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md Task 6."
    ),
    "CONFLICT_INCORRECT_TOKENIZATION": (
        "Inventory caliber '315' appears in the title only glued to a prefix letter "
        "('B315-16750-6-A1'); the word-boundary token check correctly does not read this "
        "as a bare '315' occurrence, so the listing's own different explicit label wins -- "
        "a false NO_MATCH via tokenization, not a genuine mismatch or a compatibility claim "
        "(docs/MODULE5_RISK_REGISTER.md risk #11)."
    ),
}

REFERENCE_LIST_REASONS = {
    "REFERENCE_LIST_GENUINE_AMBIGUITY": (
        "The matched part_number is itself one of 3+ separate 4-6 digit numbers in a "
        "'/' or ','-joined run -- a genuine watch-model-reference compatibility list, not a "
        "distinct spare-parts catalog code. docs/MODULE5_RISK_REGISTER.md risk #9."
    ),
    "CALIBRE_FAMILY_LIST_NOT_REFERENCE_LIST": (
        "Title contains a 3+-number list, but the matched part_number is a distinguishable "
        "hyphenated catalog code (e.g. '3135-250') separate from a caliber-family "
        "compatibility list elsewhere in the same title -- NOT a Risk #9 instance; a "
        "structural false-positive of a title-level-only pattern check, corrected in "
        "detect_multiple_reference_list_risk by requiring the matched token itself to be a "
        "bare list member."
    ),
}

# Data below re-verified against the disposable copy
# (/tmp/watchparts_disposable/module5_decisions/watchparts_dec.duckdb,
# decision_run_id='real_dec2') in the same session that authored
# docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md. candidate_key values are
# RECOMPUTED here via _candidate_key(), not copy-pasted, so they are
# guaranteed consistent with whatever database this script is run against
# (as long as the same source_table/inventory_uid/source_id/matching_rule
# identify the same evidence row -- true for both the disposable copy and
# the live database, which share the same source data).

CALIBRE_CONFLICT_POPULATION = [
    ("iuid_e74743144dbc4bd0", "match_candidates_active", 346, "PART_NUMBER_EXACT", "CROSS_REFERENCED",
     "Zifferblatt Zifferblatt Mickey Mouse Air King Ref 14000 14210 114200 Cal. 3000 - 3130", "CONFLICT_CORRECT"),
    ("iuid_7aa67be129b54573", "match_candidates_ebay_sold", 2650, "BRAND_PART_NUMBER", "NOT_APPLICABLE",
     "Uhrwerk Teil FIT 3135-510 - kompatibel mit Rolex Cal. 3135", "CONFLICT_AMBIGUOUS_CROSS_COMPATIBLE"),
    ("iuid_5658896e27f04386", "match_candidates_vcp", 540, "PART_NUMBER_EXACT", "NOT_APPLICABLE",
     "ORIGINAL ROLEX ZIFFERBLATT SUBMARINER DATE  16610 16800 40MM CAL 3135 3035 DIAL", "CONFLICT_CORRECT"),
    ("iuid_9e3755a830294dce", "match_candidates_vcp", 2458, "BRAND_PART_NUMBER", "NOT_APPLICABLE",
     "ROLEX Krone Crown Datejust 16200 36 mm GMT 16710 16700 Explorer II cal 3135 6 mm", "CONFLICT_CORRECT"),
    ("iuid_7aa67be129b54573", "match_candidates_ebay_sold", 2650, "PART_NUMBER_EXACT", "NOT_APPLICABLE",
     "Uhrwerk Teil FIT 3135-510 - kompatibel mit Rolex Cal. 3135", "CONFLICT_AMBIGUOUS_CROSS_COMPATIBLE"),
    ("iuid_7aa67be129b54573", "match_candidates_ebay_sold", 469, "PART_NUMBER_EXACT", "NOT_APPLICABLE",
     "ROLEX 3135 Führungsrad der Spule Cod. 3135-510 Kaliber: 3155, 3156, 3...", "CONFLICT_AMBIGUOUS_CROSS_COMPATIBLE"),
    ("iuid_508b99ae36244254", "match_candidates_ebay_sold", 307, "PART_NUMBER_EXACT", "NOT_APPLICABLE",
     "ROLEX GMT PEPSI B315-16750-6-A1 CAL. 3075 EINSATZ RING LÜNETTE NEU S5", "CONFLICT_INCORRECT_TOKENIZATION"),
    ("iuid_9e3755a830294dce", "match_candidates_vcp", 2458, "PART_NUMBER_EXACT", "NOT_APPLICABLE",
     "ROLEX Krone Crown Datejust 16200 36 mm GMT 16710 16700 Explorer II cal 3135 6 mm", "CONFLICT_CORRECT"),
    ("iuid_a541f0885f7d4c1a", "match_candidates_ebay_sold", 1311, "PART_NUMBER_EXACT", "NOT_APPLICABLE",
     "Original Rolex Kaliber 3135-410 Hemmungsrad", "CONFLICT_AMBIGUOUS_CROSS_COMPATIBLE"),
    ("iuid_a541f0885f7d4c1a", "match_candidates_ebay_sold", 1311, "BRAND_PART_NUMBER", "NOT_APPLICABLE",
     "Original Rolex Kaliber 3135-410 Hemmungsrad", "CONFLICT_AMBIGUOUS_CROSS_COMPATIBLE"),
    ("iuid_e74743144dbc4bd0", "match_candidates_ebay_sold", 2112, "PART_NUMBER_EXACT", "NOT_APPLICABLE",
     "Zifferblatt Zifferblatt Mickey Mouse Air King Ref 14000 14210 114200 Cal. 3000 - 3130", "CONFLICT_CORRECT"),
    ("iuid_ff8c7cdb959e4147", "match_candidates_vcp", 104, "PART_NUMBER_EXACT", "NOT_APPLICABLE",
     "ROLEX Milgauss 116400 cal 3131 Brücke 130 Ersatzteile Bridge Parts Lot original", "CONFLICT_CORRECT"),
    ("iuid_508b99ae36244254", "match_candidates_ebay_sold", 307, "BRAND_PART_NUMBER", "NOT_APPLICABLE",
     "ROLEX GMT PEPSI B315-16750-6-A1 CAL. 3075 EINSATZ RING LÜNETTE NEU S5", "CONFLICT_INCORRECT_TOKENIZATION"),
    ("iuid_7aa67be129b54573", "match_candidates_ebay_sold", 469, "BRAND_PART_NUMBER", "NOT_APPLICABLE",
     "ROLEX 3135 Führungsrad der Spule Cod. 3135-510 Kaliber: 3155, 3156, 3...", "CONFLICT_AMBIGUOUS_CROSS_COMPATIBLE"),
    ("iuid_ff8c7cdb959e4147", "match_candidates_vcp", 104, "BRAND_PART_NUMBER", "NOT_APPLICABLE",
     "ROLEX Milgauss 116400 cal 3131 Brücke 130 Ersatzteile Bridge Parts Lot original", "CONFLICT_CORRECT"),
    ("iuid_27cab933dde24bfd", "match_candidates_vcp", 540, "BRAND_PART_NUMBER", "NOT_APPLICABLE",
     "ORIGINAL ROLEX ZIFFERBLATT SUBMARINER DATE  16610 16800 40MM CAL 3135 3035 DIAL", "CONFLICT_CORRECT"),
    ("iuid_5658896e27f04386", "match_candidates_vcp", 540, "BRAND_PART_NUMBER", "NOT_APPLICABLE",
     "ORIGINAL ROLEX ZIFFERBLATT SUBMARINER DATE  16610 16800 40MM CAL 3135 3035 DIAL", "CONFLICT_CORRECT"),
    ("iuid_27cab933dde24bfd", "match_candidates_vcp", 540, "PART_NUMBER_EXACT", "NOT_APPLICABLE",
     "ORIGINAL ROLEX ZIFFERBLATT SUBMARINER DATE  16610 16800 40MM CAL 3135 3035 DIAL", "CONFLICT_CORRECT"),
]

REFERENCE_LIST_POPULATION = [
    ("iuid_9e3755a830294dce", "match_candidates_ebay_sold", 603, "PART_NUMBER_EXACT", "NOT_APPLICABLE",
     "Rolex GMT-Master Zifferblatt schwarz „Swiss only“ dial, 16700, 16710, 16750,", "REFERENCE_LIST_GENUINE_AMBIGUITY"),
    ("iuid_e74743144dbc4bd0", "match_candidates_ebay_sold", 1425, "PART_NUMBER_EXACT", "NOT_APPLICABLE",
     "Rolex Air King Silver Tritium Dial 14000/14010/114200/114234", "REFERENCE_LIST_GENUINE_AMBIGUITY"),
    ("iuid_e74743144dbc4bd0", "match_candidates_ebay_sold", 2705, "PART_NUMBER_EXACT", "NOT_APPLICABLE",
     "Rolex Air King Silver Luminova Dial 14000/14010/114200/114234", "REFERENCE_LIST_GENUINE_AMBIGUITY"),
    ("iuid_30ddfd9f9033417c", "match_candidates_vcp", 939, "BRAND_PART_NUMBER", "NOT_APPLICABLE",
     "Original ROLEX Feder für Stoßsicherung B95019-4-Y5 für 3035, 3135, 4130, 1120", "CALIBRE_FAMILY_LIST_NOT_REFERENCE_LIST"),
    ("iuid_30ddfd9f9033417c", "match_candidates_vcp", 939, "PART_NUMBER_EXACT", "NOT_APPLICABLE",
     "Original ROLEX Feder für Stoßsicherung B95019-4-Y5 für 3035, 3135, 4130, 1120", "CALIBRE_FAMILY_LIST_NOT_REFERENCE_LIST"),
    ("iuid_3ac89cf66ea44c0c", "match_candidates_active", 1564, "BRAND_PART_NUMBER", "CROSS_REFERENCED",
     "ROLEX 3135 Setting Wheel Cod. 3135-250 Calib: 3130, 3135, 3136, 3155, 3156, 3...", "CALIBRE_FAMILY_LIST_NOT_REFERENCE_LIST"),
    ("iuid_3ac89cf66ea44c0c", "match_candidates_active", 314, "BRAND_PART_NUMBER", "CROSS_REFERENCED",
     "ROLEX 3135 Überweisung Cod. 3135-250 Kaliber: 3130, 3135, 3136, 3155, 3156, 3175, 31...", "CALIBRE_FAMILY_LIST_NOT_REFERENCE_LIST"),
    ("iuid_e74743144dbc4bd0", "match_candidates_ebay_sold", 2570, "BRAND_PART_NUMBER", "NOT_APPLICABLE",
     "Rolex Air King Champagner Dial 14000/14010/114200/114234", "REFERENCE_LIST_GENUINE_AMBIGUITY"),
    ("iuid_e74743144dbc4bd0", "match_candidates_ebay_sold", 2705, "BRAND_PART_NUMBER", "NOT_APPLICABLE",
     "Rolex Air King Silver Luminova Dial 14000/14010/114200/114234", "REFERENCE_LIST_GENUINE_AMBIGUITY"),
    ("iuid_b5dcdea9f1494fd5", "match_candidates_ebay_sold", 1964, "BRAND_PART_NUMBER", "NOT_APPLICABLE",
     "ROLEX 3135 Aufladeritzel Cod. 3135-204 Kaliber: 3130, 3135, 3136, 3155, 315...", "CALIBRE_FAMILY_LIST_NOT_REFERENCE_LIST"),
    ("iuid_e74743144dbc4bd0", "match_candidates_ebay_sold", 2570, "PART_NUMBER_EXACT", "NOT_APPLICABLE",
     "Rolex Air King Champagner Dial 14000/14010/114200/114234", "REFERENCE_LIST_GENUINE_AMBIGUITY"),
    ("iuid_e74743144dbc4bd0", "match_candidates_ebay_sold", 1425, "BRAND_PART_NUMBER", "NOT_APPLICABLE",
     "Rolex Air King Silver Tritium Dial 14000/14010/114200/114234", "REFERENCE_LIST_GENUINE_AMBIGUITY"),
    ("iuid_b5dcdea9f1494fd5", "match_candidates_ebay_sold", 1964, "PART_NUMBER_EXACT", "NOT_APPLICABLE",
     "ROLEX 3135 Aufladeritzel Cod. 3135-204 Kaliber: 3130, 3135, 3136, 3155, 315...", "CALIBRE_FAMILY_LIST_NOT_REFERENCE_LIST"),
    ("iuid_9e3755a830294dce", "match_candidates_ebay_sold", 603, "BRAND_PART_NUMBER", "NOT_APPLICABLE",
     "Rolex GMT-Master Zifferblatt schwarz „Swiss only“ dial, 16700, 16710, 16750,", "REFERENCE_LIST_GENUINE_AMBIGUITY"),
    ("iuid_fc446ebae42e4d29", "match_candidates_ebay_sold", 1113, "PART_NUMBER_EXACT", "NOT_APPLICABLE",
     "ROLEX 3135 Umlenkwaage Cod. 3135-266 Calib: 3155, 3156, 3175, 3185 (OT...", "CALIBRE_FAMILY_LIST_NOT_REFERENCE_LIST"),
    ("iuid_fc446ebae42e4d29", "match_candidates_ebay_sold", 1113, "BRAND_PART_NUMBER", "NOT_APPLICABLE",
     "ROLEX 3135 Umlenkwaage Cod. 3135-266 Calib: 3155, 3156, 3175, 3185 (OT...", "CALIBRE_FAMILY_LIST_NOT_REFERENCE_LIST"),
    ("iuid_3ac89cf66ea44c0c", "match_candidates_active", 315, "PART_NUMBER_EXACT", "CROSS_REFERENCED",
     "ROLEX 3135 Setting Wheel Cod. 3135-250 Calib: 3130, 3135, 3136, 3155, 3156, 3...", "CALIBRE_FAMILY_LIST_NOT_REFERENCE_LIST"),
    ("iuid_3ac89cf66ea44c0c", "match_candidates_active", 1563, "PART_NUMBER_EXACT", "CROSS_REFERENCED",
     "ROLEX 3135 Überweisung Cod. 3135-250 Kaliber: 3130, 3135, 3136, 3155, 3156, 3175, 31...", "CALIBRE_FAMILY_LIST_NOT_REFERENCE_LIST"),
    ("iuid_3ac89cf66ea44c0c", "match_candidates_active", 314, "PART_NUMBER_EXACT", "CROSS_REFERENCED",
     "ROLEX 3135 Überweisung Cod. 3135-250 Kaliber: 3130, 3135, 3136, 3155, 3156, 3175, 31...", "CALIBRE_FAMILY_LIST_NOT_REFERENCE_LIST"),
    ("iuid_3ac89cf66ea44c0c", "match_candidates_active", 1563, "BRAND_PART_NUMBER", "CROSS_REFERENCED",
     "ROLEX 3135 Überweisung Cod. 3135-250 Kaliber: 3130, 3135, 3136, 3155, 3156, 3175, 31...", "CALIBRE_FAMILY_LIST_NOT_REFERENCE_LIST"),
    ("iuid_3ac89cf66ea44c0c", "match_candidates_active", 315, "BRAND_PART_NUMBER", "CROSS_REFERENCED",
     "ROLEX 3135 Setting Wheel Cod. 3135-250 Calib: 3130, 3135, 3136, 3155, 3156, 3...", "CALIBRE_FAMILY_LIST_NOT_REFERENCE_LIST"),
    ("iuid_3ac89cf66ea44c0c", "match_candidates_active", 1564, "PART_NUMBER_EXACT", "CROSS_REFERENCED",
     "ROLEX 3135 Setting Wheel Cod. 3135-250 Calib: 3130, 3135, 3136, 3155, 3156, 3...", "CALIBRE_FAMILY_LIST_NOT_REFERENCE_LIST"),
]


def export_validation_review_samples(conn: duckdb.DuckDBPyConnection) -> int:
    existing = conn.execute("SELECT COUNT(*) FROM validation_review_samples").fetchone()[0]
    if existing:
        return 0  # idempotent: never duplicate on rerun
    inserted = 0
    next_id = 1
    for uid, source_table, source_id, rule, colrel, title, label in CALIBRE_CONFLICT_POPULATION:
        key = _candidate_key(source_table, uid, source_id, rule)
        conn.execute(
            """
            INSERT INTO validation_review_samples (
                reviewed_case_id, validation_sample_version, candidate_key, inventory_uid,
                source_table, source_id, matching_rule, evidence_tier, collection_relationship,
                evidence_text, contradiction_flags, reviewer_label, reviewer_reason, reviewed_at
            ) VALUES (?, 'phase8_calibre_conflict_full_population_n18', ?, ?, ?, ?, ?, 'A', ?, ?, 'calibre_conflict', ?, ?, current_timestamp)
            """,
            [next_id, key, uid, source_table, source_id, rule, colrel, title, label, CALIBRE_CONFLICT_REASONS[label]],
        )
        next_id += 1
        inserted += 1
    for uid, source_table, source_id, rule, colrel, title, label in REFERENCE_LIST_POPULATION:
        key = _candidate_key(source_table, uid, source_id, rule)
        conn.execute(
            """
            INSERT INTO validation_review_samples (
                reviewed_case_id, validation_sample_version, candidate_key, inventory_uid,
                source_table, source_id, matching_rule, evidence_tier, collection_relationship,
                evidence_text, reviewer_label, reviewer_reason, reviewed_at
            ) VALUES (?, 'task5_reference_list_structural_check_n22', ?, ?, ?, ?, ?, 'A', ?, ?, ?, ?, current_timestamp)
            """,
            [next_id, key, uid, source_table, source_id, rule, colrel, title, label, REFERENCE_LIST_REASONS[label]],
        )
        next_id += 1
        inserted += 1
    return inserted


def main() -> None:
    conn = duckdb.connect(str(DB_PATH))
    try:
        n1 = seed_threshold_policy_evaluations(conn)
        n2 = export_validation_review_samples(conn)
        print(f"confirmation_threshold_policy: {n1} rows inserted (0 = already seeded, idempotent)")
        print(f"validation_review_samples: {n2} rows inserted (0 = already seeded, idempotent)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

"""
06_decide_matches.py
======================
Module 5: deterministic MATCHING-DECISION layer.

Answers exactly one question, per candidate row: "does this evidence row
correspond to this inventory item?" It never answers "is this price usable
for TMV?" — see docs/MODULE5_DECISION_LAYER_DESIGN.md for the full
semantic contract this file implements.

Reads staging_inventory + the three match_candidates_* tables read-only, plus
(also read-only) validation_policy, ref_calibre_compatibility, and
compatibility_policy_authorization — the validation-policy gate and
calibre-compatibility governance layer (docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md).
Writes only to match_decisions (one row per distinct (source, inventory_uid,
source_id, matching_rule) candidate — full-rebuild each run, same
idempotency discipline as clean_*/05b) and inventory_match_summary (derived
purely from match_decisions, also full-rebuild). Never touches raw_*,
stg_*, match_candidates_*, match_run, validation_policy,
ref_calibre_compatibility, or compatibility_policy_authorization.

No scoring. No confidence number. No TMV, price weighting, or acceptance
of price data anywhere in this file — price_evidence_status is a
structural placeholder column only (see PRICE_EVIDENCE_STATUS_PLACEHOLDER
below); real price-eligibility rules are explicitly out of scope.

MATCH_CONFIRMED requires BOTH deterministic identity checks passing AND the
candidate's validation_segment being APPROVED in validation_policy — a
validation-policy gate, not a statistical auto-approval: this file never
computes precision/confidence itself and never auto-approves a segment from
observed data. A clean Tier A candidate whose segment is not APPROVED
(VALIDATION_PENDING by default — no segment is approved in this codebase as
shipped) receives REVIEW_REQUIRED / AUTO_CONFIRM_POLICY_NOT_VALIDATED, with
deterministic_checks_passed=TRUE preserving its technical strength
separately from the operational (policy-gated) decision.

Usage:
    python scripts/06_decide_matches.py
"""

import argparse
import hashlib
import logging
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

BASE_DIR = Path(__file__).parent.parent
# DB target: WATCHPARTS_DB env var (set by the caller to point at a disposable
# copy) > default live DB. Default behaviour unchanged when unset.
DEFAULT_DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"
DB_PATH = Path(os.environ["WATCHPARTS_DB"]) if os.environ.get("WATCHPARTS_DB") else DEFAULT_DB_PATH
LOG_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"

sys.path.insert(0, str(Path(__file__).parent))
import utils  # noqa: E402
import importlib.util as _ilu  # noqa: E402
_v2_spec = _ilu.spec_from_file_location("matching_v2_verification", Path(__file__).parent / "20_matching_v2_verification.py")
matching_v2 = _ilu.module_from_spec(_v2_spec)
_v2_spec.loader.exec_module(matching_v2)

DECISION_VERSION = "v1_deterministic_conflict_risk_rules"

# ── Tier membership (docs/MODULE5_EVIDENCE_TIER_CONTRACT.md) ────────────────
TIER_A_METHODS = ("PART_NUMBER_EXACT", "BRAND_PART_NUMBER", "CALIBER_PART_NUMBER")
TIER_B_METHODS = ("CALIBER_EXACT", "BRAND_CALIBER")
TIER_C_METHODS = ("CALIBER_COMPONENT", "BRAND_CALIBER_COMPONENT")

MATCHED_FIELDS_BY_RULE = {
    "PART_NUMBER_EXACT": ("part_number",),
    "BRAND_PART_NUMBER": ("brand", "part_number"),
    "CALIBER_PART_NUMBER": ("caliber", "part_number"),
    "CALIBER_EXACT": ("caliber",),
    "BRAND_CALIBER": ("brand", "caliber"),
    "CALIBER_COMPONENT": ("caliber", "component"),
    "BRAND_CALIBER_COMPONENT": ("brand", "caliber", "component"),
}

RULE_PRECISION_REFERENCE = {
    "PART_NUMBER_EXACT": "~95% (n=20, docs/MODULE5_EVIDENCE_AUDIT_VERIFICATION.md)",
    "BRAND_PART_NUMBER": "100% (15/15, n=15, measured in this project's session history)",
    "CALIBER_PART_NUMBER": "~93.3% (n=30, docs/MODULE5_RULE3_SAFEGUARD_FINAL_VALIDATION.md)",
    "CALIBER_EXACT": "~75% (9/12, n=12, measured in this project's session history)",
    "BRAND_CALIBER": "~67% (8/12, n=12, measured in this project's session history)",
    "CALIBER_COMPONENT": "~20% (n=20, docs/MODULE5_EVIDENCE_AUDIT_VERIFICATION.md)",
    "BRAND_CALIBER_COMPONENT": "~20% (inferred, same structural mechanism as CALIBER_COMPONENT — not independently sampled)",
}

# Sources with their (candidates table, source-id column, evidence table).
SOURCES = [
    ("match_candidates_active", "active_raw_id", "stg_active_targeted"),
    ("match_candidates_ebay_sold", "ebay_sold_raw_id", "stg_historical_ebay_sold"),
    ("match_candidates_vcp", "vcp_raw_id", "stg_historical_vcp_aggregate"),
]

VALID_BRANDS = {"Rolex", "Tudor"}  # mirrors scripts/02_clean.py's VALID_BRANDS — not imported
# directly to avoid a cross-script coupling; kept identical and reused deliberately, not reinvented.

# Reused verbatim from scripts/02_clean.py's LOT_TITLE_RE (not imported — 02_clean.py has no public
# module surface for this constant; duplicated intentionally, single source of truth is documented).
LOT_TITLE_RE = re.compile(
    r"(?:\b\d+\s*x\b|\bbundle\b|\bjob\s*lot\b|\bkonvolut\b|\bsammlung\b|\blot\s+of\b|\bx\s*rolex\b)",
    re.IGNORECASE,
)

COMPATIBILITY_RE = re.compile(
    r"\b(?:fits?|for|compatible|passend\s*f(?:ü|u)r|kompatibel)\b", re.IGNORECASE,
)

# Watch-reference-list ambiguity (docs/MODULE5_RISK_REGISTER.md risk #9):
# 3+ separate 4-6 digit numbers joined by '/' or ',' is the shape of a
# compatible-watch-reference list (e.g. "14000/14010/114200/114234"), not a
# spare-parts catalog code. A single hyphenated compound (e.g. "3135-410")
# never matches this -- only ONE 4-6 digit number is present in a compound,
# so requirement 4 ("avoid flagging a single compound part number as a
# reference list") is satisfied structurally, not by a separate carve-out.
REFERENCE_LIST_RUN_RE = re.compile(r"\d{4,6}(?:\s*[/,]\s*\d{4,6}){2,}")

# Multiple-calibre-list ambiguity (docs/MODULE5_RISK_REGISTER.md risk #10):
# a caliber label immediately followed by 2+ DISTINCT numbers (e.g.
# "Kaliber: 1520, 1530, 1535...") is a caliber-family compatibility list,
# not a single differing caliber. Deliberately brand/family-agnostic --
# no specific caliber values are ever embedded here (see decide()'s
# calibre_conflict downgrade logic, which relies on this generic shape,
# never on a hardcoded Rolex/any-brand family table). Separator restricted
# to ','/'/' only (NOT '-'): a hyphen-joined pair right after a caliber
# label (e.g. 'Kaliber 3135-410') is a single compound catalog code, the
# same compound-code shape already recognized in
# detect_longer_identifier_collision -- not a second list member. Every
# genuine multi-calibre-list example observed in this project's data uses
# comma separation (e.g. 'Kaliber: 1520, 1530, 1535, 1555, 1556').
CALIBRE_LIST_RUN_RE = re.compile(
    r"\b(?:cal\.?|calibre|caliber|kaliber|calib)\s*:?\s*(\d{2,4}(?:\s*[,/]\s*\d{2,4}){1,})",
    re.IGNORECASE,
)

MODEL_WORDS = (
    "explorer", "submariner", "gmt", "master", "daytona", "datejust",
    "milgauss", "yacht-master", "yachtmaster", "sea-dweller", "seadweller",
    "day-date", "daydate", "oysterdate", "cosmograph", "turnograph",
)

CALIBRE_LABEL_RE = re.compile(r"\b(?:cal\.?|calibre|caliber|kaliber)\s*:?\s*(\d{2,4})\b", re.IGNORECASE)

PRICE_EVIDENCE_STATUS_PLACEHOLDER = "NOT_APPLICABLE"  # every row this task writes; see module docstring


def setup_logging(log_dir: Path = LOG_DIR) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=log_dir / "06_decide_matches.log",
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


def _tier_of(match_method: str) -> str:
    if match_method in TIER_A_METHODS:
        return "A"
    if match_method in TIER_B_METHODS:
        return "B"
    if match_method in TIER_C_METHODS:
        return "C"
    raise ValueError(f"Unknown match_method for tier lookup: {match_method!r}")


def _token_pattern(value) -> str:
    return r"\b" + re.escape(str(value).lower()) + r"\b"


def _alnum_len(value) -> int:
    if value is None:
        return 0
    return len([c for c in str(value) if c.isalnum()])


# ══════════════════════════════════════════════════════════════════════════
# CONTRADICTION FLAGS — affirmative evidence the relationship is incompatible.
# Each returns (bool, reason_code | None). Deterministic, regex/rule-based —
# no scoring, no ML. See docs/MODULE5_DECISION_LAYER_DESIGN.md Phase 2 for
# the full rationale and known limitations of every flag below.
# ══════════════════════════════════════════════════════════════════════════

def detect_brand_conflict(title_lower: str, inventory_brand: str) -> tuple[bool, str | None]:
    """True only if an explicit OTHER known brand appears AND the item's own
    brand does not — a title mentioning both (a cross-compatible listing) is
    not a conflict. Limitation: only covers this project's 2 known brands
    (Rolex/Tudor); cannot detect a conflict against any brand outside that
    vocabulary."""
    if not inventory_brand:
        return False, None
    own = str(inventory_brand)
    others = VALID_BRANDS - {own}
    own_present = bool(re.search(_token_pattern(own), title_lower))
    other_present = any(re.search(_token_pattern(o), title_lower) for o in others)
    if other_present and not own_present:
        return True, "BRAND_CONFLICT_OTHER_BRAND_PRESENT_OWN_ABSENT"
    return False, None


def detect_calibre_conflict(title_lower: str, inventory_caliber: str) -> tuple[bool, str | None, list]:
    """True only if an EXPLICITLY LABELLED caliber reference ('cal./caliber/
    kaliber <digits>') differs from the item's own caliber AND the item's own
    caliber token does not otherwise appear anywhere in the title. Limitation:
    only catches labelled mentions — an unlabelled differing number is never
    treated as a caliber conflict, to avoid false positives from unrelated
    digit strings. Returns the list of conflicting labelled numbers as a
    third element -- consulted only by resolve_calibre_conflict's
    verified+authorized compatibility lookup (Phase 6A); this raw detector
    itself embeds no compatibility knowledge."""
    if not inventory_caliber:
        return False, None, []
    own = str(inventory_caliber)
    own_present = bool(re.search(_token_pattern(own), title_lower))
    if own_present:
        return False, None, []
    labels = CALIBRE_LABEL_RE.findall(title_lower)
    conflicting = [c for c in labels if c != own]
    if conflicting:
        return True, "CALIBRE_CONFLICT_EXPLICIT_LABEL_MISMATCH", conflicting
    return False, None, []


def _query_cross_evidence_calibre_corroboration(
    conn: duckdb.DuckDBPyConnection, *, part_number: str, inventory_caliber: str, conflicting_labels: list,
    exclude_source_table: str, exclude_source_id: int,
) -> bool:
    """DB-backed lookup for detect_cross_evidence_calibre_corroboration --
    queried lazily, only on the rare row where calibre_conflict actually
    fires (~14-20 times in a real 374k-row run), so a live SQL scan per
    occurrence is cheaper than pre-loading every evidence title into memory
    for a check that almost never runs.

    CANONICAL EVIDENCE IDENTITY (not row/id identity) -- required so a
    corroborating hit represents a genuinely different real-world listing,
    never the same listing re-collected under a different collection-target
    inventory_uid or a different duplicate-title snapshot row:
      - stg_active_targeted: item_id is the real eBay listing identity;
        the SAME item_id is confirmed, empirically, to recur under
        multiple distinct `id`/inventory_uid rows (one per collection-target
        query that happened to surface it) -- deduplicated by item_id here,
        one title kept per distinct item_id.
      - stg_historical_vcp_aggregate: duplicate_group_id is this table's
        own, already-established identity for duplicate-title snapshot
        rows (see schema.sql's 'may overlap across duplicate-title
        snapshot rows' comment) -- deduplicated by
        COALESCE(duplicate_group_id, CAST(id AS VARCHAR)) so a row with no
        group assigned still counts as its own singleton identity.
      - stg_historical_ebay_sold: item_number is a verified-unique natural
        key per this table's own schema comment -- no duplication is
        possible by construction, so a plain per-row selection is already
        canonical.
    The row under evaluation is explicitly excluded by (source_table,
    source_id) so it can never corroborate itself, defensively, even though
    detect_calibre_conflict's own own_present precondition already makes
    genuine self-corroboration structurally impossible today."""
    pn_like = f"%{str(part_number).lower()}%"
    titles: list[str] = []

    active_rows = conn.execute(
        "SELECT id, item_id, normalized_title FROM stg_active_targeted WHERE normalized_title LIKE ?", [pn_like]
    ).fetchall()
    seen_item_ids = set()
    for row_id, item_id, title in active_rows:
        if exclude_source_table == "match_candidates_active" and row_id == exclude_source_id:
            continue
        if not title:
            continue
        key = item_id if item_id else f"__no_item_id_row_{row_id}"
        if key in seen_item_ids:
            continue
        seen_item_ids.add(key)
        titles.append(title)

    vcp_rows = conn.execute(
        "SELECT id, duplicate_group_id, normalized_title FROM stg_historical_vcp_aggregate WHERE normalized_title LIKE ?",
        [pn_like],
    ).fetchall()
    seen_group_ids = set()
    for row_id, dup_group, title in vcp_rows:
        if exclude_source_table == "match_candidates_vcp" and row_id == exclude_source_id:
            continue
        if not title:
            continue
        key = dup_group if dup_group else f"__no_group_row_{row_id}"
        if key in seen_group_ids:
            continue
        seen_group_ids.add(key)
        titles.append(title)

    ebay_sold_rows = conn.execute(
        "SELECT id, normalized_title FROM stg_historical_ebay_sold WHERE normalized_title LIKE ?", [pn_like]
    ).fetchall()
    for row_id, title in ebay_sold_rows:
        if exclude_source_table == "match_candidates_ebay_sold" and row_id == exclude_source_id:
            continue
        if title:
            titles.append(title)

    hit, _ = detect_cross_evidence_calibre_corroboration(
        titles, inventory_caliber=inventory_caliber, part_number=part_number, conflicting_labels=conflicting_labels,
    )
    return hit


def detect_cross_evidence_calibre_corroboration(
    all_evidence_titles: list, *, inventory_caliber: str, part_number: str, conflicting_labels: list,
) -> tuple[bool, str | None]:
    """True if, ANYWHERE ELSE in this run's full evidence corpus (any source),
    a title exists that mentions the SAME part_number AND the item's OWN
    caliber AND one of the SAME conflicting evidence_calibre labels found on
    the row under evaluation -- i.e. independent, real listings for the same
    catalog part number, from this project's own collected data, already
    textually place both calibers side by side. This is corroboration from
    the project's own evidence corpus, not reviewer domain knowledge and not
    a hardcoded brand/calibre table: it only fires when a second, independent,
    real listing for the identical part_number exists that itself mentions
    both numbers. Deliberately narrow -- verified empirically
    (docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md audit) to fire for exactly
    the one residual ambiguous evidence-pair (`iuid_a541f0885f7d4c1a` /
    `3135-410`) among the 7 distinct inventory items with a remaining
    calibre_conflict NO_MATCH, and not for any of the other 6 (genuinely
    different calibers, or the word-boundary tokenization case), because
    those items' OTHER evidence never happens to mention both numbers
    together. Limitation: only as good as this project's own collected
    evidence corpus -- a genuinely-compatible pair with no second listing
    happening to state both calibers together will not be caught; this is a
    disclosed, accepted gap, not silently assumed closed."""
    if not part_number or not inventory_caliber or not conflicting_labels:
        return False, None
    pn_pat = re.escape(str(part_number).lower())
    own_pat = _token_pattern(inventory_caliber)
    for title in all_evidence_titles:
        if not re.search(pn_pat, title):
            continue
        if not re.search(own_pat, title):
            continue
        if any(re.search(_token_pattern(lbl), title) for lbl in conflicting_labels):
            return True, "CROSS_EVIDENCE_CALIBRE_CORROBORATION"
    return False, None


def resolve_calibre_conflict(
    *, raw_conflict: tuple, compatibility_language_hit: bool,
    multiple_calibre_list_hit: bool, verified_and_authorized: bool,
    cross_evidence_corroboration_hit: bool = False,
) -> tuple[tuple, tuple]:
    """Resolves a raw calibre_conflict detection into its final
    (contradiction_result, unverified_compatibility_risk_result) pair.
    Four outcomes (docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md Phase 6/6A):

    1. verified_and_authorized=True (a Layer 1 VERIFIED_* row in
       ref_calibre_compatibility for this exact brand/calibre pair AND a
       Layer 2 row in compatibility_policy_authorization that accepts that
       relationship_type + verification_status both exist) -> contradiction
       fully suppressed; the relationship is established, not merely
       unresolved.
    2. Not authorized, but the SAME evidence text carries its OWN
       compatibility-claiming signal (multiple_calibre_list_risk or
       compatibility_language_risk) -> downgraded from a hard contradiction
       to REVIEW_REQUIRED via unverified_calibre_compatibility. This is a
       textual fact already present in the evidence, not domain knowledge
       added by this codebase.
    3. Neither of the above, but cross_evidence_corroboration_hit=True --
       another, independent evidence row in this project's own corpus for
       the SAME part_number textually mentions BOTH the item's own caliber
       and the conflicting evidence_calibre -> downgraded the same way.
       This closes the one residual case identified in the prior task's
       Phase 6 (route all 3 ambiguous pairs to REVIEW_REQUIRED) without
       hardcoding any brand/calibre relationship -- see
       detect_cross_evidence_calibre_corroboration.
    4. None of the above -> UNCHANGED, remains a hard contradiction
       (NO_MATCH). A single differing labelled caliber with no compatibility
       signal ANYWHERE in this project's own evidence (the current title or
       any sibling listing for the same part) is not distinguishable from a
       genuine mismatch without a verified+authorized reference. This
       remains the conservative default for the other 6 distinct items in
       the calibre_conflict population, none of which have corroborating
       evidence."""
    active, code = raw_conflict[0], raw_conflict[1]
    if not active:
        return (False, None), (False, None)
    if verified_and_authorized:
        return (False, None), (False, None)
    if compatibility_language_hit or multiple_calibre_list_hit or cross_evidence_corroboration_hit:
        return (False, None), (True, "UNVERIFIED_CALIBRE_COMPATIBILITY")
    return (active, code), (False, None)


WHOLE_WATCH_RE = re.compile(
    r"\b(?:complete\s+watch|full\s+watch|wristwatch|komplette\s+uhr|ganze\s+uhr)\b", re.IGNORECASE,
)


def detect_product_type_conflict(title_lower: str) -> tuple[bool, str | None]:
    """True if the title matches a small, explicit whole-watch vocabulary —
    the exact pattern already confirmed in this project's history (a
    part_number match against a complete Air King watch listing, not a
    spare part; docs/MODULE5_EVIDENCE_AUDIT_VERIFICATION.md). Limitation:
    vocabulary-based, not exhaustive — many whole-watch listings using
    different phrasing will not be caught; a title like 'complete watch
    parts set' would false-positive (a real, disclosed risk, not hidden)."""
    if WHOLE_WATCH_RE.search(title_lower):
        return True, "PRODUCT_TYPE_CONFLICT_WHOLE_WATCH_SIGNAL"
    return False, None


# ══════════════════════════════════════════════════════════════════════════
# RISK FLAGS — plausible but not proof of incompatibility; drive REVIEW_REQUIRED.
# ══════════════════════════════════════════════════════════════════════════

def detect_measurement_collision(title_lower: str, token: str) -> tuple[bool, str | None]:
    """True if the token's ONLY occurrence is adjacent to a decimal-looking
    neighbor (e.g. '25.6mm', '3,85'). The exact mechanism confirmed in
    docs/MODULE5_COVERAGE_LAYER_CHECKPOINT_REPORT.md's residual RULE 3 case.
    Limitation: only catches the immediate-neighbor decimal pattern; a
    measurement written with a space or other separator would be missed."""
    tok = re.escape(str(token).lower())
    all_occurrences = list(re.finditer(_token_pattern(token), title_lower))
    if not all_occurrences:
        return False, None
    decimal_occurrences = list(re.finditer(rf"(?:\d[.,]{tok}\b|\b{tok}[.,]\d)", title_lower))
    if decimal_occurrences and len(decimal_occurrences) >= len(all_occurrences):
        return True, "MEASUREMENT_COLLISION_DIMENSION_PATTERN"
    return False, None


def detect_longer_identifier_collision(title_lower: str, token: str, paired_token) -> tuple[bool, str | None]:
    """True if the token's only occurrence sits inside a longer hyphenated
    reference code that is NOT the genuine (caliber, part_number) compound
    being credited as evidence — e.g. part_number '3' matching inside an
    unrelated '1002-3'. Compares the FULL contiguous alphanumeric-and-hyphen
    segment around each occurrence against the genuine compound (in either
    order — caliber-part_number or part_number-caliber), not just the
    immediate neighbor, so a multi-hyphen genuine compound like '24-530-0'
    (caliber '24' + part_number '530-0', itself hyphenated) is correctly
    recognized rather than misread as a collision. Limitation: only
    hyphen-joined compounds; slash/no-separator formats are not covered."""
    tok = str(token).lower()
    paired = str(paired_token).lower() if paired_token else None
    for m in re.finditer(re.escape(tok), title_lower):
        start, end = m.span()
        before_ok = start == 0 or not (title_lower[start - 1].isalnum())
        after_ok = end == len(title_lower) or not (title_lower[end].isalnum())
        if not (before_ok and after_ok):
            continue  # not actually a whole-token occurrence (word-boundary check)
        hyphen_adjacent = (start > 0 and title_lower[start - 1] == "-") or \
                           (end < len(title_lower) and title_lower[end] == "-")
        if not hyphen_adjacent:
            continue  # a plain, non-hyphenated occurrence is not this collision pattern
        seg_start = start
        while seg_start > 0 and (title_lower[seg_start - 1].isalnum() or title_lower[seg_start - 1] == "-"):
            seg_start -= 1
        seg_end = end
        while seg_end < len(title_lower) and (title_lower[seg_end].isalnum() or title_lower[seg_end] == "-"):
            seg_end += 1
        segment = title_lower[seg_start:seg_end]
        if paired and segment in (f"{tok}-{paired}", f"{paired}-{tok}"):
            continue  # the genuine paired compound, not a collision
        return True, "LONGER_IDENTIFIER_COLLISION_UNRELATED_COMPOUND"
    return False, None


def detect_model_name_number_collision(title_lower: str, token: str) -> tuple[bool, str | None]:
    """True if the token directly follows a bare model-name word with
    nothing but whitespace/hyphen/roman-numeral between — the exact
    'GMT Master 2' / 'Explorer 2' pattern confirmed in
    docs/MODULE5_RULE3_SAFEGUARD_FINAL_VALIDATION.md. Restricted to short
    (<=2 alphanumeric character) tokens, matching that confirmed pattern
    exactly (the false positives were single-digit part numbers, e.g. '2')
    — a genuinely distinctive identifier sitting next to a model name is
    far more likely a real reference than a coincidental model-edition
    number, so this must not fire for it. Limitation: fixed vocabulary of
    model words (MODEL_WORDS); a model name outside that list is not
    covered."""
    if _alnum_len(token) > 2:
        return False, None
    tok = re.escape(str(token).lower())
    for word in MODEL_WORDS:
        m = re.search(rf"\b{word}\b(.{{0,15}}?){tok}\b", title_lower)
        if m and re.fullmatch(r"[\s\-iiIvV]*", m.group(1) or ""):
            return True, "MODEL_NAME_NUMBER_COLLISION"
    return False, None


def detect_proper_noun_event_collision(title_lower: str, token: str) -> tuple[bool, str | None]:
    """True if the token is immediately followed by an 'hours of/at' event
    phrase — the exact '24 Hours of Daytona' pattern confirmed in this
    project's RULE 4 precision sample (caliber '24', 2 alphanumeric
    characters). Restricted to <=2-alphanumeric-character tokens for the
    same reason as detect_model_name_number_collision — a distinctive
    identifier next to this phrase shape is implausible as a coincidence.
    Limitation: only the 'hours of/at' shape; other event/proper-noun
    phrasings are not covered."""
    if _alnum_len(token) > 2:
        return False, None
    tok = re.escape(str(token).lower())
    if re.search(rf"\b{tok}\b\s*hours?\s*(?:of|at)\b", title_lower):
        return True, "PROPER_NOUN_EVENT_COLLISION"
    return False, None


def detect_bundle_or_lot_risk(title_lower: str) -> tuple[bool, str | None]:
    """Reuses scripts/02_clean.py's LOT_TITLE_RE pattern verbatim (see
    module-level comment) — bundle/lot vocabulary means the evidence may
    describe multiple parts, not a single-item match. Limitation: keyword
    vocabulary, not exhaustive."""
    if LOT_TITLE_RE.search(title_lower):
        return True, "BUNDLE_OR_LOT_LANGUAGE_PRESENT"
    return False, None


def detect_compatibility_language_risk(title_lower: str) -> tuple[bool, str | None]:
    """True if the title contains explicit compatibility wording ('fits',
    'for', 'compatible', 'passend für') — often paired with a list of
    calibers, signalling the listing may not be specific to one reference.
    Limitation: 'for' is a common English word and this is a
    high-recall/low-precision signal by design — deliberately a RISK flag
    (drives REVIEW_REQUIRED at most), never a contradiction."""
    if COMPATIBILITY_RE.search(title_lower):
        return True, "COMPATIBILITY_LANGUAGE_PRESENT"
    return False, None


def detect_multiple_reference_list_risk(title_lower: str, token) -> tuple[bool, str | None]:
    """True if the matched token is itself one of 3+ separate 4-6 digit
    numbers joined by '/' or ',' in the title -- the watch-reference-list
    ambiguity mechanism confirmed at population scale for PART_NUMBER_EXACT/
    BRAND_PART_NUMBER (docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md Task 5:
    22/445 rows, e.g. part_number='114200' matching
    '...Dial 14000/14010/114200/114234'). Only flags when the matched token
    ITSELF participates in the list (a list elsewhere in the title not
    involving the matched identifier is not this risk). Restricted to pure
    4-6 digit tokens by construction -- a hyphenated compound catalog code
    (e.g. '3135-250') can never itself be a bare 4-6 digit list member, so
    requirement 4 (do not flag a single compound part number as a
    reference list) holds without a separate carve-out; verified directly
    against the full CALIBER_PART_NUMBER population (862 rows): every
    caliber-family-list occurrence there is a hyphenated/labelled compound
    on the matched side, never a bare list member, consistent with
    CALIBER_PART_NUMBER's structural protection against this SPECIFIC
    mechanism (not a claim of blanket immunity to every list-shaped risk --
    see detect_multiple_calibre_list_risk for the caliber-side list risk it
    remains exposed to). Limitation: only the 4-6-digit/'/'-or-','-separated
    shape; a differently formatted list (e.g. space-only) is not covered."""
    if token is None:
        return False, None
    tok = str(token).strip()
    if not tok.isdigit() or not (4 <= len(tok) <= 6):
        return False, None
    for run_match in REFERENCE_LIST_RUN_RE.finditer(title_lower):
        numbers = re.findall(r"\d{4,6}", run_match.group(0))
        if tok in numbers:
            return True, "REFERENCE_OR_COMPATIBILITY_LIST_AMBIGUITY"
    return False, None


def detect_multiple_calibre_list_risk(title_lower: str) -> tuple[bool, str | None]:
    """True if an explicit caliber label ('cal./calibre/caliber/kaliber') is
    immediately followed by 2+ DISTINCT numbers -- a caliber-family
    compatibility list (e.g. 'Kaliber: 1520, 1530, 1535, 1555, 1556...'),
    not a single differing caliber value. Purely structural: no brand or
    specific-caliber value is ever embedded in this detector -- this is
    what lets decide() distinguish a genuine cross-compatible-family
    listing from an unrelated single differing caliber without hardcoding
    which families exist (docs/MODULE5_RISK_REGISTER.md risk #10;
    docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md Task 6/Phase 6 -- 'do not add
    hardcoded Rolex calibre-family exceptions'). Title-level, not
    token-scoped, since the risk is about the EVIDENCE's own claim, not
    about which token this candidate matched on. Limitation: only the
    label-immediately-followed-by-list shape; a list presented without an
    explicit caliber label is not covered, and this is deliberately unable
    to catch a case where only ONE differing caliber is stated even if that
    single caliber happens to be a genuine (but unlisted) family sibling --
    such a case remains an unresolved, undetectable-without-a-verified-
    reference-source risk, not silently assumed away (see
    ref_calibre_compatibility / compatibility_policy_authorization)."""
    for run_match in CALIBRE_LIST_RUN_RE.finditer(title_lower):
        numbers = re.findall(r"\d{2,4}", run_match.group(1))
        if len(set(numbers)) >= 2:
            return True, "MULTIPLE_CALIBRE_LIST_PRESENT"
    return False, None


def detect_short_identifier_risk(token) -> tuple[bool, str | None]:
    """True if the evidence-anchoring token has < 3 alphanumeric characters
    — the measured high-risk length threshold from
    docs/MODULE5_RISK_REGISTER.md (risk #2/#5). Limitation: a length-only
    heuristic; a short-but-genuine identifier is indistinguishable from a
    short coincidental one by this flag alone (this is exactly RULE 3's
    already-disclosed coverage-cost trade-off, risk #7)."""
    if token is None:
        return False, None
    if _alnum_len(token) < 3:
        return True, "SHORT_IDENTIFIER_LOW_ALNUM_LENGTH"
    return False, None


def detect_ambiguous_component_type(match_method: str) -> tuple[bool, str | None]:
    """Structurally true for every Tier C (component) candidate — the rule
    proves a component word co-occurs with the caliber, never that it is
    THIS item's specific component (docs/MODULE5_EVIDENCE_AUDIT_VERIFICATION.md).
    Not a per-title heuristic; a property of the rule itself."""
    if match_method in TIER_C_METHODS:
        return True, "COMPONENT_RULE_NEVER_VALIDATES_SPECIFIC_PART"
    return False, None


def detect_evidence_missing_identifier(inventory_part_number) -> tuple[bool, str | None]:
    """True if the inventory item itself has no part_number at all —
    relevant context for any decision on this item regardless of which
    rule fired, since no rule can ever anchor on a field that doesn't exist."""
    if inventory_part_number is None or str(inventory_part_number).strip() == "":
        return True, "INVENTORY_PART_NUMBER_MISSING"
    return False, None


def _multiple_inventory_collision_map(conn: duckdb.DuckDBPyConnection, table: str, id_col: str) -> set:
    """Returns the set of (canonical_evidence_key, match_method) pairs in
    `table` where the SAME rule matches that EVIDENCE (not row) to more
    than one distinct inventory_uid — genuine same-strength ambiguity
    (e.g. two different items both hit by PART_NUMBER_EXACT on the same
    listing). Deliberately scoped PER RULE, not across all rules combined:
    an initial whole-table version (any rule, any item) was measured
    against real data and found to fire for 97% of all Tier A
    REVIEW_REQUIRED decisions, because a strong PART_NUMBER_EXACT hit for
    one item and an unrelated, much weaker CALIBER_EXACT hit for a
    different item (sharing only a common caliber) on the same row is not
    the same kind of ambiguity — it swamped the signal this flag exists
    to surface. Computed once per source table, reused for every
    candidate row from that table.

    Grouped on COALESCE(evidence_uid, id_col) — the canonical evidence
    identity, not the positional id (docs/MODULE5_POST_PHASE1_ARCHITECTURE_AUDIT.md
    Bug 2: keying on id_col alone under-fires, since one real listing can
    be represented by multiple positional rows — e.g. stg_active_targeted's
    (inventory_uid, item_id, marketplace) pairing grain — so two different
    inventory items each matching a DIFFERENT pairing-row of the SAME
    listing would never have shared an id_col value, even though they
    genuinely collide on the same real-world evidence). Falls back to
    id_col only for rows with no evidence_uid (legacy/no-identity rows)."""
    rows = conn.execute(f"""
        SELECT COALESCE(evidence_uid, CAST({id_col} AS VARCHAR)) AS canonical_key, match_method
        FROM {table}
        GROUP BY canonical_key, match_method HAVING COUNT(DISTINCT inventory_uid) > 1
    """).fetchall()
    return {(r[0], r[1]) for r in rows}


# ══════════════════════════════════════════════════════════════════════════
# VALIDATION-POLICY GATE (docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md Phase 1-3,7)
# Governs ONLY whether a deterministically-clean Tier A candidate may become
# MATCH_CONFIRMED. Never overrides a contradiction or an unresolved risk
# flag (Phase 7). No segment is ever auto-approved from computed statistics
# by this code -- only an explicit validation_policy row with
# validation_status='APPROVED' can produce APPROVED; absence, or any other
# status, resolves to VALIDATION_PENDING.
# ══════════════════════════════════════════════════════════════════════════

def _load_approved_validation_segments(conn: duckdb.DuckDBPyConnection) -> dict:
    """Pre-loads every APPROVED validation_policy row once per run (not
    once per candidate row -- this table is consulted ~370k times in a
    real run, so a per-row SQL query would be prohibitively slow). Keyed on
    the exact (matching_rule, source_table, collection_relationship)
    segment dimensions; 'ANY' as collection_relationship matches any value
    (only meaningful for non-active sources, which are always
    NOT_APPLICABLE)."""
    rows = conn.execute("""
        SELECT matching_rule, source_table, collection_relationship, confirmation_policy_version
        FROM validation_policy WHERE validation_status = 'APPROVED'
    """).fetchall()
    out = {}
    for rule, table, rel, version in rows:
        out[(rule, table, rel)] = version
    return out


def resolve_validation_status(
    approved_segments: dict, *, matching_rule: str, source_table: str, collection_relationship: str,
) -> tuple[str, str | None]:
    """Returns (validation_status, confirmation_policy_version). Only an
    exact-match or an 'ANY'-relationship APPROVED row produces APPROVED;
    every other case -- including a segment with NO validation_policy row
    at all -- resolves to VALIDATION_PENDING, the safe default (Phase 2:
    'every existing segment must be inserted or resolved as
    VALIDATION_PENDING'). This function never computes or consults
    precision/sample statistics itself."""
    version = approved_segments.get((matching_rule, source_table, collection_relationship))
    if version is None:
        version = approved_segments.get((matching_rule, source_table, "ANY"))
    if version is not None:
        return "APPROVED", version
    return "VALIDATION_PENDING", None


# ══════════════════════════════════════════════════════════════════════════
# CALIBRE-COMPATIBILITY GOVERNANCE (docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md
# Phase 6A) — two independently-required layers. Both empty in every real
# run this task performs; populated only in isolated test fixtures.
# ══════════════════════════════════════════════════════════════════════════

def _load_verified_compatibility(conn: duckdb.DuckDBPyConnection) -> dict:
    """Layer 1: verified relationships only (REVIEWER_INFERENCE_ONLY and
    UNRESOLVED rows are never eligible to suppress anything, regardless of
    Layer 2 authorization)."""
    rows = conn.execute("""
        SELECT brand, inventory_calibre, evidence_calibre, relationship_type, verification_status
        FROM ref_calibre_compatibility
        WHERE verification_status IN ('VERIFIED_FROM_PROJECT_SOURCE', 'VERIFIED_EXTERNAL_REFERENCE')
    """).fetchall()
    return {(r[0], str(r[1]), str(r[2])): (r[3], r[4]) for r in rows}


def _load_compatibility_authorizations(conn: duckdb.DuckDBPyConnection) -> list:
    """Layer 2: which relationship_type + verification_status (+ optional
    brand_limitation) combinations the active decision policy explicitly
    permits. A VERIFIED_* row's mere presence in ref_calibre_compatibility
    never authorizes anything on its own."""
    rows = conn.execute("""
        SELECT relationship_type, accepted_verification_status, brand_limitation
        FROM compatibility_policy_authorization
    """).fetchall()
    return rows


def calibre_compatibility_verified_and_authorized(
    verified_map: dict, authorizations: list, *, brand: str, inventory_calibre: str, conflicting_labels: list,
) -> bool:
    """True only if BOTH layers agree for at least one of the conflicting
    labelled numbers: (1) a VERIFIED_* ref_calibre_compatibility row exists
    for (brand, inventory_calibre, that label), AND (2) an authorization row
    accepts that exact relationship_type + verification_status (optionally
    brand-restricted). Neither layer alone is sufficient (Phase 6A)."""
    for label in conflicting_labels:
        verified = verified_map.get((brand, str(inventory_calibre), str(label)))
        if not verified:
            continue
        relationship_type, verification_status = verified
        for auth_rel, auth_status, auth_brand in authorizations:
            if auth_rel == relationship_type and auth_status == verification_status and (
                auth_brand is None or auth_brand == brand
            ):
                return True
    return False


# ══════════════════════════════════════════════════════════════════════════
# DECISION LOGIC — tier-specific, per docs/MODULE5_DECISION_LAYER_DESIGN.md Phase 3
# ══════════════════════════════════════════════════════════════════════════

def decide(
    *, match_method: str, contradiction_flags: dict, risk_flags: dict, validation_status: str = "NOT_APPLICABLE",
    v2_verification: dict | None = None,
) -> tuple[str, str, str, bool, str]:
    """Returns (match_status, match_reason_code, match_reason_text,
    deterministic_checks_passed, confirmation_policy_reason).

    Evaluation order (docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md Phase 4A/7)
    is fixed and never reordered:
      A. explicit contradiction (any tier)          -> NO_MATCH
      B. unresolved identity risk (Tier A)           -> REVIEW_REQUIRED, specific risk reason
      C. Tier A clean AND validation_status APPROVED
         AND v2_verification eligible (matching v2, docs/MATCHING_AUTOMATION_
         IMPLEMENTATION_REPORT.md §11)               -> MATCH_CONFIRMED
      C2. Tier A clean AND validation_status APPROVED
          BUT v2_verification NOT eligible            -> REVIEW_REQUIRED,
                                                         MATCHING_V2_SCORE_BELOW_THRESHOLD
                                                         (structural safeguard: an APPROVED
                                                         rule-level policy can never bypass
                                                         per-listing content verification --
                                                         the exact Implementation-B failure
                                                         mode this project rejected)
      D. Tier A clean AND validation_status is NOT APPROVED
                                                      -> REVIEW_REQUIRED, AUTO_CONFIRM_POLICY_NOT_VALIDATED
      E. Tier B plausible evidence                   -> LOW_CONFIDENCE_CANDIDATE / INSUFFICIENT_EVIDENCE (v1.1)
      F. Tier C                                       -> INSUFFICIENT_EVIDENCE (or the high-value edge case -> LOW_CONFIDENCE_CANDIDATE, v1.1)

    Matching Engine v1.1 (docs/MATCHING_ENGINE_V11_EXPERIMENT.md): the
    calibre-only / component rules (Tier B/C) that previously routed to
    REVIEW_REQUIRED now route to LOW_CONFIDENCE_CANDIDATE — kept out of the
    human review queue (their owner-adjudicated precision is ~0%), retained
    for future evaluation, never deleted, never auto-confirmed. Tier A
    (part-number rules) and the MATCH_CONFIRMED gate are UNCHANGED.

    match_reason_code is always the MOST SPECIFIC reason (contradiction >
    risk > policy) — AUTO_CONFIRM_POLICY_NOT_VALIDATED can only ever appear
    when NO contradiction and NO risk fired; it never hides a more specific
    reason. confirmation_policy_reason is a SEPARATE field recording the
    validation_status actually consulted for Tier A rows (NOT_APPLICABLE
    for Tier B/C, which never consult segment policy since they can never
    reach MATCH_CONFIRMED regardless of policy state). deterministic_checks_
    passed is TRUE iff evidence_tier='A' with zero active contradiction/risk
    — i.e. the row would be MATCH_CONFIRMED under pure deterministic rules
    alone, independent of policy state; this preserves technical rule
    strength separately from the operational decision (Phase 1). Never a
    numeric score."""
    tier = _tier_of(match_method)
    active_contradictions = {k: v for k, v in contradiction_flags.items() if v[0]}
    active_risks = {k: v for k, v in risk_flags.items() if v[0]}
    deterministic_checks_passed = tier == "A" and not active_contradictions and not active_risks

    if active_contradictions:
        code = next(iter(active_contradictions.values()))[1]
        names = ",".join(sorted(active_contradictions))
        policy_reason = validation_status if tier == "A" else "NOT_APPLICABLE"
        return "NO_MATCH", code, f"Explicit contradiction(s) found: {names}", False, policy_reason

    if tier == "A":
        if active_risks:
            code = next(iter(active_risks.values()))[1]
            names = ",".join(sorted(active_risks))
            return (
                "REVIEW_REQUIRED", code, f"Tier A candidate with unresolved risk(s): {names}",
                False, validation_status,
            )
        if validation_status == "APPROVED":
            v2_eligible = bool(v2_verification) and v2_verification.get("score", 0.0) >= matching_v2.SCORE_THRESHOLD
            if v2_eligible:
                return (
                    "MATCH_CONFIRMED", "TIER_A_CLEAN_NO_CONTRADICTION_NO_RISK",
                    "Tier A rule fired with no explicit contradiction and no unresolved identity risk, "
                    "this segment's validation policy is APPROVED, and matching v2 per-listing "
                    f"verification scored {v2_verification['score']:.2f} (>= {matching_v2.SCORE_THRESHOLD}).",
                    True, "APPROVED",
                )
            v2_score = v2_verification.get("score") if v2_verification else None
            return (
                "REVIEW_REQUIRED", "MATCHING_V2_SCORE_BELOW_THRESHOLD",
                "Tier A rule fired with no explicit contradiction and no unresolved identity risk, and "
                "this segment's validation policy is APPROVED, but per-listing matching v2 verification "
                f"scored {v2_score if v2_score is not None else 'N/A'} "
                f"(< {matching_v2.SCORE_THRESHOLD}) -- a rule-level policy approval never bypasses "
                "per-listing content verification (docs/MATCHING_AUTOMATION_IMPLEMENTATION_REPORT.md §11).",
                True, "APPROVED",
            )
        return (
            "REVIEW_REQUIRED", "AUTO_CONFIRM_POLICY_NOT_VALIDATED",
            "Tier A rule fired with no explicit contradiction and no unresolved identity risk, but "
            f"this segment's validation policy is not APPROVED (status={validation_status}) — see "
            "validation_policy table. Technically strong evidence awaiting policy validation, not a "
            "weak match.",
            True, validation_status,
        )

    if tier == "B":
        if match_method == "BRAND_CALIBER" or not risk_flags.get("short_identifier_risk", (False,))[0]:
            return (
                "LOW_CONFIDENCE_CANDIDATE", "TIER_B_PLAUSIBLE_PENDING_REVIEW", (
                    "Tier B evidence plausible (brand+caliber corroboration, or caliber not in the "
                    "highest-risk short-identifier length) but the owner-adjudicated review measured "
                    "~0% precision for calibre-only rules (Matching Engine v1.1, docs/"
                    "MATCHING_ENGINE_V11_EXPERIMENT.md). Retained as a low-confidence candidate for "
                    "future evaluation/discovery/labelling — NOT surfaced in the human review queue, "
                    "NOT deleted, NEVER auto-confirmed."
                ), False, "NOT_APPLICABLE",
            )
        return (
            "INSUFFICIENT_EVIDENCE", "TIER_B_CALIBER_ONLY_SHORT_IDENTIFIER", (
                "CALIBER_EXACT alone with a short (<3 alnum char) caliber — the weakest measured "
                "Tier B combination; too weak to even queue for review as a plausible match."
            ), False, "NOT_APPLICABLE",
        )

    # tier == "C"
    if match_method == "BRAND_CALIBER_COMPONENT" and not risk_flags.get("short_identifier_risk", (False,))[0]:
        return (
            "LOW_CONFIDENCE_CANDIDATE", "TIER_C_HIGH_VALUE_EDGE_CASE", (
                "BRAND_CALIBER_COMPONENT with a non-short caliber — the strongest Tier C combination "
                "(brand+caliber+component all agree). Previously routed to review, but the owner-"
                "adjudicated review measured 0/8 confirmed for this rule (Matching Engine v1.1, owner-"
                "approved provisional demotion, docs/MATCHING_ENGINE_V11_EXPERIMENT.md). Retained as a "
                "low-confidence candidate for future re-evaluation on complete evidence — NOT in the "
                "human review queue, NOT deleted, NEVER auto-confirmed."
            ), False, "NOT_APPLICABLE",
        )
    return (
        "INSUFFICIENT_EVIDENCE", "TIER_C_DISCOVERY_ONLY_DEFAULT", (
            "Component-tier evidence (~20% measured precision) — discovery-only by design, never a "
            "standalone match signal."
        ), False, "NOT_APPLICABLE",
    )


# ══════════════════════════════════════════════════════════════════════════
# ORCHESTRATION
# ══════════════════════════════════════════════════════════════════════════

def _candidate_key(source_table: str, inventory_uid: str, source_id: int, match_method: str,
                    evidence_uid: str | None = None) -> str:
    """Hashes on evidence_uid when available (Module 5 stable-identity
    scheme — docs/MODULE5_EVIDENCE_IDENTITY_IMPLEMENTATION_CHECKLIST.md),
    falling back to the legacy source_id-based formula otherwise, for
    backward compatibility with any row generated before evidence_uid
    existed. The 'v2|' discriminator prevents an evidence_uid-based key
    from ever colliding with a legacy source_id-based key even if the
    two happened to hash to the same raw string."""
    if evidence_uid:
        raw = f"v2|{source_table}|{inventory_uid}|{evidence_uid}|{match_method}"
    else:
        raw = f"{source_table}|{inventory_uid}|{source_id}|{match_method}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def build_match_decisions(
    conn: duckdb.DuckDBPyConnection,
    *,
    decision_run_id: str | None = None,
    inventory_uid: str | None = None,
) -> pd.DataFrame:
    """Pure computation — no writes. One row per DISTINCT (source_table,
    inventory_uid, source_id, match_method) triple across ALL accumulated
    match_run_ids (same aggregation convention as 05b_evidence_coverage_audit.py),
    not one row per match_candidate_id — re-discovery of the same evidence
    across separate candidate-generation runs decides once, not once per run."""
    decision_run_id = decision_run_id or f"decision_run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    inventory_where = "WHERE validation_status <> 'FAIL'"
    inventory_params: list[str] = []
    if inventory_uid:
        inventory_where += " AND inventory_uid = ?"
        inventory_params.append(inventory_uid)
    inventory_df = conn.execute(f"""
        SELECT inventory_uid, brand, caliber, part_number
        FROM staging_inventory {inventory_where}
    """, inventory_params).df().set_index("inventory_uid")

    # Loaded ONCE per run, not per candidate row (~370k rows in a real run) --
    # see resolve_validation_status / calibre_compatibility_verified_and_authorized.
    approved_segments = _load_approved_validation_segments(conn)
    verified_compatibility = _load_verified_compatibility(conn)
    compatibility_authorizations = _load_compatibility_authorizations(conn)

    rows: list[dict] = []

    for candidates_table, id_col, evidence_table in SOURCES:
        # collection_inventory_uid is persisted at candidate-generation
        # time on match_candidates_active only (the historical sources
        # have no per-inventory collection target). arg_max by created_at
        # picks the latest run's value, consistent with the match_run_id
        # selection. See docs/MODULE5_STATUS_AND_RUNBOOK.md §6.
        cand_has_collection = conn.execute(
            "SELECT COUNT(*) FROM information_schema.columns "
            f"WHERE table_name = '{candidates_table}' AND column_name = 'collection_inventory_uid'"
        ).fetchone()[0] > 0
        collection_select = (
            ", arg_max(collection_inventory_uid, created_at) AS collection_inventory_uid"
            if cand_has_collection else ""
        )
        cand_where = ""
        cand_params: list[str] = []
        if inventory_uid:
            cand_where = "WHERE inventory_uid = ?"
            cand_params.append(inventory_uid)
        cand_df = conn.execute(f"""
            SELECT DISTINCT inventory_uid, {id_col} AS source_id, evidence_uid, match_method,
                   arg_max(match_run_id, created_at) AS match_run_id{collection_select}
            FROM {candidates_table}
            {cand_where}
            GROUP BY inventory_uid, {id_col}, evidence_uid, match_method
        """, cand_params).df()
        if cand_df.empty:
            continue

        # ORDER BY id: makes which row survives the dedup below
        # deterministic, not dependent on DuckDB's unenforced scan order
        # (docs/MODULE5_POST_PHASE1_ARCHITECTURE_AUDIT.md Bug 4).
        evidence_raw = conn.execute(f"""
            SELECT id AS source_id, stable_evidence_uid, title, normalized_title
            FROM {evidence_table} WHERE normalized_title IS NOT NULL
            ORDER BY id
        """).df()
        evidence_df = evidence_raw.set_index("source_id")
        # Module 5 stable-identity lookup (preferred — closes the live
        # re-read vulnerability docs/MODULE5_LINEAGE_INTEGRITY_AUDIT.md
        # found at this exact point): keyed on stable_evidence_uid
        # instead of the positional id. De-duplicated because
        # stable_evidence_uid is LISTING grain while this table's row
        # grain can be finer (e.g. stg_active_targeted has one row per
        # (inventory_uid, item_id, marketplace) pairing — up to 8
        # pairing-rows can legitimately share one stable_evidence_uid,
        # all with equivalent title/price content, confirmed empirically
        # in docs/MODULE5_LINEAGE_INTEGRITY_AUDIT.md). keep="first" is
        # now deterministic (smallest id wins) because of the ORDER BY
        # above, not because of an unenforced default scan order.
        evidence_by_uid = (
            evidence_raw.dropna(subset=["stable_evidence_uid"])
            .drop_duplicates(subset=["stable_evidence_uid"], keep="first")
            .set_index("stable_evidence_uid")
        )

        collision_ids = _multiple_inventory_collision_map(conn, candidates_table, id_col)

        # active-targeted only: the collection-target inventory_uid, for
        # SELF_SOURCED/CROSS_REFERENCED. LEGACY FALLBACK ONLY — used only
        # for candidate rows generated before collection_inventory_uid was
        # persisted (NULL). New rows carry the collection target captured
        # at generation time (row.collection_inventory_uid), which is
        # stable across staging rebuilds; re-reading the current staging
        # positional id here would silently corrupt the relationship if
        # staging was rebuilt between generation and decision
        # (docs/MODULE5_STATUS_AND_RUNBOOK.md §6, reproduced).
        collection_uid_by_source_id = {}
        if evidence_table == "stg_active_targeted":
            m = conn.execute("SELECT id, inventory_uid FROM stg_active_targeted").fetchall()
            collection_uid_by_source_id = {r[0]: r[1] for r in m}

        for row in cand_df.itertuples():
            if row.inventory_uid not in inventory_df.index:
                continue  # inventory item no longer eligible (e.g. FAIL) -- skip, never fabricate
            inv = inventory_df.loc[row.inventory_uid]
            row_evidence_uid = getattr(row, "evidence_uid", None)
            # Same COALESCE(evidence_uid, id) canonical key used to build
            # collision_ids — must match exactly or the membership check
            # below is comparing apples to oranges.
            row_collision_key = row_evidence_uid or str(row.source_id)
            if row_evidence_uid:
                # Preferred path: evidence_uid is stable across staging
                # rebuilds, unlike source_id — this is what actually
                # closes the lineage defect, since it no longer re-reads
                # a positional id that may have been reassigned since
                # candidate generation. Deliberately does NOT fall back
                # to the source_id lookup when the evidence_uid can't be
                # resolved (e.g. the staging row it pointed to was
                # deleted/moved by a rebuild) — falling back to source_id
                # here would silently reintroduce the exact lineage bug
                # this scheme exists to close (a stale positional id
                # resolving to unrelated evidence). Skipping the
                # candidate is the safe behavior.
                if row_evidence_uid not in evidence_by_uid.index:
                    continue
                ev = evidence_by_uid.loc[row_evidence_uid]
            elif row.source_id in evidence_df.index:
                # Legacy fallback: candidate rows generated before
                # evidence_uid existed. Left in place for backward
                # compatibility, not relied upon for new data.
                ev = evidence_df.loc[row.source_id]
            else:
                continue
            title_lower = str(ev["normalized_title"])

            evidence_missing, evidence_missing_code = detect_evidence_missing_identifier(inv["part_number"])

            # calibre_conflict is resolved (not just detected) before entering
            # contradiction_flags -- see resolve_calibre_conflict for the three
            # possible outcomes (suppressed / downgraded to a risk / unchanged).
            raw_calibre_conflict = detect_calibre_conflict(title_lower, inv["caliber"])
            compat_lang_hit, compat_lang_code = detect_compatibility_language_risk(title_lower)
            multi_cal_list_hit, multi_cal_list_code = detect_multiple_calibre_list_risk(title_lower)
            verified_and_authorized = calibre_compatibility_verified_and_authorized(
                verified_compatibility, compatibility_authorizations,
                brand=inv["brand"], inventory_calibre=inv["caliber"], conflicting_labels=raw_calibre_conflict[2],
            )
            # Lazy: only queried when calibre_conflict actually fired and no
            # cheaper textual signal already resolved it -- rare (~14-20 rows
            # in a real 374k-row run), so the DB round-trip cost is negligible.
            cross_evidence_hit = False
            if raw_calibre_conflict[0] and not verified_and_authorized and not (compat_lang_hit or multi_cal_list_hit):
                cross_evidence_hit = _query_cross_evidence_calibre_corroboration(
                    conn, part_number=inv["part_number"], inventory_caliber=inv["caliber"],
                    conflicting_labels=raw_calibre_conflict[2],
                    exclude_source_table=candidates_table, exclude_source_id=int(row.source_id),
                )
            calibre_conflict_result, unverified_compat_risk = resolve_calibre_conflict(
                raw_conflict=raw_calibre_conflict, compatibility_language_hit=compat_lang_hit,
                multiple_calibre_list_hit=multi_cal_list_hit, verified_and_authorized=verified_and_authorized,
                cross_evidence_corroboration_hit=cross_evidence_hit,
            )

            contradiction_flags = {
                "brand_conflict": detect_brand_conflict(title_lower, inv["brand"]),
                "calibre_conflict": calibre_conflict_result,
                "product_type_conflict": detect_product_type_conflict(title_lower),
            }

            # risk_tokens = (token, paired_token) for every identifier this rule
            # actually anchors on, checked independently and OR'd together.
            # CALIBER_PART_NUMBER anchors on BOTH caliber and part_number (the >=3
            # floor only gates part_number; Phase 0 of
            # docs/MODULE5_COVERAGE_LAYER_CHECKPOINT_REPORT.md found the one
            # confirmed residual collision is on the CALIBER side of this exact
            # rule, so checking only part_number would silently miss it). Each
            # token is paired with the OTHER token from the same rule (not a
            # fixed value) so detect_longer_identifier_collision can recognize
            # the genuine compound from either side.
            if row.match_method in ("PART_NUMBER_EXACT", "BRAND_PART_NUMBER"):
                risk_tokens = [(inv["part_number"], None)]
            elif row.match_method == "CALIBER_PART_NUMBER":
                risk_tokens = [(inv["caliber"], inv["part_number"]), (inv["part_number"], inv["caliber"])]
            else:
                risk_tokens = [(inv["caliber"], None)]
            risk_tokens = [(t, p) for t, p in risk_tokens if t]

            def _any_token(detector, needs_pair: bool = False) -> tuple[bool, str | None]:
                for tok, paired in risk_tokens:
                    hit, code = detector(title_lower, tok, paired) if needs_pair else detector(title_lower, tok)
                    if hit:
                        return hit, code
                return False, None

            risk_flags = {
                "measurement_collision": _any_token(detect_measurement_collision),
                "longer_identifier_collision": _any_token(detect_longer_identifier_collision, needs_pair=True),
                "model_name_number_collision": _any_token(detect_model_name_number_collision),
                "proper_noun_or_event_collision": _any_token(detect_proper_noun_event_collision),
                "bundle_or_lot_risk": detect_bundle_or_lot_risk(title_lower),
                "multiple_inventory_collision": (
                    (row_collision_key, row.match_method) in collision_ids,
                    "MULTIPLE_INVENTORY_ITEMS_MATCH_SAME_EVIDENCE_ROW" if (row_collision_key, row.match_method) in collision_ids else None,
                ),
                # Tier B/C: caliber length (the field these rules anchor on with no
                # distinctiveness floor at all). Tier A part-number rules: part_number
                # length (PART_NUMBER_EXACT/BRAND_PART_NUMBER are already gated >=5 by
                # utils.part_number_is_distinctive, so this only ever fires for
                # CALIBER_PART_NUMBER's own >=3 floor — the exact disclosed trade-off
                # in docs/MODULE5_RISK_REGISTER.md risk #7).
                "short_identifier_risk": detect_short_identifier_risk(
                    inv["caliber"] if row.match_method in TIER_B_METHODS or row.match_method in TIER_C_METHODS
                    else inv["part_number"]
                ),
                "compatibility_language_risk": (compat_lang_hit, compat_lang_code),
                "multiple_reference_list_risk": _any_token(detect_multiple_reference_list_risk),
                "multiple_calibre_list_risk": (multi_cal_list_hit, multi_cal_list_code),
                "unverified_calibre_compatibility": unverified_compat_risk,
                "cross_evidence_calibre_corroboration": (
                    cross_evidence_hit, "CROSS_EVIDENCE_CALIBRE_CORROBORATION" if cross_evidence_hit else None
                ),
                "ambiguous_component_type": detect_ambiguous_component_type(row.match_method),
                "evidence_missing_identifier": (evidence_missing, evidence_missing_code),
            }

            if evidence_table == "stg_active_targeted":
                # Prefer the value persisted at candidate-generation time
                # (stable across staging rebuilds). Fall back to the live
                # staging re-read ONLY for legacy rows that predate the
                # persisted column (collection_inventory_uid absent/NULL).
                persisted = getattr(row, "collection_inventory_uid", None)
                if persisted is not None and not pd.isna(persisted):
                    collection_uid = persisted
                else:
                    collection_uid = collection_uid_by_source_id.get(row.source_id)
                if collection_uid is None:
                    collection_relationship = "NOT_APPLICABLE"
                elif collection_uid == row.inventory_uid:
                    collection_relationship = "SELF_SOURCED"
                else:
                    collection_relationship = "CROSS_REFERENCED"
            else:
                collection_relationship = "NOT_APPLICABLE"

            validation_status, confirmation_policy_version = resolve_validation_status(
                approved_segments, matching_rule=row.match_method, source_table=candidates_table,
                collection_relationship=collection_relationship,
            )

            # Only computed for the narrow case that can actually reach
            # MATCH_CONFIRMED (Tier A, no active contradiction/risk yet
            # known, policy already APPROVED) -- avoids per-row cost across
            # the ~1.5M-row decision run for every candidate that would
            # never reach this branch regardless.
            v2_verification = None
            if (
                _tier_of(row.match_method) == "A"
                and not any(v[0] for v in contradiction_flags.values())
                and not any(v[0] for v in risk_flags.values())
                and validation_status == "APPROVED"
            ):
                v2_verification = matching_v2.score_candidate(
                    inv["brand"], inv["part_number"], inv["caliber"], title_lower,
                )

            status, reason_code, reason_text, deterministic_checks_passed, confirmation_policy_reason = decide(
                match_method=row.match_method, contradiction_flags=contradiction_flags, risk_flags=risk_flags,
                validation_status=validation_status, v2_verification=v2_verification,
            )

            matched_fields = MATCHED_FIELDS_BY_RULE[row.match_method]

            rows.append({
                "decision_id": None,  # assigned at write time
                "decision_version": DECISION_VERSION,
                "decision_run_id": decision_run_id,
                "match_run_id": row.match_run_id,
                "candidate_key": _candidate_key(
                    candidates_table, row.inventory_uid, row.source_id, row.match_method,
                    evidence_uid=row_evidence_uid,
                ),
                "inventory_uid": row.inventory_uid,
                "source_table": candidates_table,
                "source_id": int(row.source_id),
                "evidence_uid": row_evidence_uid,
                "matching_rule": row.match_method,
                "evidence_tier": _tier_of(row.match_method),
                "match_status": status,
                "match_reason_code": reason_code,
                "match_reason_text": reason_text,
                "matched_fields": ",".join(matched_fields),
                "contradiction_flags": ",".join(k for k, v in contradiction_flags.items() if v[0]) or None,
                "risk_flags": ",".join(k for k, v in risk_flags.items() if v[0]) or None,
                "collection_relationship": collection_relationship,
                "rule_precision_reference": RULE_PRECISION_REFERENCE.get(row.match_method),
                "price_evidence_status": PRICE_EVIDENCE_STATUS_PLACEHOLDER,
                "deterministic_checks_passed": deterministic_checks_passed,
                "confirmation_policy_reason": confirmation_policy_reason,
                "confirmation_policy_version": confirmation_policy_version,
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # Structural enforcement, not a query-time filter: non-confirmed rows must
    # be NOT_APPLICABLE by construction. Asserted here so a future code change
    # that breaks this invariant fails loudly instead of silently.
    bad = df[(df["match_status"] != "MATCH_CONFIRMED") & (df["price_evidence_status"] != "NOT_APPLICABLE")]
    assert bad.empty, "price_evidence_status must be NOT_APPLICABLE for every non-MATCH_CONFIRMED row"
    return df.drop_duplicates(subset=["candidate_key"], keep="first").reset_index(drop=True)


def write_match_decisions(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    *,
    inventory_uid: str | None = None,
) -> None:
    """Write decisions. Full rebuild by default; item-scoped refresh when
    inventory_uid is provided for dashboard-triggered jobs."""
    if inventory_uid:
        conn.execute("DELETE FROM match_decisions WHERE inventory_uid = ?", [inventory_uid])
    else:
        conn.execute("DELETE FROM match_decisions")
    if df.empty:
        return
    cols = [
        "decision_version", "decision_run_id", "match_run_id", "candidate_key", "inventory_uid",
        "source_table", "source_id", "evidence_uid", "matching_rule", "evidence_tier", "match_status",
        "match_reason_code", "match_reason_text", "matched_fields", "contradiction_flags",
        "risk_flags", "collection_relationship", "rule_precision_reference", "price_evidence_status",
        "deterministic_checks_passed", "confirmation_policy_reason", "confirmation_policy_version",
    ]
    out = df[cols].copy()
    start_id = conn.execute("SELECT COALESCE(MAX(decision_id), 0) + 1 FROM match_decisions").fetchone()[0]
    out.insert(0, "decision_id", range(start_id, start_id + len(out)))
    conn.register("tmp_decisions", out)
    all_cols = ["decision_id"] + cols
    conn.execute(f"INSERT INTO match_decisions ({','.join(all_cols)}) SELECT {','.join(all_cols)} FROM tmp_decisions")
    conn.unregister("tmp_decisions")


def build_inventory_match_summary(
    conn: duckdb.DuckDBPyConnection,
    *,
    summary_run_id: str | None = None,
    inventory_uid: str | None = None,
) -> pd.DataFrame:
    """Pure derivation from match_decisions — no independent status
    assignment. One row per eligible inventory_uid; items with zero
    match_decisions rows get NO_CANDIDATES, never a fabricated row absence."""
    summary_run_id = summary_run_id or f"summary_run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    where = "WHERE validation_status <> 'FAIL'"
    params: list[str] = []
    if inventory_uid:
        where += " AND inventory_uid = ?"
        params.append(inventory_uid)
    inventory_df = conn.execute(f"SELECT inventory_uid FROM staging_inventory {where}", params).df()
    if inventory_uid:
        decisions_df = conn.execute("SELECT * FROM match_decisions WHERE inventory_uid = ?", [inventory_uid]).df()
    else:
        decisions_df = conn.execute("SELECT * FROM match_decisions").df()

    rows = []
    for uid in inventory_df["inventory_uid"]:
        item_decisions = decisions_df[decisions_df["inventory_uid"] == uid]
        total = len(item_decisions)
        confirmed = int((item_decisions["match_status"] == "MATCH_CONFIRMED").sum())
        review = int((item_decisions["match_status"] == "REVIEW_REQUIRED").sum())
        low_conf = int((item_decisions["match_status"] == "LOW_CONFIDENCE_CANDIDATE").sum())
        insufficient = int((item_decisions["match_status"] == "INSUFFICIENT_EVIDENCE").sum())
        rejected = int((item_decisions["match_status"] == "NO_MATCH").sum())
        source_count = item_decisions["source_table"].nunique()
        has_self = bool(((item_decisions["source_table"] == "match_candidates_active")
                          & (item_decisions["collection_relationship"] == "SELF_SOURCED")).any())
        has_cross = bool(((item_decisions["source_table"] == "match_candidates_active")
                           & (item_decisions["collection_relationship"] == "CROSS_REFERENCED")).any())

        if total == 0:
            status = "NO_CANDIDATES"
        elif confirmed > 0:
            status = "HAS_CONFIRMED_MATCH"
        elif review > 0:
            status = "REVIEW_PENDING"
        elif low_conf > 0:
            # v1.1: item's best evidence is calibre-only/component (out of the
            # human review queue, retained for future evaluation).
            status = "ONLY_LOW_CONFIDENCE_CANDIDATES"
        elif insufficient > 0 and rejected == 0:
            status = "ONLY_INSUFFICIENT_EVIDENCE"
        elif rejected > 0 and (rejected + insufficient) == total and confirmed == 0 and review == 0:
            # every remaining candidate is NO_MATCH (rejected==total) is the strict definition;
            # if a mix of rejected+insufficient with no confirmed/review exists, rejected still
            # dominates the analytical signal ("at least one row was actively disproved") over
            # a purely-weak-evidence read -- see docs/MODULE5_DECISION_LAYER_DESIGN.md Phase 3/6.
            status = "ALL_CANDIDATES_REJECTED" if rejected == total else "ONLY_INSUFFICIENT_EVIDENCE"
        else:
            status = "ALL_CANDIDATES_REJECTED"

        rows.append({
            "inventory_uid": uid,
            "summary_run_id": summary_run_id,
            "inventory_match_status": status,
            "confirmed_candidate_count": confirmed,
            "review_candidate_count": review,
            "low_confidence_candidate_count": low_conf,
            "insufficient_candidate_count": insufficient,
            "rejected_candidate_count": rejected,
            "total_candidate_count": total,
            "source_count": int(source_count),
            "has_self_sourced_active_evidence": has_self,
            "has_cross_referenced_active_evidence": has_cross,
        })

    return pd.DataFrame(rows)


def write_inventory_match_summary(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    *,
    inventory_uid: str | None = None,
) -> None:
    if inventory_uid:
        conn.execute("DELETE FROM inventory_match_summary WHERE inventory_uid = ?", [inventory_uid])
    else:
        conn.execute("DELETE FROM inventory_match_summary")
    if df.empty:
        return
    cols = list(df.columns)
    conn.register("tmp_summary", df[cols])
    conn.execute(f"INSERT INTO inventory_match_summary ({','.join(cols)}) SELECT {','.join(cols)} FROM tmp_summary")
    conn.unregister("tmp_summary")


def run_decision_layer(
    conn: duckdb.DuckDBPyConnection,
    *,
    decision_run_id: str | None = None,
    inventory_uid: str | None = None,
) -> dict:
    decisions_df = build_match_decisions(conn, decision_run_id=decision_run_id, inventory_uid=inventory_uid)
    write_match_decisions(conn, decisions_df, inventory_uid=inventory_uid)
    summary_df = build_inventory_match_summary(conn, summary_run_id=decision_run_id, inventory_uid=inventory_uid)
    write_inventory_match_summary(conn, summary_df, inventory_uid=inventory_uid)

    status_counts = decisions_df["match_status"].value_counts().to_dict() if not decisions_df.empty else {}
    inv_counts = summary_df["inventory_match_status"].value_counts().to_dict() if not summary_df.empty else {}
    return {
        "decision_run_id": decision_run_id,
        "total_decisions": len(decisions_df),
        "decision_status_counts": {k: int(v) for k, v in status_counts.items()},
        "inventory_status_counts": {k: int(v) for k, v in inv_counts.items()},
    }


def main() -> None:
    setup_logging()
    log_and_print("=" * 60)
    log_and_print("WATCHPARTS — STEP 6: DECIDE MATCHES (deterministic, no scoring)")
    log_and_print("=" * 60)

    # Explicit --db, matching every other pipeline script's interface
    # (WATCHPARTS_DB env var alone was too easy to silently miss -- this
    # script wrote to the live DB unintentionally during a from-zero audit
    # run that passed --db without realizing this script didn't parse it;
    # argparse also now rejects any unrecognized flag loudly instead of
    # silently ignoring it, closing that exact footgun).
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=None)
    parser.add_argument("--inventory-uid", default=None)
    args = parser.parse_args()
    db_path = Path(args.db) if args.db else DB_PATH

    log_and_print(f"Database target: {db_path}")
    conn = get_connection(db_path)
    try:
        summary = run_decision_layer(conn, inventory_uid=args.inventory_uid)
    finally:
        conn.close()

    log_and_print("")
    log_and_print(f"Decision run: {summary['decision_run_id']}")
    log_and_print(f"Total decisions: {summary['total_decisions']:,}")
    for status, count in summary["decision_status_counts"].items():
        log_and_print(f"  {status}: {count:,}")
    log_and_print("Inventory summary:")
    for status, count in summary["inventory_status_counts"].items():
        log_and_print(f"  {status}: {count:,}")
    log_and_print("✓ Decision layer complete. No scoring, no TMV, no price eligibility.")


if __name__ == "__main__":
    main()

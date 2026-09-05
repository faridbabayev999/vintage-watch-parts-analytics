"""
20_matching_v2_verification.py
================================
Module 5, matching v2: a VERIFICATION layer on top of existing (unchanged)
candidate generation (scripts/05_generate_match_candidates.py). Root cause
this addresses (docs/MATCHING_AUTOMATION_IMPLEMENTATION_REPORT.md): brand +
part-number TOKEN CO-OCCURRENCE catches related/compatible/wrong components,
not just the exact SKU -- measured at 39.5-56.5% precision against 211 real
human-reviewed candidates.

This module does NOT touch candidate generation (recall-preserving) and does
NOT touch match_decisions/MATCH_CONFIRMED/validation_policy (the gate is
unchanged -- a candidate scoring high here is still just a candidate; only a
real, measured, owner-approved validation_policy row can ever produce
MATCH_CONFIRMED). It computes a deterministic match_quality_score plus a full
explanation, for candidates to be re-sampled and re-reviewed.

Five verification checks, each 0.0-1.0:
  - part_number_exactness (weight 0.35): token-BOUNDARY match of the
    inventory part number in the listing title. \\bN\\b already rejects a
    pure numeric substring match (e.g. "4419" inside "44190" is NOT a
    boundary match) but does NOT by itself reject "4419-compatible" (that
    IS a real boundary match on the number) -- the negative-keyword check
    below is what catches that case, by design (layered, not redundant).
  - brand_match (weight 0.25): exact, whole-word brand match.
  - component_type_match (weight 0.20): reuses the already-validated (G2,
    CONDITIONAL PASS) scripts/listing_quality_classifier.py -- 1.0 if the
    title classifies as WATCH_PART, 0.0 for COMPLETE_WATCH/ACCESSORY/
    WRONG_BRAND/MANUAL_DOCUMENTATION (wrong product type), 0.5 for UNKNOWN
    (no decisive signal either way -- not evidence of wrongness).
  - caliber_match (weight 0.10): token-boundary caliber match if the
    inventory row has a caliber; 1.0 if present in inventory and matched,
    0.5 if present in inventory but absent from the title (omission is not
    proof of a wrong item), 1.0 (neutral/N-A) if inventory has no caliber
    to check against.
  - listing_quality (weight 0.10): 1.0 minus a penalty per configurable
    negative-keyword hit (docs/MATCHING_V2_NEGATIVE_KEYWORDS -- see
    NEGATIVE_KEYWORDS below), floored at 0.0.

match_quality_score is a WEIGHTED SUM, never a hard veto by itself --
consistent with the owner's instruction that MATCH_CONFIRMED must not depend
on score alone. decision_reason states plainly whether the candidate would
be ELIGIBLE_FOR_REVIEW (score >= SCORE_THRESHOLD) or NOT_ELIGIBLE, but this
script writes no decision anywhere; it only scores and explains.
"""
from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent
_spec = importlib.util.spec_from_file_location("g2_classifier", SCRIPTS_DIR / "listing_quality_classifier.py")
g2 = importlib.util.module_from_spec(_spec)
sys.path.insert(0, str(SCRIPTS_DIR))
_spec.loader.exec_module(g2)

SCORE_THRESHOLD = 0.95

WEIGHTS = {
    "part_number_exactness": 0.35,
    "brand_match": 0.25,
    "component_type_match": 0.20,
    "caliber_match": 0.10,
    "listing_quality": 0.10,
}

# Configurable exclusion vocabulary (owner spec). Word-boundary matched,
# case-insensitive. Each hit is a penalty, not an automatic veto -- see
# _listing_quality below. Kept as a plain list (not a DB reference table)
# because it is deterministic vocabulary tied to this scoring code, the
# same pattern already used for G2's own keyword lists.
NEGATIVE_KEYWORDS = [
    "compatible", "replacement", "for", "style", "alternative",
    "homage", "similar", "bundle", "lot", "generic", "aftermarket", "fits",
]
NEGATIVE_KEYWORD_PENALTY = 0.34  # ~3 hits floors the sub-score at 0


def _norm(s: str | None) -> str:
    return (s or "").strip().lower()


def _token_boundary_match(needle: str, haystack: str) -> bool:
    needle = _norm(needle)
    if not needle:
        return False
    pattern = r"\b" + re.escape(needle) + r"\b"
    return re.search(pattern, _norm(haystack)) is not None


def _part_number_exactness(inventory_part_number: str, title: str) -> float:
    return 1.0 if _token_boundary_match(inventory_part_number, title) else 0.0


def _brand_match(inventory_brand: str, title: str) -> float:
    return 1.0 if _token_boundary_match(inventory_brand, title) else 0.0


def _component_type_match(title: str, inventory_brand: str | None) -> tuple[float, dict]:
    result = g2.classify(title, inventory_brand)
    cls = result["classification"]
    if cls == "WATCH_PART":
        score = 1.0
    elif cls == "UNKNOWN":
        score = 0.5
    else:
        score = 0.0
    return score, result


def _caliber_match(inventory_caliber: str | None, title: str) -> tuple[float, str]:
    caliber = _norm(inventory_caliber)
    if not caliber:
        return 1.0, "NOT_APPLICABLE (inventory has no caliber to check)"
    if _token_boundary_match(caliber, title):
        return 1.0, "MATCHED"
    return 0.5, "ABSENT_FROM_TITLE (not proof of wrong item)"


def _listing_quality(title: str) -> tuple[float, list]:
    t = _norm(title)
    hits = [kw for kw in NEGATIVE_KEYWORDS if re.search(r"\b" + re.escape(kw) + r"\b", t)]
    score = max(0.0, 1.0 - NEGATIVE_KEYWORD_PENALTY * len(hits))
    return score, hits


def score_candidate(
    inventory_brand: str, inventory_part_number: str, inventory_caliber: str | None,
    title: str,
) -> dict:
    """Returns {score, components, positive_features, negative_features,
    decision_reason}. Deterministic; no randomness, no ML, no DB write."""
    pn_score = _part_number_exactness(inventory_part_number, title)
    brand_score = _brand_match(inventory_brand, title)
    comp_score, g2_result = _component_type_match(title, inventory_brand)
    cal_score, cal_note = _caliber_match(inventory_caliber, title)
    lq_score, negative_hits = _listing_quality(title)

    components = {
        "part_number_exactness": pn_score,
        "brand_match": brand_score,
        "component_type_match": comp_score,
        "caliber_match": cal_score,
        "listing_quality": lq_score,
    }
    score = round(sum(components[k] * WEIGHTS[k] for k in WEIGHTS), 4)

    positive_features = []
    negative_features = []
    if pn_score == 1.0:
        positive_features.append(f"part number {inventory_part_number!r} matched as a standalone token")
    else:
        negative_features.append(f"part number {inventory_part_number!r} not found as a standalone token")
    if brand_score == 1.0:
        positive_features.append(f"brand {inventory_brand!r} matched as a whole word")
    else:
        negative_features.append(f"brand {inventory_brand!r} not found as a whole word")
    if comp_score == 1.0:
        positive_features.append(f"G2 classified listing as WATCH_PART ({g2_result['classification_reason']})")
    elif comp_score == 0.0:
        negative_features.append(f"G2 classified listing as {g2_result['classification']} ({g2_result['classification_reason']})")
    if cal_score == 1.0 and cal_note == "MATCHED":
        positive_features.append("caliber matched as a standalone token")
    elif cal_score == 0.5:
        negative_features.append(f"caliber check: {cal_note}")
    if negative_hits:
        negative_features.append(f"negative keyword(s) found: {negative_hits}")
    else:
        positive_features.append("no negative/exclusion keywords found")

    decision_reason = (
        f"ELIGIBLE_FOR_REVIEW (score {score:.2f} >= threshold {SCORE_THRESHOLD})"
        if score >= SCORE_THRESHOLD
        else f"NOT_ELIGIBLE (score {score:.2f} < threshold {SCORE_THRESHOLD})"
    )

    return {
        "score": score,
        "components": components,
        "positive_features": positive_features,
        "negative_features": negative_features,
        "decision_reason": decision_reason,
        "g2_classification": g2_result["classification"],
    }

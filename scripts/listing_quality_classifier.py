"""
listing_quality_classifier.py
=============================
G2 Query Quality Classifier (Module 2.5 §0a follow-up). Deterministic,
rule-based, explainable listing-type classifier used to MEASURE whether
collected eBay evidence is genuine vintage-watch-part evidence before scaling
pilot → full extraction.

NOT a filter and NOT a model: it never deletes, filters, or modifies raw
evidence. It only derives quality metadata for reporting. No ML, no embeddings,
no probabilities — ordered keyword/pattern rules with an auditable reason and
the exact matched pattern for every decision.

Classifications: WATCH_PART, COMPLETE_WATCH, ACCESSORY, MANUAL_DOCUMENTATION,
WRONG_BRAND, UNKNOWN.

Precedence (first rule that fires wins — order chosen so explicit signals beat
ambiguous ones; see tests):
  1 MANUAL_DOCUMENTATION   3 WRONG_BRAND            4b MOVEMENT/COMPONENT
  2 ACCESSORY              4 explicit SPARE-PART        OVERRIDE (v2, strong
                              marker → WATCH_PART        terms only) → WATCH_PART
                                                      5 COMPLETE_WATCH
                                                      6 component WATCH_PART
                                                      else UNKNOWN

v2 (docs/G2_V2_CLASSIFIER_PROPOSAL.md, owner-approved with a safety
modification): step 4b fixes the G2-audit-confirmed precedence bug where
`armbanduhr`/`wristwatch` won before a co-occurring component noun
(`movement`/`uhrwerk`/etc.) was ever checked — traced to 15/44 hard-case
errors, all resolved by this override using ONLY high-confidence component
terms (COMPONENT_OVERRIDE_TERMS below). Weak/ambiguous terms like `dial`,
`hands`, `bracelet`, `bezel` are deliberately EXCLUDED from the override set
— a bare "dial" mention must not flip a genuine complete-watch listing (e.g.
"Rolex Datejust 36 White Dial Automatik Herrenuhr") into WATCH_PART. Those
terms still work as ordinary WATCH_PART signals at step 6, just not as an
override over an explicit COMPLETE_WATCH keyword.
"""
from __future__ import annotations

import re

CLASSIFIER_VERSION = "g2-quality-v3"

# Inventory brands (Rolex/Tudor project). A competitor brand present WITHOUT one
# of these signals wrong-brand contamination.
EXPECTED_BRANDS = ["rolex", "tudor"]
COMPETITOR_BRANDS = [
    "omega", "seiko", "citizen", "patek", "audemars", "vacheron", "jaeger",
    "lecoultre", "iwc", "panerai", "cartier", "breitling", "tag heuer", "heuer",
    "longines", "tissot", "hamilton", "zenith", "hublot", "montblanc", "bulova",
    "eterna", "movado", "sinn", "oris", "rado",
]

# 1 — documentation (\bbuch\b to avoid Buchse=bushing, a real part)
DOCUMENTATION = [
    "manual", "anleitung", "bedienungsanleitung", "gebrauchsanweisung", "booklet",
    "instruction", "instructions", "catalog", "catalogue", "katalog", "brochure",
    "prospekt", "papiere", "zertifikat", "certificate", "warranty card",
    r"\bpapers\b", r"\bbook\b", r"\bbuch\b", r"\bheft\b",
]
# 1b — v3 taxonomy decision (owner, 2026-07-30): replacement bracelet links and
# clasps are inventoried spare parts for this business, distinct from a
# complete bracelet (sold as a unit, no link/clasp wording) which stays
# ACCESSORY. Checked BEFORE the generic ACCESSORY bracelet/armband match so
# "Bracelet Link" / "Armband ... Ersatzglied" resolve to WATCH_PART, not
# ACCESSORY. Word-bounded: \blink\b does not match German "links" (=left);
# \bschließe\b does not match "schließen" (verb, to close).
LINK_CLASP_OVERRIDE = [
    r"\blink\b", r"\bclasp\b", r"\bschließe\b", r"\bdornschließe\b", "ersatzglied",
]
# 2 — accessories (word-bounded where a term is a substring of a part word)
ACCESSORY = [
    r"\bbox\b", "boxes", "karton", "etui", "pouch", "tasche", r"\bstrap\b",
    r"\barmband\b", "bracelet", "buckle", "dornschließe", "schließe", "display",
    r"\bstand\b", r"\bständer\b", "winder", "uhrenbeweger", r"\btool\b",
    "werkzeug", "loupe", r"\blupe\b", "watch box", "case only", "nur gehäuse",
]
# 4 — explicit spare-part markers: the listing SAYS it is parts
SPARE_PART_MARKER = [
    "ersatzteil", "ersatzteile", r"\bersatz\b", "spare part", "spare parts",
    "for parts", "parts only", r"\bteile\b", r"\bteil\b", "konvolut", "bastler",
    "for repair", "zur reparatur", "as is", "defekt",
]
# 5 — complete watch (strong signals)
# v2: herrenuhr/damenuhr added (docs/G2_V2_CLASSIFIER_PROPOSAL.md) -- direct
# analogues of armbanduhr (men's/women's watch), missing in v1 and confirmed
# by the audit as the cause of 1/2 COMPLETE_WATCH->WATCH_PART errors
# (g2v1-079: "... Automatik Herrenuhr Ref 116234 ..." had no COMPLETE_WATCH
# signal at all and lost to a bare "dial" match).
COMPLETE_WATCH = [
    "armbanduhr", "wristwatch", "wrist watch", "taschenuhr", "pocket watch",
    "complete watch", "full watch", "whole watch", "komplette uhr",
    "komplette armbanduhr", "running watch", r"\buhr komplett\b",
    "herrenuhr", "damenuhr",
]
# 4b — v2 movement/component override (docs/G2_V2_CLASSIFIER_PROPOSAL.md,
# owner-approved with a safety split): HIGH-CONFIDENCE component/movement
# terms ONLY. When one of these co-occurs with a COMPLETE_WATCH keyword, the
# listing is a movement-for-parts sale, not a complete watch -- verified
# against all 15 G2-audit hard-case errors (all resolved using exactly this
# set, 0 new keywords needed beyond what's already in WATCH_PART).
#
# Deliberately EXCLUDES weak/ambiguous terms (dial/zifferblatt, hands, bezel,
# bracelet) that also appear in genuine complete-watch listings (e.g. "White
# Dial Automatik Herrenuhr") -- those must NOT override, or a real complete
# watch mentioning its dial color would be wrongly flipped to WATCH_PART. Weak
# terms still work as normal WATCH_PART signals at step 6 (no COMPLETE_WATCH
# keyword competing), just not as an override.
COMPONENT_OVERRIDE_TERMS = [
    "movement", "uhrwerk", r"\bwerk\b", "mainspring", "zugfeder",
    "balance spring", "hairspring", "unruh", r"\bstem\b", "aufzugswelle",
    "aufzugwelle", "setting lever", "wheel", r"\bbarrel\b", r"\bbridge\b",
    r"\brotor\b", "gear",
]
# 6 — component / spare-part vocabulary (EN + DE + vintage/calibre)
WATCH_PART = [
    # English components
    r"\bpart\b", r"\bparts\b", "wheel", r"\bscrew\b", "mainspring", "hairspring",
    r"\bspring\b", r"\bbalance\b", r"\bbridge\b", r"\bstem\b", r"\bstaff\b",
    r"\bjewel\b", "pinion", "setting lever", r"\bclick\b", "ratchet", r"\brotor\b",
    r"\bbarrel\b", "crown wheel", "escape wheel", r"\bpallet\b", r"\bdial\b",
    r"\bhand\b", r"\bhands\b", "movement", r"\bcaliber\b", r"\bcalibre\b",
    r"\bcannon pinion\b", "keyless", r"\bgasket\b", r"\bcrown\b", r"\bbezel\b",
    r"\bplate\b", r"\bcock\b", "mainplate",
    # German components
    r"\brad\b", "ankerrad", "sekundenrad", "minutenrad", "schraube", r"\bfeder\b",
    "zugfeder", "spirale", "unruh", "unruhwelle", "brücke", "bruecke", r"\bwelle\b",
    "aufzugswelle", r"\bstein\b", "lagerstein", r"\btrieb\b", "sperrad", "klinke",
    "federhaus", "zeiger", "zifferblatt", r"\banker\b", "uhrwerk", r"\bwerk\b",
    r"\bkrone\b", "aufzug", "buchse", "lünette", "luenette", "zapfen", "platine",
    r"\bkloben\b", "kaliber", r"\bkal\b", r"\bcal\b",
]


def _find(text: str, patterns) -> str | None:
    for p in patterns:
        # patterns with regex metachars are used as-is; plain words matched literally
        rx = p if any(ch in p for ch in r"\b[]().*+?") else re.escape(p)
        m = re.search(rx, text)
        if m:
            return p.replace(r"\b", "")   # human-readable term (strip regex word-boundaries only)
    return None


def classify(title: str, brand: str | None = None) -> dict:
    """Return {classification, classification_reason, matched_pattern,
    classifier_version} for one listing title. Deterministic."""
    t = f" {(title or '').lower()} "

    hit = _find(t, DOCUMENTATION)
    if hit:
        return _r("MANUAL_DOCUMENTATION", "documentation keyword", hit)

    # v3: bracelet link/clasp taxonomy override -- checked before ACCESSORY so
    # it pre-empts the generic bracelet/armband match. Guarded against a
    # competitor brand (e.g. "Omega ... Link") -- a wrong-brand link is still
    # WRONG_BRAND contamination, not Rolex/Tudor part evidence; falls through
    # to the normal WRONG_BRAND check below when guarded off.
    hit = _find(t, LINK_CLASP_OVERRIDE)
    if hit:
        comp = _find(t, [re.escape(b) for b in COMPETITOR_BRANDS])
        has_expected = _find(t, [re.escape(b) for b in EXPECTED_BRANDS]) is not None
        if not (comp and not has_expected):
            return _r("WATCH_PART", "bracelet link/clasp override (inventoried spare part)", hit)

    hit = _find(t, ACCESSORY)
    if hit:
        return _r("ACCESSORY", "accessory keyword", hit)

    # WRONG_BRAND: a competitor brand present and no expected brand present
    comp = _find(t, [re.escape(b) for b in COMPETITOR_BRANDS])
    has_expected = _find(t, [re.escape(b) for b in EXPECTED_BRANDS]) is not None
    if comp and not has_expected:
        return _r("WRONG_BRAND", "competitor brand, no inventory brand", comp)

    # explicit "this is parts" marker beats the complete-watch check
    hit = _find(t, SPARE_PART_MARKER)
    if hit:
        return _r("WATCH_PART", "explicit spare-part marker", hit)

    # v2 step 4b: a COMPLETE_WATCH keyword co-occurring with a HIGH-CONFIDENCE
    # component/movement term means this is a movement-for-parts listing, not
    # a complete watch -- checked BEFORE the plain COMPLETE_WATCH match so the
    # override can pre-empt it. Weak terms (dial/hands/bezel/bracelet) are
    # excluded on purpose -- see COMPONENT_OVERRIDE_TERMS docstring.
    cw_hit = _find(t, COMPLETE_WATCH)
    if cw_hit:
        override_hit = _find(t, COMPONENT_OVERRIDE_TERMS)
        if override_hit:
            return _r("WATCH_PART", "movement/component override (strong term pre-empts complete-watch keyword)", override_hit)
        return _r("COMPLETE_WATCH", "complete-watch signal", cw_hit)

    hit = _find(t, WATCH_PART)
    if hit:
        return _r("WATCH_PART", "component / calibre vocabulary", hit)

    return _r("UNKNOWN", "no decisive keyword", None)


def _r(classification, reason, pattern):
    return {
        "classification": classification,
        "classification_reason": reason,
        "matched_pattern": pattern,
        "classifier_version": CLASSIFIER_VERSION,
    }


# ── reporting (read-only; never writes to any DB or raw table) ────────────────

def assert_db_target(conn, expected_path) -> str:
    from pathlib import Path
    rows = conn.execute("PRAGMA database_list").fetchall()
    files = {str(Path(r[2]).resolve()) for r in rows if r[2]}
    exp = str(Path(expected_path).resolve())
    assert exp in files, f"DB target mismatch: connected to {files}, expected {exp!r}"
    return exp


def classify_titles(titles) -> "list[dict]":
    return [classify(t) for t in titles]


def contamination_report(titles) -> dict:
    """Distribution + contamination rates over an iterable of titles."""
    from collections import Counter
    n = 0
    counts = Counter()
    for t in titles:
        counts[classify(t)["classification"]] += 1
        n += 1
    pct = {k: round(100 * counts.get(k, 0) / n, 1) if n else 0.0 for k in
           ["WATCH_PART", "COMPLETE_WATCH", "ACCESSORY", "MANUAL_DOCUMENTATION", "WRONG_BRAND", "UNKNOWN"]}
    return {"n": n, "counts": dict(counts), "pct": pct, "classifier_version": CLASSIFIER_VERSION}

"""
Tests for scripts/listing_quality_classifier.py (G2 quality classifier).
Deterministic rule behaviour per category, German terminology, determinism,
DB-target assertion discipline, and read-only guarantee (no raw modification,
no live writes).
"""
import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
SCHEMA = SCRIPTS / "schema.sql"
LIVE_DB = SCRIPTS.parent / "database" / "watchparts.duckdb"


def _load():
    spec = importlib.util.spec_from_file_location("lqc", SCRIPTS / "listing_quality_classifier.py")
    m = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPTS))
    spec.loader.exec_module(m)
    return m


@pytest.mark.parametrize("title,expected", [
    ("Rolex 3135 escape wheel", "WATCH_PART"),
    ("Rolex 3135 Unruh Ersatzteil", "WATCH_PART"),
    ("Rolex Zugfeder Kaliber 1570", "WATCH_PART"),          # German mainspring + calibre
    ("Rolex 3135 Armbanduhr komplett", "COMPLETE_WATCH"),
    ("Rolex Datejust wristwatch running", "COMPLETE_WATCH"),
    ("Rolex watch box and papers", "MANUAL_DOCUMENTATION"), # papers wins (doc first)
    ("Rolex leather strap 20mm", "ACCESSORY"),
    ("Rolex Uhrenbeweger winder", "ACCESSORY"),
    ("Omega Speedmaster balance wheel", "WRONG_BRAND"),     # competitor, no rolex/tudor
    ("Rolex 3135", "UNKNOWN"),                              # bare calibre = ambiguous
])
def test_classification_categories(title, expected):
    m = _load()
    assert m.classify(title)["classification"] == expected


def test_classifier_version_is_v3():
    m = _load()
    assert m.CLASSIFIER_VERSION == "g2-quality-v3"


# ── v3 taxonomy tests: bracelet link/clasp = WATCH_PART, complete bracelet =
# ACCESSORY (owner decision, 2026-07-30) ─────────────────────────────────────

@pytest.mark.parametrize("title", [
    "Genuine Rolex Clasp & Blade Pin 32-20734",                              # g2v1-013
    "Auth. Rolex Oyster Stainless Steel 14mm Bracelet Link 32-20634",        # g2v1-027
    "BRANDNEU Rolex 32-33445 GMT Armband Link Jubilee Original Maglia 126710 126720",  # g2v1-008
    "Rolex Ersatzglied, Link Präsident Armband u.a. 9,85 mm, 750 Gold poliert/matt",  # g2v1-028
])
def test_v3_link_clasp_is_watch_part(title):
    m = _load()
    assert m.classify(title)["classification"] == "WATCH_PART"


@pytest.mark.parametrize("title", [
    "Genuine Rolex Steel End Caps B32-555-0-K2 Original Jubilee Bracelet",  # complete bracelet, no link/clasp word
    "Silicone Watch Strap 18mm 20mm 22mm 24mm With Butterfly Buckle Rubber Bracelet",
])
def test_v3_complete_bracelet_without_link_clasp_stays_accessory(title):
    m = _load()
    assert m.classify(title)["classification"] == "ACCESSORY"


def test_v3_wrong_brand_link_stays_wrong_brand():
    """A competitor-brand bracelet link is still WRONG_BRAND contamination,
    not Rolex/Tudor part evidence -- the override must not bypass brand
    checking (regression caught: g2v1-004)."""
    m = _load()
    r = m.classify("SELTENE OMEGA CONSTELLATION 114ST6545 EDELSTAHL LINK 14MM")
    assert r["classification"] == "WRONG_BRAND"


def test_v3_link_word_boundary_excludes_german_links():
    """'links' (German for 'left') must not match \\blink\\b."""
    m = _load()
    hit = m._find(" rolex zifferblatt links unten ", m.LINK_CLASP_OVERRIDE)
    assert hit is None


def test_v3_schliesse_word_boundary_excludes_verb():
    """'schließen' (verb, to close) must not match \\bschließe\\b (noun, clasp)."""
    m = _load()
    hit = m._find(" schlüssel zum öffnen und schließen ", m.LINK_CLASP_OVERRIDE)
    assert hit is None


def test_v3_no_regression_on_all_44_human_labeled_rows():
    """Same no-regression guarantee as v2, re-verified after the v3 taxonomy
    change. Rows whose CORRECT classification changes because the taxonomy
    definition itself changed (e.g. g2v1-013, clasp: was ACCESSORY under the
    old taxonomy, now correctly WATCH_PART under the new one) are not
    regressions -- only a row that was correct and is now wrong counts."""
    import csv
    m = _load()
    path = SCRIPTS.parent / "reports" / "module5_pilot" / "g2_adjudication_completed.csv"
    rows = list(csv.DictReader(open(path, encoding="utf-8")))
    regressions = []
    for r in rows:
        was_correct = r["classifier_prediction"] == r["final_human_label"]
        now_correct = m.classify(r["title"])["classification"] == r["final_human_label"]
        if was_correct and not now_correct:
            regressions.append(r["sample_id"])
    assert regressions == [], f"v3 introduced new errors on previously-correct rows: {regressions}"


# ── v2 regression tests: G2 audit hard-case errors (docs/G2_FINAL_AUDIT_SUMMARY.md,
# docs/G2_V2_CLASSIFIER_PROPOSAL.md) ─────────────────────────────────────────────

@pytest.mark.parametrize("title", [
    "2 alte Rolex Uhrwerke   , 2 alte Uhrwerke für Armbanduhren  , ROLEX",       # g2v1-001
    "Rolex Vintage 1930 Men's Wrist Watch Movement  Cal 6406 Balance Spring Ok",  # g2v1-003
    "ROLEX Original Movement Cal.1600 Wrist Watch Parts Gold Dial Color Working #1",  # g2v1-009
    "Armbanduhrwerk Rolex Cal. 1210",                                            # g2v1-011
    "Rolex Vintage 1930 Men's Wrist Watch Movement  Cal 5817 Balance Spring Ok",  # g2v1-036
    "ROLEX Original Movement Cal.1600 Wrist Watch Parts Silver Dial Color For Part...",  # g2v1-038
    "Armbanduhrwerk Rolex Cal.700",                                              # g2v1-047
    "Zugfeder passend für 2030 or 2035 Rolex Kaliber Armbanduhr Mainspring 2035-4419",  # g2v1-055
    "Rolex Uhrwerk für Armbanduhr Handaufzug Kal. 1600 - 21 mm ca. 1965",         # g2v1-073
    "Zugfeder passend für 2130- 311 Rolex Kaliber Armbanduhr Mainspring 771",     # g2v1-082
    "Aufzugswelle passend für Diverse Rolex Kaliber Armbanduhr Welle Winding Stem",  # g2v1-085
    "VINTAGE GENUINE SETTING LEVER DETENT FOR ROLEX 2135 - 220 WRIST WATCH MOVEMENT",  # g2v1-087
    "Rolex Automatic Uhrwerk für Armbanduhr ETA 2671 - 20 mm Schweiz ca. 1960",   # g2v1-088
])
def test_v2_fixes_movement_component_override_cases(title):
    """These 13 titles were misclassified COMPLETE_WATCH under v1 (armbanduhr/
    wristwatch won before the co-occurring movement/component noun was ever
    checked); human-confirmed WATCH_PART. v2's override must fix all of them."""
    m = _load()
    r = m.classify(title)
    assert r["classification"] == "WATCH_PART", (title, r)


@pytest.mark.parametrize("title", [
    "Original Rolex Oysterdate Speedking Precision ref. 6430 wristwatch dial",   # g2v1-015
    "ORIGINAL ROLEX CELLINI ARMBANDUHR ZIFFERBLATT 20,00 MM FÜR KALIBER 1600 - 1601 (RD02)",  # g2v1-093
])
def test_v2_known_unresolved_weak_term_only_cases(title):
    """These 2 titles remain COMPLETE_WATCH under v2 BY DESIGN: their only
    watch-part signal is a WEAK term (dial/zifferblatt) or a term not on the
    high-confidence override list (kaliber) -- the safety modification
    deliberately excludes weak terms from the override so a real complete
    watch mentioning its dial/calibre isn't wrongly flipped. Documented as a
    known, accepted trade-off (docs/G2_V2_CLASSIFIER_PROPOSAL.md), not a bug."""
    m = _load()
    r = m.classify(title)
    assert r["classification"] == "COMPLETE_WATCH", (title, r)


def test_v2_herrenuhr_damenuhr_added():
    m = _load()
    r = m.classify("Rolex Datejust 36 White Dial Stahl / Gold Automatik Herrenuhr Ref 116234 G-Serie")  # g2v1-079
    assert r["classification"] == "COMPLETE_WATCH" and r["matched_pattern"] == "herrenuhr"


def test_v2_deferred_reference_only_pattern_not_addressed():
    """g2v1-030 ('ROLEX CELLINI 32 REF. 5112 WHITE ROMAN DIAL 18K GOLD 1995') has
    no armbanduhr/wristwatch/herrenuhr word at all -- explicitly deferred in the
    v2 proposal (harder pattern, not guessed at). Still WATCH_PART (via 'dial')
    -- documents the known limitation, doesn't hide it."""
    m = _load()
    r = m.classify("ROLEX CELLINI 32 REF. 5112 WHITE ROMAN DIAL 18K GOLD 1995")
    assert r["classification"] == "WATCH_PART"  # known unresolved case, not claimed fixed


@pytest.mark.parametrize("title,expected", [
    # owner-specified edge cases (must stay WATCH_PART)
    ("Rolex Armbanduhrwerk Cal.700", "WATCH_PART"),
    ("Rolex Vintage Wrist Watch Movement Cal 6406 Balance Spring", "WATCH_PART"),
    ("Rolex Cellini dial for caliber 1600", "WATCH_PART"),
    # owner-specified edge cases (must stay COMPLETE_WATCH -- weak term alone must not override)
    ("Rolex Datejust 36 White Dial Automatik Herrenuhr Ref 116234", "COMPLETE_WATCH"),
    ("Rolex Cellini 18K Gold Wristwatch", "COMPLETE_WATCH"),
])
def test_v2_owner_specified_edge_cases(title, expected):
    m = _load()
    r = m.classify(title)
    assert r["classification"] == expected, (title, r)


def test_v2_weak_term_alone_does_not_override_complete_watch():
    """Direct test of the safety modification: 'dial' is a WATCH_PART term but
    NOT in COMPONENT_OVERRIDE_TERMS, so it must not flip a complete-watch
    listing that only mentions dial color."""
    m = _load()
    assert "dial" not in [p.replace(r"\b", "") for p in m.COMPONENT_OVERRIDE_TERMS]
    r = m.classify("Rolex Datejust White Dial Automatik Herrenuhr")
    assert r["classification"] == "COMPLETE_WATCH"


def test_german_terminology_reduces_unknown():
    m = _load()
    # each German component term should classify as WATCH_PART, not UNKNOWN
    for t in ["Rolex Aufzugswelle", "Rolex Zeiger Satz", "Rolex Zifferblatt",
              "Rolex Ankerrad", "Rolex Federhaus", "Rolex Bruecke", "Rolex Schraube"]:
        assert m.classify(t)["classification"] == "WATCH_PART", t


def test_explicit_spare_part_marker_beats_complete_watch():
    m = _load()
    # a complete-watch word + explicit parts marker => WATCH_PART (it's parts)
    r = m.classify("Armbanduhr Ersatzteil Rolex")
    assert r["classification"] == "WATCH_PART" and r["matched_pattern"]


def test_reason_and_pattern_and_version_present():
    m = _load()
    r = m.classify("Rolex 3135 escape wheel")
    assert r["classification_reason"] and r["matched_pattern"]
    assert r["classifier_version"] == m.CLASSIFIER_VERSION


def test_deterministic():
    m = _load()
    titles = ["Rolex 3135 wheel", "Omega dial", "Rolex box", "Rolex 3135", "Rolex Unruh"]
    a = [m.classify(t) for t in titles]
    b = [m.classify(t) for t in titles]
    assert a == b


def test_contamination_report_shape():
    m = _load()
    rep = m.contamination_report(["Rolex wheel", "Rolex Armbanduhr komplett", "Rolex box", "Rolex 3135"])
    assert rep["n"] == 4 and set(rep["pct"]) >= {"WATCH_PART", "COMPLETE_WATCH", "UNKNOWN"}
    assert rep["classifier_version"] == m.CLASSIFIER_VERSION


def test_db_target_assertion(tmp_path):
    m = _load()
    db = tmp_path / "q.duckdb"
    duckdb.connect(str(db)).execute(SCHEMA.read_text())
    conn = duckdb.connect(str(db), read_only=True)
    assert m.assert_db_target(conn, db)
    with pytest.raises(AssertionError):
        m.assert_db_target(conn, tmp_path / "other.duckdb")
    conn.close()


def test_read_only_no_raw_modification(tmp_path):
    """Classifier reads titles and never writes; a read-only connection over
    the data must be sufficient (proves no raw mutation path)."""
    m = _load()
    db = tmp_path / "q.duckdb"
    w = duckdb.connect(str(db)); w.execute(SCHEMA.read_text())
    w.execute("INSERT INTO stg_active_targeted (id, title) VALUES (1,'Rolex 3135 wheel'),(2,'Omega dial')")
    w.close()
    conn = duckdb.connect(str(db), read_only=True)     # read-only handle
    m.assert_db_target(conn, db)
    titles = [r[0] for r in conn.execute("SELECT title FROM stg_active_targeted").fetchall()]
    rep = m.contamination_report(titles)
    assert rep["n"] == 2
    conn.close()
    # row count unchanged
    c = duckdb.connect(str(db), read_only=True)
    assert c.execute("SELECT COUNT(*) FROM stg_active_targeted").fetchone()[0] == 2
    c.close()

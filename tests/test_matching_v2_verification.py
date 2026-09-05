"""
Matching v2 verification-layer tests (scripts/20_matching_v2_verification.py).
Pure functions, no DB, no fixtures needed beyond plain strings.
"""
import importlib.util
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"


def _load():
    spec = importlib.util.spec_from_file_location("v2verify", SCRIPTS / "20_matching_v2_verification.py")
    m = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPTS))
    spec.loader.exec_module(m)
    return m


def test_exact_part_number_as_standalone_token_matches():
    m = _load()
    assert m._token_boundary_match("4419", "Rolex 4419 spring bar") is True


def test_part_number_rejects_longer_number_substring():
    m = _load()
    assert m._token_boundary_match("4419", "Rolex 44190 spring bar") is False
    assert m._token_boundary_match("4419", "Rolex 14419 spring bar") is False


def test_part_number_boundary_match_does_not_by_itself_reject_compatible_phrasing():
    """Token-boundary matching alone still matches '4419-compatible' -- this
    is by design; the negative-keyword layer is what catches that case."""
    m = _load()
    assert m._token_boundary_match("4419", "Rolex 4419-compatible spring bar") is True


def test_brand_match_requires_whole_word():
    m = _load()
    assert m._brand_match("Rolex", "Genuine Rolex crown") == 1.0
    assert m._brand_match("Rolex", "Rolexx replica crown") == 0.0


def test_negative_keywords_penalize_listing_quality():
    m = _load()
    score, hits = m._listing_quality("Rolex 4419 compatible replacement spring bar")
    assert hits == ["compatible", "replacement"]
    assert score == max(0.0, 1.0 - 2 * m.NEGATIVE_KEYWORD_PENALTY)


def test_clean_listing_has_no_negative_hits():
    m = _load()
    score, hits = m._listing_quality("Genuine Rolex 4419 spring bar for caliber 3135")
    # "for" is in the negative list -- even a clean listing may legitimately
    # contain it, which is exactly why it's a penalty, not a hard veto.
    assert "for" in hits
    assert score < 1.0


def test_caliber_absent_from_inventory_is_neutral_not_applicable():
    m = _load()
    score, note = m._caliber_match(None, "Rolex 4419 spring bar")
    assert score == 1.0 and "NOT_APPLICABLE" in note


def test_caliber_present_but_missing_from_title_is_partial_not_zero():
    m = _load()
    score, note = m._caliber_match("3135", "Rolex 4419 spring bar")
    assert score == 0.5 and "ABSENT_FROM_TITLE" in note


def test_score_candidate_high_confidence_true_positive_scores_above_threshold():
    m = _load()
    r = m.score_candidate("Rolex", "4419", "3135", "Genuine Rolex 4419 spring bar for caliber 3135")
    assert r["score"] > 0.8
    assert "part number '4419' matched as a standalone token" in r["positive_features"]


def test_score_candidate_known_false_match_pattern_scores_low():
    """Reproduces the exact failure mode identified in the v1 precision
    audit: brand+part-number co-occurrence with a wrong/compatible product."""
    m = _load()
    r = m.score_candidate("Rolex", "23042", "32", 'Genuine Rolex Cal 13" 72A 8681A Hour Hammer Cover New Condition')
    assert r["score"] < m.SCORE_THRESHOLD
    assert r["decision_reason"].startswith("NOT_ELIGIBLE")


def test_score_candidate_complete_watch_penalized_via_component_type():
    m = _load()
    r = m.score_candidate("Rolex", "16700", "315", "Rolex GMT Master 16700 Complete Watch automatic mens")
    assert r["components"]["component_type_match"] == 0.0
    assert r["g2_classification"] == "COMPLETE_WATCH"


def test_score_is_weighted_sum_matches_hand_computation():
    m = _load()
    r = m.score_candidate("Rolex", "4419", "3135", "Genuine Rolex 4419 spring bar for caliber 3135")
    expected = sum(r["components"][k] * m.WEIGHTS[k] for k in m.WEIGHTS)
    assert r["score"] == round(expected, 4)

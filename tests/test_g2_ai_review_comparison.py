"""
Tests for scripts/g2_ai_review_comparison.py.

Verifies: integrity check catches a tampered frozen row; dangerous-pair
detection fires correctly; needs_human_review logic (disagreement OR differs-
from-both OR dangerous pattern); adjudication file never contains a filled
final_human_label (mechanical prep only, no labels inferred).
"""
import csv
import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"


def _load():
    spec = importlib.util.spec_from_file_location("g2cmp", SCRIPTS / "g2_ai_review_comparison.py")
    m = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPTS))
    spec.loader.exec_module(m)
    return m


def _write(path, rows, cols=("sample_id", "title", "classifier_version", "ruleset_hash",
                              "predicted_class", "matched_rule", "human_label", "reviewer",
                              "label_date", "frozen_at")):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(cols)); w.writeheader()
        for r in rows:
            w.writerow({**{c: "" for c in cols}, **r})


def test_integrity_check_catches_tampered_title(tmp_path):
    m = _load()
    frozen = [{"sample_id": "s1", "title": "Rolex wheel", "predicted_class": "WATCH_PART"}]
    reviewed = [{"sample_id": "s1", "title": "TAMPERED TITLE", "predicted_class": "WATCH_PART"}]
    with pytest.raises(SystemExit):
        m._check_integrity(frozen, reviewed, "test-file")


def test_integrity_check_passes_when_matching(tmp_path):
    m = _load()
    frozen = [{"sample_id": "s1", "title": "Rolex wheel", "predicted_class": "WATCH_PART"}]
    reviewed = [{"sample_id": "s1", "title": "Rolex wheel", "predicted_class": "WATCH_PART",
                 "human_label": "WATCH_PART"}]
    m._check_integrity(frozen, reviewed, "test-file")  # must not raise


def test_dangerous_pair_detection(tmp_path, monkeypatch):
    m = _load()
    frozen = tmp_path / "frozen.csv"
    cg = tmp_path / "cg.csv"
    cl = tmp_path / "cl.csv"
    _write(frozen, [{"sample_id": "s1", "title": "t1", "predicted_class": "WATCH_PART"}])
    _write(cg, [{"sample_id": "s1", "title": "t1", "predicted_class": "WATCH_PART", "human_label": "COMPLETE_WATCH"}])
    _write(cl, [{"sample_id": "s1", "title": "t1", "predicted_class": "WATCH_PART", "human_label": "WATCH_PART"}])
    monkeypatch.setattr(m, "FROZEN", frozen)
    monkeypatch.setattr(m, "CHATGPT", cg)
    monkeypatch.setattr(m, "CLAUDE", cl)
    rows = m.build_comparison()
    assert rows[0]["risk_category"] == "COMPLETE_WATCH predicted as WATCH_PART"
    assert rows[0]["needs_human_review"] == "TRUE"  # dangerous pattern, even though not both disagree


def test_differs_from_both_flags_review(tmp_path, monkeypatch):
    m = _load()
    frozen = tmp_path / "frozen.csv"
    cg = tmp_path / "cg.csv"
    cl = tmp_path / "cl.csv"
    _write(frozen, [{"sample_id": "s1", "title": "t1", "predicted_class": "UNKNOWN"}])
    _write(cg, [{"sample_id": "s1", "title": "t1", "predicted_class": "UNKNOWN", "human_label": "ACCESSORY"}])
    _write(cl, [{"sample_id": "s1", "title": "t1", "predicted_class": "UNKNOWN", "human_label": "ACCESSORY"}])
    monkeypatch.setattr(m, "FROZEN", frozen)
    monkeypatch.setattr(m, "CHATGPT", cg)
    monkeypatch.setattr(m, "CLAUDE", cl)
    rows = m.build_comparison()
    assert rows[0]["agreement_status"] == "AGREED"       # reviewers agree with each other
    assert rows[0]["needs_human_review"] == "TRUE"        # but both differ from the classifier


def test_agreement_no_risk_no_review_needed(tmp_path, monkeypatch):
    m = _load()
    frozen = tmp_path / "frozen.csv"
    cg = tmp_path / "cg.csv"
    cl = tmp_path / "cl.csv"
    _write(frozen, [{"sample_id": "s1", "title": "t1", "predicted_class": "WATCH_PART"}])
    _write(cg, [{"sample_id": "s1", "title": "t1", "predicted_class": "WATCH_PART", "human_label": "WATCH_PART"}])
    _write(cl, [{"sample_id": "s1", "title": "t1", "predicted_class": "WATCH_PART", "human_label": "WATCH_PART"}])
    monkeypatch.setattr(m, "FROZEN", frozen)
    monkeypatch.setattr(m, "CHATGPT", cg)
    monkeypatch.setattr(m, "CLAUDE", cl)
    rows = m.build_comparison()
    assert rows[0]["needs_human_review"] == "FALSE"


def test_adjudication_file_never_prefills_final_label(tmp_path, monkeypatch):
    m = _load()
    frozen = tmp_path / "frozen.csv"
    cg = tmp_path / "cg.csv"
    cl = tmp_path / "cl.csv"
    out_adj = tmp_path / "adj.csv"
    _write(frozen, [{"sample_id": "s1", "title": "t1", "predicted_class": "WATCH_PART"}])
    _write(cg, [{"sample_id": "s1", "title": "t1", "predicted_class": "WATCH_PART", "human_label": "COMPLETE_WATCH"}])
    _write(cl, [{"sample_id": "s1", "title": "t1", "predicted_class": "WATCH_PART", "human_label": "WATCH_PART"}])
    monkeypatch.setattr(m, "FROZEN", frozen)
    monkeypatch.setattr(m, "CHATGPT", cg)
    monkeypatch.setattr(m, "CLAUDE", cl)
    monkeypatch.setattr(m, "OUT_ADJUDICATION", out_adj)
    rows = m.build_comparison()
    n = m.write_adjudication(rows)
    assert n == 1
    written = list(csv.DictReader(open(out_adj)))
    assert written[0]["final_human_label"] == ""

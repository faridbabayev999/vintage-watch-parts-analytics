"""
Tests for scripts/g2_confusion_matrix.py — computes metrics ONLY from a
human_label column; refuses to run on unlabeled/partially-labeled/invalid data
(guards against ever reporting a result that isn't from real human labels).
"""
import csv
import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"


def _load():
    spec = importlib.util.spec_from_file_location("g2cm", SCRIPTS / "g2_confusion_matrix.py")
    m = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(SCRIPTS))
    spec.loader.exec_module(m)
    return m


def _write(tmp_path, rows):
    cols = ["sample_id", "title", "classifier_version", "ruleset_hash",
            "predicted_class", "matched_rule", "human_label", "reviewer", "label_date", "frozen_at"]
    p = tmp_path / "sample.csv"
    with open(p, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader()
        for r in rows:
            w.writerow({**{c: "" for c in cols}, **r})
    return p


def test_refuses_when_all_labels_missing(tmp_path):
    m = _load()
    p = _write(tmp_path, [{"sample_id": "s1", "predicted_class": "WATCH_PART", "human_label": ""}])
    with pytest.raises(SystemExit):
        m.load_labeled(str(p))


def test_refuses_when_partially_labeled(tmp_path):
    m = _load()
    p = _write(tmp_path, [
        {"sample_id": "s1", "predicted_class": "WATCH_PART", "human_label": "WATCH_PART"},
        {"sample_id": "s2", "predicted_class": "ACCESSORY", "human_label": ""},
    ])
    with pytest.raises(SystemExit):
        m.load_labeled(str(p))


def test_refuses_invalid_label_value(tmp_path):
    m = _load()
    p = _write(tmp_path, [{"sample_id": "s1", "predicted_class": "WATCH_PART", "human_label": "MAYBE"}])
    with pytest.raises(SystemExit):
        m.load_labeled(str(p))


def test_computes_when_fully_labeled(tmp_path):
    m = _load()
    p = _write(tmp_path, [
        {"sample_id": "s1", "predicted_class": "WATCH_PART", "human_label": "WATCH_PART"},
        {"sample_id": "s2", "predicted_class": "WATCH_PART", "human_label": "COMPLETE_WATCH"},
        {"sample_id": "s3", "predicted_class": "ACCESSORY", "human_label": "ACCESSORY"},
    ])
    rows = m.load_labeled(str(p))
    assert len(rows) == 3
    assert m.accuracy(rows) == pytest.approx(2 / 3)
    pcm = m.per_class_metrics(rows)
    assert pcm["WATCH_PART"]["tp"] == 1 and pcm["WATCH_PART"]["fp"] == 1
    de = m.dangerous_errors(rows)
    assert de["complete_watch_mislabeled_as_watch_part"] == ["s2"]


def test_load_labeled_accepts_adjudication_column_names(tmp_path):
    """The adjudication file uses classifier_prediction/final_human_label
    instead of predicted_class/human_label -- must be accepted equivalently."""
    m = _load()
    cols = ["sample_id", "title", "classifier_prediction", "chatgpt_label",
            "claude_label", "final_human_label", "review_notes", "reviewer", "review_date"]
    p = tmp_path / "adj.csv"
    with open(p, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader()
        w.writerow({"sample_id": "s1", "title": "t", "classifier_prediction": "WATCH_PART",
                    "chatgpt_label": "WATCH_PART", "claude_label": "WATCH_PART",
                    "final_human_label": "WATCH_PART", "review_notes": "", "reviewer": "", "review_date": ""})
    rows = m.load_labeled(str(p))
    assert rows[0]["predicted_class"] == "WATCH_PART" and rows[0]["human_label"] == "WATCH_PART"


def test_confusion_matrix_shape(tmp_path):
    m = _load()
    p = _write(tmp_path, [{"sample_id": "s1", "predicted_class": "WATCH_PART", "human_label": "WATCH_PART"}])
    rows = m.load_labeled(str(p))
    cm = m.confusion_matrix(rows)
    assert set(cm.keys()) == set(m.CLASSES)
    assert cm["WATCH_PART"]["WATCH_PART"] == 1

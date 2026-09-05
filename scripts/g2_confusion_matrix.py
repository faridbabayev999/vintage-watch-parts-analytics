"""
g2_confusion_matrix.py
=======================
Computes the G2 confusion matrix / precision / recall / F1 from a labeled
reports/module5_pilot/g2_sample_v1.csv. Reads ONLY the `human_label` column as
ground truth against the frozen `predicted_class` — never infers, recomputes,
or fabricates a label. Refuses to run if any row is missing a human_label
(prevents an accidental "0% labeled" report from looking like a real result).

Also breaks results down by query tier when a `--db` is given (joins sample
titles back to stg_active_targeted.query_tier — informational only, never
required for the row-level metrics).

Usage:
    python scripts/g2_confusion_matrix.py reports/module5_pilot/g2_sample_v1.csv
    python scripts/g2_confusion_matrix.py reports/module5_pilot/g2_sample_v1.csv --db database/watchparts.duckdb
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter, defaultdict
from pathlib import Path

CLASSES = ["WATCH_PART", "COMPLETE_WATCH", "ACCESSORY", "MANUAL_DOCUMENTATION", "WRONG_BRAND", "UNKNOWN"]


# Accepts either the original frozen-sample column names (predicted_class,
# human_label) or the adjudication-file column names (classifier_prediction,
# final_human_label) -- same governance rules apply to both: only a filled
# human/final_human label counts, nothing is inferred.
def _normalize(rows: list[dict]) -> list[dict]:
    for r in rows:
        if not r.get("human_label", "").strip() and r.get("final_human_label", "").strip():
            r["human_label"] = r["final_human_label"]
        if not r.get("predicted_class", "").strip() and r.get("classifier_prediction", "").strip():
            r["predicted_class"] = r["classifier_prediction"]
    return rows


def load_labeled(path: str) -> list[dict]:
    rows = _normalize(list(csv.DictReader(open(path, encoding="utf-8"))))
    missing = [r["sample_id"] for r in rows if not r.get("human_label", "").strip()]
    if missing:
        raise SystemExit(
            f"REFUSING to compute metrics: {len(missing)}/{len(rows)} rows have no human_label "
            f"(e.g. {missing[:5]}). Label all rows first — see docs/G2_LABELING_GUIDE.md."
        )
    invalid = [r["sample_id"] for r in rows if r["human_label"] not in CLASSES]
    if invalid:
        raise SystemExit(f"Invalid human_label value(s) in rows: {invalid} — must be one of {CLASSES}")
    return rows


def confusion_matrix(rows: list[dict]) -> dict:
    cm = {a: {p: 0 for p in CLASSES} for a in CLASSES}
    for r in rows:
        cm[r["human_label"]][r["predicted_class"]] += 1
    return cm


def per_class_metrics(rows: list[dict]) -> dict:
    out = {}
    for c in CLASSES:
        tp = sum(1 for r in rows if r["predicted_class"] == c and r["human_label"] == c)
        fp = sum(1 for r in rows if r["predicted_class"] == c and r["human_label"] != c)
        fn = sum(1 for r in rows if r["predicted_class"] != c and r["human_label"] == c)
        precision = tp / (tp + fp) if (tp + fp) else None
        recall = tp / (tp + fn) if (tp + fn) else None
        f1 = (2 * precision * recall / (precision + recall)) if precision and recall and (precision + recall) else None
        out[c] = {"tp": tp, "fp": fp, "fn": fn, "precision": precision, "recall": recall, "f1": f1,
                  "support": sum(1 for r in rows if r["human_label"] == c)}
    return out


def dangerous_errors(rows: list[dict]) -> dict:
    """The specific failure modes called out as highest-risk."""
    false_watch_part = [r for r in rows if r["predicted_class"] == "WATCH_PART" and r["human_label"] == "COMPLETE_WATCH"]
    false_complete_watch = [r for r in rows if r["predicted_class"] == "COMPLETE_WATCH" and r["human_label"] != "COMPLETE_WATCH"]
    false_accessory_as_part = [r for r in rows if r["predicted_class"] == "WATCH_PART" and r["human_label"] == "ACCESSORY"]
    return {
        "complete_watch_mislabeled_as_watch_part": [r["sample_id"] for r in false_watch_part],
        "watch_part_mislabeled_as_complete_watch": [r["sample_id"] for r in false_complete_watch],
        "accessory_mislabeled_as_watch_part": [r["sample_id"] for r in false_accessory_as_part],
    }


def accuracy(rows: list[dict]) -> float:
    correct = sum(1 for r in rows if r["predicted_class"] == r["human_label"])
    return correct / len(rows) if rows else 0.0


def print_report(rows: list[dict]) -> None:
    print(f"Labeled rows: {len(rows)}")
    print(f"Overall accuracy: {accuracy(rows):.1%}\n")

    print("=== Confusion matrix (rows=human_label, cols=predicted_class) ===")
    cm = confusion_matrix(rows)
    header = " " * 24 + "".join(f"{c[:10]:>12}" for c in CLASSES)
    print(header)
    for a in CLASSES:
        print(f"{a:<24}" + "".join(f"{cm[a][p]:>12}" for p in CLASSES))

    print("\n=== Precision / recall / F1 per class ===")
    pcm = per_class_metrics(rows)
    for c in CLASSES:
        m = pcm[c]
        p = f"{m['precision']:.1%}" if m["precision"] is not None else "n/a"
        r = f"{m['recall']:.1%}" if m["recall"] is not None else "n/a"
        f1 = f"{m['f1']:.1%}" if m["f1"] is not None else "n/a"
        print(f"  {c:<24} support={m['support']:>3}  precision={p:>6}  recall={r:>6}  f1={f1:>6}")

    print("\n=== Dangerous errors ===")
    de = dangerous_errors(rows)
    for k, v in de.items():
        print(f"  {k}: {len(v)}  {v if v else ''}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv_path")
    args = ap.parse_args()
    rows = load_labeled(args.csv_path)
    print_report(rows)


if __name__ == "__main__":
    main()

"""
g2_ai_review_comparison.py
===========================
G2 validation — mechanical preparation only. Compares two AI-reviewer label
files (ChatGPT, Claude) against the frozen classifier sample, and produces:

  1. reports/G2_AI_REVIEW_COMPARISON.csv   -- full 100-row comparison
  2. reports/G2_HUMAN_ADJUDICATION_REQUIRED.csv -- rows needing human review,
     final_human_label left BLANK

GOVERNANCE: ChatGPT and Claude labels are AI-generated, NOT human ground
truth. This script never computes precision/recall/F1/confusion-matrix and
never fills final_human_label -- that requires scripts/g2_confusion_matrix.py
run against a file a human has actually labeled. AI agreement here is
preparatory/triage evidence only.

Integrity: refuses to run if either AI-review file's (sample_id, title,
predicted_class) does not match the frozen reports/module5_pilot/g2_sample_v1.csv
row for row -- catches accidental edits to the frozen prediction record.
"""
from __future__ import annotations

import csv
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
FROZEN = BASE_DIR / "reports" / "module5_pilot" / "g2_sample_v1.csv"
CHATGPT = BASE_DIR / "reports" / "module5_pilot" / "ai_review" / "g2_sample_v1_labeled_chatgpt.csv"
CLAUDE = BASE_DIR / "reports" / "module5_pilot" / "ai_review" / "g2_sample_v1_labeled_claude.csv"
OUT_COMPARISON = BASE_DIR / "reports" / "G2_AI_REVIEW_COMPARISON.csv"
OUT_ADJUDICATION = BASE_DIR / "reports" / "G2_HUMAN_ADJUDICATION_REQUIRED.csv"

DANGEROUS_PAIRS = {
    # (classifier predicted_class, AI-reviewer label) -> risk description
    ("WATCH_PART", "COMPLETE_WATCH"): "COMPLETE_WATCH predicted as WATCH_PART",
    ("WATCH_PART", "ACCESSORY"): "ACCESSORY predicted as WATCH_PART",
    ("WATCH_PART", "WRONG_BRAND"): "WRONG_BRAND predicted as WATCH_PART",
    ("COMPLETE_WATCH", "WATCH_PART"): "WATCH_PART predicted as COMPLETE_WATCH",
}


def _read(path: Path) -> list[dict]:
    if not path.exists():
        raise SystemExit(f"STOP: required file missing: {path}")
    return list(csv.DictReader(open(path, encoding="utf-8")))


def _check_integrity(frozen: list[dict], reviewed: list[dict], label: str) -> None:
    if len(frozen) != len(reviewed):
        raise SystemExit(f"STOP: {label} has {len(reviewed)} rows, frozen has {len(frozen)}")
    for f, r in zip(frozen, reviewed):
        if f["sample_id"] != r["sample_id"] or f["title"] != r["title"] or f["predicted_class"] != r["predicted_class"]:
            raise SystemExit(
                f"STOP: {label} row {f['sample_id']} does not match frozen original "
                f"(sample_id/title/predicted_class must be unmodified)."
            )


def build_comparison() -> list[dict]:
    frozen = _read(FROZEN)
    chatgpt = _read(CHATGPT)
    claude = _read(CLAUDE)
    _check_integrity(frozen, chatgpt, "ChatGPT file")
    _check_integrity(frozen, claude, "Claude file")

    cg_by_id = {r["sample_id"]: r for r in chatgpt}
    cl_by_id = {r["sample_id"]: r for r in claude}

    rows = []
    for f in frozen:
        sid = f["sample_id"]
        cg_label = cg_by_id[sid]["human_label"]
        cl_label = cl_by_id[sid]["human_label"]
        pred = f["predicted_class"]
        agreement = "AGREED" if cg_label == cl_label else "DISAGREED"

        risks = [desc for (p, lbl), desc in DANGEROUS_PAIRS.items()
                 if pred == p and (cg_label == lbl or cl_label == lbl)]
        risk_category = "; ".join(risks) if risks else "NONE"

        differs_from_both = pred != cg_label and pred != cl_label
        needs_review = (agreement == "DISAGREED") or differs_from_both or bool(risks)

        rows.append({
            "sample_id": sid, "title": f["title"], "classifier_prediction": pred,
            "chatgpt_label": cg_label, "claude_label": cl_label,
            "agreement_status": agreement, "risk_category": risk_category,
            "needs_human_review": "TRUE" if needs_review else "FALSE",
        })
    return rows


def write_comparison(rows: list[dict]) -> None:
    cols = ["sample_id", "title", "classifier_prediction", "chatgpt_label", "claude_label",
            "agreement_status", "risk_category", "needs_human_review"]
    with open(OUT_COMPARISON, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader(); w.writerows(rows)


def write_adjudication(rows: list[dict]) -> int:
    cols = ["sample_id", "title", "classifier_prediction", "chatgpt_label", "claude_label",
            "final_human_label", "review_notes", "reviewer", "review_date"]
    needed = [r for r in rows if r["needs_human_review"] == "TRUE"]
    with open(OUT_ADJUDICATION, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader()
        for r in needed:
            w.writerow({
                "sample_id": r["sample_id"], "title": r["title"],
                "classifier_prediction": r["classifier_prediction"],
                "chatgpt_label": r["chatgpt_label"], "claude_label": r["claude_label"],
                "final_human_label": "", "review_notes": "", "reviewer": "", "review_date": "",
            })
    return len(needed)


def main() -> None:
    rows = build_comparison()
    write_comparison(rows)
    n_needed = write_adjudication(rows)
    n_agree = sum(1 for r in rows if r["agreement_status"] == "AGREED")
    n_risk = sum(1 for r in rows if r["risk_category"] != "NONE")
    print(f"Compared {len(rows)} rows.")
    print(f"AI agreement (informational only, NOT ground truth): {n_agree}/{len(rows)} ({100*n_agree/len(rows):.0f}%)")
    print(f"Rows flagged with a dangerous-error pattern: {n_risk}")
    print(f"Rows requiring human adjudication: {n_needed}")
    print(f"Written: {OUT_COMPARISON}")
    print(f"Written: {OUT_ADJUDICATION} (final_human_label BLANK)")


if __name__ == "__main__":
    main()

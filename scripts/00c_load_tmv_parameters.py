"""
00c_load_tmv_parameters.py
============================
Seeds ref_tmv_parameters (owner decision 2026-07-30, docs/
TMV_DEMAND_PARAMETER_DESIGN.md). Same discipline as 00_load_fx_rates.py /
00b_load_scenario_rates.py: reference DATA with a source citation, never a
bare code constant for a business-sensitive weight.

Two parameters seeded:
  demand_weight (0.0, INACTIVE) -- the TMV price formula READS this weight
  (scripts/13_build_tmv.py), but 0.0 means the demand term is mathematically
  a no-op: (1 + 0.0*(D-0.5)) == 1. Demand (D) is computed and displayed
  (feat_demand.recency_score) but does not move the price until a backtest
  validates a real weight -- the architecture is ready, the number is not
  invented. See docs/TMV_DEMAND_PARAMETER_DESIGN.md.

  price_elasticity_epsilon (1.5, ACTIVE) -- used ONLY by the scenario
  simulator (scripts/17_scenario_engine.py's price/turnover-time simulation),
  never by 13_build_tmv.py's TMV or turnover calculation. Carried from
  Implementation B's own disclosed assumption (B: "epsilon=1.5 is a labelled
  assumption... not a fitted value"), owner-approved for the same disclosed,
  non-fitted use here.

Idempotent: ON CONFLICT upsert on parameter_name.

Usage:
    python scripts/00c_load_tmv_parameters.py
    python scripts/00c_load_tmv_parameters.py --db /tmp/copy.duckdb
"""
from __future__ import annotations

import argparse
import os
from datetime import date
from pathlib import Path

import duckdb

BASE_DIR = Path(__file__).parent.parent
DEFAULT_DB_PATH = BASE_DIR / "database" / "watchparts.duckdb"
SCHEMA_PATH = BASE_DIR / "scripts" / "schema.sql"

VALID_FROM = date(2026, 1, 1)

PARAMETERS = [
    # parameter_name, parameter_value, active_flag, description, source
    ("demand_weight", 0.05, True,
     "Weight of demand index (D) in the TMV price formula: TMV *= (1 + demand_weight*(D-0.5)). "
     "ACTIVE as of 2026-07-31 -- deliberately NOT backtested like alpha_trend/beta_scarcity were; "
     "this is a small, disclosed INITIAL BUSINESS PARAMETER, chosen conservatively (5%, not "
     "arbitrarily larger) so demand visibly influences price without dominating the evidence-based "
     "H/C base. Recalibrate via the same leave-one-out backtest methodology once enough confirmed "
     "sold-price data exists to measure a real coefficient -- this value is a placeholder for that, "
     "not a substitute for it.",
     "Owner decision 2026-07-31 -- explicit override of the 2026-07-30 'wait for backtest' policy, "
     "chosen for commercial usability (docs/FINAL_CLIENT_PRODUCT_REPORT.md)"),
    ("price_elasticity_epsilon", 1.5, True,
     "Scenario-only price/turnover-time assumption: simulated_days = base_days * "
     "(scenario_price/tmv_eur)^epsilon. NOT a fitted market elasticity -- does not touch the TMV "
     "formula or the backend turnover_survival calculation, used only by the scenario simulator.",
     "Carried from Implementation B's own disclosed EPS=1.5 assumption; owner-approved 2026-07-30 "
     "for scenario simulation only (docs/TMV_DEMAND_PARAMETER_DESIGN.md)"),
]


def resolve_db_path(db_path=None) -> Path:
    if db_path:
        return Path(db_path)
    if os.environ.get("WATCHPARTS_DB"):
        return Path(os.environ["WATCHPARTS_DB"])
    return DEFAULT_DB_PATH


def load(conn) -> int:
    conn.execute(SCHEMA_PATH.read_text())
    for name, value, active, desc, source in PARAMETERS:
        conn.execute("""
            INSERT INTO ref_tmv_parameters (parameter_name, parameter_value, description, active_flag, source, valid_from)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (parameter_name) DO UPDATE SET
                parameter_value = excluded.parameter_value, description = excluded.description,
                active_flag = excluded.active_flag, source = excluded.source, valid_from = excluded.valid_from
        """, [name, value, desc, active, source, VALID_FROM])
    return len(PARAMETERS)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None)
    args = ap.parse_args()
    db_path = resolve_db_path(args.db)
    conn = duckdb.connect(str(db_path))
    try:
        n = load(conn)
    finally:
        conn.close()
    print(f"Database target: {db_path}")
    print(f"TMV parameters loaded: {n}")


if __name__ == "__main__":
    main()

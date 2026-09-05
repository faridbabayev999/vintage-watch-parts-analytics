# Vintage Watch Parts Analytics

Automated pricing and market analysis tool for vintage watch spare parts.

This repository contains the reproducible codebase, custom dashboard, tests, and
Excel inventory evaluation for the submitted project. The research paper is
submitted separately and explains the analytical methodology in more depth.

> **Note on the database:** the large DuckDB snapshot (~1.6 GB,
> `database/watchparts.duckdb`) is **not** included in this repository because it
> exceeds GitHub's 100 MB file limit. Rebuild it locally from the pipeline — see
> *Setup* and *Run The Pipeline* below (`python run_pipeline.py --full-rebuild`).

## Team

Group project for the MSc Data Analytics & Decision Science *Analytics Project*
at RWTH Aachen — by Benjamin Arenas, Farid Babayev, and Vaishnavi Iyer.

## What This Project Does

The system evaluates the provided inventory list and produces:

- recommended price / True Market Value (TMV) where evidence is sufficient;
- selling horizon / turnover estimate;
- pricing and turnover confidence labels;
- calculation traceback fields for explainability;
- a custom client dashboard for reviewing inventory-level results;
- an Excel evaluation file for direct professor/client review.

The core design principle is:

> Retrieval is broad, but valuation evidence is strict.

Marketplace listings can be collected broadly, but they only affect pricing
after deterministic matching, duplicate handling, and evidence-confidence
checks.

## Open First

1. Read the submitted research paper.
2. Open the Excel deliverable:

   ```text
   outputs/professor_inventory_evaluation_20260811/Vintage_Watch_Parts_Inventory_Evaluation.xlsx
   ```

3. Follow the setup and dashboard commands below.

## Repository Structure

```text
analysis/    Read-only data-readiness EDA support script used by tests
dashboard/   Custom HTML/CSS/JS dashboard and local API server
database/    DuckDB database snapshot for offline review
data/raw/    Raw inventory and marketplace evidence files
docs/        Architecture image used in the paper/presentation
outputs/     Final Excel and CSV inventory evaluation outputs
reports/     Minimal labelled-review fixture required by the test suite
scripts/     Pipeline modules and database utilities
tests/       Regression tests for ingestion, matching, TMV, turnover, dashboard
```

Important files:

```text
run_pipeline.py
requirements.txt
.env.example
dashboard/server.py
scripts/schema.sql
database/watchparts.duckdb
data/raw/latest.csv
```

`data/raw/latest.csv` is the broad active eBay listing snapshot, not the
inventory file. The inventory file is `data/raw/inventory.csv`.

## Setup

Recommended Python version: Python 3.11 to 3.13.

From the downloaded project folder:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

For fully offline review, first rebuild the DuckDB snapshot with
`python run_pipeline.py --full-rebuild` (the snapshot itself is not committed
here due to its size). No eBay credentials are needed to rebuild from the
included raw data.

Credentials are only needed if you want to refresh live eBay marketplace data.
In that case:

```bash
cp .env.example .env
```

Then add valid eBay production credentials to `.env`.

Do not commit `.env` or token-cache files.

## Run The Pipeline

Offline/default review mode:

```bash
python run_pipeline.py
```

This refreshes reference tables and rebuilds the final dashboard contract from
the included DuckDB snapshot. It is the recommended professor/reviewer command:
fast, repeatable, and offline-friendly.

Advanced full rebuild from raw/staging through matching and TMV:

```bash
python run_pipeline.py --full-rebuild
```

The full rebuild is slower because it regenerates matching candidates and model
tables. It may also append audit/bookkeeping rows used by the development
pipeline.

To include live eBay collection:

```bash
python run_pipeline.py --full-rebuild --collect-live-ebay
```

Live collection requires valid eBay API credentials and may produce different
results because marketplace listings change over time.

## Run The Dashboard

The primary dashboard is the custom HTML/CSS/JS dashboard in `dashboard/`.

```bash
python run_pipeline.py
python dashboard/server.py
```

Open:

```text
http://localhost:8080/
```

If port `8080` is busy:

```bash
python dashboard/server.py 8090
```

Then open `http://localhost:8090/`.

The dashboard reads:

```text
database/watchparts.duckdb
```

The final dashboard table is:

```text
dashboard_inventory_pricing
```

That table is the backend-owned client contract: one row per eligible inventory
item with final price, confidence, evidence counts, selling horizon, scenario
prices, and explanation fields already reconciled.

## Expected Submitted Results

For the included submitted database snapshot:

```text
Eligible inventory rows: 728
Priced rows:             575
No recommendation rows:  153
Physical stock units:    2,935
```

Quick verification:

```bash
python scripts/db_tools/table_counts.py
```

Or query the final table:

```bash
python scripts/db_tools/run_sql.py "SELECT COUNT(*) FROM dashboard_inventory_pricing;"
```

## Excel Deliverable

The professor-facing inventory evaluation file is:

```text
outputs/professor_inventory_evaluation_20260811/Vintage_Watch_Parts_Inventory_Evaluation.xlsx
```

It contains the evaluated inventory, recommended pricing information, selling
horizon fields, confidence labels, and explanation notes.

## Tests

Focused validation suite:

```bash
python -m pytest tests/test_generate_queries.py tests/test_tmv_component_formulas.py tests/test_turnover.py tests/test_dashboard_contract.py -q
```

Full test suite:

```bash
python -m pytest -q
```

Verified submission result:

```text
716 passed, 1 warning
```

## Notes For Reviewers

- The dashboard is custom HTML/CSS/JS served by `dashboard/server.py`; it is not
  the Streamlit dashboard.
- DuckDB allows only one writer at a time. If the dashboard shows a lock error,
  stop other running pipeline/dashboard processes and try again.
- Low confidence or no recommendation means insufficient trustworthy evidence,
  not that the part has no value.
- Turnover is an evidence-based selling-horizon estimate, not a guaranteed sale
  date.
- The `database/watchparts.duckdb` snapshot is **not** committed to this public
  repository because of its size (~1.6 GB). Rebuild it locally with
  `python run_pipeline.py --full-rebuild` before running the dashboard.

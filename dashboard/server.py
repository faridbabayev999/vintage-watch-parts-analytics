import http.server
import json
import urllib.parse
import sys
import importlib.util
import math
import codecs
from pathlib import Path

# Safe encoding configuration on Windows for redirected stdout (handles non-ASCII characters like checkmarks)
if sys.platform.startswith("win"):
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(errors="replace")
            sys.stderr.reconfigure(errors="replace")
    except Exception:
        pass

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = BASE_DIR / "scripts"
DASHBOARD_DIR = BASE_DIR / "dashboard"

def clean_nans(data):
    if isinstance(data, dict):
        return {k: clean_nans(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [clean_nans(v) for v in data]
    elif isinstance(data, float):
        if math.isnan(data) or math.isinf(data):
            return None
        return data
    else:
        return data


def number_or_zero(value):
    """Return zero for None/NaN/pandas NA without evaluating pd.NA as bool."""
    if value is None:
        return 0
    try:
        # Works for float NaN and pandas.NA without forcing pd.NA through bool().
        import pandas as pd
        if pd.isna(value):
            return 0
    except Exception:
        pass
    return value


def is_duckdb_lock_error(exc):
    message = str(exc).lower()
    return "could not set lock" in message or "conflicting lock" in message


def lock_busy_message():
    return (
        "The database is busy finishing another dashboard/pipeline update. "
        "Please wait a few seconds and click the button again. "
        "If it keeps happening, restart the dashboard server."
    )

def build_portfolio_turnover_rollup(conn):
    """Sum the granular turnover forecast stored in the dashboard contract."""
    bucket_defs = [
        ("0 - 7 days", "units_sold_0_7"),
        ("8 - 30 days", "units_sold_8_30"),
        ("31 - 90 days", "units_sold_31_90"),
        ("91 - 183 days", "units_sold_91_183"),
        ("184 - 365 days", "units_sold_184_365"),
        ("366 - 730 days", "units_sold_366_730"),
        ("731 - 1065 days", "units_sold_731_1065"),
        ("1066+ days", "units_sold_1066_plus"),
    ]
    select_parts = []
    for _, col in bucket_defs:
        select_parts.append(f"COALESCE(SUM({col}), 0) AS {col}")
        select_parts.append(
            f"COALESCE(SUM(COALESCE({col}, 0) * COALESCE(recommended_price_eur, 0)), 0) AS {col}_revenue"
        )
    select_parts.append("COALESCE(SUM(units_remaining), 0) AS units_remaining")
    select_parts.append("COALESCE(SUM(potential_revenue_eur), 0) AS potential_revenue_eur")

    row = conn.execute(f"""
        SELECT {", ".join(select_parts)}
        FROM dashboard_inventory_pricing
        WHERE COALESCE(stock_quantity, 0) > 0
    """).fetchone()
    values = dict(zip([d[0] for d in conn.description], row))

    bucket_totals = []
    for label, col in bucket_defs:
        units = float(values.get(col) or 0.0)
        revenue = float(values.get(f"{col}_revenue") or 0.0)
        bucket_totals.append({
            "label": label,
            "units": round(units, 2),
            "revenue": round(revenue, 2),
        })

    days_30_units = sum(float(values.get(col) or 0.0) for _, col in bucket_defs[:2])
    days_30_revenue = sum(float(values.get(f"{col}_revenue") or 0.0) for _, col in bucket_defs[:2])
    days_90_units = sum(float(values.get(col) or 0.0) for _, col in bucket_defs[:3])
    days_90_revenue = sum(float(values.get(f"{col}_revenue") or 0.0) for _, col in bucket_defs[:3])

    return {
        "method": "granular_turnover_rollup",
        "description": "Deterministic sum of item-level turnover buckets from dashboard_inventory_pricing.",
        "days_30": {
            "mean_sold": round(days_30_units, 2),
            "p5_sold": round(days_30_units, 2),
            "p95_sold": round(days_30_units, 2),
            "mean_revenue": round(days_30_revenue, 2),
            "p5_revenue": round(days_30_revenue, 2),
            "p95_revenue": round(days_30_revenue, 2),
            "bucket_label": "0 - 7d + 8 - 30d",
        },
        "days_90": {
            "mean_sold": round(days_90_units, 2),
            "p5_sold": round(days_90_units, 2),
            "p95_sold": round(days_90_units, 2),
            "mean_revenue": round(days_90_revenue, 2),
            "p5_revenue": round(days_90_revenue, 2),
            "p95_revenue": round(days_90_revenue, 2),
            "bucket_label": "0 - 7d + 8 - 30d + 31 - 90d",
        },
        "bucket_totals": bucket_totals,
        "units_remaining": round(float(values.get("units_remaining") or 0.0), 2),
        "potential_revenue_eur": round(float(values.get("potential_revenue_eur") or 0.0), 2),
    }

def top_calibers(items, n=4):
    cal_data = {}
    for it in items:
        cal = it.get("caliber") or "Unknown"
        if cal in ("None", "unknown", ""):
            cal = "Unknown"
        price = number_or_zero(it.get("tmv_eur"))
        stock = number_or_zero(it.get("stock"))
        val = price * stock
        
        if cal not in cal_data:
            cal_data[cal] = {"caliber": cal, "count": 0, "value_eur": 0.0}
        cal_data[cal]["count"] += stock
        cal_data[cal]["value_eur"] += val
        
    sorted_cals = sorted(cal_data.values(), key=lambda x: -x["value_eur"])
    
    if len(sorted_cals) <= n:
        for c in sorted_cals:
            c["value_eur"] = round(c["value_eur"], 2)
        return sorted_cals
        
    top_n = sorted_cals[:n]
    others_count = sum(c["count"] for c in sorted_cals[n:])
    others_value = sum(c["value_eur"] for c in sorted_cals[n:])
    
    for c in top_n:
        c["value_eur"] = round(c["value_eur"], 2)
        
    top_n.append({
        "caliber": "Others",
        "count": others_count,
        "value_eur": round(others_value, 2)
    })
    return top_n

def top_brands_value(items, n=3):
    brand_data = {}
    for it in items:
        b = it.get("brand") or "Unknown"
        price = number_or_zero(it.get("tmv_eur"))
        stock = number_or_zero(it.get("stock"))
        val = price * stock
        if b not in brand_data:
            brand_data[b] = {"brand": b, "value_eur": 0.0}
        brand_data[b]["value_eur"] += val
        
    sorted_brands = sorted(brand_data.values(), key=lambda x: -x["value_eur"])
    total_val = sum(b["value_eur"] for b in sorted_brands) or 1.0
    
    for b in sorted_brands:
        b["value_eur"] = round(b["value_eur"], 2)
        b["pct"] = round(100.0 * b["value_eur"] / total_val, 1)
        
    return sorted_brands

ACCEPTED_EVIDENCE_TIERS = ("AUTO_CONFIRMED", "HIGH_CONFIDENCE")


def _contract_item_fields(conn, canonical_inventory_id):
    """Read the canonical dashboard contract row for one item.

    The HTML dashboard should display backend-persisted client values from
    dashboard_inventory_pricing, not rebuild pricing logic in the browser.
    """
    try:
        row = conn.execute("""
            SELECT *
            FROM dashboard_inventory_pricing
            WHERE canonical_inventory_id = ?
            LIMIT 1
        """, [canonical_inventory_id]).fetchone()
        if not row:
            return {}
        cols = [d[0] for d in conn.description]
        r = dict(zip(cols, row))
        base_tmv = r.get("base_tmv_eur")
        historical_value = r.get("historical_value_h")
        current_value = r.get("current_value_c")
        price_trend = r.get("price_trend_p")
        scarcity = r.get("scarcity_score_s")
        trace_base = base_tmv
        if trace_base is None:
            if historical_value is not None:
                trace_base = historical_value
            elif current_value is not None:
                trace_base = current_value
        trend_adjustment = (
            float(trace_base) * float(price_trend)
            if trace_base is not None and price_trend is not None
            else None
        )
        scarcity_adjustment = (
            float(trace_base) * (1.0 + float(price_trend or 0.0)) * 0.10 * (float(scarcity) - 0.50)
            if trace_base is not None and scarcity is not None
            else None
        )
        return {
            "inventory_uid": r.get("inventory_uid"),
            "pricing_status": r.get("pricing_status"),
            "pricing_confidence": r.get("pricing_confidence"),
            "confidence_tier": r.get("pricing_confidence"),
            "pricing_state": r.get("pricing_confidence"),
            "pricing_state_label": r.get("confidence_label") or (
                f"{str(r.get('pricing_confidence') or 'LOW').title()} confidence"
            ),
            "tmv_eur": r.get("recommended_price_eur"),
            "tmv_low_eur": r.get("price_lower_bound_eur"),
            "tmv_high_eur": r.get("price_upper_bound_eur"),
            "base_tmv_eur": r.get("base_tmv_eur"),
            "historical_value_eur": r.get("historical_value_h"),
            "current_value_eur": r.get("current_value_c"),
            "demand_index": r.get("demand_index_d"),
            "market_dynamics": r.get("scarcity_score_s"),
            "price_trend": r.get("price_trend_p"),
            "demand_adjustment_eur": r.get("demand_adjustment_eur"),
            "trace_base_value_eur": trace_base,
            "trend_adjustment_eur": trend_adjustment,
            "scarcity_adjustment_eur": scarcity_adjustment,
            "recommendation_reason": r.get("recommendation_reason"),
            "confidence_reason": r.get("confidence_reason"),
            "market_evidence_active": r.get("active_evidence_count") or 0,
            "market_evidence_sold": r.get("historical_evidence_count") or 0,
            "evidence_depth": r.get("total_unique_evidence_count") or 0,
            "valuation_basis": r.get("evidence_basis"),
            "median_days_to_sell": r.get("median_days_to_sell"),
            "sell_time_display": r.get("sell_time_display"),
            "turnover_confidence": r.get("turnover_confidence"),
            "turnover_method": r.get("turnover_method"),
            "turnover_evidence_status": r.get("turnover_evidence_status"),
            "turnover_reason": r.get("turnover_reason"),
            "prob_sell_30d": r.get("probability_sell_30d"),
            "prob_sell_90d": r.get("probability_sell_90d"),
            "virtual_price_eur": r.get("virtual_price_eur"),
            "germany_price_eur": r.get("germany_price_eur"),
            "us_price_eur": r.get("us_price_eur"),
            "germany_shipping_eur": r.get("germany_shipping_eur"),
            "us_shipping_eur": r.get("us_shipping_eur"),
            "us_tax_eur": r.get("us_tax_eur"),
            "us_duty_eur": r.get("us_duty_eur"),
            "calculation_version": r.get("calculation_version"),
            "generated_at": r.get("generated_at"),
            "no_recommendation_reason": r.get("no_recommendation_reason"),
        }
    except Exception as exc:
        print("Contract lookup failed:", exc)
        return {}


def _accepted_active_matches(conn, canonical_inventory_id):
    try:
        rows = conn.execute("""
            SELECT title, price_eur, item_id, marketplace, stable_evidence_uid
            FROM (
                SELECT
                    a.title,
                    a.price_eur,
                    a.item_id,
                    a.marketplace,
                    a.stable_evidence_uid,
                    ROW_NUMBER() OVER (
                        PARTITION BY COALESCE(a.stable_evidence_uid, a.item_id, CAST(a.id AS VARCHAR))
                        ORDER BY a.fetched_at DESC NULLS LAST, a.id DESC
                    ) AS rn
                FROM evidence_confidence_classification ecc
                JOIN stg_active_targeted a
                  ON ecc.source_table = 'match_candidates_active'
                 AND a.id = ecc.source_id
                JOIN staging_inventory si
                  ON ecc.inventory_uid = si.inventory_uid
                WHERE si.canonical_inventory_id = ?
                  AND ecc.confidence_tier IN ('AUTO_CONFIRMED', 'HIGH_CONFIDENCE')
            )
            WHERE rn = 1
            ORDER BY price_eur ASC NULLS LAST
        """, [canonical_inventory_id]).fetchall()
        out = []
        for title, price, item_id, marketplace, stable_uid in rows:
            item_number = item_id
            if item_number and "|" in item_number:
                parts = item_number.split("|")
                if len(parts) > 1:
                    item_number = parts[1]
            url = f"https://www.ebay.com/itm/{item_number}" if item_number else "#"
            out.append({
                "title": title or "Unknown active listing",
                "price_eur": round(price, 2) if price is not None else None,
                "url": url,
                "marketplace": marketplace,
                "evidence_uid": stable_uid,
            })
        return out
    except Exception as exc:
        print("Error fetching accepted active evidence:", exc)
        return []


def _accepted_historical_matches(conn, canonical_inventory_id):
    rows_out = []
    try:
        rows = conn.execute("""
            SELECT title, price_eur, url, sold_date, stable_evidence_uid
            FROM (
                SELECT
                    h.title,
                    h.price_eur,
                    h.url,
                    h.sold_date,
                    h.stable_evidence_uid,
                    ROW_NUMBER() OVER (
                        PARTITION BY COALESCE(h.stable_evidence_uid, h.item_number, CAST(h.id AS VARCHAR))
                        ORDER BY h.sold_date DESC NULLS LAST, h.id DESC
                    ) AS rn
                FROM evidence_confidence_classification ecc
                JOIN stg_historical_ebay_sold h
                  ON ecc.source_table = 'match_candidates_ebay_sold'
                 AND h.id = ecc.source_id
                JOIN staging_inventory si
                  ON ecc.inventory_uid = si.inventory_uid
                WHERE si.canonical_inventory_id = ?
                  AND ecc.confidence_tier IN ('AUTO_CONFIRMED', 'HIGH_CONFIDENCE')
            )
            WHERE rn = 1
            ORDER BY sold_date DESC NULLS LAST, price_eur DESC NULLS LAST
        """, [canonical_inventory_id]).fetchall()
        for title, price, url, sold_date, stable_uid in rows:
            rows_out.append({
                "title": title or "Unknown historical sale",
                "price_eur": round(price, 2) if price is not None else None,
                "url": url or "#",
                "sold_date": str(sold_date) if sold_date else None,
                "source": "eBay sold",
                "evidence_uid": stable_uid,
            })
    except Exception as exc:
        print("Error fetching accepted eBay sold evidence:", exc)

    try:
        rows = conn.execute("""
            SELECT title, avg_price_eur, last_sold_date, total_sold, stable_evidence_uid
            FROM (
                SELECT
                    v.title,
                    v.avg_price_eur,
                    v.last_sold_date,
                    v.total_sold,
                    v.stable_evidence_uid,
                    ROW_NUMBER() OVER (
                        PARTITION BY COALESCE(v.stable_evidence_uid, CAST(v.id AS VARCHAR))
                        ORDER BY v.last_sold_date DESC NULLS LAST, v.id DESC
                    ) AS rn
                FROM evidence_confidence_classification ecc
                JOIN stg_historical_vcp_aggregate v
                  ON ecc.source_table = 'match_candidates_vcp'
                 AND v.id = ecc.source_id
                JOIN staging_inventory si
                  ON ecc.inventory_uid = si.inventory_uid
                WHERE si.canonical_inventory_id = ?
                  AND ecc.confidence_tier IN ('AUTO_CONFIRMED', 'HIGH_CONFIDENCE')
            )
            WHERE rn = 1
            ORDER BY last_sold_date DESC NULLS LAST, avg_price_eur DESC NULLS LAST
        """, [canonical_inventory_id]).fetchall()
        for title, price, last_sold_date, total_sold, stable_uid in rows:
            label = title or "VCP historical aggregate"
            if total_sold:
                label = f"{label} ({int(total_sold)} sold in aggregate)"
            rows_out.append({
                "title": label,
                "price_eur": round(price, 2) if price is not None else None,
                "url": "#",
                "sold_date": str(last_sold_date) if last_sold_date else None,
                "source": "VCP aggregate",
                "evidence_uid": stable_uid,
            })
    except Exception as exc:
        print("Error fetching accepted VCP evidence:", exc)

    return rows_out

# Load the dashboard data script dynamically
def load_data_module():
    spec = importlib.util.spec_from_file_location("dash16_data", SCRIPTS_DIR / "16_dashboard_data.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m

data_mod = load_data_module()

# Setup paths to import scripts
sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(SCRIPTS_DIR))

# Dynamically load utils and clean modules
def load_utils_module():
    spec = importlib.util.spec_from_file_location("utils_mod", SCRIPTS_DIR / "utils.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m

def load_clean_module():
    spec = importlib.util.spec_from_file_location("clean_mod", SCRIPTS_DIR / "02_clean.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m

utils_mod = load_utils_module()
clean_mod = load_clean_module()

# Ensure bulk update directory exists and template is stored
BULK_UPDATE_DIR = DASHBOARD_DIR / "bulk_update"
BULK_UPDATE_DIR.mkdir(parents=True, exist_ok=True)
TEMPLATE_PATH = BULK_UPDATE_DIR / "inventory_template.csv"

# Pre-populate template if it doesn't exist
if not TEMPLATE_PATH.exists():
    with open(TEMPLATE_PATH, "w") as f:
        f.write("Rolex/Tudor,Calibre,P-number,Stock\nRolex,1030,6900/585,1\nTudor,390,7004,5\n")

class DashboardAPIHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        # We serve files from the dashboard directory
        super().__init__(*args, directory=str(DASHBOARD_DIR), **kwargs)

    def do_GET(self):
        parsed_url = urllib.parse.urlparse(self.path)
        path = parsed_url.path
        query_params = urllib.parse.parse_qs(parsed_url.query)

        # Route API requests
        if path.startswith("/api/"):
            try:
                self.handle_api(path, query_params)
            except Exception as e:
                self.send_error_response(500, str(e))
        else:
            # Fallback to standard static file serving
            super().do_GET()

    def do_POST(self):
        parsed_url = urllib.parse.urlparse(self.path)
        path = parsed_url.path

        if path.startswith("/api/"):
            try:
                content_length = int(self.headers.get('Content-Length', 0))
                post_data = self.rfile.read(content_length)
                payload = json.loads(post_data.decode('utf-8'))
                self.handle_api_post(path, payload)
            except Exception as e:
                self.send_error_response(500, str(e))
        else:
            self.send_error_response(404, "Not found")

    def handle_api(self, path, params):
        # Establish connection to DuckDB read-only
        conn = data_mod.connect_readonly()
        
        try:
            # Assert DB target per standard rules
            db_path = data_mod.resolve_db_path()
            data_mod.assert_db_target(conn, db_path)

            if path == "/api/portfolio":
                # Portfolio Overview & Summary Statistics
                items = data_mod.load_items(conn)
                overview = data_mod.portfolio_overview(conn)
                price_dist = data_mod.price_distribution_bins(items, bin_width=25, max_bins=5)
                sell_dist = data_mod.sell_time_distribution(items)
                brands = top_brands_value(items)
                calibers = top_calibers(items)
                market = data_mod.market_summary(conn)
                freshness = data_mod.data_freshness(conn)
                sell_days = [
                    it.get("median_days_to_sell")
                    for it in items
                    if it.get("median_days_to_sell") is not None
                ]
                if sell_days:
                    sell_days_sorted = sorted(sell_days)
                    overview["typical_sell_time"] = f"{round(sell_days_sorted[len(sell_days_sorted) // 2])} d"
                else:
                    overview["typical_sell_time"] = "Insufficient data"
                overview["total_physical_stock"] = conn.execute(
                    "SELECT COALESCE(SUM(stock_quantity), 0) FROM dashboard_inventory_pricing "
                    "WHERE COALESCE(stock_quantity, 0) > 0"
                ).fetchone()[0]
                overview["total_portfolio_value_eur"] = sum(
                    number_or_zero(it.get("tmv_eur")) * number_or_zero(it.get("stock"))
                    for it in items
                )
                overview["high_demand_n"] = sum(
                    1 for it in items if (it.get("demand_index") or 0) >= 0.70
                )
                
                # Sum the granular turnover forecast from the dashboard contract.
                try:
                    simulation = build_portfolio_turnover_rollup(conn)
                except Exception as e:
                    print("Portfolio turnover rollup failed:", e)
                    simulation = None
                
                response_data = {
                    "overview": overview,
                    "price_distribution": price_dist,
                    "sell_time_distribution": sell_dist,
                    "top_brands": brands,
                    "top_calibers": calibers,
                    "market_summary": market,
                    "freshness": {
                        "tmv_computed_at": str(freshness.get("tmv_computed_at") or "—"),
                        "latest_sold_date": str(freshness.get("latest_sold_date") or "—"),
                        "latest_active_fetch": str(freshness.get("latest_active_fetch") or "—")
                    },
                    "simulation": simulation
                }
                self.send_json_response(200, response_data)

            elif path == "/api/items":
                # Load all priced and unpriced items
                priced = data_mod.load_items(conn)
                unpriced = data_mod.load_unpriced_items(conn)
                self.send_json_response(200, {
                    "priced": priced,
                    "unpriced": unpriced
                })

            elif path == "/api/item":
                # Single item lookup
                item_ids = params.get("id")
                if not item_ids:
                    self.send_error_response(400, "Missing 'id' parameter.")
                    return
                
                item_id = item_ids[0]
                item = data_mod.get_item_by_id(conn, item_id)
                if not item:
                    self.send_error_response(404, f"Item with id '{item_id}' not found.")
                    return
                contract_fields = _contract_item_fields(conn, item_id)
                item.update(contract_fields)
                
                # Fetch scenarios
                scenarios_resp = data_mod.item_scenarios(conn, item["tmv_eur"])
                scenarios = scenarios_resp["scenarios"] if scenarios_resp.get("ok") else None
                
                # Fetch simulated days table (default -10%, 0%, 10%)
                sim_table = None
                if item["tmv_eur"] is not None and item["median_days_to_sell"] is not None:
                    sim_table = data_mod.price_time_table(conn, item["tmv_eur"], item["median_days_to_sell"])

                # Fetch only accepted evidence used by pricing. Rejected, low,
                # and review candidates are not shown as supporting evidence.
                active_matches = _accepted_active_matches(conn, item_id)
                historical_matches = _accepted_historical_matches(conn, item_id)

                self.send_json_response(200, {
                    "item": item,
                    "scenarios": scenarios,
                    "price_time_table": sim_table,
                    "active_matches": active_matches,
                    "historical_matches": historical_matches
                })

            elif path == "/api/simulate":
                # Detailed single simulation request
                item_ids = params.get("id")
                pct_vals = params.get("pct")
                if not item_ids or not pct_vals:
                    self.send_error_response(400, "Missing 'id' or 'pct' parameter.")
                    return
                
                item_id = item_ids[0]
                try:
                    pct = float(pct_vals[0])
                except ValueError:
                    self.send_error_response(400, "Invalid 'pct' parameter. Must be a number.")
                    return

                item = data_mod.get_item_by_id(conn, item_id)
                if not item:
                    self.send_error_response(404, f"Item with id '{item_id}' not found.")
                    return

                if item["tmv_eur"] is not None and item["median_days_to_sell"] is not None:
                    sim = data_mod.price_time_table(conn, item["tmv_eur"], item["median_days_to_sell"], pct_points=(pct,))[0]
                    self.send_json_response(200, {"success": True, "simulation": sim})
                else:
                    self.send_json_response(200, {"success": False, "message": "Cannot simulate for this item."})
            
            elif path == "/api/check_item":
                brand = params.get("brand", [""])[0]
                caliber = params.get("caliber", [""])[0]
                part_number = params.get("part_number", [""])[0]
                
                canon_id = utils_mod.slugify_canonical_id(brand, caliber, part_number)
                
                row = conn.execute(
                    "SELECT stock, inventory_uid FROM staging_inventory WHERE canonical_inventory_id = ?",
                    (canon_id,)
                ).fetchone()
                
                if row:
                    self.send_json_response(200, {
                        "exists": True,
                        "current_stock": row[0],
                        "canonical_id": canon_id,
                        "inventory_uid": row[1]
                    })
                else:
                    self.send_json_response(200, {
                        "exists": False,
                        "canonical_id": canon_id
                    })

            elif path == "/api/job":
                job_ids = params.get("id")
                if not job_ids:
                    self.send_error_response(400, "Missing 'id' parameter.")
                    return
                job_id = job_ids[0]
                row = conn.execute("""
                    SELECT job_id, job_type, status, brand, caliber, part_number,
                           canonical_inventory_id, requested_at, started_at,
                           finished_at, result_summary, error_message
                    FROM dashboard_pipeline_jobs
                    WHERE job_id = ?
                    LIMIT 1
                """, [job_id]).fetchone()
                if not row:
                    self.send_error_response(404, f"Job '{job_id}' not found.")
                    return
                job = dict(zip([d[0] for d in conn.description], row))
                contract = data_mod.dashboard_contract_row(conn, job.get("canonical_inventory_id"))
                events = data_mod.pipeline_job_events(conn, job_id, limit=8)
                self.send_json_response(200, {
                    "job": job,
                    "contract": contract,
                    "events": events,
                })
            
            elif path == "/api/template":
                if TEMPLATE_PATH.exists():
                    with open(TEMPLATE_PATH, "r") as f:
                        template_content = f.read()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/csv")
                    self.send_header("Content-Disposition", "attachment; filename=inventory_template.csv")
                    response_bytes = template_content.encode("utf-8")
                    self.send_header("Content-Length", str(len(response_bytes)))
                    self.end_headers()
                    self.wfile.write(response_bytes)
                else:
                    self.send_error_response(404, "Template not found.")
            else:
                self.send_error_response(404, "API endpoint not found.")

        finally:
            conn.close()

    def handle_api_post(self, path, payload):
        db_path = data_mod.resolve_db_path()

        if path == "/api/add_or_update_item":
            brand = payload.get("brand")
            caliber = payload.get("caliber", "").strip()
            part_number = payload.get("part_number", "").strip()
            try:
                stock = int(payload.get("stock", 0))
            except (ValueError, TypeError):
                self.send_error_response(400, "Invalid stock value.")
                return
            stock_mode = payload.get("stock_mode", "set")

            if not brand or not part_number:
                self.send_error_response(400, "Brand and part number are required.")
                return

            try:
                result = data_mod.append_inventory_item(
                    brand, caliber, part_number, stock, stock_mode=stock_mode
                )
                
                # Start job if enqueued
                if result.get("job_id"):
                    data_mod.start_pipeline_job(result["job_id"])

                self.send_json_response(200, {
                    "success": True,
                    "message": result["note"],
                    "job_id": result.get("job_id"),
                    "canonical_id": utils_mod.slugify_canonical_id(brand, caliber, part_number)
                })
            except Exception as e:
                if is_duckdb_lock_error(e):
                    self.send_error_response(409, lock_busy_message())
                else:
                    self.send_error_response(500, f"Operation failed: {str(e)}")

        elif path == "/api/validate_bulk":
            filename = payload.get("filename", "bulk_update.csv")
            content = payload.get("content", "")
            if not content:
                self.send_error_response(400, "Empty content.")
                return

            try:
                import io
                import pandas as pd
                df = pd.read_csv(io.StringIO(content))
                required_cols = ["Rolex/Tudor", "Calibre", "P-number", "Stock"]
                if not all(col in df.columns for col in required_cols):
                    self.send_error_response(400, f"CSV format error. Must contain: {required_cols}")
                    return

                rows = []
                for _, r in df.iterrows():
                    rows.append({
                        "brand": str(r.get("Rolex/Tudor", "")).strip(),
                        "caliber": str(r.get("Calibre", "")).strip() if not pd.isna(r.get("Calibre")) else "",
                        "part_number": str(r.get("P-number", "")).strip() if not pd.isna(r.get("P-number")) else "",
                        "stock": r.get("Stock")
                    })

                validation = data_mod.validate_inventory_upload_rows(rows)
                self.send_json_response(200, validation)
            except Exception as e:
                self.send_error_response(500, f"CSV parsing failed: {str(e)}")

        elif path == "/api/bulk_update":
            filename = payload.get("filename", "bulk_update.csv")
            content = payload.get("content", "")
            if not content:
                self.send_error_response(400, "Empty content.")
                return

            save_path = BULK_UPDATE_DIR / filename
            try:
                with open(save_path, "w", encoding="utf-8") as f:
                    f.write(content)

                import io
                import pandas as pd
                df = pd.read_csv(io.StringIO(content))
                
                required_cols = ["Rolex/Tudor", "Calibre", "P-number", "Stock"]
                if not all(col in df.columns for col in required_cols):
                    self.send_error_response(400, f"CSV format error. Must contain: {required_cols}")
                    return

                rows = []
                for _, r in df.iterrows():
                    rows.append({
                        "brand": str(r.get("Rolex/Tudor", "")).strip(),
                        "caliber": str(r.get("Calibre", "")).strip() if not pd.isna(r.get("Calibre")) else "",
                        "part_number": str(r.get("P-number", "")).strip() if not pd.isna(r.get("P-number")) else "",
                        "stock": r.get("Stock")
                    })

                # Dry-run validate
                validation = data_mod.validate_inventory_upload_rows(rows)
                if not validation["ok"]:
                    self.send_error_response(400, f"Validation check failed: {validation['errors']}")
                    return

                result = data_mod.append_inventory_rows(rows)
                self.send_json_response(200, {
                    "success": True, 
                    "message": f"✓ Imported {result['rows_added']} new row(s), updated stock for {result['stock_updated']} existing row(s), queued {len(result['job_ids'])} job(s).",
                    "job_ids": result.get("job_ids", [])
                })
            except Exception as e:
                if is_duckdb_lock_error(e):
                    self.send_error_response(409, lock_busy_message())
                else:
                    self.send_error_response(500, f"Bulk update processing failed: {str(e)}")
        else:
            self.send_error_response(404, "API endpoint not found.")

    def send_json_response(self, status, data):
        cleaned_data = clean_nans(data)
        response_bytes = json.dumps(cleaned_data, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(response_bytes)))
        self.end_headers()
        self.wfile.write(response_bytes)

    def send_error_response(self, status, message):
        self.send_json_response(status, {"error": message})

def run_server(port=8080):
    server_address = ("", port)
    httpd = http.server.HTTPServer(server_address, DashboardAPIHandler)
    print(f"Vintage Watch Spare Parts Dashboard Server is running at http://localhost:{port}/")
    print("Press Ctrl+C to terminate.")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server.")
        httpd.server_close()

if __name__ == "__main__":
    port = 8080
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            pass
    run_server(port)

from pathlib import Path
import importlib.util

import duckdb


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "24_reconcile_status_and_identity.py"

spec = importlib.util.spec_from_file_location("phase78", SCRIPT)
phase78 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(phase78)


def test_reconcile_historical_status_and_populate_evidence_registries_idempotently():
    conn = duckdb.connect(":memory:")
    conn.execute((ROOT / "scripts" / "schema.sql").read_text())

    conn.execute(
        """
        INSERT INTO historical_extraction_status
            (inventory_uid, canonical_inventory_id, time_bucket, extraction_status)
        VALUES
            ('inv-1', 'rolex_7_330', 'all', 'not_started'),
            ('inv-2', 'rolex_22_16_1', 'all', 'not_started')
        """
    )
    conn.execute(
        """
        INSERT INTO stg_active_targeted (
            id, raw_id, item_id, title, price_eur, condition_standard, marketplace,
            fetched_at, stable_evidence_uid, observation_uid
        ) VALUES
            (1, 10, 'A1', 'Rolex part active older', 100.0, 'USED', 'EBAY_DE',
             TIMESTAMP '2026-01-01 00:00:00', 'EV-ACTIVE-A1', 'OBS-A1-OLD'),
            (2, 11, 'A1', 'Rolex part active newer', 120.0, 'USED', 'EBAY_DE',
             TIMESTAMP '2026-01-02 00:00:00', 'EV-ACTIVE-A1', 'OBS-A1-NEW')
        """
    )
    conn.execute(
        """
        INSERT INTO stg_historical_ebay_sold (
            id, raw_id, item_number, title, sold_date, price_eur,
            condition_standard, source_filename, stable_evidence_uid, observation_uid
        ) VALUES
            (1, 20, 'S1', 'Rolex sold part', DATE '2026-01-03', 90.0,
             'USED', 'sold.csv', 'EV-SOLD-S1', 'OBS-S1')
        """
    )
    conn.execute(
        """
        INSERT INTO match_candidates_ebay_sold (
            match_candidate_id, match_run_id, inventory_uid, ebay_sold_raw_id,
            evidence_uid, match_method, evidence_json
        ) VALUES
            (1, 'run-1', 'inv-1', 1, 'EV-SOLD-S1', 'EXACT', '{}')
        """
    )

    first_hist = phase78.reconcile_historical_extraction_status(conn)
    first_evidence = phase78.populate_evidence_registries(conn)
    second_hist = phase78.reconcile_historical_extraction_status(conn)
    second_evidence = phase78.populate_evidence_registries(conn)

    assert first_hist["before_not_started"] == 2
    assert first_hist["still_not_started"] == 0
    assert first_hist["done"] == 2
    assert first_hist["with_historical_candidates"] == 1
    assert second_hist["updated"] == 0

    assert first_evidence["identity_inserted"] == 2
    assert first_evidence["observation_inserted"] == 3
    assert first_evidence["current_observations"] == 2
    assert second_evidence["identity_inserted"] == 0
    assert second_evidence["observation_inserted"] == 0

    current = conn.execute(
        """
        SELECT observation_uid
        FROM evidence_observation
        WHERE stable_evidence_uid = 'EV-ACTIVE-A1' AND is_current
        """
    ).fetchone()[0]
    assert current == "OBS-A1-NEW"

    statuses = conn.execute(
        """
        SELECT inventory_uid, extraction_status, ingestion_status, notes
        FROM historical_extraction_status
        ORDER BY inventory_uid
        """
    ).fetchall()
    assert statuses[0][1:3] == ("done", "ingested")
    assert "historical candidate" in statuses[0][3]
    assert statuses[1][1:3] == ("done", "ingested")
    assert "no item-level historical candidate" in statuses[1][3]

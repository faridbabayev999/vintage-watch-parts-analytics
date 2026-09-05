-- =============================================================
-- WATCHPARTS PROJECT — DATABASE SCHEMA (DuckDB)
-- =============================================================
-- Why DuckDB over SQLite:
--   - Native MEDIAN(), QUANTILE(), STDDEV() functions
--   - Columnar storage: aggregation queries 10-100x faster
--   - Can query CSV files directly without loading first
--   - Built for analytical workloads — exactly what we are doing
--
-- LAYERS:
--   raw_*   = exact copy of source data, never modified
--   stg_*   = cleaned, normalised, analysis-ready
--   feat_*  = computed metrics, scores, recommendations
--   ref_*   = reference/lookup tables
-- =============================================================


-- -------------------------------------------------------------
-- REFERENCE TABLES
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ref_exchange_rates (
    from_currency   VARCHAR NOT NULL,
    to_currency     VARCHAR NOT NULL DEFAULT 'EUR',
    rate            DOUBLE  NOT NULL,
    valid_date      DATE    NOT NULL,
    source          VARCHAR,
    created_at      TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (from_currency, to_currency, valid_date)
);

-- Module 6 (TMV scenario engine) reference tables. Same pattern as
-- ref_exchange_rates: every rate is dated, sourced, and looked up ASOF a
-- given date -- never a bare Python constant. No default/assumed rate is
-- shipped without a `source` citation (see scripts/00b_load_scenario_rates.py).
CREATE TABLE IF NOT EXISTS ref_shipping_rates (
    country         VARCHAR NOT NULL,
    shipping_cost   DOUBLE  NOT NULL,
    currency        VARCHAR NOT NULL DEFAULT 'EUR',
    valid_from      DATE    NOT NULL,
    source          VARCHAR,
    created_at      TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (country, valid_from)
);

CREATE TABLE IF NOT EXISTS ref_customs_rates (
    hs_code         VARCHAR NOT NULL,
    country         VARCHAR NOT NULL,
    duty_rate       DOUBLE  NOT NULL,
    valid_from      DATE    NOT NULL,
    source          VARCHAR,
    created_at      TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (hs_code, country, valid_from)
);

CREATE TABLE IF NOT EXISTS ref_tax_rates (
    country         VARCHAR NOT NULL,
    tax_type        VARCHAR NOT NULL,
    rate            DOUBLE  NOT NULL,
    valid_from      DATE    NOT NULL,
    source          VARCHAR,
    created_at      TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (country, tax_type, valid_from)
);

-- Configurable TMV/scenario parameters (owner decision 2026-07-30, docs/
-- TMV_DEMAND_PARAMETER_DESIGN.md). active_flag=FALSE means the parameter is
-- read but has NO effect (e.g. demand_weight=0.0 -> the TMV formula term it
-- controls is a no-op) -- inactive is a deliberate, disclosed state, not a
-- missing value. Never a bare Python constant for a business-sensitive weight.
CREATE TABLE IF NOT EXISTS ref_tmv_parameters (
    parameter_name  VARCHAR PRIMARY KEY,
    parameter_value DOUBLE  NOT NULL,
    description     VARCHAR,
    active_flag     BOOLEAN NOT NULL DEFAULT TRUE,
    source          VARCHAR,
    valid_from      DATE,
    created_at      TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS ref_condition_map (
    condition_raw       VARCHAR PRIMARY KEY,
    condition_standard  VARCHAR NOT NULL,  -- New / Excellent / Good / Acceptable / For_Parts
    language            VARCHAR            -- EN / IT / DE / FR / ES / ZH
);

-- Module 4: 'Neu' and 'Neu (Sonstige)' are the eBay item-wise sold source's
-- two most common condition values after 'Gebraucht' (778 and 535 of 2,832
-- rows respectively, verified against live data) and were previously
-- unmapped — grounded, unambiguous German "New" variants, not guesses.
-- ON CONFLICT DO NOTHING keeps this idempotent across reapplications and
-- never overwrites a value someone may have curated by hand since.
INSERT INTO ref_condition_map (condition_raw, condition_standard, language) VALUES
    ('Neu', 'New', 'DE'),
    ('Neu (Sonstige)', 'New', 'DE')
ON CONFLICT (condition_raw) DO NOTHING;

CREATE TABLE IF NOT EXISTS ref_brand_synonyms (
    brand_raw        VARCHAR PRIMARY KEY,
    brand_canonical  VARCHAR NOT NULL,      -- Rolex / Tudor
    created_at       TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS ref_caliber_variants (
    caliber_raw        VARCHAR PRIMARY KEY,
    caliber_canonical  VARCHAR NOT NULL,
    created_at         TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS ref_part_number_variants (
    part_number_raw        VARCHAR PRIMARY KEY,
    part_number_canonical  VARCHAR NOT NULL,
    created_at             TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS ingestion_log (
    source_type      VARCHAR,
    source_filename  VARCHAR,
    file_hash        VARCHAR,
    upload_batch_id  VARCHAR,
    ingested_at      TIMESTAMP DEFAULT current_timestamp,
    rows_inserted    INTEGER,
    status           VARCHAR,
    PRIMARY KEY (source_type, source_filename, file_hash)
);

CREATE TABLE IF NOT EXISTS dashboard_pipeline_jobs (
    job_id              VARCHAR PRIMARY KEY,
    trigger_source      VARCHAR NOT NULL,
    job_type            VARCHAR NOT NULL,
    status              VARCHAR NOT NULL,
    brand               VARCHAR,
    caliber             VARCHAR,
    part_number         VARCHAR,
    stock               INTEGER,
    canonical_inventory_id VARCHAR,
    inventory_uid       VARCHAR,
    requested_at        TIMESTAMP DEFAULT current_timestamp,
    started_at          TIMESTAMP,
    finished_at         TIMESTAMP,
    step_timings_json   VARCHAR,
    result_summary      VARCHAR,
    error_message       VARCHAR
);

CREATE TABLE IF NOT EXISTS dashboard_pipeline_job_events (
    event_id            BIGINT,
    job_id              VARCHAR NOT NULL,
    event_at            TIMESTAMP DEFAULT current_timestamp,
    event_type          VARCHAR NOT NULL,
    message             VARCHAR,
    PRIMARY KEY (event_id)
);

ALTER TABLE dashboard_pipeline_jobs ADD COLUMN IF NOT EXISTS trigger_source VARCHAR;
ALTER TABLE dashboard_pipeline_jobs ADD COLUMN IF NOT EXISTS job_type VARCHAR;
ALTER TABLE dashboard_pipeline_jobs ADD COLUMN IF NOT EXISTS status VARCHAR;
ALTER TABLE dashboard_pipeline_jobs ADD COLUMN IF NOT EXISTS brand VARCHAR;
ALTER TABLE dashboard_pipeline_jobs ADD COLUMN IF NOT EXISTS caliber VARCHAR;
ALTER TABLE dashboard_pipeline_jobs ADD COLUMN IF NOT EXISTS part_number VARCHAR;
ALTER TABLE dashboard_pipeline_jobs ADD COLUMN IF NOT EXISTS stock INTEGER;
ALTER TABLE dashboard_pipeline_jobs ADD COLUMN IF NOT EXISTS canonical_inventory_id VARCHAR;
ALTER TABLE dashboard_pipeline_jobs ADD COLUMN IF NOT EXISTS inventory_uid VARCHAR;
ALTER TABLE dashboard_pipeline_jobs ADD COLUMN IF NOT EXISTS requested_at TIMESTAMP;
ALTER TABLE dashboard_pipeline_jobs ADD COLUMN IF NOT EXISTS started_at TIMESTAMP;
ALTER TABLE dashboard_pipeline_jobs ADD COLUMN IF NOT EXISTS finished_at TIMESTAMP;
ALTER TABLE dashboard_pipeline_jobs ADD COLUMN IF NOT EXISTS step_timings_json VARCHAR;
ALTER TABLE dashboard_pipeline_jobs ADD COLUMN IF NOT EXISTS result_summary VARCHAR;
ALTER TABLE dashboard_pipeline_jobs ADD COLUMN IF NOT EXISTS error_message VARCHAR;


-- -------------------------------------------------------------
-- RAW LAYER — exact copy of source, never modified
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw_historical (
    id                  INTEGER,
    row_hash            VARCHAR,
    title               VARCHAR,
    avg_price_eur       DOUBLE,
    format              VARCHAR,
    avg_shipping_eur    DOUBLE,
    free_shipping_pct   INTEGER,
    total_sold          INTEGER,
    total_sales_eur     DOUBLE,
    last_sold           VARCHAR,   -- raw German date string e.g. "7. Aug 2025"
    bids                VARCHAR,   -- raw string, often "-"
    removed             VARCHAR,
    source_file         VARCHAR,   -- legacy column: historically the physical container filename (see physical_container_file)
    -- original_source_file / physical_container_file are two different facts,
    -- never collapsed into one column: original_source_file is the row's OWN
    -- provenance as supplied by the export itself (e.g. which search/page
    -- produced this row); physical_container_file is simply the CSV file
    -- 01_ingest.py actually opened to read this row. A row with no row-level
    -- provenance of its own falls back to physical_container_file here, but
    -- that is a fallback, never the normal case.
    original_source_file     VARCHAR,
    physical_container_file  VARCHAR,
    ingested_at         TIMESTAMP DEFAULT current_timestamp
);

-- Module 4: eBay item-wise sold-listing observations (EBAY_SOLD_LISTING
-- source_type / 'listing' row_grain, per stg_historical's already-added
-- source_type/row_grain columns). Deliberately a SEPARATE table from
-- raw_historical, not a shared one with extra nullable columns: the two
-- sources have fundamentally different grain (raw_historical is one row
-- per aggregated part across its whole sale history; this is one row per
-- individual observed sold listing) and the locked source-strategy
-- recommendation (docs/module4_historical_source_strategy.md) is
-- source-separated estimators, never concatenated — a shared raw table
-- would invite exactly that concatenation by making it look like one
-- unified rowset upstream.
CREATE TABLE IF NOT EXISTS raw_historical_ebay_sold (
    id                  INTEGER,
    row_hash            VARCHAR,       -- sha256(item_number) — item_number is the natural dedup key
    item_number         VARCHAR,       -- eBay's own listing id, globally unique per real listing
    title               VARCHAR,
    price_eur           DOUBLE,
    currency            VARCHAR,
    condition           VARCHAR,
    seller_type         VARCHAR,
    sold_date_iso       DATE,
    sold_date_raw       VARCHAR,       -- original scraped string, preserved as-is
    is_sold             BOOLEAN,
    shipping_eur        DOUBLE,
    free_shipping       BOOLEAN,
    best_offer          BOOLEAN,       -- price/negotiation semantics unconfirmed — see source-strategy doc §3
    location            VARCHAR,
    seller              VARCHAR,
    url                 VARCHAR,
    source_page         VARCHAR,       -- which scrape page produced this row (pagination-completeness audit trail)
    upload_batch_id     VARCHAR,
    source_filename     VARCHAR,
    file_hash           VARCHAR,
    ingested_at         TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS raw_active_broad (
    id                          INTEGER,
    row_hash                    VARCHAR,
    collected_at_utc            VARCHAR,
    keyword                     VARCHAR,
    source_country              VARCHAR,
    source_marketplace_id       VARCHAR,
    item_id                     VARCHAR,
    legacy_item_id              VARCHAR,
    title                       VARCHAR,
    price_value                 DOUBLE,
    price_currency              VARCHAR,
    condition                   VARCHAR,
    condition_id                DOUBLE,
    buying_options              VARCHAR,
    item_web_url                VARCHAR,
    image_url                   VARCHAR,
    seller_username             VARCHAR,
    seller_feedback_score       INTEGER,
    seller_feedback_percentage  DOUBLE,
    shipping_cost_value         DOUBLE,
    shipping_cost_currency      VARCHAR,
    item_location_country       VARCHAR,
    item_location_city          VARCHAR,
    category_ids                VARCHAR,
    category_names              VARCHAR,
    listing_marketplace_id      VARCHAR,
    item_creation_date          VARCHAR,
    ingested_at                 TIMESTAMP DEFAULT current_timestamp
);
ALTER TABLE raw_active_broad ALTER legacy_item_id TYPE VARCHAR;

-- Uploaded inventory rows exactly as received from the source file.
-- Source column mapping:
--   "Rolex/Tudor" -> raw_rolex_tudor
--   "Calibre"     -> raw_calibre
--   "P-number"    -> raw_p_number
--   "Stock"       -> raw_stock
-- Calibre and P-number stay TEXT so values like "7", "330", and "16-1"
-- are never converted to numbers, dates, or formulas.
CREATE TABLE IF NOT EXISTS raw_inventory (
    id                  INTEGER,
    row_hash            VARCHAR,
    upload_batch_id     VARCHAR,
    source_filename     VARCHAR,
    file_hash           VARCHAR,
    ingested_at         TIMESTAMP DEFAULT current_timestamp,
    raw_rolex_tudor     VARCHAR,
    raw_calibre         VARCHAR,
    raw_p_number        VARCHAR,
    raw_stock           VARCHAR,
    validation_status   VARCHAR,
    validation_notes    VARCHAR
);
ALTER TABLE raw_inventory ADD COLUMN IF NOT EXISTS row_hash VARCHAR;

CREATE TABLE IF NOT EXISTS inventory_validation_report (
    upload_batch_id     VARCHAR,
    source_filename     VARCHAR,
    check_name          VARCHAR,
    check_status        VARCHAR,   -- PASS / WARNING / FAIL
    row_id              INTEGER,
    column_name         VARCHAR,
    raw_value           VARCHAR,
    validation_message  VARCHAR,
    created_at          TIMESTAMP DEFAULT current_timestamp
);

-- Filled by Module 3 (scripts/04_collect_targeted_active.py writes CSV,
-- scripts/01_ingest.py --targeted ingests it). Keyed on inventory_uid for
-- the same correction-stability reason as inventory_stock_history and
-- search_queries; canonical_inventory_id is kept as a descriptive column.
-- One row per (collection_batch_id, item_id): the same real eBay listing
-- can legitimately reappear across batches (re-escalated on a later run),
-- and idempotent ingestion is handled at the row level in 01_ingest.py,
-- not by a table constraint.
CREATE TABLE IF NOT EXISTS raw_active_targeted (
    id                          INTEGER,
    collection_batch_id         VARCHAR,
    inventory_uid               VARCHAR,
    canonical_inventory_id      VARCHAR,   -- descriptive only, not part of the key
    query_text                  VARCHAR,   -- the exact query row consumed from search_queries
    query_tier                  INTEGER,
    query_template_version      VARCHAR,
    marketplace_id              VARCHAR,   -- which eBay marketplace this listing was found via (e.g. EBAY_DE)
    fetched_at                  VARCHAR,   -- raw ISO8601 string, exactly as produced by the collector
    item_id                     VARCHAR,
    row_hash                    VARCHAR,
    title                       VARCHAR,
    price_value                 DOUBLE,
    price_currency              VARCHAR,
    condition                   VARCHAR,
    condition_id                DOUBLE,
    buying_options              VARCHAR,
    item_web_url                VARCHAR,
    image_url                   VARCHAR,
    seller_username             VARCHAR,
    seller_feedback_score       INTEGER,
    seller_feedback_percentage  DOUBLE,
    shipping_cost_value         DOUBLE,
    shipping_cost_currency      VARCHAR,
    item_location_country       VARCHAR,
    item_location_city          VARCHAR,
    item_creation_date          VARCHAR,
    ingested_at                 TIMESTAMP DEFAULT current_timestamp
);

-- Module 3 batch lifecycle. finished_at stays NULL until the run completes
-- cleanly — --resume finds the latest NULL-finished_at batch and continues
-- it rather than starting a new one. stop_reason/chunks_completed/
-- fully_processed/last_chunk_id are a durable record of why/where the most
-- recent invocation of this batch stopped, persisted at every stop point
-- (not only on successful completion) so auditability doesn't depend on
-- ephemeral log output.
CREATE TABLE IF NOT EXISTS collection_batches (
    collection_batch_id  VARCHAR PRIMARY KEY,
    started_at           TIMESTAMP,
    finished_at          TIMESTAMP,
    config_snapshot      VARCHAR,   -- JSON of the configurable constants used for this batch
    stop_reason          VARCHAR,
    chunks_completed     INTEGER,
    fully_processed      BOOLEAN,
    last_chunk_id        VARCHAR,
    status               VARCHAR,   -- SUCCESS / FAILED / INCOMPLETE — see reconcile_batch_state()
    expected_pairs_json  VARCHAR    -- JSON list of [inventory_uid, marketplace_id] this batch was started
                                     -- for, captured once at start_batch() time. NULL for batches created
                                     -- before this column existed — reconcile_batch_state() never guesses
                                     -- completeness for those, it leaves them exactly as last recorded.
);

-- One durable chunk = one atomically-written CSV file. A row here only
-- ever exists once csv_written_at is set — there is deliberately no
-- "started but not written" row, since nothing should be treated as
-- resumable-skip-safe based on a chunk that never finished writing its
-- CSV. ingested_at is set once scripts/01_ingest.py --targeted has
-- confirmed (via ingestion_log) that source_filename was successfully
-- ingested; NULL here means "durably collected, not yet ingested" — a
-- safe, retryable state, never a lost one.
CREATE TABLE IF NOT EXISTS collection_chunks (
    chunk_id              VARCHAR PRIMARY KEY,
    collection_batch_id   VARCHAR,
    source_filename       VARCHAR,
    csv_sha256            VARCHAR,   -- computed once, right after the atomic rename; already_processed
                                      -- re-verifies the file against this before trusting an un-ingested chunk
    started_at            TIMESTAMP,
    csv_written_at        TIMESTAMP,
    ingested_at           TIMESTAMP,
    items_attempted       INTEGER,
    calls_made            INTEGER
);

-- One row per (batch, inventory item, marketplace) attempted. A row here
-- is only ever inserted AFTER its chunk's CSV has been atomically written
-- (see collection_chunks) — never immediately after the API call succeeds.
-- This is what makes a row here safe to treat as "already done" on
-- --resume: the underlying data is guaranteed durable on disk, not just
-- sitting in a since-crashed process's memory. chunk_id links each
-- progress row back to the exact durable file its results live in.
CREATE TABLE IF NOT EXISTS collection_progress (
    collection_batch_id      VARCHAR,
    chunk_id                 VARCHAR,
    inventory_uid            VARCHAR,
    marketplace_id           VARCHAR,
    highest_tier_attempted   INTEGER,
    resolved_tier            INTEGER,   -- NULL if unresolved
    api_calls                INTEGER,
    listings_found           INTEGER,   -- unique item_id count from this marketplace alone
    outcome_reason           VARCHAR,   -- success / tier_exhaustion / no_executable_queries
    processed_at             TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (collection_batch_id, inventory_uid, marketplace_id)
);


-- -------------------------------------------------------------
-- STAGING LAYER — cleaned, normalised
-- -------------------------------------------------------------

-- DEPRECATED (as of the two-source-separated staging design below): superseded
-- by stg_historical_vcp_aggregate and stg_historical_ebay_sold. Kept unchanged
-- here — not dropped, not renamed, not repurposed, still populated by the
-- existing clean_historical() — for backward compatibility with anything still
-- reading it. New work should read the two source-specific tables instead. See
-- docs/MODULE4_HISTORICAL_SOURCE_CONTRACTS.md for the full rationale.
CREATE TABLE IF NOT EXISTS stg_historical (
    id                  INTEGER PRIMARY KEY,
    raw_id              INTEGER,

    -- Source-aware historical contract (docs/module4_historical_source_strategy.md
    -- §9). row_grain is the load-bearing distinction: 'aggregate' rows (today,
    -- exclusively VERKAEUFER_COCKPIT_AGGREGATE) summarize total_sold/total_sales_eur
    -- sales under avg_price_eur; 'listing' rows (e.g. a future EBAY_SOLD_LISTING
    -- source) represent one individually priced/dated sold-listing observation
    -- under observed_price_eur/condition instead. These are never flattened into
    -- one shared "transaction price" column — aggregate and listing evidence stay
    -- structurally distinguishable so downstream code can never accidentally
    -- treat an averaged row as an independent per-unit observation, or vice versa.
    -- source_type is currently always 'VERKAEUFER_COCKPIT_AGGREGATE' in practice —
    -- no ingestion path writes 'EBAY_SOLD_LISTING' rows yet (see the Module 4
    -- historical-ingestion-foundation work); this column exists so that when such
    -- a path is built, it has somewhere correct to write to without another migration.
    source_type         VARCHAR,       -- 'VERKAEUFER_COCKPIT_AGGREGATE' / 'EBAY_SOLD_LISTING'
    row_grain           VARCHAR,       -- 'aggregate' / 'listing'
    source_record_id    VARCHAR,       -- the source's own row/listing identifier (e.g. eBay item_number); NULL for aggregate rows, which have no equivalent
    observed_price_eur  DOUBLE,        -- actual per-unit price for a 'listing'-grain row; NULL for 'aggregate'-grain rows (which have avg_price_eur instead)
    condition           VARCHAR,       -- listing-grain condition value; NULL for aggregate-grain rows, which have no condition concept
    original_source_file     VARCHAR, -- row-level provenance, promoted from raw_historical (see raw_historical's comment)
    physical_container_file  VARCHAR, -- ingested container filename, promoted from raw_historical

    title               VARCHAR,
    normalized_title    VARCHAR,
    brand               VARCHAR,
    search_keyword      VARCHAR,
    format              VARCHAR,       -- 'fixed_price' or 'auction'
    is_auction          BOOLEAN,

    avg_price_eur       DOUBLE,
    avg_shipping_eur    DOUBLE,
    avg_landed_cost_eur DOUBLE,        -- avg_price_eur + avg_shipping_eur
    total_sold          INTEGER,
    total_sales_eur     DOUBLE,
    free_shipping_pct   INTEGER,

    last_sold_date      DATE,
    last_sold_year      INTEGER,
    last_sold_month     INTEGER,

    removed             BOOLEAN,
    has_bids            BOOLEAN,

    -- dual-currency: rate is ECB EUR->USD rate on (or nearest business day before) last_sold_date
    avg_price_usd       DOUBLE,
    avg_landed_cost_usd DOUBLE,
    eur_usd_rate_used   DOUBLE,
    fx_rate_date        DATE,       -- actual ECB date the rate came from (may differ from last_sold_date on weekends/holidays)
    fx_rate_is_fallback BOOLEAN,    -- TRUE if no rate existed on/before this date and we used the earliest available rate instead

    -- Module 1 scenario prices/costs
    price_virtual_eur               DOUBLE,
    landed_cost_de_eur              DOUBLE,
    landed_cost_us_eur              DOUBLE,
    shipping_de_eur                 DOUBLE,
    shipping_us_eur                 DOUBLE,
    estimated_import_charges_us_eur DOUBLE,

    -- matching filled by 04_match.py
    matched_canonical_inventory_id VARCHAR,
    matched_product_id  INTEGER,
    match_confidence    VARCHAR,       -- HIGH / MEDIUM / LOW / UNMATCHED
    match_method        VARCHAR,       -- regex / fuzzy / llm / null
    match_score         DOUBLE,

    created_at          TIMESTAMP DEFAULT current_timestamp
);

-- Module 4 source-separated staging (docs/MODULE4_HISTORICAL_SOURCE_CONTRACTS.md).
-- One staged row = one VCP/Terapeak AGGREGATE observation (never a
-- transaction). Duplicate titles are NEVER collapsed or summed here — see
-- title_duplicate_group_size/duplicate_group_id, which make the ambiguity
-- queryable instead of hiding it.
CREATE TABLE IF NOT EXISTS stg_historical_vcp_aggregate (
    id                          INTEGER PRIMARY KEY,
    raw_id                      INTEGER,

    title                       VARCHAR,
    normalized_title            VARCHAR,
    brand                       VARCHAR,   -- grounded via source_file (see clean_historical_vcp_aggregate docstring) — never guessed
    search_keyword              VARCHAR,   -- same grounding; NULL for generic (non-category-suffixed) pages, not a failure

    format_standard             VARCHAR,   -- 'fixed_price' / 'auction' / 'unknown'
    is_auction                  BOOLEAN,

    avg_price_eur               DOUBLE,    -- AGGREGATE average price, never a single transaction's price
    avg_shipping_eur            DOUBLE,
    avg_landed_cost_eur         DOUBLE,
    shipping_value_reliability  VARCHAR,   -- OBSERVED_NONZERO / ZERO_CONFIRMED_FREE_SHIPPING / ZERO_AMBIGUOUS

    avg_price_usd                DOUBLE,
    avg_landed_cost_usd          DOUBLE,
    eur_usd_rate_used            DOUBLE,
    fx_rate_date                 DATE,
    fx_rate_is_fallback          BOOLEAN,

    total_sold                   INTEGER,   -- may overlap across duplicate-title snapshot rows — never summed across a duplicate group
    total_sales_eur              DOUBLE,    -- derived upstream (avg_price_eur * total_sold), not an independent figure
    free_shipping_pct            INTEGER,

    last_sold_date                DATE,     -- the MOST RECENT sale only, not a transaction-date series
    last_sold_year                INTEGER,
    last_sold_month               INTEGER,

    title_duplicate_group_size    INTEGER,  -- how many raw rows share this exact title; 1 = no duplication
    duplicate_group_id            VARCHAR,  -- deterministic id (hash of title) shared by every row in a duplicate-title group

    original_source_file          VARCHAR,  -- promoted from raw_historical; NULL for current live data (see Step 0 findings)
    physical_container_file       VARCHAR,  -- promoted from raw_historical; NULL for current live data
    source_file                   VARCHAR,  -- promoted from raw_historical; the column actually populated with per-row page provenance today

    row_hash                      VARCHAR,
    cleaned_at                    TIMESTAMP DEFAULT current_timestamp,

    matched_product_id           INTEGER,
    match_confidence             VARCHAR,
    match_method                  VARCHAR,
    match_score                    DOUBLE
);

-- One staged row = one eBay item-wise SOLD-LISTING observation (one
-- individual card, not an aggregate). item_number is a verified-unique
-- natural key.
CREATE TABLE IF NOT EXISTS stg_historical_ebay_sold (
    id                          INTEGER PRIMARY KEY,
    raw_id                      INTEGER,

    item_number                 VARCHAR,
    title                       VARCHAR,
    normalized_title            VARCHAR,
    sold_date                   DATE,

    price_original               DOUBLE,
    currency_original            VARCHAR,
    price_eur                    DOUBLE,
    price_usd                    DOUBLE,
    price_reliability            VARCHAR,   -- CONFIRMED_DISPLAYED_SOLD_PRICE / LISTED_PRICE_PROXY_BEST_OFFER

    shipping_original             DOUBLE,
    shipping_currency_original    VARCHAR,   -- inferred = currency_original (no separate shipping-currency field exists in this source)
    shipping_eur                  DOUBLE,    -- NULL when unknown and not proven free — never silently zeroed, see cleaner docstring
    shipping_usd                  DOUBLE,
    landed_cost_eur               DOUBLE,    -- NULL whenever shipping_eur is NULL
    landed_cost_usd                DOUBLE,

    eur_usd_rate_used             DOUBLE,
    fx_rate_date                  DATE,
    fx_rate_is_fallback           BOOLEAN,

    condition_raw                 VARCHAR,
    condition_standard            VARCHAR,   -- via ref_condition_map; NULL for unmapped/contaminated values, never guessed
    seller_type                   VARCHAR,
    free_shipping                 BOOLEAN,
    has_best_offer_option         BOOLEAN,

    location_raw                  VARCHAR,   -- known contaminated tail (see source contract doc) — never treated as a clean country field
    possible_multi_unit_lot       BOOLEAN,   -- title-text heuristic flag only, never an automatic exclusion

    seller                        VARCHAR,
    url                           VARCHAR,
    source_page                   VARCHAR,
    upload_batch_id               VARCHAR,
    source_filename                VARCHAR,
    file_hash                      VARCHAR,

    extraction_completeness       VARCHAR,   -- constant 'UNKNOWN' for this snapshot — pagination completeness unconfirmed

    row_hash                       VARCHAR,
    cleaned_at                     TIMESTAMP DEFAULT current_timestamp,

    matched_product_id            INTEGER,
    match_confidence              VARCHAR,
    match_method                   VARCHAR,
    match_score                     DOUBLE
);

CREATE TABLE IF NOT EXISTS stg_active_broad (
    id                          INTEGER PRIMARY KEY,
    raw_id                      INTEGER,

    item_id                     VARCHAR UNIQUE,
    title                       VARCHAR,
    normalized_title            VARCHAR,

    price_original              DOUBLE,
    price_currency_original     VARCHAR,
    price_eur                   DOUBLE,
    shipping_eur                DOUBLE,
    landed_cost_eur             DOUBLE,

    -- dual-currency: USD is exact if original currency was already USD, else bridged via EUR
    price_usd                   DOUBLE,
    landed_cost_usd             DOUBLE,
    fx_to_eur_rate_used         DOUBLE,   -- rate used to convert original currency -> EUR
    eur_usd_rate_used           DOUBLE,   -- rate used to bridge EUR -> USD (NULL if original currency was already USD)
    fx_rate_date                DATE,     -- ECB date the rate(s) came from, keyed off collected_at_utc
    fx_rate_is_fallback         BOOLEAN,

    -- Module 1 scenario prices/costs
    price_virtual_eur               DOUBLE,
    landed_cost_de_eur              DOUBLE,
    landed_cost_us_eur              DOUBLE,
    shipping_de_eur                 DOUBLE,
    shipping_us_eur                 DOUBLE,
    estimated_import_charges_us_eur DOUBLE,

    condition_raw               VARCHAR,
    condition_standard          VARCHAR,  -- New / Excellent / Good / Acceptable / For_Parts

    is_auction                  BOOLEAN,
    accepts_best_offer          BOOLEAN,

    seller_username             VARCHAR,
    seller_feedback_score       INTEGER,
    seller_feedback_percentage  DOUBLE,

    item_location_country       VARCHAR,
    marketplace                 VARCHAR,

    collected_at_utc            TIMESTAMP,
    item_creation_date          TIMESTAMP,
    days_listed                 INTEGER,

    -- matching filled by 04_match.py
    matched_canonical_inventory_id VARCHAR,
    matched_product_id          INTEGER,
    match_confidence            VARCHAR,
    match_method                VARCHAR,
    match_score                 DOUBLE,

    created_at                  TIMESTAMP DEFAULT current_timestamp
);

-- Cleaned inventory rows.
-- canonical_inventory_id is deterministic from brand + caliber + part_number.
-- Examples:
--   Rolex + 7  + 330  -> rolex_7_330
--   Rolex + 22 + 16-1 -> rolex_22_16_1
-- caliber and part_number are both nullable: a blank Calibre cell is a
-- WARNING (stored as SQL NULL), and a date-corrupted or blank part_number
-- is a FAIL (also stored as SQL NULL in this cleaned layer — the raw text
-- is preserved in raw_inventory and inventory_validation_report). "unknown"
-- only ever appears inside the generated canonical_inventory_id string.
CREATE TABLE IF NOT EXISTS staging_inventory (
    canonical_inventory_id      VARCHAR PRIMARY KEY,
    upload_batch_id             VARCHAR,
    brand                       VARCHAR NOT NULL,
    caliber                     VARCHAR,
    part_number                 VARCHAR,
    stock                       INTEGER,
    condition                   VARCHAR,
    source_filename             VARCHAR,
    ingested_at                 TIMESTAMP,
    inventory_uid               VARCHAR,
    validation_status           VARCHAR,
    part_number_is_distinctive  BOOLEAN,
    created_at                  TIMESTAMP DEFAULT current_timestamp,
    UNIQUE (brand, caliber, part_number)
);

-- Manual overrides applied to raw_inventory rows before validation, keyed
-- by the original raw_inventory.id. Lets a bad brand/caliber/part_number
-- be corrected without ever editing raw_inventory itself.
CREATE TABLE IF NOT EXISTS inventory_corrections (
    raw_inventory_id      INTEGER,
    corrected_brand        VARCHAR,
    corrected_caliber      VARCHAR,
    corrected_part_number  VARCHAR,
    corrected_by           VARCHAR,
    corrected_at           TIMESTAMP DEFAULT current_timestamp,
    notes                  VARCHAR
);

-- Append-only proposal+decision ledger sitting BETWEEN validation and
-- inventory_corrections. raw_inventory is never touched by any part of
-- this: a candidate is a PROPOSAL, generated deterministically from the
-- validation report by a general corruption-pattern detector (never
-- hardcoded to specific row ids or values), classified AUTO_REPAIR_ALLOWED
-- / USER_CONFIRMATION_REQUIRED / UNRESOLVED, and only becomes a real
-- correction (a row in inventory_corrections) once either the system
-- auto-applies an AUTO_REPAIR_ALLOWED candidate or a human approves a
-- USER_CONFIRMATION_REQUIRED one. UNRESOLVED candidates are never
-- auto-applied under any circumstance.
CREATE TABLE IF NOT EXISTS inventory_repair_candidates (
    id                  INTEGER PRIMARY KEY,
    raw_inventory_id    INTEGER NOT NULL,
    upload_batch_id     VARCHAR,
    column_name         VARCHAR NOT NULL,   -- 'brand' / 'caliber' / 'part_number'
    raw_value           VARCHAR,            -- the exact corrupted value, for audit
    proposed_value      VARCHAR,            -- NULL for UNRESOLVED (no safe guess exists)
    classification       VARCHAR NOT NULL,   -- AUTO_REPAIR_ALLOWED / USER_CONFIRMATION_REQUIRED / UNRESOLVED
    confidence          VARCHAR NOT NULL,   -- HIGH / MEDIUM / LOW
    repair_rule         VARCHAR NOT NULL,   -- short deterministic-rule label, e.g. 'excel_date_coercion_day_and_month_default'
    repair_evidence     VARCHAR,            -- free-text explanation (e.g. sibling part-number convention observed)
    status              VARCHAR NOT NULL DEFAULT 'PROPOSED',  -- PROPOSED / AUTO_APPLIED / APPROVED / REJECTED / SUPERSEDED
    generated_at        TIMESTAMP DEFAULT current_timestamp,
    decided_by          VARCHAR,
    decided_at          TIMESTAMP,
    notes               VARCHAR
);
CREATE SEQUENCE IF NOT EXISTS inventory_repair_candidates_id_seq START 1;

-- Append-only. Keeps inventory_uid stable across corrections and re-uploads:
-- keyed on the MIN(raw_inventory_id) anchor of a merged duplicate group, so
-- a later correction that changes canonical_inventory_id doesn't orphan the uid.
CREATE TABLE IF NOT EXISTS inventory_uid_registry (
    raw_inventory_id    INTEGER PRIMARY KEY,
    inventory_uid        VARCHAR NOT NULL,
    first_assigned_at    TIMESTAMP DEFAULT current_timestamp
);

-- Append-only stock snapshot, one row per (inventory_uid, upload_batch_id).
-- Keyed on inventory_uid rather than canonical_inventory_id: the latter can
-- change (a correction, or a cleaning-rule fix) while the physical item is
-- unchanged, and re-keying on canonical id would accumulate a stale +
-- fresh row pair forever. inventory_uid is the stable identity.
CREATE TABLE IF NOT EXISTS inventory_stock_history (
    canonical_inventory_id  VARCHAR,
    upload_batch_id          VARCHAR,
    stock                    INTEGER,
    inventory_uid            VARCHAR NOT NULL,
    observed_at              TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (inventory_uid, upload_batch_id)
);

-- Cleaned version of targeted listings — collected AS A CANDIDATE for a
-- specific inventory item (inventory_uid/canonical_inventory_id are the
-- direct collection-target link, known since 04_collect_targeted_active.py
-- chose the query), never a validated match: a candidate row can still be
-- irrelevant contamination (see the query-tier productivity audit) until
-- Module 5 scores match_confidence/match_method/match_score below.
CREATE TABLE IF NOT EXISTS stg_active_targeted (
    id                          INTEGER PRIMARY KEY,
    raw_id                      INTEGER,

    inventory_uid               VARCHAR,   -- collection-target link, not a validated match
    canonical_inventory_id      VARCHAR,   -- descriptive only, not part of any key
    query_text                  VARCHAR,   -- the exact query that surfaced this candidate
    query_tier                  INTEGER,   -- 1/2/4/5 — see 03_generate_queries.py's tier rules
    query_template_version      VARCHAR,

    item_id                     VARCHAR,
    title                       VARCHAR,
    normalized_title             VARCHAR,

    price_original               DOUBLE,
    price_currency_original      VARCHAR,
    price_eur                   DOUBLE,
    shipping_eur                DOUBLE,
    landed_cost_eur             DOUBLE,

    -- dual-currency, mirroring stg_active_broad's FX handling exactly
    price_usd                   DOUBLE,
    landed_cost_usd             DOUBLE,
    fx_to_eur_rate_used         DOUBLE,
    eur_usd_rate_used           DOUBLE,
    fx_rate_date                DATE,
    fx_rate_is_fallback         BOOLEAN,

    -- Module 1 scenario prices/costs
    price_virtual_eur               DOUBLE,
    landed_cost_de_eur              DOUBLE,
    landed_cost_us_eur              DOUBLE,
    shipping_de_eur                 DOUBLE,
    shipping_us_eur                 DOUBLE,
    estimated_import_charges_us_eur DOUBLE,

    condition_raw                VARCHAR,
    condition_standard          VARCHAR,
    is_auction                  BOOLEAN,
    accepts_best_offer          BOOLEAN,
    seller_username             VARCHAR,
    seller_feedback_score       INTEGER,
    seller_feedback_percentage  DOUBLE,
    item_location_country       VARCHAR,
    marketplace                 VARCHAR,
    fetched_at                  TIMESTAMP,
    item_creation_date          TIMESTAMP,
    days_listed                 INTEGER,

    -- provenance back to the raw collection event
    collection_batch_id         VARCHAR,
    row_hash                    VARCHAR,

    -- candidate-relevance scoring, filled by Module 5 — never fabricated here
    matched_canonical_inventory_id VARCHAR,
    matched_product_id          INTEGER,
    match_confidence            VARCHAR,
    match_method                VARCHAR,
    match_score                 DOUBLE,

    created_at                  TIMESTAMP DEFAULT current_timestamp
);


-- -------------------------------------------------------------
-- MODULE 1 TABLES — inventory search, matching, and coverage
-- -------------------------------------------------------------

-- Purely derived data — full DELETE + INSERT rebuild every run by
-- 03_generate_queries.py. Keyed on inventory_uid (not canonical_inventory_id)
-- so a correction never orphans queries; canonical_inventory_id is kept
-- as a descriptive column for readability only.
CREATE TABLE IF NOT EXISTS search_queries (
    inventory_uid            VARCHAR,
    canonical_inventory_id   VARCHAR,
    tier                     INTEGER,
    query_text               VARCHAR,
    uses_lexicon             BOOLEAN,
    query_template_version   VARCHAR,
    created_at               TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (inventory_uid, tier, query_text)
);

CREATE TABLE IF NOT EXISTS matched_current_listings (
    canonical_inventory_id  VARCHAR,
    listing_id              VARCHAR,
    raw_title               VARCHAR,
    match_confidence        VARCHAR,
    match_reason            VARCHAR,
    matched_brand           VARCHAR,
    matched_caliber         VARCHAR,
    matched_part_number     VARCHAR,
    PRIMARY KEY (canonical_inventory_id, listing_id)
);

CREATE TABLE IF NOT EXISTS unmatched_pool (
    listing_id          VARCHAR PRIMARY KEY,
    raw_title           VARCHAR,
    reason_unmatched    VARCHAR,
    created_at          TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS coverage_report (
    canonical_inventory_id          VARCHAR PRIMARY KEY,
    active_match_count              INTEGER,
    high_confidence_match_count     INTEGER,
    medium_confidence_match_count   INTEGER,
    low_confidence_match_count      INTEGER,
    historical_match_count          INTEGER,
    coverage_status                 VARCHAR
);

-- Module 5 — matching foundation (docs/MODULE5_MATCHING_FOUNDATION_DESIGN.md).
-- CANDIDATE generation only. No match_confidence, final_match,
-- accepted_match, or rejected_match column exists anywhere here, by
-- design — a row means "this evidence and this inventory item are
-- plausible together under one deterministic rule," nothing stronger.
-- Distinct from the pre-existing matched_current_listings/unmatched_pool/
-- coverage_report tables (unused dead scaffolding, keyed on the
-- less-stable canonical_inventory_id) — these new tables key on
-- inventory_uid throughout.

-- One row per invocation of scripts/05_generate_match_candidates.py.
CREATE TABLE IF NOT EXISTS match_run (
    match_run_id               VARCHAR PRIMARY KEY,
    created_at                  TIMESTAMP DEFAULT current_timestamp,
    algorithm_version            VARCHAR,   -- rule-set version label, e.g. 'v1_exact_token_rules'
    inventory_snapshot_reference  VARCHAR   -- what inventory state this run was generated against (e.g. a staging_inventory row count/hash), for reproducibility audit
);

-- Candidates against stg_active_targeted. active_raw_id references
-- stg_active_targeted.id (the cleaned/normalized row — matching runs
-- against normalized_title, which only exists post-cleaning; not
-- raw_active_targeted.id).
-- evidence_uid UNIQUE constraint (added alongside the legacy
-- active_raw_id one, not replacing it — docs/MODULE5_POST_PHASE1_ARCHITECTURE_AUDIT.md
-- Bug 1): DuckDB has no ALTER TABLE ADD CONSTRAINT, so this can only be
-- expressed here, at CREATE TABLE time. Safe because live has zero rows
-- in this table today (confirmed in docs/MODULE5_PRE_IMPLEMENTATION_BASELINE_AND_AUDIT.md)
-- — CREATE TABLE IF NOT EXISTS will create it fresh, with both
-- constraints, the first time this ever runs against live.
CREATE TABLE IF NOT EXISTS match_candidates_active (
    match_candidate_id   INTEGER PRIMARY KEY,
    match_run_id           VARCHAR NOT NULL,
    inventory_uid            VARCHAR NOT NULL,
    active_raw_id              INTEGER NOT NULL,  -- stg_active_targeted.id
    evidence_uid                 VARCHAR,  -- Module 5 stable evidence identity (nullable: populated once staging carries it)
    collection_inventory_uid     VARCHAR,  -- the inventory_uid whose targeted query collected this listing, captured at
                                           -- candidate-generation time so SELF_SOURCED/CROSS_REFERENCED is stable across
                                           -- staging rebuilds (docs/MODULE5_STATUS_AND_RUNBOOK.md §6). NULL for legacy rows.
    match_method                 VARCHAR NOT NULL,  -- PART_NUMBER_EXACT / CALIBER_EXACT / BRAND_CALIBER / BRAND_PART_NUMBER
    evidence_json                  VARCHAR NOT NULL,  -- JSON: {rule, inventory_value, title, matched_tokens}
    created_at                       TIMESTAMP DEFAULT current_timestamp,
    UNIQUE (match_run_id, inventory_uid, active_raw_id, match_method),
    UNIQUE (match_run_id, inventory_uid, evidence_uid, match_method)
);

-- Candidates against stg_historical_ebay_sold. Same structure.
CREATE TABLE IF NOT EXISTS match_candidates_ebay_sold (
    match_candidate_id   INTEGER PRIMARY KEY,
    match_run_id           VARCHAR NOT NULL,
    inventory_uid            VARCHAR NOT NULL,
    ebay_sold_raw_id            INTEGER NOT NULL,  -- stg_historical_ebay_sold.id
    evidence_uid                 VARCHAR,
    match_method                 VARCHAR NOT NULL,
    evidence_json                  VARCHAR NOT NULL,
    created_at                       TIMESTAMP DEFAULT current_timestamp,
    UNIQUE (match_run_id, inventory_uid, ebay_sold_raw_id, match_method),
    UNIQUE (match_run_id, inventory_uid, evidence_uid, match_method)
);

-- Candidates against stg_historical_vcp_aggregate. Same structure.
CREATE TABLE IF NOT EXISTS match_candidates_vcp (
    match_candidate_id   INTEGER PRIMARY KEY,
    match_run_id           VARCHAR NOT NULL,
    inventory_uid            VARCHAR NOT NULL,
    vcp_raw_id                  INTEGER NOT NULL,  -- stg_historical_vcp_aggregate.id
    evidence_uid                 VARCHAR,
    match_method                 VARCHAR NOT NULL,
    evidence_json                  VARCHAR NOT NULL,
    created_at                       TIMESTAMP DEFAULT current_timestamp,
    UNIQUE (match_run_id, inventory_uid, vcp_raw_id, match_method),
    UNIQUE (match_run_id, inventory_uid, evidence_uid, match_method)
);

-- Audit output ONLY (scripts/05b_evidence_coverage_audit.py) — a computed
-- summary derived from match_candidates_*, never a source of new
-- evidence, never scored. Never claims match_confidence — evidence_category
-- (A/B/C/D) is a coarse, deterministic, rule-based bucket, not a score.
-- Full-rebuild each audit run, same idempotency discipline as the
-- clean_* functions.
CREATE TABLE IF NOT EXISTS inventory_evidence_coverage (
    inventory_uid                   VARCHAR PRIMARY KEY,
    brand                           VARCHAR,
    caliber                         VARCHAR,
    part_number                     VARCHAR,
    active_candidate_count          INTEGER,
    ebay_sold_candidate_count       INTEGER,
    vcp_candidate_count             INTEGER,
    part_number_candidate_count     INTEGER,  -- PART_NUMBER_EXACT + BRAND_PART_NUMBER, all 3 sources combined
    caliber_candidate_count         INTEGER,  -- CALIBER_EXACT + BRAND_CALIBER (bare, non-component), all 3 sources
    component_candidate_count       INTEGER,  -- CALIBER_COMPONENT + BRAND_CALIBER_COMPONENT, all 3 sources
    evidence_category               VARCHAR,  -- A / B / C / D — see audit script docstring for exact criteria
    audit_run_id                    VARCHAR,
    computed_at                     TIMESTAMP DEFAULT current_timestamp
);

-- Module 5 — deterministic MATCHING-DECISION layer (scripts/06_decide_matches.py,
-- docs/MODULE5_DECISION_LAYER_DESIGN.md). Answers only "does this evidence row
-- correspond to this inventory item?" — never a price/TMV eligibility question.
-- One row per DISTINCT (source_table, inventory_uid, source_id, matching_rule)
-- candidate, not per match_candidate_id: evidence re-discovered by a later
-- candidate-generation run decides once, not once per accumulated run (same
-- aggregation convention as inventory_evidence_coverage). Full-rebuild each
-- decision run, same idempotency discipline as clean_*/05b. No numeric
-- confidence score anywhere — match_status is one of exactly five
-- deterministic values, never a continuous number (MATCH_CONFIRMED /
-- NO_MATCH / REVIEW_REQUIRED / INSUFFICIENT_EVIDENCE / LOW_CONFIDENCE_CANDIDATE;
-- the last added in Matching Engine v1.1 for calibre-only/component rules).
CREATE TABLE IF NOT EXISTS match_decisions (
    decision_id                INTEGER PRIMARY KEY,
    decision_version             VARCHAR NOT NULL,  -- e.g. 'v1_deterministic_conflict_risk_rules' — preserves historical interpretability across rule-engine revisions
    decision_run_id                VARCHAR NOT NULL,
    match_run_id                     VARCHAR,        -- lineage: which candidate-generation run this decision is anchored to (arg_max by created_at when re-discovered across runs)
    candidate_key                      VARCHAR NOT NULL,  -- deterministic sha256(source_table|inventory_uid|source_id|matching_rule)[:24] — stable across reruns
    inventory_uid                        VARCHAR NOT NULL,
    source_table                           VARCHAR NOT NULL,  -- match_candidates_active / match_candidates_ebay_sold / match_candidates_vcp
    source_id                                INTEGER NOT NULL,  -- active_raw_id / ebay_sold_raw_id / vcp_raw_id, per source_table
    matching_rule                              VARCHAR NOT NULL,  -- the match_method this decision was computed for
    evidence_tier                                VARCHAR NOT NULL,  -- A / B / C, per docs/MODULE5_EVIDENCE_TIER_CONTRACT.md
    match_status                                   VARCHAR NOT NULL,  -- MATCH_CONFIRMED / NO_MATCH / REVIEW_REQUIRED / INSUFFICIENT_EVIDENCE / LOW_CONFIDENCE_CANDIDATE (v1.1)
    match_reason_code                                VARCHAR NOT NULL,  -- explainable, from the fixed reason-code catalogue — never free text alone
    match_reason_text                                  VARCHAR,        -- human-readable elaboration of match_reason_code
    matched_fields                                       VARCHAR,        -- comma-separated inventory fields this rule anchors on, e.g. 'caliber,part_number'
    contradiction_flags                                    VARCHAR,        -- comma-separated flag names that fired (NULL if none) — see docs/MODULE5_DECISION_LAYER_DESIGN.md Phase 2
    risk_flags                                               VARCHAR,        -- comma-separated flag names that fired (NULL if none)
    collection_relationship                                    VARCHAR NOT NULL,  -- SELF_SOURCED / CROSS_REFERENCED (active-targeted only) / NOT_APPLICABLE (VCP, eBay-sold — no per-item collection concept exists for these sources)
    rule_precision_reference                                     VARCHAR,        -- the measured-precision citation this rule's tier placement rests on, for audit
    price_evidence_status                                          VARCHAR NOT NULL DEFAULT 'NOT_APPLICABLE',  -- structural placeholder only in this task; real price-eligibility rules are out of scope. Enforced: NOT_APPLICABLE for every match_status != MATCH_CONFIRMED (checked in code, not a DB constraint, consistent with this project's existing style)
    deterministic_checks_passed                                      BOOLEAN,        -- TRUE iff evidence_tier='A' with zero active contradiction/risk flags -- i.e. the row WOULD be MATCH_CONFIRMED under pure deterministic rules alone, independent of validation-policy state. Preserves technical rule strength separately from the operational decision (docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md Phase 1).
    confirmation_policy_reason                                       VARCHAR,        -- the validation_policy.validation_status consulted for this row's segment (APPROVED / VALIDATION_PENDING / NOT_APPROVED), or NOT_APPLICABLE for Tier B/C (which never consult segment policy). Distinct from match_reason_code: never allowed to hide a more specific contradiction/risk reason.
    confirmation_policy_version                                      VARCHAR,        -- confirmation_policy_version of the APPROVED validation_policy row actually consulted, if any; NULL when no APPROVED row applied (VALIDATION_PENDING/NOT_APPROVED/NOT_APPLICABLE)
    decided_at                                                       TIMESTAMP DEFAULT current_timestamp,
    UNIQUE (candidate_key, decision_version)
);

-- Module 5 — validation-policy gate (docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md).
-- Governs ONLY whether a Tier A segment's deterministically-clean candidates
-- may be assigned MATCH_CONFIRMED; it never overrides a contradiction or an
-- unresolved risk flag. One row per (matching_rule, source_table,
-- collection_relationship, confirmation_policy_version) segment definition.
-- No row in this table is inserted as APPROVED by this task -- every
-- existing segment starts VALIDATION_PENDING, per explicit instruction.
CREATE TABLE IF NOT EXISTS validation_policy (
    validation_policy_id      INTEGER PRIMARY KEY,
    confirmation_policy_version VARCHAR NOT NULL,  -- e.g. 'v1_validation_policy_gate' -- ties this row to a specific threshold_policy_version's approval decision
    validation_segment          VARCHAR NOT NULL,  -- human-readable label, e.g. 'CALIBER_PART_NUMBER|match_candidates_active|SELF_SOURCED'
    matching_rule                 VARCHAR NOT NULL,
    source_table                    VARCHAR NOT NULL,
    collection_relationship           VARCHAR NOT NULL,  -- SELF_SOURCED / CROSS_REFERENCED / NOT_APPLICABLE / ANY (ANY = applies regardless, only meaningful for non-active sources where collection_relationship is always NOT_APPLICABLE)
    required_risk_profile               VARCHAR,        -- description of what must already be clear, e.g. 'NO_CONTRADICTION_NO_RISK' (the only profile this task defines)
    validation_status                     VARCHAR NOT NULL,  -- APPROVED / VALIDATION_PENDING / NOT_APPROVED / NOT_APPLICABLE
    reviewed_sample_size                    INTEGER,
    true_match_count                          INTEGER,
    false_match_count                           INTEGER,
    ambiguous_count                               INTEGER,
    unreviewable_count                              INTEGER,
    observed_precision                                DOUBLE,
    confidence_method                                   VARCHAR,  -- e.g. 'CLOPPER_PEARSON_EXACT' / 'WILSON'
    confidence_level                                      DOUBLE,  -- e.g. 0.95
    precision_lower_bound                                   DOUBLE,
    approval_threshold_version                                VARCHAR,  -- confirmation_threshold_policy.threshold_policy_version this approval was evaluated against, if any
    policy_reason                                               VARCHAR,  -- free-text justification, required whenever validation_status='APPROVED'
    approved_by                                                   VARCHAR,
    approved_at                                                     TIMESTAMP,
    created_at                                                        TIMESTAMP DEFAULT current_timestamp
);

-- Module 5 — threshold-policy CONTRACT: what a segment must clear to become
-- APPROVED. Purely a documentation/evaluation record in this task -- no row
-- here is marked active/enforced, and scripts/06_decide_matches.py never
-- reads this table to auto-approve anything; validation_policy.validation_status
-- is the only thing the decision engine consults. Storing 90%/85%-style
-- figures here as PROPOSALS, never as hardcoded constants in code.
CREATE TABLE IF NOT EXISTS confirmation_threshold_policy (
    threshold_policy_id       INTEGER PRIMARY KEY,
    threshold_policy_version    VARCHAR NOT NULL,
    policy_purpose                 VARCHAR NOT NULL,  -- EXPLORATORY / INTERNAL_OPERATIONAL / COMPETITION_READY
    min_reviewed_sample_size         INTEGER,
    min_observed_precision             DOUBLE,
    min_precision_lower_bound            DOUBLE,
    confidence_method                      VARCHAR,
    confidence_level                         DOUBLE,
    required_edge_case_representation          VARCHAR,  -- free-text description, e.g. 'SELF_SOURCED+CROSS_REFERENCED, >=1 reference-list case, >=1 calibre-family-list case'
    max_unresolved_critical_risk_count           INTEGER,
    is_active                                      BOOLEAN NOT NULL DEFAULT FALSE,  -- never TRUE for any row inserted by this task
    notes                                            VARCHAR,
    created_at                                         TIMESTAMP DEFAULT current_timestamp
);

-- Module 5 — Layer 1 (reference verification) of calibre-compatibility
-- governance (docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md Phase 6A). Records
-- WHETHER a claimed brand/calibre relationship is factually supported.
-- Deliberately NOT populated by this task -- the Rolex 3130/3135-family
-- claim used earlier in this project's own analysis is REVIEWER_INFERENCE_ONLY
-- (asserted from general domain familiarity, not a cited source) and must
-- not be inserted here as if it were verified. A row's mere presence here
-- NEVER authorizes automatic suppression of calibre_conflict on its own --
-- see compatibility_policy_authorization (Layer 2).
CREATE TABLE IF NOT EXISTS ref_calibre_compatibility (
    compatibility_id      INTEGER PRIMARY KEY,
    brand                    VARCHAR NOT NULL,
    inventory_calibre          VARCHAR NOT NULL,
    evidence_calibre             VARCHAR NOT NULL,
    relationship_type              VARCHAR NOT NULL,  -- e.g. 'SAME_FAMILY_SHARED_PARTS'
    source_reference                  VARCHAR,        -- citation: manufacturer document, horological reference, etc. NULL only permitted when verification_status='UNRESOLVED'
    source_quality                      VARCHAR,        -- e.g. 'MANUFACTURER_SERVICE_DOCUMENT' / 'RECOGNIZED_HOROLOGICAL_REFERENCE' / 'UNSOURCED'
    verification_status                   VARCHAR NOT NULL,  -- VERIFIED_FROM_PROJECT_SOURCE / VERIFIED_EXTERNAL_REFERENCE / REVIEWER_INFERENCE_ONLY / UNRESOLVED
    verification_version                    VARCHAR,
    verified_by                               VARCHAR,
    verified_at                                 TIMESTAMP,
    valid_from                                    DATE,
    valid_to                                        DATE
);

-- Module 5 — Layer 2 (decision-policy authorisation) of calibre-compatibility
-- governance. A verified relationship_type in ref_calibre_compatibility does
-- NOT, by itself, affect decisions -- the ACTIVE confirmation_policy_version
-- must separately, explicitly authorise which relationship_type +
-- verification_status + source_quality combinations it accepts. Empty in
-- this task (no policy version authorises anything yet); populated only in
-- isolated test fixtures to prove the two-layer gate works.
CREATE TABLE IF NOT EXISTS compatibility_policy_authorization (
    authorization_id            INTEGER PRIMARY KEY,
    confirmation_policy_version   VARCHAR NOT NULL,
    relationship_type               VARCHAR NOT NULL,
    accepted_verification_status      VARCHAR NOT NULL,  -- one of ref_calibre_compatibility.verification_status's allowed values
    acceptable_source_quality           VARCHAR,
    brand_limitation                      VARCHAR,        -- NULL = no brand restriction
    source_limitation                       VARCHAR,        -- NULL = no source_table restriction
    authorized_by                             VARCHAR,
    authorized_at                               TIMESTAMP DEFAULT current_timestamp
);

-- Module 5 — reproducible export/lineage of every manually-reviewed
-- candidate this project's validation work has ever labelled, so sample
-- membership and cross-sample overlap (pooled Tier A n=90, SELF_SOURCED
-- n=20, CALIBER_PART_NUMBER n=30, calibre_conflict full-population n=18)
-- is explicit and queryable, not re-derived from a non-reproducible reseed.
-- Labels are never invented here -- only real, already-performed manual
-- reviews are recorded (docs/MODULE5_VALIDATION_POLICY_ANALYSIS.md Task 3's
-- disclosed ORDER BY/random.sample() reproducibility gap).
CREATE TABLE IF NOT EXISTS validation_review_samples (
    reviewed_case_id           INTEGER PRIMARY KEY,
    validation_sample_version    VARCHAR NOT NULL,  -- identifies which sampling exercise this row belongs to
    candidate_key                  VARCHAR NOT NULL,
    match_run_id                     VARCHAR,
    inventory_uid                      VARCHAR NOT NULL,
    source_table                         VARCHAR NOT NULL,
    source_id                              INTEGER NOT NULL,
    matching_rule                            VARCHAR NOT NULL,
    evidence_tier                              VARCHAR,
    collection_relationship                      VARCHAR,
    evidence_text                                  VARCHAR,  -- full untruncated title
    matched_tokens                                   VARCHAR,
    contradiction_flags                                VARCHAR,
    risk_flags                                           VARCHAR,
    reviewer_label                                         VARCHAR NOT NULL,  -- e.g. TRUE_MATCH / AMBIGUOUS / FALSE_MATCH / CONFLICT_CORRECT / CONFLICT_AMBIGUOUS_CROSS_COMPATIBLE / CONFLICT_INCORRECT_TOKENIZATION
    reviewer_reason                                          VARCHAR,
    reviewed_at                                                TIMESTAMP,
    UNIQUE (validation_sample_version, candidate_key)
);

-- Module 5 — derived, one row per eligible inventory_uid, PURELY computed from
-- match_decisions (never an independent status assignment). Full-rebuild each
-- decision run.
CREATE TABLE IF NOT EXISTS inventory_match_summary (
    inventory_uid                      VARCHAR PRIMARY KEY,
    summary_run_id                       VARCHAR NOT NULL,
    inventory_match_status                 VARCHAR NOT NULL,  -- HAS_CONFIRMED_MATCH / REVIEW_PENDING / ONLY_LOW_CONFIDENCE_CANDIDATES (v1.1) / ONLY_INSUFFICIENT_EVIDENCE / ALL_CANDIDATES_REJECTED / NO_CANDIDATES
    confirmed_candidate_count                INTEGER,
    review_candidate_count                     INTEGER,
    low_confidence_candidate_count               INTEGER,  -- v1.1: calibre-only/component candidates, out of the review queue, retained
    insufficient_candidate_count                 INTEGER,
    rejected_candidate_count                       INTEGER,
    total_candidate_count                            INTEGER,
    source_count                                       INTEGER,  -- distinct source_table values with >=1 decision for this item
    has_self_sourced_active_evidence                     BOOLEAN,  -- active-targeted only; FALSE (not NULL) when no active-targeted evidence exists at all
    has_cross_referenced_active_evidence                   BOOLEAN,
    last_evaluated_at                                        TIMESTAMP DEFAULT current_timestamp
);

-- Never rebuilt, never overwritten — may carry real human progress.
-- Keyed on inventory_uid (not canonical_inventory_id) for the same
-- correction-stability reason as inventory_stock_history and search_queries.
CREATE TABLE IF NOT EXISTS historical_extraction_status (
    inventory_uid           VARCHAR,
    canonical_inventory_id  VARCHAR,  -- descriptive only, not part of the key
    time_bucket             VARCHAR,
    extraction_status       VARCHAR,  -- not_started / in_progress / done / manual_review_needed
    source_filename         VARCHAR,
    extraction_date         DATE,
    ingestion_status        VARCHAR,  -- pending / ingested / failed
    notes                   VARCHAR,
    updated_at              TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (inventory_uid, time_bucket)
);

-- Empty for now — populated by a future lexicon mining/bootstrapping prompt.
CREATE TABLE IF NOT EXISTS ref_part_name_lexicon (
    caliber      VARCHAR,
    part_number  VARCHAR,
    part_name    VARCHAR,
    language     VARCHAR,
    source       VARCHAR,
    confidence   DOUBLE,
    created_at   TIMESTAMP DEFAULT current_timestamp
);


-- -------------------------------------------------------------
-- FEATURE LAYER — computed metrics and recommendations
-- (populated by 05_features.py, built later)
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS feat_market_supply (
    canonical_inventory_id  VARCHAR PRIMARY KEY,
    active_listing_count    INTEGER,
    unique_seller_count     INTEGER,
    min_landed_cost_eur     DOUBLE,
    max_landed_cost_eur     DOUBLE,
    median_landed_cost_eur  DOUBLE,
    p25_landed_cost_eur     DOUBLE,   -- 25th percentile
    p75_landed_cost_eur     DOUBLE,   -- 75th percentile
    price_spread_eur        DOUBLE,   -- max - min
    hhi_score               DOUBLE,   -- market concentration
    dominant_seller         VARCHAR,
    dominant_seller_share   DOUBLE,
    computed_at             TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS feat_demand (
    canonical_inventory_id  VARCHAR PRIMARY KEY,
    total_sold              INTEGER,
    dataset_months          DOUBLE,
    sales_velocity_monthly  DOUBLE,   -- total_sold / dataset_months
    last_sold_date          DATE,
    days_since_last_sale    INTEGER,
    recency_score           DOUBLE,   -- 0-1, higher = more recently sold
    price_trend_slope       DOUBLE,   -- EUR per month, from historical prices over time
    computed_at             TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS feat_pricing (
    canonical_inventory_id  VARCHAR PRIMARY KEY,
    brand                   VARCHAR,
    caliber                 VARCHAR,
    part_number             VARCHAR,
    condition               VARCHAR,
    stock                   INTEGER,

    -- pricing model outputs
    recommended_price_eur   DOUBLE,
    weight_historical       DOUBLE,
    weight_market           DOUBLE,
    confidence_tier         VARCHAR,  -- HIGH / MEDIUM / LOW
    scarcity_score          DOUBLE,
    scarcity_flag           VARCHAR,  -- SCARCE / OVERSUPPLIED / BALANCED
    competitive_position    DOUBLE,   -- (client_price - market_median) / market_median
    -- Phase 4 (dashboard integration): additive, persists H/C dollar values
    -- build() already computes -- no formula/weight change, output only.
    historical_value_eur    DOUBLE,
    current_value_eur       DOUBLE,
    -- Task 6 (price recommendation transparency): plain-text explanation of
    -- why recommended_price_eur has the value it has -- which basis (H or C)
    -- it rests on, the trend/scarcity nudge direction, and confidence tier.
    -- recommended_price_eur remains numerically == tmv_eur; this column adds
    -- disclosure, not a new multiplier (owner instruction, 2026-07-30).
    recommendation_reason   VARCHAR,

    -- survival model output
    median_days_to_sell     DOUBLE,
    prob_sold_30_days       DOUBLE,
    prob_sold_90_days       DOUBLE,

    -- revenue forecast
    expected_revenue_base   DOUBLE,
    expected_revenue_low    DOUBLE,
    expected_revenue_high   DOUBLE,

    -- action signal for dashboard
    action                  VARCHAR,  -- RAISE / REDUCE / HOLD
    action_reason           VARCHAR,

    computed_at             TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS feat_competitor (
    seller_username             VARCHAR PRIMARY KEY,
    total_listings              INTEGER,
    market_share_pct            DOUBLE,
    median_price_eur            DOUBLE,
    avg_feedback_score          DOUBLE,
    avg_feedback_pct            DOUBLE,
    pct_fixed_price             DOUBLE,
    pct_auction                 DOUBLE,
    pct_best_offer              DOUBLE,
    primary_countries           VARCHAR,  -- comma-separated top countries
    computed_at                 TIMESTAMP DEFAULT current_timestamp
);


-- -------------------------------------------------------------
-- FUTURE OUTPUT TABLES — built later, not Module 1 logic yet
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tmv_results (
    canonical_inventory_id  VARCHAR PRIMARY KEY,
    tmv_eur                 DOUBLE,
    tmv_low_eur             DOUBLE,
    tmv_high_eur            DOUBLE,
    confidence_tier         VARCHAR,
    computed_at             TIMESTAMP DEFAULT current_timestamp
);

-- DISCLAIMER: Turnover estimates selling velocity based on historical sales
-- behavior. It is NOT a price elasticity model and does not estimate price
-- response. median_days_to_sell / probabilities derive from sold COUNT + dates
-- only (hazard rate), never from price/TMV. Consumes MATCH_CONFIRMED evidence
-- only (same scope as TMV). See docs/MODULE4_TURNOVER.md.
CREATE TABLE IF NOT EXISTS turnover_survival (
    canonical_inventory_id  VARCHAR PRIMARY KEY,
    median_days_to_sell     DOUBLE,
    probability_sell_30d    DOUBLE,
    probability_sell_90d    DOUBLE,
    -- JSON list of {bucket, expected_units}, 8 buckets (0-7 .. 1066+), integrated
    -- from the same hazard-rate survival curve over stock quantity Q. Additive
    -- (Task 7); never recomputed downstream, read verbatim by the dashboard.
    turnover_bucket_forecast VARCHAR,
    computed_at             TIMESTAMP DEFAULT current_timestamp
);

ALTER TABLE tmv_results ADD COLUMN IF NOT EXISTS valuation_basis VARCHAR;
ALTER TABLE tmv_results ADD COLUMN IF NOT EXISTS price_reliability VARCHAR;


-- -------------------------------------------------------------
-- MODULE 2 MIGRATIONS — additive only, safe to re-run every time.
-- CREATE TABLE IF NOT EXISTS above only affects brand-new databases;
-- these statements retrofit tables that already existed before Module 2.
-- -------------------------------------------------------------

ALTER TABLE staging_inventory ALTER COLUMN caliber DROP NOT NULL;
ALTER TABLE staging_inventory ALTER COLUMN part_number DROP NOT NULL;

ALTER TABLE staging_inventory ADD COLUMN IF NOT EXISTS inventory_uid VARCHAR;
ALTER TABLE staging_inventory ADD COLUMN IF NOT EXISTS validation_status VARCHAR;
ALTER TABLE staging_inventory ADD COLUMN IF NOT EXISTS part_number_is_distinctive BOOLEAN;

ALTER TABLE stg_historical ADD COLUMN IF NOT EXISTS normalized_title VARCHAR;
ALTER TABLE stg_historical ADD COLUMN IF NOT EXISTS price_virtual_eur DOUBLE;
ALTER TABLE stg_historical ADD COLUMN IF NOT EXISTS landed_cost_de_eur DOUBLE;
ALTER TABLE stg_historical ADD COLUMN IF NOT EXISTS landed_cost_us_eur DOUBLE;
ALTER TABLE stg_historical ADD COLUMN IF NOT EXISTS shipping_de_eur DOUBLE;
ALTER TABLE stg_historical ADD COLUMN IF NOT EXISTS shipping_us_eur DOUBLE;
ALTER TABLE stg_historical ADD COLUMN IF NOT EXISTS estimated_import_charges_us_eur DOUBLE;

-- Module 4 pre-implementation foundation (source-aware historical contract):
ALTER TABLE stg_historical ADD COLUMN IF NOT EXISTS source_type VARCHAR;
ALTER TABLE stg_historical ADD COLUMN IF NOT EXISTS row_grain VARCHAR;
ALTER TABLE stg_historical ADD COLUMN IF NOT EXISTS source_record_id VARCHAR;
ALTER TABLE stg_historical ADD COLUMN IF NOT EXISTS observed_price_eur DOUBLE;
ALTER TABLE stg_historical ADD COLUMN IF NOT EXISTS condition VARCHAR;
ALTER TABLE stg_historical ADD COLUMN IF NOT EXISTS original_source_file VARCHAR;
ALTER TABLE stg_historical ADD COLUMN IF NOT EXISTS physical_container_file VARCHAR;

ALTER TABLE stg_active_broad ADD COLUMN IF NOT EXISTS normalized_title VARCHAR;
ALTER TABLE stg_active_broad ADD COLUMN IF NOT EXISTS price_virtual_eur DOUBLE;
ALTER TABLE stg_active_broad ADD COLUMN IF NOT EXISTS landed_cost_de_eur DOUBLE;
ALTER TABLE stg_active_broad ADD COLUMN IF NOT EXISTS landed_cost_us_eur DOUBLE;
ALTER TABLE stg_active_broad ADD COLUMN IF NOT EXISTS shipping_de_eur DOUBLE;
ALTER TABLE stg_active_broad ADD COLUMN IF NOT EXISTS shipping_us_eur DOUBLE;
ALTER TABLE stg_active_broad ADD COLUMN IF NOT EXISTS estimated_import_charges_us_eur DOUBLE;

ALTER TABLE inventory_stock_history ADD COLUMN IF NOT EXISTS inventory_uid VARCHAR;

-- Module 5 validation-policy gate: retrofit match_decisions (created in
-- commit 06cc557, before this gate existed) with the three new columns.
-- Additive only -- match_decisions' existing columns and rows are untouched.
ALTER TABLE match_decisions ADD COLUMN IF NOT EXISTS deterministic_checks_passed BOOLEAN;
ALTER TABLE match_decisions ADD COLUMN IF NOT EXISTS confirmation_policy_reason VARCHAR;
ALTER TABLE match_decisions ADD COLUMN IF NOT EXISTS confirmation_policy_version VARCHAR;

-- Targeted-active evidence preservation + clean_active_targeted() foundation.
-- The live table predates this shape (it was created with a since-removed
-- product_id column and no canonical_inventory_id at all — CREATE TABLE IF
-- NOT EXISTS never retrofits that). product_id is left in place, unused,
-- rather than dropped — additive only, per this project's standing rule.
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS inventory_uid VARCHAR;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS canonical_inventory_id VARCHAR;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS query_text VARCHAR;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS query_tier INTEGER;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS query_template_version VARCHAR;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS normalized_title VARCHAR;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS price_original DOUBLE;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS price_currency_original VARCHAR;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS price_usd DOUBLE;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS landed_cost_usd DOUBLE;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS fx_to_eur_rate_used DOUBLE;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS eur_usd_rate_used DOUBLE;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS fx_rate_date DATE;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS fx_rate_is_fallback BOOLEAN;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS price_virtual_eur DOUBLE;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS landed_cost_de_eur DOUBLE;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS landed_cost_us_eur DOUBLE;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS shipping_de_eur DOUBLE;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS shipping_us_eur DOUBLE;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS estimated_import_charges_us_eur DOUBLE;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS condition_raw VARCHAR;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS fetched_at TIMESTAMP;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS item_creation_date TIMESTAMP;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS collection_batch_id VARCHAR;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS row_hash VARCHAR;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS matched_canonical_inventory_id VARCHAR;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS matched_product_id INTEGER;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS match_confidence VARCHAR;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS match_method VARCHAR;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS match_score DOUBLE;

-- Pipeline state bookkeeping fix: collection_batches predates the status/
-- expected_pairs_json columns needed for reconcile_batch_state() to
-- distinguish a genuinely-finished batch from one that only LOOKS stuck
-- (e.g. stop_reason='ingestion_failed' left permanently after a later
-- --reconcile-only actually fixed the ingestion).
ALTER TABLE collection_batches ADD COLUMN IF NOT EXISTS status VARCHAR;
ALTER TABLE collection_batches ADD COLUMN IF NOT EXISTS expected_pairs_json VARCHAR;

-- -------------------------------------------------------------
-- MODULE 5 EVIDENCE IDENTITY — additive only, per
-- docs/MODULE5_EVIDENCE_IDENTITY_IMPLEMENTATION_CHECKLIST.md §0/§1.
-- Replaces the positional stg_id = range(1, len(df)+1) pattern
-- (docs/MODULE5_LINEAGE_INTEGRITY_AUDIT.md) with a deterministic,
-- content-derived identity. Legacy positional-id columns
-- (active_raw_id, ebay_sold_raw_id, vcp_raw_id, source_id) are
-- NEVER dropped or renamed here -- see scripts/evidence_identity.py
-- for the grain-separation rationale.
--
-- Live database has zero rows in match_candidates_*/match_decisions
-- as of this migration (docs/MODULE5_PRE_IMPLEMENTATION_BASELINE_AND_AUDIT.md)
-- -- this is a clean production build, not a migration of existing
-- live decisions. No LEGACY/backfill machinery is introduced here.
-- -------------------------------------------------------------

-- Grain 2: stable evidence identity. One row per real-world evidence
-- object (a listing, or a VCP cluster). inventory_uid never appears
-- here -- see evidence_identity.py's module docstring.
CREATE TABLE IF NOT EXISTS evidence_identity (
    stable_evidence_uid   VARCHAR PRIMARY KEY,
    identity_type         VARCHAR,   -- 'INDIVIDUAL_LISTING' | 'AGGREGATE_CLUSTER'
    identity_source        VARCHAR,   -- 'natural_key' | 'fallback_hash'
    identity_confidence    VARCHAR,   -- 'HIGH' | 'LOW' | 'UNCONFIRMED'
    source_system          VARCHAR,   -- e.g. 'raw_active_targeted'
    natural_key_type       VARCHAR,   -- 'ITEM_ID' | 'ITEM_NUMBER' | 'DUPLICATE_GROUP_ID' | 'FALLBACK_CONTENT_HASH'
    marketplace             VARCHAR,   -- nullable; not applicable to sold-eBay (no marketplace column on that source)
    natural_key_value       VARCHAR,
    first_seen_at           TIMESTAMP DEFAULT current_timestamp
);

-- Grain 3: evidence observation identity. One row per distinct
-- snapshot of an evidence object. Never collapsed across genuinely
-- different raw rows -- confirmed necessary today (not just future-
-- proofing) by the 222 VCP duplicate_group_id groups with multiple,
-- differently-priced rows.
CREATE TABLE IF NOT EXISTS evidence_observation (
    observation_uid       VARCHAR PRIMARY KEY,
    stable_evidence_uid   VARCHAR NOT NULL,   -- references evidence_identity.stable_evidence_uid
    raw_id                 INTEGER,
    observed_at             TIMESTAMP,
    price                   DOUBLE,
    condition               VARCHAR,
    title_snapshot           VARCHAR,
    is_current               BOOLEAN,
    created_at               TIMESTAMP DEFAULT current_timestamp
);

-- Additive stable_evidence_uid/observation_uid columns on every staging
-- table that evidence_identity.py's add_*_identity_columns() populates.
-- stg_historical is deliberately excluded here: its natural key
-- (source_record_id) is confirmed unpopulated
-- (docs/MODULE5_EVIDENCE_IDENTITY_IMPLEMENTATION_CHECKLIST.md §5) --
-- adding the column now with no populating code would be a silent
-- always-NULL column, worse than not having it yet.
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS stable_evidence_uid VARCHAR;
ALTER TABLE stg_active_targeted ADD COLUMN IF NOT EXISTS observation_uid VARCHAR;
ALTER TABLE stg_active_broad ADD COLUMN IF NOT EXISTS stable_evidence_uid VARCHAR;
ALTER TABLE stg_active_broad ADD COLUMN IF NOT EXISTS observation_uid VARCHAR;
ALTER TABLE stg_historical_ebay_sold ADD COLUMN IF NOT EXISTS stable_evidence_uid VARCHAR;
ALTER TABLE stg_historical_ebay_sold ADD COLUMN IF NOT EXISTS observation_uid VARCHAR;
ALTER TABLE stg_historical_vcp_aggregate ADD COLUMN IF NOT EXISTS stable_evidence_uid VARCHAR;
ALTER TABLE stg_historical_vcp_aggregate ADD COLUMN IF NOT EXISTS observation_uid VARCHAR;

-- Grain 4 (candidate relationship) additive columns: evidence_uid is
-- the new join mechanism: (match_run_id, inventory_uid, evidence_uid,
-- match_method). Legacy active_raw_id/ebay_sold_raw_id/vcp_raw_id/
-- source_id columns are untouched and still populated.
ALTER TABLE match_candidates_active ADD COLUMN IF NOT EXISTS evidence_uid VARCHAR;
ALTER TABLE match_candidates_ebay_sold ADD COLUMN IF NOT EXISTS evidence_uid VARCHAR;
ALTER TABLE match_candidates_vcp ADD COLUMN IF NOT EXISTS evidence_uid VARCHAR;
ALTER TABLE match_decisions ADD COLUMN IF NOT EXISTS evidence_uid VARCHAR;

-- collection_inventory_uid: the collection-target inventory_uid captured at
-- candidate-generation time (docs/MODULE5_STATUS_AND_RUNBOOK.md §6). Only
-- match_candidates_active carries it (only the targeted-active source is
-- collected per inventory item); the historical sources have no such
-- concept, so their collection_relationship is always NOT_APPLICABLE.
ALTER TABLE match_candidates_active ADD COLUMN IF NOT EXISTS collection_inventory_uid VARCHAR;

-- Evidence confidence engine (docs/AUTONOMOUS_PRODUCTION_READINESS_REPORT.md).
-- ALGORITHMIC classification, fully separate from the human-governed
-- validation_policy/MATCH_CONFIRMED gate -- never writes to those tables,
-- never claims human review. One row per (candidate_key), full-rebuild each
-- run (same idempotency discipline as match_decisions).
CREATE TABLE IF NOT EXISTS evidence_confidence_classification (
    classification_id      INTEGER PRIMARY KEY,
    classification_run_id    VARCHAR NOT NULL,
    candidate_key               VARCHAR NOT NULL,
    inventory_uid                  VARCHAR NOT NULL,
    matching_rule                    VARCHAR NOT NULL,
    source_table                       VARCHAR NOT NULL,
    source_id                            INTEGER NOT NULL,
    evidence_uid                           VARCHAR,
    v2_score                                 DOUBLE NOT NULL,
    confidence_tier                            VARCHAR NOT NULL,  -- AUTO_CONFIRMED / HIGH_CONFIDENCE / MEDIUM_CONFIDENCE / LOW_CONFIDENCE / REJECTED
    tier_reason                                  VARCHAR NOT NULL,  -- plain-text: which rule placed it in this tier
    positive_features                              VARCHAR,  -- JSON list
    negative_features                                VARCHAR,  -- JSON list
    computed_at                                        TIMESTAMP DEFAULT current_timestamp,
    UNIQUE (classification_run_id, candidate_key)
);

-- Autonomous ALGORITHMIC TMV/turnover path (docs/AUTONOMOUS_PRODUCTION_
-- READINESS_REPORT.md, scripts/22_build_confidence_tmv.py). Consumes
-- evidence_confidence_classification (AUTO_CONFIRMED/HIGH_CONFIDENCE only),
-- reuses 13_build_tmv.py's exact formula. NEVER the same table as
-- tmv_results/turnover_survival (the human-governed MATCH_CONFIRMED path) --
-- kept structurally separate so the two are never confused at the schema
-- level, not just by convention.
CREATE TABLE IF NOT EXISTS tmv_results_algorithmic (
    canonical_inventory_id  VARCHAR PRIMARY KEY,
    tmv_eur                 DOUBLE,
    tmv_low_eur              DOUBLE,
    tmv_high_eur               DOUBLE,
    confidence_tier               VARCHAR NOT NULL,  -- AUTO_CONFIRMED / HIGH_CONFIDENCE (item-level, weakest contributing evidence)
    evidence_basis_type             VARCHAR NOT NULL DEFAULT 'ALGORITHMIC',
    valuation_basis                    VARCHAR,
    historical_value_eur                  DOUBLE,
    current_value_eur                        DOUBLE,
    -- Scarcity (S), price trend (P), demand index (D) -- ALWAYS computed by
    -- 13_build_tmv.py's build() and ALREADY baked into tmv_eur via the
    -- formula's beta*(S-0.5) and alpha*P nudges, for both evidence paths.
    -- Added 2026-08-01 -- previously computed but discarded here, so no
    -- client view could show the real number (found via a client screenshot
    -- showing scarcity as a flat 0 on every row, which is factually wrong:
    -- real S ranges 0-0.98 across algorithmic items, mean 0.47).
    scarcity_score                              DOUBLE,
    price_trend                                    DOUBLE,
    demand_index                                      DOUBLE,
    recommendation_reason                       VARCHAR,
    computed_at                                    TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS turnover_survival_algorithmic (
    canonical_inventory_id  VARCHAR PRIMARY KEY,
    median_days_to_sell     DOUBLE,
    probability_sell_30d    DOUBLE,
    probability_sell_90d    DOUBLE,
    turnover_bucket_forecast VARCHAR,
    computed_at             TIMESTAMP DEFAULT current_timestamp
);

-- Additive migration (2026-08-01): tmv_results_algorithmic predates
-- scarcity_score/price_trend/demand_index -- CREATE TABLE IF NOT EXISTS
-- above only affects brand-new databases, so any existing database needs
-- this explicit ALTER to retrofit the columns.
ALTER TABLE tmv_results_algorithmic ADD COLUMN IF NOT EXISTS scarcity_score DOUBLE;
ALTER TABLE tmv_results_algorithmic ADD COLUMN IF NOT EXISTS price_trend DOUBLE;
ALTER TABLE tmv_results_algorithmic ADD COLUMN IF NOT EXISTS demand_index DOUBLE;

-- -------------------------------------------------------------
-- DASHBOARD CONTRACT — authoritative client-facing output.
-- Populated by scripts/23_build_dashboard_contract.py. One row per
-- eligible inventory item (staging_inventory.validation_status <> 'FAIL').
-- The dashboard should read this table instead of reconstructing business
-- joins live from raw/staging/model tables.
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dashboard_inventory_pricing (
    inventory_uid                   VARCHAR PRIMARY KEY,
    canonical_inventory_id          VARCHAR,
    brand                           VARCHAR,
    caliber                         VARCHAR,
    part_number                     VARCHAR,
    description                     VARCHAR,
    stock_quantity                  INTEGER,
    validation_status               VARCHAR,

    pricing_status                  VARCHAR,
    pricing_confidence              VARCHAR,
    confidence_label                VARCHAR,
    confidence_score                DOUBLE,
    pricing_method                  VARCHAR,
    recommended_price_eur           DOUBLE,
    price_lower_bound_eur           DOUBLE,
    price_upper_bound_eur           DOUBLE,
    base_tmv_eur                    DOUBLE,
    confidence_reason               VARCHAR,
    recommendation_reason           VARCHAR,

    historical_value_h              DOUBLE,
    current_value_c                 DOUBLE,
    demand_index_d                  DOUBLE,
    scarcity_score_s                DOUBLE,
    price_trend_p                   DOUBLE,
    demand_adjustment_eur           DOUBLE,

    active_evidence_count           INTEGER,
    historical_evidence_count       INTEGER,
    unique_active_evidence_count    INTEGER,
    unique_historical_evidence_count INTEGER,
    total_unique_evidence_count     INTEGER,
    evidence_basis                  VARCHAR,
    active_price_median_eur         DOUBLE,
    active_price_iqr_ratio          DOUBLE,
    active_price_dispersion         DOUBLE,
    active_price_min_eur            DOUBLE,
    active_price_max_eur            DOUBLE,
    active_price_range_ratio        DOUBLE,
    active_price_mad_eur            DOUBLE,
    active_outlier_count            INTEGER,
    active_duplicate_observation_count INTEGER,
    condition_assumption            VARCHAR,
    authenticity_assessment_status  VARCHAR,
    active_pricing_caveat           VARCHAR,
    historical_price_median_eur     DOUBLE,
    historical_price_iqr_ratio      DOUBLE,
    historical_active_gap_ratio     DOUBLE,
    ask_to_sold_adjustment          DOUBLE,
    adjustment_source               VARCHAR,
    adjustment_support_count        INTEGER,
    adjustment_hierarchy_level      VARCHAR,

    median_days_to_sell             DOUBLE,
    turnover_confidence             VARCHAR,
    turnover_method                 VARCHAR,
    sell_time_lower_days            INTEGER,
    sell_time_upper_days            INTEGER,
    sell_time_display               VARCHAR,
    turnover_evidence_status        VARCHAR,
    turnover_reason                 VARCHAR,
    turnover_support_evidence_count INTEGER,
    turnover_support_item_count     INTEGER,
    turnover_support_level          VARCHAR,
    probability_sell_30d            DOUBLE,
    probability_sell_90d            DOUBLE,

    units_sold_0_7                  DOUBLE,
    units_sold_8_30                 DOUBLE,
    units_sold_31_90                DOUBLE,
    units_sold_91_183               DOUBLE,
    units_sold_184_365              DOUBLE,
    units_sold_366_730              DOUBLE,
    units_sold_731_1065             DOUBLE,
    units_sold_1066_plus            DOUBLE,
    units_remaining                 DOUBLE,
    potential_revenue               DOUBLE,
    potential_revenue_eur           DOUBLE,

    virtual_price_eur               DOUBLE,
    germany_price_eur               DOUBLE,
    us_price_eur                    DOUBLE,
    germany_shipping_eur            DOUBLE,
    us_shipping_eur                 DOUBLE,
    us_tax_eur                      DOUBLE,
    us_duty_eur                     DOUBLE,

    no_recommendation_reason        VARCHAR,
    calculation_version             VARCHAR,
    source_run_id                   VARCHAR,
    generated_at                    TIMESTAMP DEFAULT current_timestamp
);

ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS validation_status VARCHAR;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS confidence_label VARCHAR;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS confidence_score DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS pricing_method VARCHAR;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS price_lower_bound_eur DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS price_upper_bound_eur DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS confidence_reason VARCHAR;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS unique_active_evidence_count INTEGER;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS unique_historical_evidence_count INTEGER;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS active_price_median_eur DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS active_price_iqr_ratio DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS active_price_dispersion DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS active_price_min_eur DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS active_price_max_eur DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS active_price_range_ratio DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS active_price_mad_eur DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS active_outlier_count INTEGER;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS active_duplicate_observation_count INTEGER;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS condition_assumption VARCHAR;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS authenticity_assessment_status VARCHAR;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS active_pricing_caveat VARCHAR;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS historical_price_median_eur DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS historical_price_iqr_ratio DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS historical_active_gap_ratio DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS ask_to_sold_adjustment DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS adjustment_source VARCHAR;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS adjustment_support_count INTEGER;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS adjustment_hierarchy_level VARCHAR;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS turnover_confidence VARCHAR;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS turnover_method VARCHAR;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS sell_time_lower_days INTEGER;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS sell_time_upper_days INTEGER;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS turnover_reason VARCHAR;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS turnover_support_evidence_count INTEGER;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS turnover_support_item_count INTEGER;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS turnover_support_level VARCHAR;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS probability_sell_30d DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS probability_sell_90d DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS potential_revenue_eur DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS germany_shipping_eur DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS us_shipping_eur DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS us_tax_eur DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS us_duty_eur DOUBLE;
ALTER TABLE dashboard_inventory_pricing ADD COLUMN IF NOT EXISTS source_run_id VARCHAR;

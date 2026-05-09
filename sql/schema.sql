-- =============================================================================
-- schema.sql
-- Marketing Analytics Pipeline — Task 3: SQL Schema & Query Layer
-- =============================================================================
--
-- ARCHITECTURE: Star schema
--   Fact tables  : fact_campaign_performance, fact_sales
--   Dimensions   : dim_date, dim_campaign, dim_channel
--   Views        : vw_powerbi_*  (Power BI aggregations)
--                  vw_ai_*       (AI tool flexible queries)
--
-- DATABASE: SQLite (compatible syntax throughout)
--           To migrate to PostgreSQL, replace TEXT → VARCHAR, INTEGER → SERIAL
--           for surrogate keys, and add explicit FOREIGN KEY constraints.
--
-- SOURCE TABLES (loaded by clean_data.py):
--   cleaned_campaigns.db → campaigns_clean
--   cleaned_shopify.db   → shopify_clean
--
-- NOTE: This file is designed to be run inside a single SQLite database that
--       has both source tables attached. The Python loader (load_schema.py)
--       handles attaching both source DBs before executing this file.
-- =============================================================================


-- =============================================================================
-- 0. TEARDOWN  (makes this file safely re-runnable)
-- =============================================================================

DROP VIEW  IF EXISTS vw_ai_flexible_performance;
DROP VIEW  IF EXISTS vw_ai_kpi_summary;
DROP VIEW  IF EXISTS vw_powerbi_monthly_performance;
DROP VIEW  IF EXISTS vw_powerbi_region_performance;
DROP VIEW  IF EXISTS vw_powerbi_campaign_performance;
DROP VIEW  IF EXISTS vw_powerbi_channel_performance;
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS fact_campaign_performance;
DROP TABLE IF EXISTS dim_channel;
DROP TABLE IF EXISTS dim_campaign;
DROP TABLE IF EXISTS dim_date;
-- Drop staging tables loaded by load_schema.py (cleaned up after all inserts)
DROP TABLE IF EXISTS campaigns_clean;
DROP TABLE IF EXISTS shopify_clean;


-- =============================================================================
-- 1. DIMENSION TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- dim_date
-- Purpose: Central time axis shared by both fact tables.
--          Splitting date into calendar parts here means fact tables stay lean
--          and time intelligence (group by quarter, week) never needs a STRFTIME
--          call in every downstream query — it's pre-computed once.
-- -----------------------------------------------------------------------------
CREATE TABLE dim_date (
    date_id     TEXT    PRIMARY KEY,   -- ISO-8601 date string: 'YYYY-MM-DD'
    day_of_week INTEGER NOT NULL,      -- 0 = Sunday … 6 = Saturday (SQLite strftime)
    week        INTEGER NOT NULL,      -- ISO week number 1–53
    month       INTEGER NOT NULL,      -- Calendar month 1–12
    month_name  TEXT    NOT NULL,      -- 'January' … 'December'
    quarter     INTEGER NOT NULL,      -- Fiscal/calendar quarter 1–4
    year        INTEGER NOT NULL       -- Full 4-digit year
);

-- Populate dim_date from every distinct date across both source tables.
-- UNION (not UNION ALL) guarantees uniqueness.
INSERT INTO dim_date (date_id, day_of_week, week, month, month_name, quarter, year)
SELECT
    date_val,
    CAST(strftime('%w', date_val) AS INTEGER),          -- 0=Sun, 6=Sat
    CAST(strftime('%W', date_val) AS INTEGER),          -- ISO week
    CAST(strftime('%m', date_val) AS INTEGER),          -- month number
    CASE CAST(strftime('%m', date_val) AS INTEGER)
        WHEN 1  THEN 'January'   WHEN 2  THEN 'February'
        WHEN 3  THEN 'March'     WHEN 4  THEN 'April'
        WHEN 5  THEN 'May'       WHEN 6  THEN 'June'
        WHEN 7  THEN 'July'      WHEN 8  THEN 'August'
        WHEN 9  THEN 'September' WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'  WHEN 12 THEN 'December'
    END,
    CASE
        WHEN CAST(strftime('%m', date_val) AS INTEGER) BETWEEN 1 AND 3  THEN 1
        WHEN CAST(strftime('%m', date_val) AS INTEGER) BETWEEN 4 AND 6  THEN 2
        WHEN CAST(strftime('%m', date_val) AS INTEGER) BETWEEN 7 AND 9  THEN 3
        ELSE 4
    END,
    CAST(strftime('%Y', date_val) AS INTEGER)
FROM (
    SELECT DISTINCT substr(date, 1, 10) AS date_val FROM campaigns_clean
    UNION
    SELECT DISTINCT substr(date, 1, 10) AS date_val FROM shopify_clean
)
WHERE date_val IS NOT NULL
ORDER BY date_val;


-- -----------------------------------------------------------------------------
-- dim_campaign
-- Purpose: Deduplicated campaign attributes.
--          Surrogate key (campaign_id) insulates fact tables from natural-key
--          changes (e.g., a campaign renamed mid-flight).
-- Grain: one row per unique (brand, campaign_name, status, region) combination.
-- -----------------------------------------------------------------------------
CREATE TABLE dim_campaign (
    campaign_id   INTEGER PRIMARY KEY,   -- Surrogate key (auto-assigned)
    brand         TEXT    NOT NULL,      -- Advertising brand / data source
    campaign_name TEXT    NOT NULL,      -- Platform campaign name
    status        TEXT    NOT NULL,      -- e.g. 'Active', 'Paused', 'Unknown'
    region        TEXT    NOT NULL       -- Geographic target: 'India', 'UAE', etc.
);

INSERT INTO dim_campaign (campaign_id, brand, campaign_name, status, region)
SELECT
    ROW_NUMBER() OVER (ORDER BY brand, campaign_name, region, status),
    brand, campaign_name, status, region
FROM (
    SELECT DISTINCT brand, campaign_name, status, region
    FROM campaigns_clean
)
ORDER BY brand, campaign_name, region, status;


-- -----------------------------------------------------------------------------
-- dim_channel
-- Purpose: Deduplicated Shopify sales channels.
--          Kept separate from dim_campaign because channels belong to the
--          Shopify domain and should not be conflated with ad-platform data.
-- Grain: one row per unique (brand, sales_channel) combination.
-- -----------------------------------------------------------------------------
CREATE TABLE dim_channel (
    channel_id    INTEGER PRIMARY KEY,   -- Surrogate key
    brand         TEXT    NOT NULL,      -- Brand this channel belongs to
    sales_channel TEXT    NOT NULL       -- e.g. 'Online Store', 'Instagram'
);

INSERT INTO dim_channel (channel_id, brand, sales_channel)
SELECT
    ROW_NUMBER() OVER (ORDER BY brand, sales_channel),
    brand, sales_channel
FROM (
    SELECT DISTINCT brand, sales_channel
    FROM shopify_clean
)
ORDER BY brand, sales_channel;


-- =============================================================================
-- 2. FACT TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- fact_campaign_performance
-- Purpose: One row per (date, campaign) reporting grain from the ad platform.
--          All additive measures (spend, impressions, clicks, purchases) live
--          here. Derived ratios (CTR, CPC, CPM, ROAS, ROI) are stored as
--          pre-computed columns to avoid re-division at query time.
-- Grain: one row per (date_id, campaign_id).
-- -----------------------------------------------------------------------------
CREATE TABLE fact_campaign_performance (
    perf_id              INTEGER PRIMARY KEY,  -- Surrogate row key

    -- Foreign keys to dimensions
    date_id              TEXT    NOT NULL REFERENCES dim_date(date_id),
    campaign_id          INTEGER NOT NULL REFERENCES dim_campaign(campaign_id),

    -- Additive measures
    spend_inr            REAL    NOT NULL DEFAULT 0,
    impressions          REAL    NOT NULL DEFAULT 0,
    clicks               REAL    NOT NULL DEFAULT 0,
    purchases            REAL    NOT NULL DEFAULT 0,
    conversion_value_inr REAL    NOT NULL DEFAULT 0,

    -- Pre-computed derived metrics (avoids division at query time)
    -- All division-by-zero cases are already set to 0 by clean_data.py
    ctr_pct              REAL    NOT NULL DEFAULT 0,  -- clicks / impressions × 100
    cpc_inr              REAL    NOT NULL DEFAULT 0,  -- spend / clicks
    cpm_inr              REAL    NOT NULL DEFAULT 0,  -- spend / impressions × 1000
    roas                 REAL    NOT NULL DEFAULT 0,  -- conversion_value / spend
    roi_pct              REAL    NOT NULL DEFAULT 0,  -- (conv_value - spend) / spend × 100

    -- Data quality flags (set by clean_data.py)
    metric_recalc_flag   INTEGER NOT NULL DEFAULT 0,  -- 1 = source inputs were corrected
    is_unusual_spend     INTEGER NOT NULL DEFAULT 0,  -- 1 = Z-score > 2.0 on spend
    is_unusual_cpc       INTEGER NOT NULL DEFAULT 0   -- 1 = Z-score > 2.0 on CPC
);

INSERT INTO fact_campaign_performance (
    date_id, campaign_id,
    spend_inr, impressions, clicks, purchases, conversion_value_inr,
    ctr_pct, cpc_inr, cpm_inr, roas, roi_pct,
    metric_recalc_flag, is_unusual_spend, is_unusual_cpc
)
SELECT
    substr(c.date, 1, 10)   AS date_id,
    d.campaign_id,
    c.spend_inr,
    c.impressions,
    c.clicks,
    c.purchases,
    c.conversion_value_inr,
    c.CTR_pct               AS ctr_pct,
    c.CPC_INR               AS cpc_inr,
    c.CPM_INR               AS cpm_inr,
    c.ROAS                  AS roas,
    c.ROI_pct               AS roi_pct,
    c.metric_recalc_flag,
    c.is_unusual_spend,
    c.is_unusual_cpc
FROM campaigns_clean c
JOIN dim_campaign d
  ON  c.brand         = d.brand
  AND c.campaign_name = d.campaign_name
  AND c.status        = d.status
  AND c.region        = d.region;


-- -----------------------------------------------------------------------------
-- fact_sales
-- Purpose: Shopify order-level sales data.
--          Linked to dim_date and dim_channel so sales can be sliced by time
--          and channel independently, without touching campaign data.
-- Grain: one row per (date, order_id, brand, channel, region).
-- -----------------------------------------------------------------------------
CREATE TABLE fact_sales (
    sale_id         INTEGER PRIMARY KEY,  -- Surrogate row key

    -- Foreign keys
    date_id         TEXT    NOT NULL REFERENCES dim_date(date_id),
    channel_id      INTEGER NOT NULL REFERENCES dim_channel(channel_id),

    -- Natural key preserved for traceability
    order_id        TEXT    NOT NULL,

    -- Descriptive attributes kept on fact for direct Power BI filtering
    brand           TEXT    NOT NULL,
    region          TEXT    NOT NULL,

    -- Additive measures
    total_sales_inr REAL    NOT NULL DEFAULT 0,
    returns_inr     REAL    NOT NULL DEFAULT 0,
    total_orders    REAL    NOT NULL DEFAULT 0,

    -- Derived: net revenue removes returns
    net_revenue_inr REAL    GENERATED ALWAYS AS (total_sales_inr - returns_inr) STORED
);

INSERT INTO fact_sales (
    date_id, channel_id, order_id, brand, region,
    total_sales_inr, returns_inr, total_orders
)
SELECT
    substr(s.date, 1, 10)   AS date_id,
    ch.channel_id,
    s.order_id,
    s.brand,
    s.region,
    s.total_sales_inr,
    s.returns_inr,
    s.total_orders
FROM shopify_clean s
JOIN dim_channel ch
  ON s.brand         = ch.brand
 AND s.sales_channel = ch.sales_channel;


-- =============================================================================
-- 3. INDEXES
-- =============================================================================
--
-- Index strategy: index every column that appears in WHERE, JOIN ON, or GROUP BY
-- in the views below. SQLite uses B-tree indexes; each index trades write speed
-- for read speed. We accept that cost because this is an analytics (read-heavy)
-- workload where inserts happen once per pipeline run, not continuously.
-- -----------------------------------------------------------------------------

-- fact_campaign_performance indexes
-- date_id: every Power BI and AI query filters or groups by date / month / quarter.
CREATE INDEX idx_fcp_date       ON fact_campaign_performance (date_id);

-- campaign_id: the join key from fact to dim_campaign in every query.
CREATE INDEX idx_fcp_campaign   ON fact_campaign_performance (campaign_id);

-- Composite (date_id, campaign_id): the most common query pattern is
-- "performance for campaign X over date range Y" — covering index avoids
-- a second table lookup after the date scan.
CREATE INDEX idx_fcp_date_camp  ON fact_campaign_performance (date_id, campaign_id);

-- fact_sales indexes
-- date_id: time-based slicing is the primary filter on sales queries.
CREATE INDEX idx_fs_date        ON fact_sales (date_id);

-- channel_id: join key to dim_channel; also used in channel breakdown queries.
CREATE INDEX idx_fs_channel     ON fact_sales (channel_id);

-- brand: Power BI page 1 slices all KPIs by brand; this avoids a full table scan.
CREATE INDEX idx_fs_brand       ON fact_sales (brand);

-- dim_date indexes
-- (month, year): the Power BI monthly-performance view groups here constantly.
CREATE INDEX idx_dd_month_year  ON dim_date (month, year);

-- quarter, year: for quarterly roll-ups in the AI tool.
CREATE INDEX idx_dd_quarter     ON dim_date (quarter, year);

-- dim_campaign indexes
-- brand: frequent filter — "show me all campaigns for Brand A".
CREATE INDEX idx_dc_brand       ON dim_campaign (brand);

-- region: geographic slicing is a core Power BI slicer.
CREATE INDEX idx_dc_region      ON dim_campaign (region);


-- =============================================================================
-- 4. POWER BI VIEWS
-- =============================================================================
--
-- These views are the ONLY data source Power BI should connect to.
-- Never expose raw fact or staging tables to the report layer.
-- Each view is materialised at query time; no intermediate temp tables needed.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- vw_powerbi_monthly_performance
-- Purpose: Page 2 (Spend & Channel Trends) — monthly aggregates by brand.
--          Groups spend, impressions, clicks, revenue, and ROAS by month/year
--          so Power BI line charts always work with pre-aggregated grain.
-- -----------------------------------------------------------------------------
CREATE VIEW vw_powerbi_monthly_performance AS
SELECT
    dd.year,
    dd.quarter,
    dd.month,
    dd.month_name,
    dc.brand,
    dc.region,

    -- Ad platform metrics (summed)
    SUM(fcp.spend_inr)            AS total_spend_inr,
    SUM(fcp.impressions)          AS total_impressions,
    SUM(fcp.clicks)               AS total_clicks,
    SUM(fcp.purchases)            AS total_ad_purchases,
    SUM(fcp.conversion_value_inr) AS total_conversion_value_inr,

    -- Shopify revenue (LEFT JOIN: not every campaign date has a sale)
    COALESCE(SUM(fs_agg.total_sales_inr), 0)  AS total_shopify_revenue_inr,
    COALESCE(SUM(fs_agg.returns_inr), 0)      AS total_returns_inr,
    COALESCE(SUM(fs_agg.net_revenue_inr), 0)  AS total_net_revenue_inr,
    COALESCE(SUM(fs_agg.total_orders), 0)     AS total_shopify_orders,

    -- Re-derived ratios at the aggregated grain
    -- (pre-computed per-row ratios must NOT be averaged — they must be recalculated)
    ROUND(
        100.0 * SUM(fcp.clicks) / NULLIF(SUM(fcp.impressions), 0), 4
    )                                         AS agg_ctr_pct,
    ROUND(
        SUM(fcp.spend_inr) / NULLIF(SUM(fcp.clicks), 0), 2
    )                                         AS agg_cpc_inr,
    ROUND(
        1000.0 * SUM(fcp.spend_inr) / NULLIF(SUM(fcp.impressions), 0), 2
    )                                         AS agg_cpm_inr,
    ROUND(
        SUM(fcp.conversion_value_inr) / NULLIF(SUM(fcp.spend_inr), 0), 4
    )                                         AS agg_roas,
    ROUND(
        100.0 * (SUM(fcp.conversion_value_inr) - SUM(fcp.spend_inr))
              / NULLIF(SUM(fcp.spend_inr), 0), 2
    )                                         AS agg_roi_pct

FROM fact_campaign_performance fcp
JOIN dim_date     dd ON fcp.date_id     = dd.date_id
JOIN dim_campaign dc ON fcp.campaign_id = dc.campaign_id
LEFT JOIN (
    -- Pre-aggregate Shopify to brand + date level before joining
    -- to avoid row multiplication when one campaign date has many orders
    SELECT
        date_id,
        brand,
        SUM(total_sales_inr)  AS total_sales_inr,
        SUM(returns_inr)      AS returns_inr,
        SUM(net_revenue_inr)  AS net_revenue_inr,
        SUM(total_orders)     AS total_orders
    FROM fact_sales
    GROUP BY date_id, brand
) fs_agg ON fcp.date_id = fs_agg.date_id AND dc.brand = fs_agg.brand

GROUP BY
    dd.year, dd.quarter, dd.month, dd.month_name,
    dc.brand, dc.region;


-- -----------------------------------------------------------------------------
-- vw_powerbi_campaign_performance
-- Purpose: Page 1 (Campaign Overview) — one row per campaign with totals.
--          Used by campaign-level bar charts, tables, and KPI cards.
-- -----------------------------------------------------------------------------
CREATE VIEW vw_powerbi_campaign_performance AS
SELECT
    dc.brand,
    dc.campaign_name,
    dc.status,
    dc.region,

    MIN(fcp.date_id)              AS first_seen_date,
    MAX(fcp.date_id)              AS last_seen_date,

    SUM(fcp.spend_inr)            AS total_spend_inr,
    SUM(fcp.impressions)          AS total_impressions,
    SUM(fcp.clicks)               AS total_clicks,
    SUM(fcp.purchases)            AS total_purchases,
    SUM(fcp.conversion_value_inr) AS total_conversion_value_inr,

    ROUND(
        100.0 * SUM(fcp.clicks) / NULLIF(SUM(fcp.impressions), 0), 4
    )                             AS agg_ctr_pct,
    ROUND(
        SUM(fcp.spend_inr) / NULLIF(SUM(fcp.clicks), 0), 2
    )                             AS agg_cpc_inr,
    ROUND(
        1000.0 * SUM(fcp.spend_inr) / NULLIF(SUM(fcp.impressions), 0), 2
    )                             AS agg_cpm_inr,
    ROUND(
        SUM(fcp.conversion_value_inr) / NULLIF(SUM(fcp.spend_inr), 0), 4
    )                             AS agg_roas,
    ROUND(
        100.0 * (SUM(fcp.conversion_value_inr) - SUM(fcp.spend_inr))
              / NULLIF(SUM(fcp.spend_inr), 0), 2
    )                             AS agg_roi_pct,

    -- Anomaly summary: how many days did this campaign flag unusual spend/CPC?
    SUM(fcp.is_unusual_spend)     AS days_unusual_spend,
    SUM(fcp.is_unusual_cpc)       AS days_unusual_cpc

FROM fact_campaign_performance fcp
JOIN dim_campaign dc ON fcp.campaign_id = dc.campaign_id

GROUP BY
    dc.brand, dc.campaign_name, dc.status, dc.region;


-- -----------------------------------------------------------------------------
-- vw_powerbi_region_performance
-- Purpose: Page 3 (Conversion & ROI) — geographic breakdown.
--          Feeds map visuals and regional KPI tables.
-- -----------------------------------------------------------------------------
CREATE VIEW vw_powerbi_region_performance AS
SELECT
    dd.year,
    dd.month,
    dd.month_name,
    dc.brand,
    dc.region,

    SUM(fcp.spend_inr)            AS total_spend_inr,
    SUM(fcp.impressions)          AS total_impressions,
    SUM(fcp.clicks)               AS total_clicks,
    SUM(fcp.purchases)            AS total_purchases,
    SUM(fcp.conversion_value_inr) AS total_conversion_value_inr,

    ROUND(
        SUM(fcp.conversion_value_inr) / NULLIF(SUM(fcp.spend_inr), 0), 4
    )                             AS agg_roas,
    ROUND(
        100.0 * (SUM(fcp.conversion_value_inr) - SUM(fcp.spend_inr))
              / NULLIF(SUM(fcp.spend_inr), 0), 2
    )                             AS agg_roi_pct

FROM fact_campaign_performance fcp
JOIN dim_date     dd ON fcp.date_id     = dd.date_id
JOIN dim_campaign dc ON fcp.campaign_id = dc.campaign_id

GROUP BY dd.year, dd.month, dd.month_name, dc.brand, dc.region;


-- -----------------------------------------------------------------------------
-- vw_powerbi_channel_performance
-- Purpose: Shopify channel breakdown — revenue and orders by sales channel.
--          Used in the sales/channel breakdown section of the dashboard.
-- -----------------------------------------------------------------------------
CREATE VIEW vw_powerbi_channel_performance AS
SELECT
    dd.year,
    dd.month,
    dd.month_name,
    fs.brand,
    ch.sales_channel,
    fs.region,

    SUM(fs.total_sales_inr)  AS total_sales_inr,
    SUM(fs.returns_inr)      AS total_returns_inr,
    SUM(fs.net_revenue_inr)  AS total_net_revenue_inr,
    SUM(fs.total_orders)     AS total_orders,

    ROUND(
        SUM(fs.total_sales_inr) / NULLIF(SUM(fs.total_orders), 0), 2
    )                        AS avg_order_value_inr

FROM fact_sales     fs
JOIN dim_date       dd ON fs.date_id    = dd.date_id
JOIN dim_channel    ch ON fs.channel_id = ch.channel_id

GROUP BY dd.year, dd.month, dd.month_name, fs.brand, ch.sales_channel, fs.region;


-- =============================================================================
-- 5. AI TOOL VIEWS
-- =============================================================================
--
-- The AI tool queries these views directly. They are designed to be flexible:
-- every useful filter dimension (date range, brand, region, campaign) is exposed
-- as a column so the AI can append a WHERE clause without knowing the schema.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- vw_ai_flexible_performance
-- Purpose: Row-level daily view that the AI tool can filter freely.
--          Deliberately wide — exposes all dimensions and metrics on one row
--          so the AI never needs to write a JOIN itself.
--
-- Example AI queries against this view:
--   WHERE brand = 'Brand A' AND date_id BETWEEN '2026-01-01' AND '2026-01-31'
--   WHERE region = 'India' AND agg_roas > 2.0
--   WHERE campaign_name LIKE '%TOF%' AND month = 2
-- -----------------------------------------------------------------------------
CREATE VIEW vw_ai_flexible_performance AS
SELECT
    -- Time dimensions (all exposed for flexible filtering)
    fcp.date_id,
    dd.day_of_week,
    dd.week,
    dd.month,
    dd.month_name,
    dd.quarter,
    dd.year,

    -- Campaign dimensions
    dc.brand,
    dc.campaign_name,
    dc.status          AS campaign_status,
    dc.region,

    -- Ad platform metrics (daily grain)
    fcp.spend_inr,
    fcp.impressions,
    fcp.clicks,
    fcp.purchases,
    fcp.conversion_value_inr,
    fcp.ctr_pct,
    fcp.cpc_inr,
    fcp.cpm_inr,
    fcp.roas,
    fcp.roi_pct,

    -- Data quality context
    fcp.metric_recalc_flag,
    fcp.is_unusual_spend,
    fcp.is_unusual_cpc,

    -- Shopify sales for the same brand + day (may be NULL if no Shopify data)
    fs_day.total_sales_inr      AS shopify_sales_inr,
    fs_day.returns_inr          AS shopify_returns_inr,
    fs_day.net_revenue_inr      AS shopify_net_revenue_inr,
    fs_day.total_orders         AS shopify_orders,
    fs_day.avg_order_value_inr  AS shopify_avg_order_value_inr

FROM fact_campaign_performance fcp
JOIN dim_date     dd  ON fcp.date_id     = dd.date_id
JOIN dim_campaign dc  ON fcp.campaign_id = dc.campaign_id
LEFT JOIN (
    -- Shopify aggregated to brand + date to prevent fan-out
    SELECT
        date_id,
        brand,
        SUM(total_sales_inr)                                     AS total_sales_inr,
        SUM(returns_inr)                                         AS returns_inr,
        SUM(net_revenue_inr)                                     AS net_revenue_inr,
        SUM(total_orders)                                        AS total_orders,
        ROUND(SUM(total_sales_inr) / NULLIF(SUM(total_orders), 0), 2) AS avg_order_value_inr
    FROM fact_sales
    GROUP BY date_id, brand
) fs_day ON fcp.date_id = fs_day.date_id AND dc.brand = fs_day.brand;


-- -----------------------------------------------------------------------------
-- vw_ai_kpi_summary
-- Purpose: Pre-aggregated KPI rollup for the AI tool's quick-answer queries.
--          When the user asks "what is Brand A's total ROAS?" the AI should
--          query this view, not scan the full fact table.
-- Grain: one row per (brand, region, month, year).
-- -----------------------------------------------------------------------------
CREATE VIEW vw_ai_kpi_summary AS
SELECT
    dc.brand,
    dc.region,
    dd.year,
    dd.quarter,
    dd.month,
    dd.month_name,

    COUNT(DISTINCT dc.campaign_name)      AS active_campaigns,

    SUM(fcp.spend_inr)                    AS total_spend_inr,
    SUM(fcp.impressions)                  AS total_impressions,
    SUM(fcp.clicks)                       AS total_clicks,
    SUM(fcp.purchases)                    AS total_purchases,
    SUM(fcp.conversion_value_inr)         AS total_conversion_value_inr,

    ROUND(
        100.0 * SUM(fcp.clicks) / NULLIF(SUM(fcp.impressions), 0), 4
    )                                     AS agg_ctr_pct,
    ROUND(
        SUM(fcp.spend_inr) / NULLIF(SUM(fcp.clicks), 0), 2
    )                                     AS agg_cpc_inr,
    ROUND(
        1000.0 * SUM(fcp.spend_inr) / NULLIF(SUM(fcp.impressions), 0), 2
    )                                     AS agg_cpm_inr,
    ROUND(
        SUM(fcp.conversion_value_inr) / NULLIF(SUM(fcp.spend_inr), 0), 4
    )                                     AS agg_roas,
    ROUND(
        100.0 * (SUM(fcp.conversion_value_inr) - SUM(fcp.spend_inr))
              / NULLIF(SUM(fcp.spend_inr), 0), 2
    )                                     AS agg_roi_pct,

    -- Shopify KPIs for the same brand + period
    COALESCE(SUM(fs_agg.total_sales_inr), 0)   AS shopify_revenue_inr,
    COALESCE(SUM(fs_agg.returns_inr), 0)        AS shopify_returns_inr,
    COALESCE(SUM(fs_agg.net_revenue_inr), 0)    AS shopify_net_revenue_inr,
    COALESCE(SUM(fs_agg.total_orders), 0)       AS shopify_total_orders

FROM fact_campaign_performance fcp
JOIN dim_date     dd  ON fcp.date_id     = dd.date_id
JOIN dim_campaign dc  ON fcp.campaign_id = dc.campaign_id
LEFT JOIN (
    SELECT
        date_id, brand,
        SUM(total_sales_inr)  AS total_sales_inr,
        SUM(returns_inr)      AS returns_inr,
        SUM(net_revenue_inr)  AS net_revenue_inr,
        SUM(total_orders)     AS total_orders
    FROM fact_sales
    GROUP BY date_id, brand
) fs_agg ON fcp.date_id = fs_agg.date_id AND dc.brand = fs_agg.brand

GROUP BY
    dc.brand, dc.region,
    dd.year, dd.quarter, dd.month, dd.month_name;


-- =============================================================================
-- END OF SCHEMA
-- =============================================================================

-- Clean up staging source tables — they are no longer needed once all
-- fact/dimension tables are populated. The warehouse only retains star schema
-- objects. Source data lives in cleaned_campaigns.db and cleaned_shopify.db.
DROP TABLE IF EXISTS campaigns_clean;
DROP TABLE IF EXISTS shopify_clean;

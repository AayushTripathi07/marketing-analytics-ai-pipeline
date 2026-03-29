-- Drop existing tables to allow rerunnability
DROP TABLE IF EXISTS fact_campaign_performance;
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_campaign;
DROP TABLE IF EXISTS dim_date;
DROP VIEW IF EXISTS vw_daily_brand_summary;
DROP VIEW IF EXISTS vw_overall_performance;

-- ==========================================
-- DIMENSION TABLES
-- ==========================================

-- 1. Date Dimension
CREATE TABLE dim_date AS
SELECT DISTINCT date
FROM (
    SELECT date FROM campaigns_clean
    UNION
    SELECT date FROM shopify_clean
)
WHERE date IS NOT NULL;

-- 2. Campaign Dimension
CREATE TABLE dim_campaign AS
SELECT 
    ROW_NUMBER() OVER(ORDER BY brand, campaign_name, region) as campaign_id,
    brand,
    campaign_name,
    status,
    region
FROM (
    SELECT DISTINCT brand, campaign_name, status, region
    FROM campaigns_clean
);


-- ==========================================
-- FACT TABLES
-- ==========================================

-- 3. Fact Campaign Performance
CREATE TABLE fact_campaign_performance AS
SELECT 
    c.date,
    d.campaign_id,
    c.brand,
    c.campaign_name,
    c.region,
    c.status,
    c.spend_inr,
    c.impressions,
    c.clicks,
    c.purchases,
    c.conversion_value_inr,
    c.CTR_Pct as ctr_pct,
    c.CPC_INR as cpc_inr,
    c.CPM_INR as cpm_inr,
    c.ROAS as roas,
    c.is_unusual_spend,
    c.is_unusual_cpc
FROM campaigns_clean c
JOIN dim_campaign d 
  ON c.brand = d.brand 
 AND c.campaign_name = d.campaign_name 
 AND c.region = d.region
 AND c.status = d.status;

-- 4. Fact Sales
CREATE TABLE fact_sales AS
SELECT 
    date,
    order_id,
    brand,
    sales_channel,
    region,
    total_sales_inr,
    returns_inr,
    total_orders
FROM shopify_clean;

-- ==========================================
-- AI/REPORTING VIEWS
-- ==========================================

-- A consolidated view of daily performance per brand combining Ads & Sales
CREATE VIEW vw_daily_brand_summary AS
SELECT 
    COALESCE(c.date, s.date) as date,
    COALESCE(c.brand, s.brand) as brand,
    COALESCE(c.daily_spend, 0) as ad_spend_inr,
    COALESCE(c.daily_impressions, 0) as impressions,
    COALESCE(c.daily_clicks, 0) as clicks,
    COALESCE(c.daily_ad_purchases, 0) as ad_conversions,
    COALESCE(s.daily_sales, 0) as shopify_revenue_inr,
    COALESCE(s.daily_returns, 0) as shopify_returns_inr,
    COALESCE(s.daily_orders, 0) as shopify_orders
FROM (
    -- Aggregate campaigns per brand per day
    SELECT 
        fcp.date,
        dc.brand,
        SUM(fcp.spend_inr) as daily_spend,
        SUM(fcp.impressions) as daily_impressions,
        SUM(fcp.clicks) as daily_clicks,
        SUM(fcp.purchases) as daily_ad_purchases
    FROM fact_campaign_performance fcp
    JOIN dim_campaign dc ON fcp.campaign_id = dc.campaign_id
    GROUP BY fcp.date, dc.brand
) c
FULL OUTER JOIN (
    -- Aggregate sales per brand per day
    SELECT 
        date,
        brand,
        SUM(total_sales_inr) as daily_sales,
        SUM(returns_inr) as daily_returns,
        SUM(total_orders) as daily_orders
    FROM fact_sales
    GROUP BY date, brand
) s ON c.date = s.date AND c.brand = s.brand;

-- High-level overall KPI view for AI quick answers
CREATE VIEW vw_overall_performance AS
SELECT 
    brand,
    SUM(ad_spend_inr) as total_ad_spend,
    SUM(shopify_revenue_inr) as total_revenue,
    SUM(shopify_orders) as total_orders,
    SUM(ad_spend_inr) / NULLIF(SUM(clicks), 0) as average_cpc,
    SUM(shopify_revenue_inr) / NULLIF(SUM(ad_spend_inr), 0) as combined_roas
FROM vw_daily_brand_summary
GROUP BY brand;

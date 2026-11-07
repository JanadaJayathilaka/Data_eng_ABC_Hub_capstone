-- ============================================================================
-- PulseTrack: Data Mart Schema, Dimensions, Facts & Business KPI Views
-- ============================================================================
-- Target Schema: pulsetrack_dm
-- Architecture: Kimball Multi-Fact Star Schema supporting all Question 1 KPIs
-- Analytical Areas Supported:
--   1. Advertiser Performance (impressions, clicks, CTR, conversions, CVR, revenue, rev/conv, rev/click)
--   2. Campaign Performance (impressions, clicks, conversions, CVR, revenue, adjusted revenue, campaign ranking)
--   3. Creative Performance (impressions, clicks, CTR, conversions, CVR)
--   4. Partner Performance (clicks, conversions, attributed conversions, attributed revenue, attribution %)
--   5. Account Manager Performance (conversions, revenue, advertiser & campaign portfolio performance)
--   6. Time Analysis (Year, Quarter, Month, Day, MoM, QoQ, YoY, Top-N, % contribution, trend)
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS pulsetrack_dm;

-- ----------------------------------------------------------------------------
-- 1. Conformed Dimension Views (Zero storage overhead, 100% consistent with Gold)
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW pulsetrack_dm.dim_date AS
SELECT * FROM pulsetrack_gold.dim_date;

CREATE OR REPLACE VIEW pulsetrack_dm.dim_advertiser AS
SELECT * FROM pulsetrack_gold.dim_advertiser;

CREATE OR REPLACE VIEW pulsetrack_dm.dim_campaign AS
SELECT * FROM pulsetrack_gold.dim_campaign;

CREATE OR REPLACE VIEW pulsetrack_dm.dim_creative AS
SELECT * FROM pulsetrack_gold.dim_creative;

CREATE OR REPLACE VIEW pulsetrack_dm.dim_partner AS
SELECT * FROM pulsetrack_gold.dim_partner;

CREATE OR REPLACE VIEW pulsetrack_dm.dim_account_manager AS
SELECT * FROM pulsetrack_gold.dim_account_manager;

CREATE OR REPLACE VIEW pulsetrack_dm.dim_offer AS
SELECT * FROM pulsetrack_gold.dim_offer;

CREATE OR REPLACE VIEW pulsetrack_dm.dim_geo AS
SELECT * FROM pulsetrack_gold.dim_geo;

CREATE OR REPLACE VIEW pulsetrack_dm.dim_device AS
SELECT * FROM pulsetrack_gold.dim_device;


-- ----------------------------------------------------------------------------
-- 2. Data Mart Fact Tables
-- ----------------------------------------------------------------------------

-- Fact 1: Unified Daily Marketing Funnel Performance
-- Grain: One row per Date × Advertiser × Offer × Campaign × Creative × Partner × AM × Geo × Device
CREATE TABLE IF NOT EXISTS pulsetrack_dm.dm_fact_marketing_performance_daily (
    marketing_fact_key  BIGSERIAL PRIMARY KEY,
    date_key            INTEGER NOT NULL REFERENCES pulsetrack_gold.dim_date(date_key),
    advertiser_key      INTEGER NOT NULL REFERENCES pulsetrack_gold.dim_advertiser(advertiser_key),
    offer_key           INTEGER REFERENCES pulsetrack_gold.dim_offer(offer_key),
    campaign_key        INTEGER NOT NULL REFERENCES pulsetrack_gold.dim_campaign(campaign_key),
    creative_key        INTEGER REFERENCES pulsetrack_gold.dim_creative(creative_key),
    partner_key         INTEGER NOT NULL REFERENCES pulsetrack_gold.dim_partner(partner_key),
    account_manager_key INTEGER REFERENCES pulsetrack_gold.dim_account_manager(account_manager_key),
    geo_key             INTEGER REFERENCES pulsetrack_gold.dim_geo(geo_key),
    device_key          INTEGER REFERENCES pulsetrack_gold.dim_device(device_key),

    -- Additive Base Measures
    impressions         INTEGER NOT NULL DEFAULT 0,
    clicks              INTEGER NOT NULL DEFAULT 0,
    conversions         INTEGER NOT NULL DEFAULT 0,
    revenue             NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    adjusted_revenue    NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    payout              NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    margin              NUMERIC(14, 2) NOT NULL DEFAULT 0.00,

    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dm_mkt_date ON pulsetrack_dm.dm_fact_marketing_performance_daily(date_key);
CREATE INDEX IF NOT EXISTS idx_dm_mkt_adv ON pulsetrack_dm.dm_fact_marketing_performance_daily(advertiser_key);
CREATE INDEX IF NOT EXISTS idx_dm_mkt_camp ON pulsetrack_dm.dm_fact_marketing_performance_daily(campaign_key);
CREATE INDEX IF NOT EXISTS idx_dm_mkt_cr ON pulsetrack_dm.dm_fact_marketing_performance_daily(creative_key);
CREATE INDEX IF NOT EXISTS idx_dm_mkt_part ON pulsetrack_dm.dm_fact_marketing_performance_daily(partner_key);
CREATE INDEX IF NOT EXISTS idx_dm_mkt_am ON pulsetrack_dm.dm_fact_marketing_performance_daily(account_manager_key);


-- Fact 2: Partner Multi-Touch Attribution Summary
-- Grain: One row per Date × Partner × Campaign
CREATE TABLE IF NOT EXISTS pulsetrack_dm.dm_fact_partner_attribution_daily (
    partner_fact_key        BIGSERIAL PRIMARY KEY,
    date_key                INTEGER NOT NULL REFERENCES pulsetrack_gold.dim_date(date_key),
    partner_key             INTEGER NOT NULL REFERENCES pulsetrack_gold.dim_partner(partner_key),
    campaign_key            INTEGER NOT NULL REFERENCES pulsetrack_gold.dim_campaign(campaign_key),

    -- Attribution Measures
    total_clicks            INTEGER NOT NULL DEFAULT 0,
    direct_conversions      INTEGER NOT NULL DEFAULT 0,
    attributed_conversions  NUMERIC(10, 4) NOT NULL DEFAULT 0.0000,
    attributed_revenue      NUMERIC(14, 2) NOT NULL DEFAULT 0.00,

    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dm_part_date ON pulsetrack_dm.dm_fact_partner_attribution_daily(date_key);
CREATE INDEX IF NOT EXISTS idx_dm_part_partner ON pulsetrack_dm.dm_fact_partner_attribution_daily(partner_key);
CREATE INDEX IF NOT EXISTS idx_dm_part_campaign ON pulsetrack_dm.dm_fact_partner_attribution_daily(campaign_key);


-- Fact 3: Account Manager Portfolio Performance
-- Grain: One row per Date × Account Manager × Advertiser
CREATE TABLE IF NOT EXISTS pulsetrack_dm.dm_fact_account_manager_performance_daily (
    am_fact_key             BIGSERIAL PRIMARY KEY,
    date_key                INTEGER NOT NULL REFERENCES pulsetrack_gold.dim_date(date_key),
    account_manager_key     INTEGER NOT NULL REFERENCES pulsetrack_gold.dim_account_manager(account_manager_key),
    advertiser_key          INTEGER NOT NULL REFERENCES pulsetrack_gold.dim_advertiser(advertiser_key),

    -- AM Performance Measures
    conversions             INTEGER NOT NULL DEFAULT 0,
    revenue                 NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    adjusted_revenue        NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    payout                  NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    margin                  NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    active_campaigns_count  INTEGER NOT NULL DEFAULT 0,

    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dm_am_date ON pulsetrack_dm.dm_fact_account_manager_performance_daily(date_key);
CREATE INDEX IF NOT EXISTS idx_dm_am_am ON pulsetrack_dm.dm_fact_account_manager_performance_daily(account_manager_key);
CREATE INDEX IF NOT EXISTS idx_dm_am_adv ON pulsetrack_dm.dm_fact_account_manager_performance_daily(advertiser_key);


-- ----------------------------------------------------------------------------
-- 3. Business Presentation Views (All Question 1 KPI Specifications)
-- ----------------------------------------------------------------------------

-- View 1: Advertiser Performance
CREATE OR REPLACE VIEW pulsetrack_dm.vw_advertiser_performance AS
SELECT
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.full_date,
    a.advertiser_id,
    a.advertiser_name,
    SUM(f.impressions)      AS impressions,
    SUM(f.clicks)           AS clicks,
    ROUND(SUM(f.clicks)::numeric / NULLIF(SUM(f.impressions), 0) * 100, 2) AS ctr,
    SUM(f.conversions)      AS conversions,
    ROUND(SUM(f.conversions)::numeric / NULLIF(SUM(f.clicks), 0) * 100, 2) AS conversion_rate,
    SUM(f.revenue)          AS revenue,
    SUM(f.adjusted_revenue) AS adjusted_revenue,
    ROUND(SUM(f.revenue) / NULLIF(SUM(f.conversions), 0), 2) AS revenue_per_conversion,
    ROUND(SUM(f.revenue) / NULLIF(SUM(f.clicks), 0), 2)      AS revenue_per_click
FROM pulsetrack_dm.dm_fact_marketing_performance_daily f
JOIN pulsetrack_gold.dim_date d ON f.date_key = d.date_key
JOIN pulsetrack_gold.dim_advertiser a ON f.advertiser_key = a.advertiser_key
GROUP BY d.year, d.quarter, d.month, d.month_name, d.full_date, a.advertiser_id, a.advertiser_name;


-- View 2: Campaign Performance
CREATE OR REPLACE VIEW pulsetrack_dm.vw_campaign_performance AS
SELECT
    d.year,
    d.quarter,
    d.month,
    d.full_date,
    c.campaign_id,
    c.campaign_name,
    SUM(f.impressions)      AS impressions,
    SUM(f.clicks)           AS clicks,
    SUM(f.conversions)      AS conversions,
    ROUND(SUM(f.conversions)::numeric / NULLIF(SUM(f.clicks), 0) * 100, 2) AS conversion_rate,
    SUM(f.revenue)          AS revenue,
    SUM(f.adjusted_revenue) AS adjusted_revenue,
    DENSE_RANK() OVER (PARTITION BY d.year, d.month ORDER BY SUM(f.revenue) DESC) AS campaign_ranking_by_revenue,
    DENSE_RANK() OVER (PARTITION BY d.year, d.month ORDER BY SUM(f.adjusted_revenue) DESC) AS campaign_ranking_by_adjusted_revenue
FROM pulsetrack_dm.dm_fact_marketing_performance_daily f
JOIN pulsetrack_gold.dim_date d ON f.date_key = d.date_key
JOIN pulsetrack_gold.dim_campaign c ON f.campaign_key = c.campaign_key
GROUP BY d.year, d.quarter, d.month, d.full_date, c.campaign_id, c.campaign_name;


-- View 3: Creative Performance
CREATE OR REPLACE VIEW pulsetrack_dm.vw_creative_performance AS
SELECT
    d.year,
    d.month,
    cr.creative_id,
    cr.creative_name,
    cr.creative_type,
    c.campaign_name,
    SUM(f.impressions) AS impressions,
    SUM(f.clicks)      AS clicks,
    ROUND(SUM(f.clicks)::numeric / NULLIF(SUM(f.impressions), 0) * 100, 2) AS ctr,
    SUM(f.conversions) AS conversions,
    ROUND(SUM(f.conversions)::numeric / NULLIF(SUM(f.clicks), 0) * 100, 2) AS conversion_rate
FROM pulsetrack_dm.dm_fact_marketing_performance_daily f
JOIN pulsetrack_gold.dim_date d ON f.date_key = d.date_key
JOIN pulsetrack_gold.dim_creative cr ON f.creative_key = cr.creative_key
JOIN pulsetrack_gold.dim_campaign c ON f.campaign_key = c.campaign_key
GROUP BY d.year, d.month, cr.creative_id, cr.creative_name, cr.creative_type, c.campaign_name;


-- View 4: Partner Performance & Attribution
CREATE OR REPLACE VIEW pulsetrack_dm.vw_partner_performance AS
WITH platform_total AS (
    SELECT date_key, SUM(revenue) AS total_platform_rev
    FROM pulsetrack_dm.dm_fact_marketing_performance_daily
    GROUP BY date_key
)
SELECT
    d.year,
    d.month,
    d.full_date,
    p.partner_id,
    p.partner_name,
    c.campaign_name,
    f.total_clicks            AS clicks,
    f.direct_conversions      AS conversions,
    f.attributed_conversions  AS attributed_conversions,
    f.attributed_revenue      AS attributed_revenue,
    ROUND(f.attributed_revenue / NULLIF(pt.total_platform_rev, 0) * 100, 2) AS attribution_percentage
FROM pulsetrack_dm.dm_fact_partner_attribution_daily f
JOIN pulsetrack_gold.dim_date d ON f.date_key = d.date_key
JOIN pulsetrack_gold.dim_partner p ON f.partner_key = p.partner_key
JOIN pulsetrack_gold.dim_campaign c ON f.campaign_key = c.campaign_key
LEFT JOIN platform_total pt ON f.date_key = pt.date_key;


-- View 5: Account Manager Performance
CREATE OR REPLACE VIEW pulsetrack_dm.vw_account_manager_performance AS
SELECT
    d.year,
    d.month,
    am.am_id,
    am.am_name,
    a.advertiser_name,
    f.conversions,
    f.revenue,
    f.adjusted_revenue,
    f.margin,
    f.active_campaigns_count
FROM pulsetrack_dm.dm_fact_account_manager_performance_daily f
JOIN pulsetrack_gold.dim_date d ON f.date_key = d.date_key
JOIN pulsetrack_gold.dim_account_manager am ON f.account_manager_key = am.account_manager_key
JOIN pulsetrack_gold.dim_advertiser a ON f.advertiser_key = a.advertiser_key;


-- View 6: Time-Based Comparative Trends (MoM, QoQ, YoY, Top-N, Contribution)
CREATE OR REPLACE VIEW pulsetrack_dm.vw_time_trend_analysis AS
WITH monthly_summary AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        a.advertiser_name,
        SUM(f.revenue)          AS monthly_revenue,
        SUM(f.adjusted_revenue) AS monthly_adjusted_revenue,
        SUM(f.conversions)      AS monthly_conversions
    FROM pulsetrack_dm.dm_fact_marketing_performance_daily f
    JOIN pulsetrack_gold.dim_date d ON f.date_key = d.date_key
    JOIN pulsetrack_gold.dim_advertiser a ON f.advertiser_key = a.advertiser_key
    GROUP BY d.year, d.month, d.month_name, a.advertiser_name
),
monthly_metrics AS (
    SELECT
        ms.*,
        SUM(ms.monthly_revenue) OVER (PARTITION BY ms.year, ms.month) AS total_monthly_revenue,
        DENSE_RANK() OVER (PARTITION BY ms.year, ms.month ORDER BY ms.monthly_revenue DESC) AS top_n_rank,
        LAG(ms.monthly_revenue, 1) OVER (PARTITION BY ms.advertiser_name ORDER BY ms.year, ms.month) AS prev_month_revenue,
        LAG(ms.monthly_revenue, 3) OVER (PARTITION BY ms.advertiser_name ORDER BY ms.year, ms.month) AS prev_quarter_revenue,
        LAG(ms.monthly_revenue, 12) OVER (PARTITION BY ms.advertiser_name ORDER BY ms.year, ms.month) AS prev_year_revenue
    FROM monthly_summary ms
)
SELECT
    year,
    month,
    month_name,
    advertiser_name,
    monthly_revenue,
    monthly_adjusted_revenue,
    monthly_conversions,
    top_n_rank,
    ROUND(monthly_revenue / NULLIF(total_monthly_revenue, 0) * 100, 2) AS percentage_contribution,
    ROUND(((monthly_revenue - prev_month_revenue) / NULLIF(prev_month_revenue, 0)) * 100, 2) AS mom_growth_pct,
    ROUND(((monthly_revenue - prev_quarter_revenue) / NULLIF(prev_quarter_revenue, 0)) * 100, 2) AS qoq_growth_pct,
    ROUND(((monthly_revenue - prev_year_revenue) / NULLIF(prev_year_revenue, 0)) * 100, 2) AS yoy_growth_pct
FROM monthly_metrics;


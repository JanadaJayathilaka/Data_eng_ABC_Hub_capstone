-- ============================================================================
-- PulseTrack: Data Mart Schema & Advertiser Performance Tables DDL
-- ============================================================================
-- Business Area: Advertiser Performance
-- Target Schema: pulsetrack_dm
-- Supporting KPIs:
--   1. Impressions (Total valid ad impressions)
--   2. Clicks (Total valid clicks)
--   3. CTR (Click-Through Rate = Clicks / Impressions * 100)
--   4. Conversions (Total unique valid conversions)
--   5. Conversion Rate (Conversions / Clicks * 100)
--   6. Revenue (Total conversion revenue)
--   7. Revenue per Conversion (Revenue / Conversions)
--   8. Revenue per Click (Revenue / Clicks)
-- Time Analysis & Trends:
--   Year, Quarter, Month, Day, MoM, QoQ, YoY, Top-N Ranking, Percentage Contribution
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS pulsetrack_dm;

-- ----------------------------------------------------------------------------
-- 1. Conformed Dimensions for Data Mart (Exposed via schema abstraction)
-- ----------------------------------------------------------------------------

-- Conformed Date Dimension View
CREATE OR REPLACE VIEW pulsetrack_dm.dim_date AS
SELECT 
    date_key,
    full_date,
    day,
    day_name,
    week,
    month,
    month_name,
    quarter,
    year
FROM pulsetrack_gold.dim_date;

-- Conformed Advertiser Dimension View (Preserving SCD2 attributes)
CREATE OR REPLACE VIEW pulsetrack_dm.dim_advertiser AS
SELECT 
    advertiser_key,
    advertiser_id,
    advertiser_name,
    account_manager_key,
    effective_from,
    effective_to,
    is_current
FROM pulsetrack_gold.dim_advertiser;

-- Conformed Account Manager Dimension View
CREATE OR REPLACE VIEW pulsetrack_dm.dim_account_manager AS
SELECT 
    account_manager_key,
    am_id,
    am_name,
    effective_from,
    effective_to,
    is_current
FROM pulsetrack_gold.dim_account_manager;


-- ----------------------------------------------------------------------------
-- 2. Aggregated Fact Table: Advertiser Daily Performance
-- ----------------------------------------------------------------------------
-- Grain: One row per Date (Day) + Advertiser + Account Manager at event time.
-- Pre-aggregates atomic fact_impression, fact_click, and fact_conversion.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pulsetrack_dm.dm_fact_advertiser_performance_daily (
    fact_advertiser_key BIGSERIAL PRIMARY KEY,
    date_key            INTEGER NOT NULL REFERENCES pulsetrack_gold.dim_date(date_key),
    advertiser_key      INTEGER NOT NULL REFERENCES pulsetrack_gold.dim_advertiser(advertiser_key),
    account_manager_key INTEGER REFERENCES pulsetrack_gold.dim_account_manager(account_manager_key),
    
    -- Additive Base Measures
    impressions         INTEGER NOT NULL DEFAULT 0,
    clicks              INTEGER NOT NULL DEFAULT 0,
    conversions         INTEGER NOT NULL DEFAULT 0,
    revenue             NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    net_revenue         NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    payout              NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    
    -- Audit & Lineage Tracking
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_dm_fact_advertiser_date UNIQUE (date_key, advertiser_key)
);

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_dm_adv_date ON pulsetrack_dm.dm_fact_advertiser_performance_daily(date_key);
CREATE INDEX IF NOT EXISTS idx_dm_adv_advertiser ON pulsetrack_dm.dm_fact_advertiser_performance_daily(advertiser_key);
CREATE INDEX IF NOT EXISTS idx_dm_adv_account_manager ON pulsetrack_dm.dm_fact_advertiser_performance_daily(account_manager_key);


-- ----------------------------------------------------------------------------
-- 3. Business KPI Analytical View: Advertiser Daily Performance
-- ----------------------------------------------------------------------------
-- Calculates all non-additive rates and averages cleanly for BI tools / users.
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW pulsetrack_dm.vw_advertiser_performance_daily AS
SELECT
    -- Date Hierarchy
    d.date_key,
    d.full_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.day,
    d.day_name,

    -- Advertiser & Account Manager Attributes
    a.advertiser_key,
    a.advertiser_id,
    a.advertiser_name,
    COALESCE(am.am_id, 'UNKNOWN') AS am_id,
    COALESCE(am.am_name, 'Unassigned') AS am_name,

    -- Base Aggregated Measures
    f.impressions,
    f.clicks,
    f.conversions,
    f.revenue,
    f.net_revenue,
    f.payout,

    -- Calculated Non-Additive Metrics (Handled safely with NULLIF)
    -- CTR: Clicks / Impressions * 100
    ROUND(f.clicks::NUMERIC / NULLIF(f.impressions, 0) * 100, 2) AS ctr,

    -- Conversion Rate: Conversions / Clicks * 100
    ROUND(f.conversions::NUMERIC / NULLIF(f.clicks, 0) * 100, 2) AS conversion_rate,

    -- Revenue per Conversion: Revenue / Conversions
    ROUND(f.revenue / NULLIF(f.conversions, 0), 2) AS revenue_per_conversion,

    -- Revenue per Click: Revenue / Clicks
    ROUND(f.revenue / NULLIF(f.clicks, 0), 2) AS revenue_per_click

FROM pulsetrack_dm.dm_fact_advertiser_performance_daily f
JOIN pulsetrack_gold.dim_date d 
    ON f.date_key = d.date_key
JOIN pulsetrack_gold.dim_advertiser a 
    ON f.advertiser_key = a.advertiser_key
LEFT JOIN pulsetrack_gold.dim_account_manager am 
    ON f.account_manager_key = am.account_manager_key;


-- ----------------------------------------------------------------------------
-- 4. Management Comparative View: Monthly Advertiser Trends & Rankings
-- ----------------------------------------------------------------------------
-- Provides Month-over-Month (MoM), Year-over-Year (YoY), Top-N Ranking,
-- and Percentage Contribution to monthly total revenue.
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW pulsetrack_dm.vw_advertiser_performance_monthly AS
WITH monthly_base AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        a.advertiser_id,
        a.advertiser_name,
        SUM(f.impressions) AS total_impressions,
        SUM(f.clicks)      AS total_clicks,
        SUM(f.conversions) AS total_conversions,
        SUM(f.revenue)     AS total_revenue,
        SUM(f.net_revenue) AS total_net_revenue
    FROM pulsetrack_dm.dm_fact_advertiser_performance_daily f
    JOIN pulsetrack_gold.dim_date d ON f.date_key = d.date_key
    JOIN pulsetrack_gold.dim_advertiser a ON f.advertiser_key = a.advertiser_key
    GROUP BY d.year, d.month, d.month_name, a.advertiser_id, a.advertiser_name
),
monthly_with_totals AS (
    SELECT
        mb.*,
        -- Total revenue across ALL advertisers in the same month (for % contribution)
        SUM(mb.total_revenue) OVER (PARTITION BY mb.year, mb.month) AS monthly_total_platform_revenue,
        -- Rank advertisers within each month (Top-N)
        DENSE_RANK() OVER (PARTITION BY mb.year, mb.month ORDER BY mb.total_revenue DESC) AS monthly_revenue_rank,
        -- Previous month's revenue for MoM
        LAG(mb.total_revenue) OVER (PARTITION BY mb.advertiser_id ORDER BY mb.year, mb.month) AS prev_month_revenue,
        -- Previous year's same month revenue for YoY
        LAG(mb.total_revenue, 12) OVER (PARTITION BY mb.advertiser_id ORDER BY mb.year, mb.month) AS prev_year_revenue
    FROM monthly_base mb
)
SELECT
    year,
    month,
    month_name,
    advertiser_id,
    advertiser_name,
    total_impressions,
    total_clicks,
    total_conversions,
    total_revenue,
    total_net_revenue,

    -- Calculated Ratios
    ROUND(total_clicks::NUMERIC / NULLIF(total_impressions, 0) * 100, 2) AS ctr,
    ROUND(total_conversions::NUMERIC / NULLIF(total_clicks, 0) * 100, 2) AS conversion_rate,
    ROUND(total_revenue / NULLIF(total_conversions, 0), 2) AS revenue_per_conversion,
    ROUND(total_revenue / NULLIF(total_clicks, 0), 2) AS revenue_per_click,

    -- Management Requirements: Ranking & Contribution
    monthly_revenue_rank,
    ROUND((total_revenue / NULLIF(monthly_total_platform_revenue, 0)) * 100, 2) AS revenue_pct_contribution,

    -- Management Requirements: MoM & YoY Changes
    ROUND(total_revenue - prev_month_revenue, 2) AS mom_revenue_change,
    ROUND(((total_revenue - prev_month_revenue) / NULLIF(prev_month_revenue, 0)) * 100, 2) AS mom_revenue_growth_pct,
    ROUND(total_revenue - prev_year_revenue, 2) AS yoy_revenue_change,
    ROUND(((total_revenue - prev_year_revenue) / NULLIF(prev_year_revenue, 0)) * 100, 2) AS yoy_revenue_growth_pct

FROM monthly_with_totals;


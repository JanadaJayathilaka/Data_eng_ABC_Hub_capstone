-- ============================================================================
-- PulseTrack: Advanced OLAP Cube & Semantic Analytical Model
-- ============================================================================
-- Question 8: Built directly on top of the PulseTrack Data Mart (pulsetrack_dm)
-- Schema: pulsetrack_olap
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS pulsetrack_olap;

-- ----------------------------------------------------------------------------
-- 1. Core Semantic Analytical Model (Logical Presentation Layer)
-- ----------------------------------------------------------------------------
-- Unifies the Data Mart facts and dimensions with hierarchical drill paths
-- and dynamic non-additive KPI calculations.
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW pulsetrack_olap.vw_semantic_marketing_model AS
WITH platform_monthly_totals AS (
    SELECT 
        d.year,
        d.month,
        SUM(f.revenue) AS platform_monthly_revenue
    FROM pulsetrack_dm.dm_fact_marketing_performance_daily f
    JOIN pulsetrack_dm.dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month
)
SELECT
    -- ========================================================================
    -- 1. Dimensions & Hierarchies
    -- ========================================================================
    -- Date Hierarchy: Year -> Quarter -> Month -> Day
    d.date_key,
    d.full_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.day,
    d.day_name,

    -- Advertiser Hierarchy: Advertiser -> Campaign -> Offer
    a.advertiser_key,
    a.advertiser_id,
    a.advertiser_name,
    c.campaign_key,
    c.campaign_id,
    c.campaign_name,
    o.offer_key,
    o.offer_id,
    COALESCE(o.offer_name, 'Unknown Offer') AS offer_name,

    -- Campaign Hierarchy: Campaign -> Creative
    cr.creative_key,
    cr.creative_id,
    COALESCE(cr.creative_name, 'Non-Creative Placement') AS creative_name,
    COALESCE(cr.creative_type, 'Standard') AS creative_type,

    -- Partner Hierarchy: Partner Type -> Partner Name -> Partner ID
    p.partner_key,
    p.partner_id,
    p.partner_name,
    COALESCE(p.partner_type, 'Standard Affiliate') AS partner_type,

    -- Account Manager & Contextual Dimensions
    am.account_manager_key,
    COALESCE(am.am_name, 'Unassigned') AS account_manager_name,
    COALESCE(g.country, 'Unknown') AS country,
    COALESCE(dv.device_type, 'Unknown') AS device_type,

    -- ========================================================================
    -- 2. Additive Base Measures
    -- ========================================================================
    f.impressions,
    f.clicks,
    f.conversions,
    f.revenue,
    f.adjusted_revenue,
    f.payout,
    f.margin,
    COALESCE(pa.attributed_conversions, 0.0000) AS attributed_conversions,
    COALESCE(pa.attributed_revenue, 0.00)       AS attributed_revenue,

    -- ========================================================================
    -- 3. Non-Additive Calculated Measures & KPIs
    -- ========================================================================
    -- CTR (Click-Through Rate): Clicks / Impressions * 100
    ROUND(f.clicks::numeric / NULLIF(f.impressions, 0) * 100, 2) AS ctr,

    -- Conversion Rate: Conversions / Clicks * 100
    ROUND(f.conversions::numeric / NULLIF(f.clicks, 0) * 100, 2) AS conversion_rate,

    -- Revenue per Click: Revenue / Clicks
    ROUND(f.revenue / NULLIF(f.clicks, 0), 2) AS revenue_per_click,

    -- Revenue per Conversion: Revenue / Conversions
    ROUND(f.revenue / NULLIF(f.conversions, 0), 2) AS revenue_per_conversion,

    -- Attribution Percentage: Attributed Revenue / Total Platform Revenue * 100
    ROUND(COALESCE(pa.attributed_revenue, 0.00) / NULLIF(pmt.platform_monthly_revenue, 0) * 100, 2) AS attribution_percentage,

    -- Contribution Percentage: Revenue / Monthly Total Platform Revenue * 100
    ROUND(f.revenue / NULLIF(pmt.platform_monthly_revenue, 0) * 100, 2) AS contribution_percentage

FROM pulsetrack_dm.dm_fact_marketing_performance_daily f
JOIN pulsetrack_dm.dim_date d ON f.date_key = d.date_key
JOIN pulsetrack_dm.dim_advertiser a ON f.advertiser_key = a.advertiser_key
JOIN pulsetrack_dm.dim_campaign c ON f.campaign_key = c.campaign_key
LEFT JOIN pulsetrack_dm.dim_offer o ON f.offer_key = o.offer_key
LEFT JOIN pulsetrack_dm.dim_creative cr ON f.creative_key = cr.creative_key
JOIN pulsetrack_dm.dim_partner p ON f.partner_key = p.partner_key
LEFT JOIN pulsetrack_dm.dim_account_manager am ON f.account_manager_key = am.account_manager_key
LEFT JOIN pulsetrack_dm.dim_geo g ON f.geo_key = g.geo_key
LEFT JOIN pulsetrack_dm.dim_device dv ON f.device_key = dv.device_key
LEFT JOIN pulsetrack_dm.dm_fact_partner_attribution_daily pa 
    ON f.date_key = pa.date_key 
   AND f.partner_key = pa.partner_key 
   AND f.campaign_key = pa.campaign_key
LEFT JOIN platform_monthly_totals pmt 
    ON d.year = pmt.year AND d.month = pmt.month;


-- ----------------------------------------------------------------------------
-- 2. Materialized Summary Cube (Pre-Aggregated ROLAP Acceleration)
-- ----------------------------------------------------------------------------
-- Pre-aggregates the multidimensional intersections for sub-second executive
-- querying across Date, Advertiser, Campaign, and Partner hierarchies.
-- ----------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS pulsetrack_olap.cube_executive_performance_summary AS
SELECT
    d.year,
    d.quarter,
    d.month,
    a.advertiser_id,
    a.advertiser_name,
    c.campaign_id,
    c.campaign_name,
    p.partner_id,
    p.partner_name,
    COALESCE(p.partner_type, 'Standard Affiliate') AS partner_type,

    -- Additive Base Aggregates
    SUM(f.impressions)         AS total_impressions,
    SUM(f.clicks)              AS total_clicks,
    SUM(f.conversions)         AS total_conversions,
    SUM(f.revenue)             AS total_revenue,
    SUM(f.adjusted_revenue)    AS total_adjusted_revenue,
    SUM(f.payout)              AS total_payout,
    SUM(f.margin)              AS total_margin,
    SUM(COALESCE(pa.attributed_conversions, 0.0000)) AS total_attributed_conversions,
    SUM(COALESCE(pa.attributed_revenue, 0.00))       AS total_attributed_revenue

FROM pulsetrack_dm.dm_fact_marketing_performance_daily f
JOIN pulsetrack_dm.dim_date d ON f.date_key = d.date_key
JOIN pulsetrack_dm.dim_advertiser a ON f.advertiser_key = a.advertiser_key
JOIN pulsetrack_dm.dim_campaign c ON f.campaign_key = c.campaign_key
JOIN pulsetrack_dm.dim_partner p ON f.partner_key = p.partner_key
LEFT JOIN pulsetrack_dm.dm_fact_partner_attribution_daily pa 
    ON f.date_key = pa.date_key 
   AND f.partner_key = pa.partner_key 
   AND f.campaign_key = pa.campaign_key
GROUP BY
    d.year, d.quarter, d.month,
    a.advertiser_id, a.advertiser_name,
    c.campaign_id, c.campaign_name,
    p.partner_id, p.partner_name, p.partner_type;

-- High-Performance Composite Indexes for Cube Slicing & Dicing
CREATE INDEX IF NOT EXISTS idx_cube_time ON pulsetrack_olap.cube_executive_performance_summary(year, quarter, month);
CREATE INDEX IF NOT EXISTS idx_cube_adv ON pulsetrack_olap.cube_executive_performance_summary(advertiser_id);
CREATE INDEX IF NOT EXISTS idx_cube_camp ON pulsetrack_olap.cube_executive_performance_summary(campaign_id);
CREATE INDEX IF NOT EXISTS idx_cube_part ON pulsetrack_olap.cube_executive_performance_summary(partner_id);


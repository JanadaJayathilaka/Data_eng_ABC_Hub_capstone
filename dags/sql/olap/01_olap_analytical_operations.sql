-- ============================================================================
-- PulseTrack: Advanced OLAP Operations Demonstration (Question 8)
-- ============================================================================
-- Demonstrates all 11 required OLAP operations directly against the Semantic
-- Analytical Model / Materialized OLAP Cube in schema pulsetrack_olap.
-- ============================================================================

-- ============================================================================
-- 1. ROLL-UP (Aggregating up the Date hierarchy: Day -> Month -> Quarter -> Year)
-- ============================================================================
-- Demonstrates aggregation moving from fine granularity (Day) to higher levels (Quarter/Year).
SELECT 
    year,
    quarter,
    SUM(total_impressions)      AS impressions,
    SUM(total_clicks)           AS clicks,
    SUM(total_conversions)      AS conversions,
    SUM(total_revenue)          AS revenue,
    SUM(total_adjusted_revenue) AS adjusted_revenue,
    ROUND(SUM(total_clicks)::numeric / NULLIF(SUM(total_impressions), 0) * 100, 2) AS ctr,
    ROUND(SUM(total_conversions)::numeric / NULLIF(SUM(total_clicks), 0) * 100, 2) AS conversion_rate
FROM pulsetrack_olap.cube_executive_performance_summary
GROUP BY ROLLUP (year, quarter)
ORDER BY year, quarter;


-- ============================================================================
-- 2. DRILL-DOWN (Navigating down the hierarchy: Advertiser -> Campaign -> Creative)
-- ============================================================================
-- Allows business analysts to inspect fine-grained creative performance within a campaign.
SELECT 
    advertiser_name,
    campaign_name,
    creative_name,
    creative_type,
    SUM(impressions) AS impressions,
    SUM(clicks)      AS clicks,
    SUM(conversions) AS conversions,
    SUM(revenue)     AS revenue,
    ROUND(SUM(clicks)::numeric / NULLIF(SUM(impressions), 0) * 100, 2) AS ctr
FROM pulsetrack_olap.vw_semantic_marketing_model
WHERE advertiser_id = 'ADV001'
GROUP BY advertiser_name, campaign_name, creative_name, creative_type
ORDER BY advertiser_name, campaign_name, revenue DESC;


-- ============================================================================
-- 3. SLICE (Filtering on a single dimension: e.g., Q3 2025)
-- ============================================================================
-- Takes a 1-dimensional slice across the multi-dimensional dataset.
SELECT 
    advertiser_name,
    campaign_name,
    SUM(total_clicks)      AS clicks,
    SUM(total_conversions) AS conversions,
    SUM(total_revenue)     AS revenue
FROM pulsetrack_olap.cube_executive_performance_summary
WHERE year = 2025 AND quarter = 3
GROUP BY advertiser_name, campaign_name
ORDER BY revenue DESC;


-- ============================================================================
-- 4. DICE (Multi-dimensional filtering across 3+ dimensions)
-- ============================================================================
-- Slices across Date, Country, and Partner Type simultaneously.
SELECT 
    year,
    month_name,
    partner_type,
    country,
    SUM(impressions) AS impressions,
    SUM(clicks)      AS clicks,
    SUM(revenue)     AS revenue
FROM pulsetrack_olap.vw_semantic_marketing_model
WHERE year = 2025 
  AND month IN (7, 8, 9)
  AND country IN ('United States', 'Canada', 'United Kingdom')
  AND partner_type = 'CPA Affiliate'
GROUP BY year, month, month_name, partner_type, country
ORDER BY year, month, revenue DESC;


-- ============================================================================
-- 5. PIVOT (Cross-Tabulation: Quarters along columns, Campaigns along rows)
-- ============================================================================
-- Transforms dimensional orientation for matrix executive reporting.
SELECT 
    campaign_name,
    SUM(CASE WHEN quarter = 1 THEN total_revenue ELSE 0 END) AS q1_revenue,
    SUM(CASE WHEN quarter = 2 THEN total_revenue ELSE 0 END) AS q2_revenue,
    SUM(CASE WHEN quarter = 3 THEN total_revenue ELSE 0 END) AS q3_revenue,
    SUM(CASE WHEN quarter = 4 THEN total_revenue ELSE 0 END) AS q4_revenue,
    SUM(total_revenue) AS full_year_revenue
FROM pulsetrack_olap.cube_executive_performance_summary
WHERE year = 2025
GROUP BY campaign_name
ORDER BY full_year_revenue DESC;


-- ============================================================================
-- 6. TOP-N (Top 5 revenue-generating campaigns per quarter)
-- ============================================================================
WITH quarterly_ranked AS (
    SELECT 
        year,
        quarter,
        campaign_name,
        SUM(total_revenue) AS revenue,
        DENSE_RANK() OVER (PARTITION BY year, quarter ORDER BY SUM(total_revenue) DESC) AS rank
    FROM pulsetrack_olap.cube_executive_performance_summary
    GROUP BY year, quarter, campaign_name
)
SELECT * 
FROM quarterly_ranked
WHERE rank <= 5
ORDER BY year, quarter, rank;


-- ============================================================================
-- 7. RANKING (Dynamic platform-wide & advertiser-specific ranking)
-- ============================================================================
SELECT 
    advertiser_name,
    campaign_name,
    SUM(total_revenue)          AS revenue,
    SUM(total_adjusted_revenue) AS adjusted_revenue,
    DENSE_RANK() OVER (ORDER BY SUM(total_revenue) DESC)          AS platform_revenue_rank,
    DENSE_RANK() OVER (ORDER BY SUM(total_adjusted_revenue) DESC) AS platform_adjusted_revenue_rank
FROM pulsetrack_olap.cube_executive_performance_summary
GROUP BY advertiser_name, campaign_name
ORDER BY platform_revenue_rank;


-- ============================================================================
-- 8. PERCENTAGE CONTRIBUTION (Campaign share of advertiser and platform revenue)
-- ============================================================================
WITH campaign_totals AS (
    SELECT 
        advertiser_name,
        campaign_name,
        SUM(total_revenue) AS campaign_revenue
    FROM pulsetrack_olap.cube_executive_performance_summary
    GROUP BY advertiser_name, campaign_name
)
SELECT 
    advertiser_name,
    campaign_name,
    campaign_revenue,
    -- Contribution to the specific Advertiser
    ROUND(campaign_revenue / NULLIF(SUM(campaign_revenue) OVER (PARTITION BY advertiser_name), 0) * 100, 2) AS pct_of_advertiser_revenue,
    -- Contribution to the entire Platform
    ROUND(campaign_revenue / NULLIF(SUM(campaign_revenue) OVER (), 0) * 100, 2) AS pct_of_total_platform_revenue
FROM campaign_totals
ORDER BY advertiser_name, campaign_revenue DESC;


-- ============================================================================
-- 9. MONTH-OVER-MONTH (MoM Growth & Trend Analysis)
-- ============================================================================
WITH monthly_perf AS (
    SELECT 
        year,
        month,
        SUM(total_revenue) AS revenue
    FROM pulsetrack_olap.cube_executive_performance_summary
    GROUP BY year, month
)
SELECT 
    year,
    month,
    revenue,
    LAG(revenue, 1) OVER (ORDER BY year, month) AS prior_month_revenue,
    ROUND(revenue - LAG(revenue, 1) OVER (ORDER BY year, month), 2) AS mom_dollar_change,
    ROUND(((revenue - LAG(revenue, 1) OVER (ORDER BY year, month)) / NULLIF(LAG(revenue, 1) OVER (ORDER BY year, month), 0)) * 100, 2) AS mom_growth_pct
FROM monthly_perf
ORDER BY year, month;


-- ============================================================================
-- 10. QUARTER-OVER-QUARTER (QoQ Growth Analysis)
-- ============================================================================
WITH quarterly_perf AS (
    SELECT 
        year,
        quarter,
        SUM(total_revenue) AS revenue
    FROM pulsetrack_olap.cube_executive_performance_summary
    GROUP BY year, quarter
)
SELECT 
    year,
    quarter,
    revenue,
    LAG(revenue, 1) OVER (ORDER BY year, quarter) AS prior_quarter_revenue,
    ROUND(revenue - LAG(revenue, 1) OVER (ORDER BY year, quarter), 2) AS qoq_dollar_change,
    ROUND(((revenue - LAG(revenue, 1) OVER (ORDER BY year, quarter)) / NULLIF(LAG(revenue, 1) OVER (ORDER BY year, quarter), 0)) * 100, 2) AS qoq_growth_pct
FROM quarterly_perf
ORDER BY year, quarter;


-- ============================================================================
-- 11. YEAR-OVER-YEAR (YoY Growth Analysis)
-- ============================================================================
WITH monthly_annual_perf AS (
    SELECT 
        year,
        month,
        SUM(total_revenue) AS revenue
    FROM pulsetrack_olap.cube_executive_performance_summary
    GROUP BY year, month
)
SELECT 
    year,
    month,
    revenue,
    LAG(revenue, 12) OVER (ORDER BY year, month) AS prior_year_month_revenue,
    ROUND(revenue - LAG(revenue, 12) OVER (ORDER BY year, month), 2) AS yoy_dollar_change,
    ROUND(((revenue - LAG(revenue, 12) OVER (ORDER BY year, month)) / NULLIF(LAG(revenue, 12) OVER (ORDER BY year, month), 0)) * 100, 2) AS yoy_growth_pct
FROM monthly_annual_perf
ORDER BY year, month;


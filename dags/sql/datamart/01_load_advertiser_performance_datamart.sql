-- ============================================================================
-- PulseTrack: Data Mart Population - Full Load / Refresh
-- ============================================================================
-- Business Area: Advertiser Performance
-- Source: EDW Gold Layer (fact_impression, fact_click, fact_conversion, dim_advertiser)
-- Target: pulsetrack_dm.dm_fact_advertiser_performance_daily
-- Technique: Atomic CTE pre-aggregation + Key Union to prevent Cartesian fanout traps.
-- ============================================================================

WITH imp_agg AS (
    SELECT 
        date_key,
        advertiser_key,
        SUM(impression_count) AS impressions
    FROM pulsetrack_gold.fact_impression
    WHERE advertiser_key IS NOT NULL AND advertiser_key != -1
    GROUP BY date_key, advertiser_key
),

clk_agg AS (
    SELECT 
        date_key,
        advertiser_key,
        SUM(click_count) AS clicks
    FROM pulsetrack_gold.fact_click
    WHERE advertiser_key IS NOT NULL AND advertiser_key != -1
    GROUP BY date_key, advertiser_key
),

conv_agg AS (
    SELECT 
        conversion_date_key AS date_key,
        advertiser_key,
        MAX(account_manager_key) AS account_manager_key,
        SUM(conversion_count) AS conversions,
        SUM(revenue)          AS revenue,
        SUM(net_revenue)      AS net_revenue,
        SUM(payout)           AS payout
    FROM pulsetrack_gold.fact_conversion
    WHERE advertiser_key IS NOT NULL AND advertiser_key != -1
    GROUP BY conversion_date_key, advertiser_key
),

all_keys AS (
    SELECT date_key, advertiser_key FROM imp_agg
    UNION
    SELECT date_key, advertiser_key FROM clk_agg
    UNION
    SELECT date_key, advertiser_key FROM conv_agg
)

SELECT 
    ak.date_key,
    ak.advertiser_key,
    COALESCE(c.account_manager_key, da.account_manager_key, -1) AS account_manager_key,
    COALESCE(i.impressions, 0) AS impressions,
    COALESCE(cl.clicks, 0) AS clicks,
    COALESCE(c.conversions, 0) AS conversions,
    COALESCE(c.revenue, 0.00) AS revenue,
    COALESCE(c.net_revenue, 0.00) AS net_revenue,
    COALESCE(c.payout, 0.00) AS payout,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM all_keys ak
LEFT JOIN pulsetrack_gold.dim_advertiser da
    ON ak.advertiser_key = da.advertiser_key
LEFT JOIN imp_agg i
    ON ak.date_key = i.date_key AND ak.advertiser_key = i.advertiser_key
LEFT JOIN clk_agg cl
    ON ak.date_key = cl.date_key AND ak.advertiser_key = cl.advertiser_key
LEFT JOIN conv_agg c
    ON ak.date_key = c.date_key AND ak.advertiser_key = c.advertiser_key;


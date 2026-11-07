-- ============================================================================
-- PulseTrack Data Mart: Full Load for dm_fact_marketing_performance_daily
-- ============================================================================
-- Business Areas: Advertiser, Campaign, Creative Performance & Time Analysis
-- Source: EDW Gold (fact_impression, fact_click, fact_conversion, dim_advertiser)
-- ============================================================================

WITH imp_agg AS (
    SELECT 
        date_key,
        advertiser_key,
        offer_key,
        campaign_key,
        creative_key,
        partner_key,
        geo_key,
        device_key,
        SUM(impression_count) AS impressions
    FROM pulsetrack_gold.fact_impression
    WHERE advertiser_key IS NOT NULL AND advertiser_key != -1
    GROUP BY date_key, advertiser_key, offer_key, campaign_key, creative_key, partner_key, geo_key, device_key
),

clk_agg AS (
    SELECT 
        date_key,
        advertiser_key,
        offer_key,
        campaign_key,
        creative_key,
        partner_key,
        geo_key,
        device_key,
        SUM(click_count) AS clicks
    FROM pulsetrack_gold.fact_click
    WHERE advertiser_key IS NOT NULL AND advertiser_key != -1
    GROUP BY date_key, advertiser_key, offer_key, campaign_key, creative_key, partner_key, geo_key, device_key
),

conv_agg AS (
    SELECT 
        conversion_date_key AS date_key,
        advertiser_key,
        offer_key,
        campaign_key,
        partner_key,
        account_manager_key,
        geo_key,
        device_key,
        SUM(conversion_count) AS conversions,
        SUM(revenue)          AS revenue,
        SUM(net_revenue)      AS adjusted_revenue,
        SUM(payout)           AS payout,
        SUM(margin)           AS margin
    FROM pulsetrack_gold.fact_conversion
    WHERE advertiser_key IS NOT NULL AND advertiser_key != -1
    GROUP BY conversion_date_key, advertiser_key, offer_key, campaign_key, partner_key, account_manager_key, geo_key, device_key
),

all_keys AS (
    SELECT date_key, advertiser_key, offer_key, campaign_key, creative_key, partner_key, geo_key, device_key FROM imp_agg
    UNION
    SELECT date_key, advertiser_key, offer_key, campaign_key, creative_key, partner_key, geo_key, device_key FROM clk_agg
    UNION
    SELECT date_key, advertiser_key, offer_key, campaign_key, -1 AS creative_key, partner_key, geo_key, device_key FROM conv_agg
)

SELECT 
    ak.date_key,
    ak.advertiser_key,
    ak.offer_key,
    ak.campaign_key,
    ak.creative_key,
    ak.partner_key,
    COALESCE(da.account_manager_key, -1) AS account_manager_key,
    ak.geo_key,
    ak.device_key,
    COALESCE(i.impressions, 0)          AS impressions,
    COALESCE(cl.clicks, 0)              AS clicks,
    COALESCE(cv.conversions, 0)         AS conversions,
    COALESCE(cv.revenue, 0.00)          AS revenue,
    COALESCE(cv.adjusted_revenue, 0.00) AS adjusted_revenue,
    COALESCE(cv.payout, 0.00)           AS payout,
    COALESCE(cv.margin, 0.00)           AS margin,
    CURRENT_TIMESTAMP                   AS created_at
FROM all_keys ak
LEFT JOIN pulsetrack_gold.dim_advertiser da
    ON ak.advertiser_key = da.advertiser_key
LEFT JOIN imp_agg i
    ON ak.date_key = i.date_key 
   AND ak.advertiser_key = i.advertiser_key
   AND ak.offer_key = i.offer_key
   AND ak.campaign_key = i.campaign_key
   AND ak.creative_key = i.creative_key
   AND ak.partner_key = i.partner_key
   AND ak.geo_key = i.geo_key
   AND ak.device_key = i.device_key
LEFT JOIN clk_agg cl
    ON ak.date_key = cl.date_key 
   AND ak.advertiser_key = cl.advertiser_key
   AND ak.offer_key = cl.offer_key
   AND ak.campaign_key = cl.campaign_key
   AND ak.creative_key = cl.creative_key
   AND ak.partner_key = cl.partner_key
   AND ak.geo_key = cl.geo_key
   AND ak.device_key = cl.device_key
LEFT JOIN conv_agg cv
    ON ak.date_key = cv.date_key 
   AND ak.advertiser_key = cv.advertiser_key
   AND ak.offer_key = cv.offer_key
   AND ak.campaign_key = cv.campaign_key
   AND ak.partner_key = cv.partner_key
   AND ak.geo_key = cv.geo_key
   AND ak.device_key = cv.device_key;


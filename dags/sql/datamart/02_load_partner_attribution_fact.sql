-- ============================================================================
-- PulseTrack Data Mart: Full Load for dm_fact_partner_attribution_daily
-- ============================================================================
-- Business Area: Partner Performance & Multi-Touch Attribution
-- Source: EDW Gold (fact_click, fact_conversion, bridge_conversion_touchpoint)
-- ============================================================================

WITH partner_clicks AS (
    SELECT 
        date_key,
        partner_key,
        campaign_key,
        SUM(click_count) AS total_clicks
    FROM pulsetrack_gold.fact_click
    WHERE partner_key IS NOT NULL AND partner_key != -1
    GROUP BY date_key, partner_key, campaign_key
),

partner_direct_conv AS (
    SELECT 
        conversion_date_key AS date_key,
        partner_key,
        campaign_key,
        SUM(conversion_count) AS direct_conversions
    FROM pulsetrack_gold.fact_conversion
    WHERE partner_key IS NOT NULL AND partner_key != -1
    GROUP BY conversion_date_key, partner_key, campaign_key
),

partner_attributed AS (
    SELECT 
        fc.conversion_date_key AS date_key,
        fk.partner_key,
        fc.campaign_key,
        SUM(b.attribution_weight) AS attributed_conversions,
        SUM(b.attribution_weight * fc.net_revenue) AS attributed_revenue
    FROM pulsetrack_gold.bridge_conversion_touchpoint b
    JOIN pulsetrack_gold.fact_conversion fc ON b.conversion_key = fc.conversion_key
    JOIN pulsetrack_gold.fact_click fk ON b.click_key = fk.click_key
    WHERE fk.partner_key IS NOT NULL AND fk.partner_key != -1
    GROUP BY fc.conversion_date_key, fk.partner_key, fc.campaign_key
),

all_partner_keys AS (
    SELECT date_key, partner_key, campaign_key FROM partner_clicks
    UNION
    SELECT date_key, partner_key, campaign_key FROM partner_direct_conv
    UNION
    SELECT date_key, partner_key, campaign_key FROM partner_attributed
)

SELECT 
    apk.date_key,
    apk.partner_key,
    apk.campaign_key,
    COALESCE(pc.total_clicks, 0)                AS total_clicks,
    COALESCE(pdc.direct_conversions, 0)         AS direct_conversions,
    COALESCE(pa.attributed_conversions, 0.0000) AS attributed_conversions,
    COALESCE(pa.attributed_revenue, 0.00)       AS attributed_revenue,
    CURRENT_TIMESTAMP                           AS created_at
FROM all_partner_keys apk
LEFT JOIN partner_clicks pc 
    ON apk.date_key = pc.date_key 
   AND apk.partner_key = pc.partner_key 
   AND apk.campaign_key = pc.campaign_key
LEFT JOIN partner_direct_conv pdc
    ON apk.date_key = pdc.date_key 
   AND apk.partner_key = pdc.partner_key 
   AND apk.campaign_key = pdc.campaign_key
LEFT JOIN partner_attributed pa
    ON apk.date_key = pa.date_key 
   AND apk.partner_key = pa.partner_key 
   AND apk.campaign_key = pa.campaign_key;


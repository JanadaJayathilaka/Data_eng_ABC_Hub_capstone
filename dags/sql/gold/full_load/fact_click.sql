-- ============================================================================
-- PulseTrack: Gold Layer - Full Load SQL for fact_click
-- ============================================================================
-- Grain: One click event (middle of the marketing funnel).
-- Joins: Resolves all conformed dimension surrogate keys, including granular
-- device attributes (device_type, operating_system, browser) from dim_device.
-- Measures: click_count (always 1).
-- ============================================================================

WITH cleaned_clicks AS (
    SELECT
        clk.click_id,
        clk.click_time,
        clk.creative_id,
        clk.campaign_id,
        clk.partner_id,
        clk.country,
        clk.device,
        clk.operating_system,
        clk.browser
    FROM pulsetrack_silver.clicks clk
    WHERE clk.click_id IS NOT NULL AND TRIM(clk.click_id) != ''
)

SELECT
    ROW_NUMBER() OVER (ORDER BY clk.click_time, clk.click_id) AS click_key,
    clk.click_id,
    COALESCE(dd.date_key, -1) AS date_key,
    COALESCE(da.advertiser_key, -1) AS advertiser_key,
    COALESCE(d_off.offer_key, -1) AS offer_key,
    COALESCE(dc.campaign_key, -1) AS campaign_key,
    COALESCE(dcr.creative_key, -1) AS creative_key,
    COALESCE(dp.partner_key, -1) AS partner_key,
    COALESCE(dg.geo_key, -1) AS geo_key,
    COALESCE(ddv.device_key, -1) AS device_key,
    1 AS click_count
FROM cleaned_clicks clk
LEFT JOIN pulsetrack_silver.campaigns c
    ON clk.campaign_id = c.campaign_id
LEFT JOIN pulsetrack_gold.dim_date dd
    ON TO_CHAR(clk.click_time, 'YYYYMMDD')::INTEGER = dd.date_key
LEFT JOIN pulsetrack_gold.dim_campaign dc
    ON clk.campaign_id = dc.campaign_id AND dc.is_current = TRUE
LEFT JOIN pulsetrack_gold.dim_creative dcr
    ON clk.creative_id = dcr.creative_id AND dcr.is_current = TRUE
LEFT JOIN pulsetrack_gold.dim_offer d_off
    ON c.offer_id = d_off.offer_id AND d_off.is_current = TRUE
LEFT JOIN pulsetrack_gold.dim_advertiser da
    ON c.advertiser_id = da.advertiser_id AND da.is_current = TRUE
LEFT JOIN pulsetrack_gold.dim_partner dp
    ON clk.partner_id = dp.partner_id AND dp.is_current = TRUE
LEFT JOIN pulsetrack_gold.dim_geo dg
    ON COALESCE(clk.country, 'Unknown') = dg.country
LEFT JOIN pulsetrack_gold.dim_device ddv
    ON clk.device = ddv.device_type
   AND clk.operating_system = ddv.operating_system
   AND clk.browser = ddv.browser;

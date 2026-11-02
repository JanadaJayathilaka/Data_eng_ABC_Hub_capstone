-- ============================================================================
-- PulseTrack: Gold Layer - Full Load SQL for fact_conversion
-- ============================================================================
-- Grain: One conversion event with folded late-arriving revenue adjustments.
-- Joins: Resolves conformed dimensions, matching the exact SCD2 advertiser and
-- account manager valid at conversion time.
-- Measures: conversion_count, revenue, payout, net_revenue, margin.
-- ============================================================================

WITH adjustments_agg AS (
    SELECT
        conversion_id,
        COALESCE(SUM(delta_amount), 0.00) AS total_delta
    FROM pulsetrack_silver.revenue_adjustments
    WHERE conversion_id IS NOT NULL
    GROUP BY conversion_id
),

cleaned_conversions AS (
    SELECT
        conv.conversion_id,
        conv.conversion_time,
        conv.campaign_id,
        conv.partner_id,
        conv.country,
        conv.device,
        conv.account_manager,
        COALESCE(conv.revenue, 0.00) AS revenue,
        COALESCE(conv.payout, 0.00) AS payout,
        COALESCE(conv.revenue, 0.00) + COALESCE(adj.total_delta, 0.00) AS net_revenue,
        (COALESCE(conv.revenue, 0.00) + COALESCE(adj.total_delta, 0.00)) - COALESCE(conv.payout, 0.00) AS margin
    FROM pulsetrack_silver.conversions conv
    LEFT JOIN adjustments_agg adj
        ON conv.conversion_id = adj.conversion_id
    WHERE conv.conversion_id IS NOT NULL AND TRIM(conv.conversion_id) != ''
)

SELECT
    ROW_NUMBER() OVER (ORDER BY conv.conversion_time, conv.conversion_id) AS conversion_key,
    conv.conversion_id,
    COALESCE(dd.date_key, -1) AS conversion_date_key,
    COALESCE(da.advertiser_key, -1) AS advertiser_key,
    COALESCE(d_off.offer_key, -1) AS offer_key,
    COALESCE(dc.campaign_key, -1) AS campaign_key,
    COALESCE(dp.partner_key, -1) AS partner_key,
    COALESCE(da.account_manager_key, dam.account_manager_key, -1) AS account_manager_key,
    COALESCE(dg.geo_key, -1) AS geo_key,
    COALESCE(ddv.device_key, -1) AS device_key,
    1 AS conversion_count,
    conv.revenue,
    conv.payout,
    conv.net_revenue,
    conv.margin
FROM cleaned_conversions conv
LEFT JOIN pulsetrack_silver.campaigns c
    ON conv.campaign_id = c.campaign_id
LEFT JOIN pulsetrack_gold.dim_date dd
    ON TO_CHAR(conv.conversion_time, 'YYYYMMDD')::INTEGER = dd.date_key
LEFT JOIN pulsetrack_gold.dim_campaign dc
    ON conv.campaign_id = dc.campaign_id AND dc.is_current = TRUE
LEFT JOIN pulsetrack_gold.dim_offer d_off
    ON c.offer_id = d_off.offer_id AND d_off.is_current = TRUE
-- Match advertiser and AM valid at conversion time (SCD2 match with current fallback)
LEFT JOIN LATERAL (
    SELECT
        da_sub.advertiser_key,
        da_sub.account_manager_key
    FROM pulsetrack_gold.dim_advertiser da_sub
    WHERE da_sub.advertiser_id = c.advertiser_id
    ORDER BY
        CASE
            WHEN conv.conversion_time::DATE >= da_sub.effective_from
             AND (da_sub.effective_to IS NULL OR conv.conversion_time::DATE < da_sub.effective_to) THEN 1
            WHEN da_sub.is_current = TRUE THEN 2
            ELSE 3
        END,
        da_sub.effective_from DESC
    LIMIT 1
) da ON TRUE
LEFT JOIN pulsetrack_gold.dim_account_manager dam
    ON conv.account_manager = dam.am_name AND dam.is_current = TRUE
LEFT JOIN pulsetrack_gold.dim_partner dp
    ON conv.partner_id = dp.partner_id AND dp.is_current = TRUE
LEFT JOIN pulsetrack_gold.dim_geo dg
    ON COALESCE(conv.country, 'Unknown') = dg.country
LEFT JOIN pulsetrack_gold.dim_device ddv
    ON conv.device = ddv.device_type
   AND ddv.operating_system = 'Unknown'
   AND ddv.browser = 'Unknown';

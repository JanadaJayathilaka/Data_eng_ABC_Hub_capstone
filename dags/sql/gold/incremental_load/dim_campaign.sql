-- ============================================================================
-- PulseTrack: Gold Layer - Incremental Load SQL for dim_campaign
-- ============================================================================
-- Inserts new campaigns and resolves surrogate keys for offer and advertiser.
-- ============================================================================

WITH new_campaigns AS (
    SELECT
        c.campaign_id,
        COALESCE(NULLIF(TRIM(c.campaign_name), ''), 'Unknown') AS campaign_name,
        COALESCE(d_off.offer_key, -1) AS offer_key,
        COALESCE(da.advertiser_key, -1) AS advertiser_key,
        CASE
            WHEN c.end_date IS NOT NULL AND c.end_date < CURRENT_DATE THEN 'completed'
            ELSE 'active'
        END AS campaign_status,
        c.created_at,
        c.updated_at,
        COALESCE(c.start_date, (c.created_at)::DATE, '2025-01-01'::DATE) AS effective_from,
        c.end_date AS effective_to,
        CASE
            WHEN c.end_date IS NOT NULL AND c.end_date < CURRENT_DATE THEN FALSE
            ELSE TRUE
        END AS is_current
    FROM pulsetrack_silver.campaigns c
    LEFT JOIN pulsetrack_gold.dim_offer d_off
        ON c.offer_id = d_off.offer_id AND d_off.is_current = TRUE
    LEFT JOIN pulsetrack_gold.dim_advertiser da
        ON c.advertiser_id = da.advertiser_id AND da.is_current = TRUE
    WHERE c.campaign_id IS NOT NULL AND TRIM(c.campaign_id) != ''
      AND c.campaign_id NOT IN (SELECT campaign_id FROM pulsetrack_gold.dim_campaign)
)

SELECT
    campaign_id,
    campaign_name,
    offer_key,
    advertiser_key,
    campaign_status,
    created_at,
    updated_at,
    effective_from,
    effective_to,
    is_current
FROM new_campaigns;

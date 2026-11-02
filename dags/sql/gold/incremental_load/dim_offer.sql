-- ============================================================================
-- PulseTrack: Gold Layer - Incremental Load SQL for dim_offer
-- ============================================================================
-- Inserts new offers and links to current conformed advertisers.
-- ============================================================================

WITH new_offers AS (
    SELECT
        o.offer_id,
        COALESCE(NULLIF(TRIM(o.offer_name), ''), 'Unknown') AS offer_name,
        COALESCE(da.advertiser_key, -1) AS advertiser_key,
        'active' AS offer_status,
        o.created_at,
        o.updated_at,
        COALESCE((o.created_at)::DATE, '2025-01-01'::DATE) AS effective_from,
        NULL::DATE AS effective_to,
        TRUE AS is_current
    FROM pulsetrack_silver.offers o
    LEFT JOIN pulsetrack_gold.dim_advertiser da
        ON o.advertiser_id = da.advertiser_id AND da.is_current = TRUE
    WHERE o.offer_id IS NOT NULL AND TRIM(o.offer_id) != ''
      AND o.offer_id NOT IN (SELECT offer_id FROM pulsetrack_gold.dim_offer)
)

SELECT
    offer_id,
    offer_name,
    advertiser_key,
    offer_status,
    created_at,
    updated_at,
    effective_from,
    effective_to,
    is_current
FROM new_offers;

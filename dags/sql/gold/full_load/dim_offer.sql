-- ============================================================================
-- PulseTrack: Gold Layer - Full Load SQL for dim_offer
-- ============================================================================
-- Loads offers from pulsetrack_silver.offers and resolves the foreign key
-- advertiser_key against pulsetrack_gold.dim_advertiser.
-- Includes a default Unknown record (-1).
-- ============================================================================

WITH silver_offers AS (
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
)

-- 1. Default Unknown record for NULL / unmapped offers
SELECT
    -1 AS offer_key,
    'UNKNOWN' AS offer_id,
    'Unknown' AS offer_name,
    -1 AS advertiser_key,
    'active' AS offer_status,
    '1970-01-01 00:00:00'::TIMESTAMP AS created_at,
    '1970-01-01 00:00:00'::TIMESTAMP AS updated_at,
    '1970-01-01'::DATE AS effective_from,
    NULL::DATE AS effective_to,
    TRUE AS is_current

UNION ALL

-- 2. Conformed Offers
SELECT
    ROW_NUMBER() OVER (ORDER BY offer_id) AS offer_key,
    offer_id,
    offer_name,
    advertiser_key,
    offer_status,
    created_at,
    updated_at,
    effective_from,
    effective_to,
    is_current
FROM silver_offers
ORDER BY offer_key;

-- ============================================================================
-- PulseTrack: Gold Layer - Full Load SQL for dim_creative
-- ============================================================================
-- Loads creatives from pulsetrack_silver.creatives and resolves campaign_key
-- against pulsetrack_gold.dim_campaign. Includes a default Unknown record (-1).
-- ============================================================================

WITH silver_creatives AS (
    SELECT
        cr.creative_id,
        COALESCE(NULLIF(TRIM(cr.creative_name), ''), 'Unknown') AS creative_name,
        COALESCE(dc.campaign_key, -1) AS campaign_key,
        COALESCE(NULLIF(TRIM(cr.format), ''), 'standard') AS creative_type,
        'active' AS status,
        cr.created_at,
        cr.updated_at,
        COALESCE((cr.created_at)::DATE, '2025-01-01'::DATE) AS effective_from,
        NULL::DATE AS effective_to,
        TRUE AS is_current
    FROM pulsetrack_silver.creatives cr
    LEFT JOIN pulsetrack_gold.dim_campaign dc
        ON cr.campaign_id = dc.campaign_id AND dc.is_current = TRUE
    WHERE cr.creative_id IS NOT NULL AND TRIM(cr.creative_id) != ''
)

-- 1. Default Unknown record for NULL / unmapped creatives
SELECT
    -1 AS creative_key,
    'UNKNOWN' AS creative_id,
    'Unknown' AS creative_name,
    -1 AS campaign_key,
    'standard' AS creative_type,
    'active' AS status,
    '1970-01-01 00:00:00'::TIMESTAMP AS created_at,
    '1970-01-01 00:00:00'::TIMESTAMP AS updated_at,
    '1970-01-01'::DATE AS effective_from,
    NULL::DATE AS effective_to,
    TRUE AS is_current

UNION ALL

-- 2. Conformed Creatives
SELECT
    ROW_NUMBER() OVER (ORDER BY creative_id) AS creative_key,
    creative_id,
    creative_name,
    campaign_key,
    creative_type,
    status,
    created_at,
    updated_at,
    effective_from,
    effective_to,
    is_current
FROM silver_creatives
ORDER BY creative_key;

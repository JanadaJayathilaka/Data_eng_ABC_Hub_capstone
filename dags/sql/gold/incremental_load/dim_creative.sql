-- ============================================================================
-- PulseTrack: Gold Layer - Incremental Load SQL for dim_creative
-- ============================================================================
-- Inserts new creatives linked to current campaign dimension surrogate keys.
-- ============================================================================

WITH new_creatives AS (
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
      AND cr.creative_id NOT IN (SELECT creative_id FROM pulsetrack_gold.dim_creative)
)

SELECT
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
FROM new_creatives;

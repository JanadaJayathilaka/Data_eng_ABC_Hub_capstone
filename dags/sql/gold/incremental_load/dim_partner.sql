-- ============================================================================
-- PulseTrack: Gold Layer - Incremental Load SQL for dim_partner
-- ============================================================================
-- Inserts new conformed partners appearing in pulsetrack_silver.partners.
-- ============================================================================

WITH new_partners AS (
    SELECT
        partner_id,
        COALESCE(NULLIF(TRIM(partner_name), ''), 'Unknown') AS partner_name,
        COALESCE(NULLIF(TRIM(channel), ''), 'Affiliate') AS partner_type,
        'active' AS status,
        created_at,
        updated_at,
        COALESCE(join_date, (created_at)::DATE, '2025-01-01'::DATE) AS effective_from,
        NULL::DATE AS effective_to,
        TRUE AS is_current
    FROM pulsetrack_silver.partners
    WHERE partner_id IS NOT NULL AND TRIM(partner_id) != ''
      AND partner_id NOT IN (SELECT partner_id FROM pulsetrack_gold.dim_partner)
)

SELECT
    partner_id,
    partner_name,
    partner_type,
    status,
    created_at,
    updated_at,
    effective_from,
    effective_to,
    is_current
FROM new_partners;

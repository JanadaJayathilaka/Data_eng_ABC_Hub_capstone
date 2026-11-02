-- ============================================================================
-- PulseTrack: Gold Layer - Full Load SQL for dim_partner
-- ============================================================================
-- Loads conformed partners from pulsetrack_silver.partners (which already
-- unites relational partners and MongoDB partner profiles to resolve the
-- 40 missing portal orphans). Includes a default Unknown record (-1).
-- ============================================================================

WITH silver_partners AS (
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
)

-- 1. Default Unknown record for NULL / unmapped partners
SELECT
    -1 AS partner_key,
    'UNKNOWN' AS partner_id,
    'Unknown' AS partner_name,
    'Unknown' AS partner_type,
    'active' AS status,
    '1970-01-01 00:00:00'::TIMESTAMP AS created_at,
    '1970-01-01 00:00:00'::TIMESTAMP AS updated_at,
    '1970-01-01'::DATE AS effective_from,
    NULL::DATE AS effective_to,
    TRUE AS is_current

UNION ALL

-- 2. Conformed Partners
SELECT
    ROW_NUMBER() OVER (ORDER BY partner_id) AS partner_key,
    partner_id,
    partner_name,
    partner_type,
    status,
    created_at,
    updated_at,
    effective_from,
    effective_to,
    is_current
FROM silver_partners
ORDER BY partner_key;

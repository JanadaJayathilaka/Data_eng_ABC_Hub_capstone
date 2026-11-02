-- ============================================================================
-- PulseTrack: Gold Layer - Full Load SQL for dim_account_manager
-- ============================================================================
-- Loads conformed account managers from pulsetrack_silver.account_managers,
-- cross-referencing MongoDB advertiser account manager history to resolve any
-- missing names (e.g. AM013 -> Linda Muller). Includes an Unknown record (-1).
-- ============================================================================

WITH mongo_am_names AS (
    SELECT DISTINCT ON (am_id)
        am_id,
        am_name
    FROM pulsetrack_silver.mongo_advertiser_accounts
    WHERE am_name IS NOT NULL AND TRIM(am_name) != ''
    ORDER BY am_id, is_current DESC, valid_from DESC
),

enriched_am AS (
    SELECT
        am.am_id,
        COALESCE(NULLIF(TRIM(am.am_name), ''), mam.am_name, 'Unknown') AS am_name,
        am.created_at,
        am.updated_at,
        COALESCE(am.hire_date, (am.created_at)::DATE, '2025-01-01'::DATE) AS effective_from,
        NULL::DATE AS effective_to,
        TRUE AS is_current
    FROM pulsetrack_silver.account_managers am
    LEFT JOIN mongo_am_names mam
        ON am.am_id = mam.am_id
)

-- 1. Default Unknown record for NULL / unmapped account managers
SELECT
    -1 AS account_manager_key,
    'UNKNOWN' AS am_id,
    'Unknown' AS am_name,
    '1970-01-01 00:00:00'::TIMESTAMP AS created_at,
    '1970-01-01 00:00:00'::TIMESTAMP AS updated_at,
    '1970-01-01'::DATE AS effective_from,
    NULL::DATE AS effective_to,
    TRUE AS is_current

UNION ALL

-- 2. Conformed Account Managers
SELECT
    ROW_NUMBER() OVER (ORDER BY am_id) AS account_manager_key,
    am_id,
    am_name,
    created_at,
    updated_at,
    effective_from,
    effective_to,
    is_current
FROM enriched_am
ORDER BY account_manager_key;

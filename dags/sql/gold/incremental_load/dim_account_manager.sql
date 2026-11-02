-- ============================================================================
-- PulseTrack: Gold Layer - Incremental Load SQL for dim_account_manager
-- ============================================================================
-- Inserts new account managers and enriches missing names via MongoDB history.
-- ============================================================================

WITH mongo_am_names AS (
    SELECT DISTINCT ON (am_id)
        am_id,
        am_name
    FROM pulsetrack_silver.mongo_advertiser_accounts
    WHERE am_name IS NOT NULL AND TRIM(am_name) != ''
    ORDER BY am_id, is_current DESC, valid_from DESC
),

new_am AS (
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
    WHERE am.am_id IS NOT NULL AND TRIM(am.am_id) != ''
      AND am.am_id NOT IN (SELECT am_id FROM pulsetrack_gold.dim_account_manager)
)

SELECT
    am_id,
    am_name,
    created_at,
    updated_at,
    effective_from,
    effective_to,
    is_current
FROM new_am;

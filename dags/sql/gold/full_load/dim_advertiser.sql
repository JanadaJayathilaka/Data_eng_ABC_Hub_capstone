-- ============================================================================
-- PulseTrack: Gold Layer - Full Load SQL for dim_advertiser
-- ============================================================================
-- Conforms advertisers across relational and MongoDB sources, preserving the
-- true SCD Type 2 account manager history timeline (valid_from, valid_to, is_current)
-- from MongoDB CRM, and linking account_manager_key to dim_account_manager.
-- Includes a default Unknown record (-1).
-- ============================================================================

WITH mongo_history AS (
    SELECT
        maa.advertiser_id,
        COALESCE(NULLIF(TRIM(maa.display_name), ''), NULLIF(TRIM(maa.legal_name), ''), adv.advertiser_name, 'Unknown') AS advertiser_name,
        COALESCE(dam.account_manager_key, -1) AS account_manager_key,
        COALESCE(maa.created_at, adv.created_at, '2025-01-01 00:00:00'::TIMESTAMP) AS created_at,
        COALESCE(maa.updated_at, adv.updated_at, '2025-01-01 00:00:00'::TIMESTAMP) AS updated_at,
        COALESCE(maa.valid_from, adv.signup_date, (adv.created_at)::DATE, '2025-01-01'::DATE) AS effective_from,
        maa.valid_to AS effective_to,
        COALESCE(maa.is_current, TRUE) AS is_current
    FROM pulsetrack_silver.mongo_advertiser_accounts maa
    FULL OUTER JOIN pulsetrack_silver.advertisers adv
        ON maa.advertiser_id = adv.advertiser_id
    LEFT JOIN pulsetrack_gold.dim_account_manager dam
        ON COALESCE(maa.am_id, adv.account_manager_id) = dam.am_id
    WHERE COALESCE(maa.advertiser_id, adv.advertiser_id) IS NOT NULL
)

-- 1. Default Unknown record for NULL / unmapped advertisers
SELECT
    -1 AS advertiser_key,
    'UNKNOWN' AS advertiser_id,
    'Unknown' AS advertiser_name,
    -1 AS account_manager_key,
    '1970-01-01 00:00:00'::TIMESTAMP AS created_at,
    '1970-01-01 00:00:00'::TIMESTAMP AS updated_at,
    '1970-01-01'::DATE AS effective_from,
    NULL::DATE AS effective_to,
    TRUE AS is_current

UNION ALL

-- 2. Conformed Advertisers (SCD2 Versions)
SELECT
    ROW_NUMBER() OVER (ORDER BY advertiser_id, effective_from) AS advertiser_key,
    advertiser_id,
    advertiser_name,
    account_manager_key,
    created_at,
    updated_at,
    effective_from,
    effective_to,
    is_current
FROM mongo_history
ORDER BY advertiser_key;

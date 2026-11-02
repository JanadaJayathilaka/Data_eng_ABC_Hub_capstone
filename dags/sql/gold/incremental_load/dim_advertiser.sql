-- ============================================================================
-- PulseTrack: Gold Layer - Incremental Load SQL for dim_advertiser
-- ============================================================================
-- Synchronizes SCD2 advertiser versions from Silver MongoDB and relational sources.
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

SELECT
    mh.advertiser_id,
    mh.advertiser_name,
    mh.account_manager_key,
    mh.created_at,
    mh.updated_at,
    mh.effective_from,
    mh.effective_to,
    mh.is_current
FROM mongo_history mh
WHERE NOT EXISTS (
    SELECT 1 FROM pulsetrack_gold.dim_advertiser da
    WHERE da.advertiser_id = mh.advertiser_id
      AND da.effective_from = mh.effective_from
);

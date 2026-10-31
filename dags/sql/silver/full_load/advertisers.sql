WITH bronze_am AS (
    SELECT DISTINCT NULLIF(TRIM(am_id), '') AS am_id
    FROM pulsetrack_bronze.account_managers
    WHERE NULLIF(TRIM(am_id), '') IS NOT NULL
),

cleaned AS (
    SELECT
        NULLIF(TRIM(adv.advertiser_id), '') AS advertiser_id,
        COALESCE(NULLIF(TRIM(adv.advertiser_name), ''), 'Unknown Advertiser') AS advertiser_name,
        NULLIF(TRIM(adv.vertical), '') AS vertical,
        
        -- Referential integrity check: only keep valid account_manager_id, otherwise NULL
        CASE 
            WHEN am.am_id IS NOT NULL THEN NULLIF(TRIM(adv.account_manager_id), '')
            ELSE NULL 
        END AS account_manager_id,
        
        LOWER(NULLIF(TRIM(adv.status), '')) AS status,
        COALESCE(
            pulsetrack_silver.parse_date(adv.signup_date),
            (pulsetrack_silver.parse_timestamp(adv.created_at))::DATE,
            (pulsetrack_silver.parse_timestamp(adv.updated_at))::DATE
        ) AS signup_date,
        pulsetrack_silver.parse_country(adv.billing_country) AS billing_country,
        COALESCE(
            pulsetrack_silver.parse_timestamp(adv.created_at),
            (pulsetrack_silver.parse_date(adv.signup_date))::TIMESTAMP,
            pulsetrack_silver.parse_timestamp(adv.updated_at)
        ) AS created_at,
        COALESCE(
            pulsetrack_silver.parse_timestamp(adv.updated_at),
            pulsetrack_silver.parse_timestamp(adv.created_at),
            (pulsetrack_silver.parse_date(adv.signup_date))::TIMESTAMP
        ) AS updated_at

    FROM pulsetrack_bronze.advertisers adv
    LEFT JOIN bronze_am am 
           ON NULLIF(TRIM(adv.account_manager_id), '') = am.am_id
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY advertiser_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST
        ) AS rn
    FROM cleaned
    WHERE advertiser_id IS NOT NULL
)

SELECT
    advertiser_id,
    advertiser_name,
    vertical,
    account_manager_id,
    status,
    signup_date,
    billing_country,
    created_at,
    updated_at
FROM deduplicated
WHERE rn = 1;

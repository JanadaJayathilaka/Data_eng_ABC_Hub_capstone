WITH cleaned AS (
    SELECT
        NULLIF(TRIM(advertiser_id), '') AS advertiser_id,
        COALESCE(NULLIF(TRIM(advertiser_name), ''), 'Unknown Advertiser') AS advertiser_name,
        NULLIF(TRIM(vertical), '') AS vertical,
        NULLIF(TRIM(account_manager_id), '') AS account_manager_id,
        LOWER(NULLIF(TRIM(status), '')) AS status,

        pulsetrack_silver.parse_date(signup_date) AS signup_date,
        pulsetrack_silver.parse_country(billing_country) AS billing_country,
        pulsetrack_silver.parse_timestamp(created_at) AS created_at,
        pulsetrack_silver.parse_timestamp(updated_at) AS updated_at

    FROM pulsetrack_bronze.advertisers
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

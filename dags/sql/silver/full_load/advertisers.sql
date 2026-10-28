WITH deduplicated AS (
    SELECT
        TRIM(advertiser_id) AS advertiser_id,
        TRIM(advertiser_name) AS advertiser_name,
        TRIM(vertical) AS vertical,
        NULLIF(TRIM(account_manager_id), '') AS account_manager_id,
        LOWER(TRIM(status)) AS status,
        pulsetrack_silver.parse_date(signup_date) AS signup_date,
        pulsetrack_silver.parse_country(billing_country) AS billing_country,
        pulsetrack_silver.parse_timestamp(created_at) AS created_at,
        pulsetrack_silver.parse_timestamp(updated_at) AS updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(advertiser_id)
            ORDER BY pulsetrack_silver.parse_timestamp(updated_at) DESC NULLS LAST
        ) AS rn
    FROM pulsetrack_bronze.advertisers
    WHERE TRIM(advertiser_id) IS NOT NULL AND TRIM(advertiser_id) != ''
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

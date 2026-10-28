WITH deduplicated AS (
    SELECT
        TRIM(conversion_id) AS conversion_id,
        NULLIF(TRIM(click_id), '') AS click_id,
        TRIM(campaign_id) AS campaign_id,
        pulsetrack_silver.parse_partner_id(partner_id) AS partner_id,
        pulsetrack_silver.parse_timestamp(conversion_time) AS conversion_time,
        pulsetrack_silver.parse_money(revenue) AS revenue,
        pulsetrack_silver.parse_money(payout) AS payout,
        'USD' AS currency,
        pulsetrack_silver.parse_status(status) AS status,
        pulsetrack_silver.parse_country(country) AS country,
        pulsetrack_silver.parse_device(device) AS device,
        NULLIF(TRIM(sub_id), '') AS sub_id,
        NULLIF(TRIM(account_manager), '') AS account_manager,
        pulsetrack_silver.parse_timestamp(created_at) AS created_at,
        pulsetrack_silver.parse_timestamp(updated_at) AS updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(conversion_id)
            ORDER BY pulsetrack_silver.parse_timestamp(updated_at) DESC NULLS LAST
        ) AS rn
    FROM pulsetrack_bronze.conversions
    WHERE TRIM(conversion_id) IS NOT NULL 
      AND TRIM(conversion_id) != ''
      -- Filter out is_test truthy records ('test', '1', 'true')
      AND (is_test IS NULL OR LOWER(TRIM(is_test)) NOT IN ('test', '1', 'true', 't', 'yes', 'y'))
)
SELECT
    conversion_id,
    click_id,
    campaign_id,
    partner_id,
    conversion_time,
    revenue,
    payout,
    currency,
    status,
    country,
    device,
    sub_id,
    account_manager,
    created_at,
    updated_at
FROM deduplicated
WHERE rn = 1;

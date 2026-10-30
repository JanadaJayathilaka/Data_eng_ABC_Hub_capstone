WITH cleaned AS (
    SELECT
        NULLIF(TRIM(conversion_id), '') AS conversion_id,
        NULLIF(TRIM(click_id), '') AS click_id,
        NULLIF(TRIM(campaign_id), '') AS campaign_id,
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
        pulsetrack_silver.parse_timestamp(updated_at) AS updated_at

    FROM pulsetrack_bronze.conversions
    WHERE (is_test IS NULL OR LOWER(TRIM(is_test)) NOT IN ('test', '1', 'true', 't', 'yes', 'y'))
      AND (
          pulsetrack_silver.parse_timestamp(updated_at) > '{{ last_watermark }}'::TIMESTAMP
          OR pulsetrack_silver.parse_timestamp(created_at) > '{{ last_watermark }}'::TIMESTAMP
      )
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY conversion_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST
        ) AS rn
    FROM cleaned
    WHERE conversion_id IS NOT NULL
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

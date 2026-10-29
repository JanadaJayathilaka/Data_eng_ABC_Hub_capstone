WITH cleaned AS (
    SELECT
        NULLIF(TRIM(click_id), '') AS click_id,
        NULLIF(TRIM(impression_id), '') AS impression_id,
        NULLIF(TRIM(campaign_id), '') AS campaign_id,
        NULLIF(TRIM(creative_id), '') AS creative_id,
        pulsetrack_silver.parse_partner_id(partner_id) AS partner_id,
        pulsetrack_silver.parse_timestamp(click_time) AS click_time,
        pulsetrack_silver.parse_country(country) AS country,
        pulsetrack_silver.parse_device(device) AS device,
        pulsetrack_silver.parse_boolean(is_unique) AS is_unique,
        pulsetrack_silver.parse_timestamp(created_at) AS created_at,
        pulsetrack_silver.parse_timestamp(updated_at) AS updated_at

    FROM pulsetrack_bronze.clicks
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY click_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST
        ) AS rn
    FROM cleaned
    WHERE click_id IS NOT NULL
)

SELECT
    click_id,
    impression_id,
    campaign_id,
    creative_id,
    partner_id,
    click_time,
    country,
    device,
    is_unique,
    created_at,
    updated_at
FROM deduplicated
WHERE rn = 1;

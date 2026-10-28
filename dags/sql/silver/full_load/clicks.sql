WITH deduplicated AS (
    SELECT
        TRIM(click_id) AS click_id,
        NULLIF(TRIM(impression_id), '') AS impression_id,
        TRIM(campaign_id) AS campaign_id,
        NULLIF(TRIM(creative_id), '') AS creative_id,
        pulsetrack_silver.parse_partner_id(partner_id) AS partner_id,
        pulsetrack_silver.parse_timestamp(click_time) AS click_time,
        pulsetrack_silver.parse_country(country) AS country,
        pulsetrack_silver.parse_device(device) AS device,
        pulsetrack_silver.parse_boolean(is_unique) AS is_unique,
        pulsetrack_silver.parse_timestamp(created_at) AS created_at,
        pulsetrack_silver.parse_timestamp(updated_at) AS updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(click_id)
            ORDER BY pulsetrack_silver.parse_timestamp(updated_at) DESC NULLS LAST
        ) AS rn
    FROM pulsetrack_bronze.clicks
    WHERE TRIM(click_id) IS NOT NULL AND TRIM(click_id) != ''
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

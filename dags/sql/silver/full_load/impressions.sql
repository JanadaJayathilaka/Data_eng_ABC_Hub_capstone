WITH deduplicated AS (
    SELECT
        TRIM(impression_id) AS impression_id,
        TRIM(campaign_id) AS campaign_id,
        NULLIF(TRIM(creative_id), '') AS creative_id,
        pulsetrack_silver.parse_partner_id(partner_id) AS partner_id,
        pulsetrack_silver.parse_timestamp(impression_time) AS impression_time,
        pulsetrack_silver.parse_country(country) AS country,
        pulsetrack_silver.parse_device(device) AS device,
        pulsetrack_silver.parse_timestamp(created_at) AS created_at,
        pulsetrack_silver.parse_timestamp(updated_at) AS updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(impression_id)
            ORDER BY pulsetrack_silver.parse_timestamp(updated_at) DESC NULLS LAST
        ) AS rn
    FROM pulsetrack_bronze.impressions
    WHERE TRIM(impression_id) IS NOT NULL AND TRIM(impression_id) != ''
)
SELECT
    impression_id,
    campaign_id,
    creative_id,
    partner_id,
    impression_time,
    country,
    device,
    created_at,
    updated_at
FROM deduplicated
WHERE rn = 1;

WITH cleaned AS (
    SELECT
        NULLIF(TRIM(impression_id), '') AS impression_id,
        NULLIF(TRIM(campaign_id), '') AS campaign_id,
        NULLIF(TRIM(creative_id), '') AS creative_id,
        pulsetrack_silver.parse_partner_id(partner_id) AS partner_id,
        COALESCE(
            pulsetrack_silver.parse_timestamp(impression_time),
            pulsetrack_silver.parse_timestamp(created_at),
            pulsetrack_silver.parse_timestamp(updated_at)
        ) AS impression_time,
        pulsetrack_silver.parse_country(country) AS country,
        pulsetrack_silver.parse_device(device) AS device,
        COALESCE(
            pulsetrack_silver.parse_timestamp(created_at),
            pulsetrack_silver.parse_timestamp(impression_time),
            pulsetrack_silver.parse_timestamp(updated_at)
        ) AS created_at,
        COALESCE(
            pulsetrack_silver.parse_timestamp(updated_at),
            pulsetrack_silver.parse_timestamp(created_at),
            pulsetrack_silver.parse_timestamp(impression_time)
        ) AS updated_at

    FROM pulsetrack_bronze.impressions
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY impression_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST
        ) AS rn
    FROM cleaned
    WHERE impression_id IS NOT NULL
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

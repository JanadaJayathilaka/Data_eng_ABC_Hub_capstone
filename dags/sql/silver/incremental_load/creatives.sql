WITH cleaned AS (
    SELECT
        NULLIF(TRIM(creative_id), '') AS creative_id,
        NULLIF(TRIM(campaign_id), '') AS campaign_id,
        NULLIF(TRIM(creative_name), '') AS creative_name,
        NULLIF(TRIM(format), '') AS format,
        NULLIF(TRIM(size), '') AS size,
        NULLIF(TRIM(landing_url), '') AS landing_url,
        pulsetrack_silver.parse_timestamp(created_at) AS created_at,
        pulsetrack_silver.parse_timestamp(updated_at) AS updated_at

    FROM pulsetrack_bronze.creatives
    WHERE pulsetrack_silver.parse_timestamp(updated_at) > '{{ last_watermark }}'::TIMESTAMP
       OR pulsetrack_silver.parse_timestamp(created_at) > '{{ last_watermark }}'::TIMESTAMP
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY creative_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST
        ) AS rn
    FROM cleaned
    WHERE creative_id IS NOT NULL
)

SELECT
    creative_id,
    campaign_id,
    creative_name,
    format,
    size,
    landing_url,
    created_at,
    updated_at
FROM deduplicated
WHERE rn = 1;

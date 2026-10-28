WITH deduplicated AS (
    SELECT
        TRIM(creative_id) AS creative_id,
        TRIM(campaign_id) AS campaign_id,
        TRIM(creative_name) AS creative_name,
        TRIM(format) AS format,
        TRIM(size) AS size,
        TRIM(landing_url) AS landing_url,
        pulsetrack_silver.parse_timestamp(created_at) AS created_at,
        pulsetrack_silver.parse_timestamp(updated_at) AS updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(creative_id)
            ORDER BY pulsetrack_silver.parse_timestamp(updated_at) DESC NULLS LAST
        ) AS rn
    FROM pulsetrack_bronze.creatives
    WHERE TRIM(creative_id) IS NOT NULL AND TRIM(creative_id) != ''
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

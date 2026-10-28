WITH deduplicated AS (
    SELECT
        TRIM(campaign_id) AS campaign_id,
        TRIM(campaign_name) AS campaign_name,
        TRIM(offer_id) AS offer_id,
        TRIM(advertiser_id) AS advertiser_id,
        UPPER(TRIM(price_type)) AS price_type,
        pulsetrack_silver.parse_date(start_date) AS start_date,
        pulsetrack_silver.parse_date(end_date) AS end_date,
        pulsetrack_silver.parse_timestamp(created_at) AS created_at,
        pulsetrack_silver.parse_timestamp(updated_at) AS updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(campaign_id)
            ORDER BY pulsetrack_silver.parse_timestamp(updated_at) DESC NULLS LAST
        ) AS rn
    FROM pulsetrack_bronze.campaigns
    WHERE TRIM(campaign_id) IS NOT NULL AND TRIM(campaign_id) != ''
)
SELECT
    campaign_id,
    campaign_name,
    offer_id,
    advertiser_id,
    price_type,
    start_date,
    end_date,
    created_at,
    updated_at
FROM deduplicated
WHERE rn = 1;

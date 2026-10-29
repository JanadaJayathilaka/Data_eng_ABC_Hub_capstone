WITH cleaned AS (
    SELECT
        NULLIF(TRIM(campaign_id), '') AS campaign_id,
        NULLIF(TRIM(campaign_name), '') AS campaign_name,
        NULLIF(TRIM(offer_id), '') AS offer_id,
        NULLIF(TRIM(advertiser_id), '') AS advertiser_id,
        UPPER(NULLIF(TRIM(price_type), '')) AS price_type,

        pulsetrack_silver.parse_date(start_date) AS start_date,
        pulsetrack_silver.parse_date(end_date) AS end_date,
        pulsetrack_silver.parse_timestamp(created_at) AS created_at,
        pulsetrack_silver.parse_timestamp(updated_at) AS updated_at

    FROM pulsetrack_bronze.campaigns
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY campaign_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST
        ) AS rn
    FROM cleaned
    WHERE campaign_id IS NOT NULL
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

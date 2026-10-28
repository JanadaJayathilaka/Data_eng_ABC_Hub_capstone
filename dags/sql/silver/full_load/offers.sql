WITH deduplicated AS (
    SELECT
        TRIM(offer_id) AS offer_id,
        TRIM(offer_name) AS offer_name,
        TRIM(advertiser_id) AS advertiser_id,
        TRIM(vertical) AS vertical,
        pulsetrack_silver.parse_money(default_payout) AS default_payout,
        UPPER(TRIM(payout_type)) AS payout_type,
        pulsetrack_silver.parse_timestamp(created_at) AS created_at,
        pulsetrack_silver.parse_timestamp(updated_at) AS updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(offer_id)
            ORDER BY pulsetrack_silver.parse_timestamp(updated_at) DESC NULLS LAST
        ) AS rn
    FROM pulsetrack_bronze.offers
    WHERE TRIM(offer_id) IS NOT NULL AND TRIM(offer_id) != ''
)
SELECT
    offer_id,
    offer_name,
    advertiser_id,
    vertical,
    default_payout,
    payout_type,
    created_at,
    updated_at
FROM deduplicated
WHERE rn = 1;

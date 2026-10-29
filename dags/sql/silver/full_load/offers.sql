WITH cleaned AS (
    SELECT
        NULLIF(TRIM(offer_id), '') AS offer_id,
        NULLIF(TRIM(offer_name), '') AS offer_name,
        NULLIF(TRIM(advertiser_id), '') AS advertiser_id,
        NULLIF(TRIM(vertical), '') AS vertical,
        pulsetrack_silver.parse_money(default_payout) AS default_payout,
        UPPER(NULLIF(TRIM(payout_type), '')) AS payout_type,
        pulsetrack_silver.parse_timestamp(created_at) AS created_at,
        pulsetrack_silver.parse_timestamp(updated_at) AS updated_at

    FROM pulsetrack_bronze.offers
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY offer_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST
        ) AS rn
    FROM cleaned
    WHERE offer_id IS NOT NULL
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

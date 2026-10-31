WITH bronze_offers AS (
    SELECT DISTINCT
        NULLIF(TRIM(offer_id), '') AS offer_id,
        NULLIF(TRIM(advertiser_id), '') AS advertiser_id
    FROM pulsetrack_bronze.offers
    WHERE NULLIF(TRIM(offer_id), '') IS NOT NULL
),

bronze_advertisers AS (
    SELECT DISTINCT NULLIF(TRIM(advertiser_id), '') AS advertiser_id
    FROM pulsetrack_bronze.advertisers
    WHERE NULLIF(TRIM(advertiser_id), '') IS NOT NULL
),

cleaned AS (
    SELECT
        NULLIF(TRIM(c.campaign_id), '') AS campaign_id,
        NULLIF(TRIM(c.campaign_name), '') AS campaign_name,

        -- Referential integrity check on offer_id
        CASE 
            WHEN o.offer_id IS NOT NULL THEN NULLIF(TRIM(c.offer_id), '')
            ELSE NULL 
        END AS offer_id,

        -- Referential integrity & orphan resolution
        COALESCE(adv.advertiser_id, o_adv.advertiser_id) AS advertiser_id,

        -- Standardize price_type to lowercase and trimmed ('cpa', 'cpc', 'cpm', 'fixed')
        LOWER(NULLIF(TRIM(c.price_type), '')) AS price_type,

        COALESCE(
            pulsetrack_silver.parse_date(c.start_date),
            (pulsetrack_silver.parse_timestamp(c.created_at))::DATE,
            (pulsetrack_silver.parse_timestamp(c.updated_at))::DATE
        ) AS start_date,
        pulsetrack_silver.parse_date(c.end_date) AS end_date,
        COALESCE(
            pulsetrack_silver.parse_timestamp(c.created_at),
            (pulsetrack_silver.parse_date(c.start_date))::TIMESTAMP,
            pulsetrack_silver.parse_timestamp(c.updated_at)
        ) AS created_at,
        COALESCE(
            pulsetrack_silver.parse_timestamp(c.updated_at),
            pulsetrack_silver.parse_timestamp(c.created_at),
            (pulsetrack_silver.parse_date(c.start_date))::TIMESTAMP
        ) AS updated_at

    FROM pulsetrack_bronze.campaigns c
    LEFT JOIN bronze_offers o 
           ON NULLIF(TRIM(c.offer_id), '') = o.offer_id
    LEFT JOIN bronze_advertisers adv 
           ON NULLIF(TRIM(c.advertiser_id), '') = adv.advertiser_id
    LEFT JOIN bronze_advertisers o_adv
           ON o.advertiser_id = o_adv.advertiser_id
    WHERE COALESCE(
            pulsetrack_silver.parse_timestamp(c.updated_at),
            pulsetrack_silver.parse_timestamp(c.created_at),
            (pulsetrack_silver.parse_date(c.start_date))::TIMESTAMP
        ) > '{{ last_watermark }}'::TIMESTAMP
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

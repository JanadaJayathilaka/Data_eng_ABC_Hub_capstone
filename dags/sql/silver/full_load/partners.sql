WITH bronze_partners AS (
    SELECT
        pulsetrack_silver.parse_partner_id(partner_id) AS partner_id,
        NULLIF(TRIM(partner_name), '') AS partner_name,
        LOWER(TRIM(channel)) AS channel,
        NULLIF(TRIM(media_buyer), '') AS media_buyer,
        pulsetrack_silver.parse_date(join_date) AS join_date,
        NULLIF(TRIM(payment_terms), '') AS payment_terms,
        pulsetrack_silver.parse_timestamp(created_at) AS created_at,
        pulsetrack_silver.parse_timestamp(updated_at) AS updated_at
    FROM pulsetrack_bronze.partners
    WHERE pulsetrack_silver.parse_partner_id(partner_id) IS NOT NULL
),
mongo_partners AS (
    SELECT
        pulsetrack_silver.parse_partner_id(document->>'partner_id') AS partner_id,
        NULLIF(TRIM(document->>'name'), '') AS partner_name,
        LOWER(TRIM(document->'channels'->>0)) AS channel,
        NULLIF(TRIM(document->'contact'->>'person'), '') AS media_buyer,
        pulsetrack_silver.parse_date(document->'joinedAt'->>'$date') AS join_date,
        NULLIF(TRIM(document->'payment'->>'terms'), '') AS payment_terms,
        pulsetrack_silver.parse_timestamp(document->'createdAt'->>'$date') AS created_at,
        pulsetrack_silver.parse_timestamp(document->'updatedAt'->>'$date') AS updated_at
    FROM pulsetrack_bronze.mongo_partner_profiles
    WHERE pulsetrack_silver.parse_partner_id(document->>'partner_id') IS NOT NULL
),
combined AS (
    SELECT
        COALESCE(b.partner_id, m.partner_id) AS partner_id,
        COALESCE(b.partner_name, m.partner_name, 'Unknown Partner') AS partner_name,
        COALESCE(b.channel, m.channel) AS channel,
        COALESCE(b.media_buyer, m.media_buyer) AS media_buyer,
        COALESCE(b.join_date, m.join_date) AS join_date,
        COALESCE(b.payment_terms, m.payment_terms) AS payment_terms,
        COALESCE(b.created_at, m.created_at) AS created_at,
        COALESCE(b.updated_at, m.updated_at) AS updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(b.partner_id, m.partner_id)
            ORDER BY
                COALESCE(b.updated_at, m.updated_at) DESC NULLS LAST,
                COALESCE(b.created_at, m.created_at) DESC NULLS LAST
        ) AS rn
    FROM bronze_partners b
    FULL OUTER JOIN mongo_partners m ON b.partner_id = m.partner_id
)
SELECT
    partner_id,
    partner_name,
    channel,
    media_buyer,
    join_date,
    payment_terms,
    created_at,
    updated_at
FROM combined
WHERE rn = 1;

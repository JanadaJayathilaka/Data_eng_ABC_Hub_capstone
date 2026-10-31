WITH cleaned AS (
    SELECT
        pulsetrack_silver.parse_partner_id(COALESCE(partner_id, document->>'partner_id')) AS partner_id,
        NULLIF(TRIM(document->>'name'), '') AS partner_name,
        NULLIF(TRIM(document->'contact'->>'person'), '') AS contact_person,
        NULLIF(TRIM(document->'contact'->>'email'), '') AS contact_email,
        NULLIF(TRIM(document->'contact'->>'phone'), '') AS contact_phone,
        LOWER(NULLIF(TRIM(document->'channels'->>0), '')) AS primary_channel,
        NULLIF(TRIM(document->'payment'->>'terms'), '') AS payment_terms,
        pulsetrack_silver.parse_money(document->'payment'->>'min_payout') AS min_payout,
        LOWER(NULLIF(TRIM(document->'payment'->>'method'), '')) AS payment_method,
        COALESCE(
            pulsetrack_silver.parse_date(COALESCE(document->'joinedAt'->>'$date', document->>'joinedAt')),
            (pulsetrack_silver.parse_timestamp(COALESCE(document->'createdAt'->>'$date', document->>'createdAt')))::DATE,
            (pulsetrack_silver.parse_timestamp(COALESCE(document->'updatedAt'->>'$date', document->>'updatedAt')))::DATE
        ) AS joined_at,
        COALESCE(
            pulsetrack_silver.parse_timestamp(COALESCE(document->'createdAt'->>'$date', document->>'createdAt')),
            (pulsetrack_silver.parse_date(COALESCE(document->'joinedAt'->>'$date', document->>'joinedAt')))::TIMESTAMP,
            pulsetrack_silver.parse_timestamp(COALESCE(document->'updatedAt'->>'$date', document->>'updatedAt'))
        ) AS created_at,
        COALESCE(
            pulsetrack_silver.parse_timestamp(COALESCE(document->'updatedAt'->>'$date', document->>'updatedAt')),
            pulsetrack_silver.parse_timestamp(COALESCE(document->'createdAt'->>'$date', document->>'createdAt')),
            (pulsetrack_silver.parse_date(COALESCE(document->'joinedAt'->>'$date', document->>'joinedAt')))::TIMESTAMP
        ) AS updated_at
    FROM pulsetrack_bronze.mongo_partner_profiles
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY partner_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST
        ) AS rn
    FROM cleaned
    WHERE partner_id IS NOT NULL
)

SELECT
    partner_id,
    partner_name,
    contact_person,
    contact_email,
    contact_phone,
    primary_channel,
    payment_terms,
    min_payout,
    payment_method,
    joined_at,
    created_at,
    updated_at
FROM deduplicated
WHERE rn = 1;

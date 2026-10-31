WITH unnested AS (
    SELECT
        TRIM(document->>'advertiser_id') AS advertiser_id,
        NULLIF(TRIM(document->>'legal_name'), '') AS legal_name,
        NULLIF(TRIM(document->>'display_name'), '') AS display_name,
        NULLIF(TRIM(document->>'vertical'), '') AS vertical,
        LOWER(TRIM(document->>'status')) AS status,
        pulsetrack_silver.parse_country(document->'billing'->>'country') AS billing_country,
        TRIM(history->>'am_id') AS am_id,
        TRIM(history->>'am_name') AS am_name,
        COALESCE(
            pulsetrack_silver.parse_date(history->>'from'),
            (pulsetrack_silver.parse_timestamp(COALESCE(document->'createdAt'->>'$date', document->>'createdAt')))::DATE,
            (pulsetrack_silver.parse_timestamp(COALESCE(document->'updatedAt'->>'$date', document->>'updatedAt')))::DATE
        ) AS valid_from,
        pulsetrack_silver.parse_date(history->>'to') AS valid_to,
        (history->>'to' IS NULL) AS is_current,
        COALESCE(
            pulsetrack_silver.parse_timestamp(COALESCE(document->'createdAt'->>'$date', document->>'createdAt')),
            (pulsetrack_silver.parse_date(history->>'from'))::TIMESTAMP,
            pulsetrack_silver.parse_timestamp(COALESCE(document->'updatedAt'->>'$date', document->>'updatedAt'))
        ) AS created_at,
        COALESCE(
            pulsetrack_silver.parse_timestamp(COALESCE(document->'updatedAt'->>'$date', document->>'updatedAt')),
            pulsetrack_silver.parse_timestamp(COALESCE(document->'createdAt'->>'$date', document->>'createdAt')),
            (pulsetrack_silver.parse_date(history->>'from'))::TIMESTAMP
        ) AS updated_at
    FROM pulsetrack_bronze.mongo_advertiser_accounts,
    LATERAL jsonb_array_elements(document->'account_manager_history') AS history
)
SELECT DISTINCT
    advertiser_id,
    legal_name,
    display_name,
    vertical,
    status,
    billing_country,
    am_id,
    am_name,
    valid_from,
    valid_to,
    is_current,
    created_at,
    updated_at
FROM unnested
WHERE advertiser_id IS NOT NULL AND am_id IS NOT NULL;

WITH cleaned AS (
    SELECT
        COALESCE(NULLIF(TRIM(event_id), ''), NULLIF(TRIM(event_payload->>'event_id'), '')) AS event_id,
        LOWER(NULLIF(TRIM(event_payload->>'event_type'), '')) AS event_type,
        COALESCE(NULLIF(TRIM(conversion_id), ''), NULLIF(TRIM(event_payload->>'conversion_id'), '')) AS conversion_id,
        NULLIF(TRIM(event_payload->>'campaign_id'), '') AS campaign_id,
        pulsetrack_silver.parse_partner_id(event_payload->>'partner_id') AS partner_id,
        NULLIF(TRIM(event_payload->>'click_id'), '') AS click_id,
        pulsetrack_silver.parse_money(COALESCE(event_payload->>'revenue_amount', event_payload->>'revenue')) AS revenue,
        pulsetrack_silver.parse_money(event_payload->>'payout') AS payout,
        COALESCE(NULLIF(TRIM(event_payload->>'currency'), ''), 'USD') AS currency,
        pulsetrack_silver.parse_status(event_payload->>'status') AS status,
        COALESCE(
            pulsetrack_silver.parse_timestamp(event_payload->>'occurred_at'),
            to_timestamp(message_timestamp / 1000.0) AT TIME ZONE 'UTC'
        ) AS occurred_at,
        partition_id,
        offset_id,
        COALESCE(
            to_timestamp(message_timestamp / 1000.0) AT TIME ZONE 'UTC',
            pulsetrack_silver.parse_timestamp(event_payload->>'occurred_at')
        ) AS message_timestamp
    FROM pulsetrack_bronze.kafka_pulsetrack_conversions
    WHERE (event_payload->>'is_test' IS NULL OR LOWER(TRIM(event_payload->>'is_test')) NOT IN ('test', '1', 'true', 't', 'yes', 'y'))
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY
                offset_id DESC,
                message_timestamp DESC NULLS LAST
        ) AS rn
    FROM cleaned
    WHERE event_id IS NOT NULL
)

SELECT
    event_id,
    event_type,
    conversion_id,
    campaign_id,
    partner_id,
    click_id,
    revenue,
    payout,
    currency,
    status,
    occurred_at,
    partition_id,
    offset_id,
    message_timestamp
FROM deduplicated
WHERE rn = 1;

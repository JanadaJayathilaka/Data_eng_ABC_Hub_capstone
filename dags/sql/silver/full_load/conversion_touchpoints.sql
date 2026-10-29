WITH cleaned AS (
    SELECT
        NULLIF(TRIM(touchpoint_id), '') AS touchpoint_id,
        NULLIF(TRIM(conversion_id), '') AS conversion_id,
        NULLIF(TRIM(click_id), '') AS click_id,
        LOWER(NULLIF(TRIM(touch_position), '')) AS touch_position,
        pulsetrack_silver.parse_money(weight) AS weight,
        pulsetrack_silver.parse_timestamp(created_at) AS created_at,
        pulsetrack_silver.parse_timestamp(updated_at) AS updated_at

    FROM pulsetrack_bronze.conversion_touchpoints
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY touchpoint_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST
        ) AS rn
    FROM cleaned
    WHERE touchpoint_id IS NOT NULL
)

SELECT
    touchpoint_id,
    conversion_id,
    click_id,
    touch_position,
    weight,
    created_at,
    updated_at
FROM deduplicated
WHERE rn = 1;

WITH deduplicated AS (
    SELECT
        TRIM(touchpoint_id) AS touchpoint_id,
        TRIM(conversion_id) AS conversion_id,
        TRIM(click_id) AS click_id,
        LOWER(TRIM(touch_position)) AS touch_position,
        pulsetrack_silver.parse_money(weight) AS weight,
        pulsetrack_silver.parse_timestamp(created_at) AS created_at,
        pulsetrack_silver.parse_timestamp(updated_at) AS updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(touchpoint_id)
            ORDER BY pulsetrack_silver.parse_timestamp(updated_at) DESC NULLS LAST
        ) AS rn
    FROM pulsetrack_bronze.conversion_touchpoints
    WHERE TRIM(touchpoint_id) IS NOT NULL AND TRIM(touchpoint_id) != ''
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

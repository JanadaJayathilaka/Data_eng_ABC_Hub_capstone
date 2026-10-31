WITH cleaned AS (
    SELECT
        NULLIF(TRIM(adjustment_id), '') AS adjustment_id,
        NULLIF(TRIM(conversion_id), '') AS conversion_id,
        pulsetrack_silver.parse_money(delta_amount) AS delta_amount,
        NULLIF(TRIM(reason), '') AS reason,
        COALESCE(
            pulsetrack_silver.parse_timestamp(adjusted_at),
            pulsetrack_silver.parse_timestamp(created_at),
            pulsetrack_silver.parse_timestamp(updated_at)
        ) AS adjusted_at,
        COALESCE(
            pulsetrack_silver.parse_timestamp(created_at),
            pulsetrack_silver.parse_timestamp(adjusted_at),
            pulsetrack_silver.parse_timestamp(updated_at)
        ) AS created_at,
        COALESCE(
            pulsetrack_silver.parse_timestamp(updated_at),
            pulsetrack_silver.parse_timestamp(created_at),
            pulsetrack_silver.parse_timestamp(adjusted_at)
        ) AS updated_at

    FROM pulsetrack_bronze.revenue_adjustments
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY adjustment_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST
        ) AS rn
    FROM cleaned
    WHERE adjustment_id IS NOT NULL
)

SELECT
    adjustment_id,
    conversion_id,
    delta_amount,
    reason,
    adjusted_at,
    created_at,
    updated_at
FROM deduplicated
WHERE rn = 1;

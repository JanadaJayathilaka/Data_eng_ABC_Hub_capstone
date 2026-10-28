WITH deduplicated AS (
    SELECT
        TRIM(adjustment_id) AS adjustment_id,
        TRIM(conversion_id) AS conversion_id,
        pulsetrack_silver.parse_money(delta_amount) AS delta_amount,
        TRIM(reason) AS reason,
        pulsetrack_silver.parse_timestamp(adjusted_at) AS adjusted_at,
        pulsetrack_silver.parse_timestamp(created_at) AS created_at,
        pulsetrack_silver.parse_timestamp(updated_at) AS updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(adjustment_id)
            ORDER BY pulsetrack_silver.parse_timestamp(updated_at) DESC NULLS LAST
        ) AS rn
    FROM pulsetrack_bronze.revenue_adjustments
    WHERE TRIM(adjustment_id) IS NOT NULL AND TRIM(adjustment_id) != ''
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

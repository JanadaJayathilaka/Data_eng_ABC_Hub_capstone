WITH cleaned AS (
    SELECT
        NULLIF(TRIM(am_id), '') AS am_id,
        NULLIF(TRIM(am_name), '') AS am_name,
        NULLIF(TRIM(team), '') AS team,
        NULLIF(TRIM(region), '') AS region,

        COALESCE(
            pulsetrack_silver.parse_date(hire_date),
            (pulsetrack_silver.parse_timestamp(created_at))::DATE,
            (pulsetrack_silver.parse_timestamp(updated_at))::DATE
        ) AS hire_date,

        COALESCE(
            pulsetrack_silver.parse_timestamp(created_at),
            (pulsetrack_silver.parse_date(hire_date))::TIMESTAMP,
            pulsetrack_silver.parse_timestamp(updated_at)
        ) AS created_at,

        COALESCE(
            pulsetrack_silver.parse_timestamp(updated_at),
            pulsetrack_silver.parse_timestamp(created_at),
            (pulsetrack_silver.parse_date(hire_date))::TIMESTAMP
        ) AS updated_at

    FROM pulsetrack_bronze.account_managers
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY am_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST
        ) AS rn
    FROM cleaned
    WHERE am_id IS NOT NULL
)

SELECT
    am_id,
    am_name,
    team,
    region,
    hire_date,
    created_at,
    updated_at
FROM deduplicated
WHERE rn = 1;
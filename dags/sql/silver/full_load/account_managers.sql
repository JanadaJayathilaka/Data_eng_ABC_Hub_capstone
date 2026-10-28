WITH deduplicated AS (
    SELECT
        TRIM(am_id) AS am_id,
        NULLIF(TRIM(am_name), '') AS am_name,
        TRIM(team) AS team,
        TRIM(region) AS region,
        pulsetrack_silver.parse_date(hire_date) AS hire_date,
        pulsetrack_silver.parse_timestamp(created_at) AS created_at,
        pulsetrack_silver.parse_timestamp(updated_at) AS updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(am_id)
            ORDER BY pulsetrack_silver.parse_timestamp(updated_at) DESC NULLS LAST
        ) AS rn
    FROM pulsetrack_bronze.account_managers
    WHERE TRIM(am_id) IS NOT NULL AND TRIM(am_id) != ''
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

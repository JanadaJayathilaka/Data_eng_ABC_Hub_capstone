-- ============================================================================
-- PulseTrack: Gold Layer - Full Load SQL for dim_date
-- ============================================================================
-- Generates a comprehensive calendar date dimension covering the platform's
-- entire historical and projected event window (2024 to 2027), plus a default
-- unknown row (-1) for handling missing/null event dates gracefully.
-- ============================================================================

WITH date_spine AS (
    SELECT generate_series(
        '2024-01-01'::DATE,
        '2026-12-31'::DATE,
        '1 day'::INTERVAL
    )::DATE AS d
)

-- 1. Default Unknown record for NULL / invalid dates in facts
SELECT
    -1 AS date_key,
    '1970-01-01'::DATE AS full_date,
    0 AS day,
    'Unknown' AS day_name,
    0 AS week,
    0 AS month,
    'Unknown' AS month_name,
    0 AS quarter,
    0 AS year

UNION ALL

-- 2. Calendar Dates
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_key,
    d AS full_date,
    EXTRACT(DAY FROM d)::INTEGER AS day,
    TRIM(TO_CHAR(d, 'Day')) AS day_name,
    EXTRACT(WEEK FROM d)::INTEGER AS week,
    EXTRACT(MONTH FROM d)::INTEGER AS month,
    TRIM(TO_CHAR(d, 'Month')) AS month_name,
    EXTRACT(QUARTER FROM d)::INTEGER AS quarter,
    EXTRACT(YEAR FROM d)::INTEGER AS year
FROM date_spine
ORDER BY date_key;

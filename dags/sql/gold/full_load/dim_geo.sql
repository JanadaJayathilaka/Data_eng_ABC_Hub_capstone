-- ============================================================================
-- PulseTrack: Gold Layer - Full Load SQL for dim_geo
-- ============================================================================
-- Extracts all distinct standardized country codes appearing across Silver
-- facts and dimensions (impressions, clicks, conversions, advertisers), maps
-- them to their regional groupings, and includes a default Unknown row (-1).
-- ============================================================================

WITH distinct_countries AS (
    SELECT DISTINCT country
    FROM (
        SELECT country FROM pulsetrack_silver.impressions WHERE country IS NOT NULL AND TRIM(country) != ''
        UNION
        SELECT country FROM pulsetrack_silver.clicks WHERE country IS NOT NULL AND TRIM(country) != ''
        UNION
        SELECT country FROM pulsetrack_silver.conversions WHERE country IS NOT NULL AND TRIM(country) != ''
        UNION
        SELECT billing_country AS country FROM pulsetrack_silver.advertisers WHERE billing_country IS NOT NULL AND TRIM(billing_country) != ''
    ) sub
),

mapped_geo AS (
    SELECT
        country,
        CASE
            WHEN country IN ('US', 'USA', 'United States') THEN 'US'
            WHEN country IN ('CA', 'CAN', 'Canada') THEN 'CA'
            WHEN country IN ('GB', 'UK', 'United Kingdom') THEN 'GB'
            WHEN country IN ('AU', 'AUS', 'Australia') THEN 'AU'
            ELSE 'Other'
        END AS region,
        'Unknown' AS city,
        'Unknown' AS postal_code
    FROM distinct_countries
)

-- 1. Default Unknown record for NULL / unmapped countries in facts
SELECT
    -1 AS geo_key,
    'Unknown' AS country,
    'Unknown' AS region,
    'Unknown' AS city,
    'Unknown' AS postal_code

UNION ALL

-- 2. Conformed Countries
SELECT
    ROW_NUMBER() OVER (ORDER BY country) AS geo_key,
    country,
    region,
    city,
    postal_code
FROM mapped_geo
ORDER BY geo_key;

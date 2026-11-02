-- ============================================================================
-- PulseTrack: Gold Layer - Incremental Load SQL for dim_geo
-- ============================================================================
-- Discovers new distinct country codes appearing in Silver and inserts them.
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

new_geo AS (
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
    WHERE country NOT IN (SELECT country FROM pulsetrack_gold.dim_geo)
)

SELECT
    country,
    region,
    city,
    postal_code
FROM new_geo
ON CONFLICT (country) DO NOTHING;

-- ============================================================================
-- PulseTrack: Gold Layer - Incremental Load SQL for dim_device
-- ============================================================================
-- Discovers new device combinations from Silver clicks and inserts them.
-- ============================================================================

WITH distinct_devices AS (
    SELECT DISTINCT
        COALESCE(NULLIF(TRIM(device), ''), 'unknown') AS device_type,
        COALESCE(NULLIF(TRIM(operating_system), ''), 'Unknown') AS operating_system,
        COALESCE(NULLIF(TRIM(browser), ''), 'Unknown') AS browser,
        CASE
            WHEN LOWER(operating_system) LIKE '%android%' THEN 'Android Phone'
            WHEN LOWER(operating_system) LIKE '%ios%' THEN 'Apple iPhone'
            WHEN LOWER(operating_system) LIKE '%windows%' THEN 'PC'
            WHEN LOWER(operating_system) LIKE '%mac%' THEN 'Macintosh'
            WHEN LOWER(device) = 'tablet' THEN 'Tablet'
            ELSE 'Unknown Model'
        END AS device_model
    FROM pulsetrack_silver.clicks
    WHERE device IS NOT NULL OR operating_system IS NOT NULL OR browser IS NOT NULL
)

SELECT
    d.device_type,
    d.operating_system,
    d.browser,
    d.device_model
FROM distinct_devices d
WHERE NOT EXISTS (
    SELECT 1 FROM pulsetrack_gold.dim_device dd
    WHERE dd.device_type = d.device_type
      AND dd.operating_system = d.operating_system
      AND dd.browser = d.browser
)
ON CONFLICT (device_type, operating_system, browser) DO NOTHING;

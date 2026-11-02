-- ============================================================================
-- PulseTrack: Gold Layer - Full Load SQL for dim_device
-- ============================================================================
-- Extracts granular device attributes (device_type, operating_system, browser,
-- device_model) parsed from user-agent headers in pulsetrack_silver.clicks,
-- combined with higher-level device categories for impressions and conversions,
-- and includes an Unknown default record (-1).
-- ============================================================================

WITH click_devices AS (
    SELECT DISTINCT
        device AS device_type,
        operating_system,
        browser,
        CASE
            WHEN operating_system = 'iOS 16' THEN 'Apple iPhone'
            WHEN operating_system = 'macOS' THEN 'Macintosh'
            WHEN operating_system = 'Windows 10' THEN 'PC'
            WHEN operating_system = 'Android 13' THEN 'Android Phone'
            WHEN operating_system = 'Tablet OS' THEN 'Tablet'
            ELSE 'Unknown Model'
        END AS device_model
    FROM pulsetrack_silver.clicks
    WHERE device IS NOT NULL AND TRIM(device) != ''
),

general_devices AS (
    SELECT DISTINCT
        device AS device_type,
        'Unknown' AS operating_system,
        'Unknown' AS browser,
        'Unknown Model' AS device_model
    FROM (
        SELECT device FROM pulsetrack_silver.impressions WHERE device IS NOT NULL AND TRIM(device) != ''
        UNION
        SELECT device FROM pulsetrack_silver.conversions WHERE device IS NOT NULL AND TRIM(device) != ''
    ) d
),

all_combos AS (
    SELECT * FROM general_devices
    UNION
    SELECT * FROM click_devices
)

-- 1. Default Unknown record for NULL / unmapped devices in facts
SELECT
    -1 AS device_key,
    'unknown' AS device_type,
    'Unknown' AS operating_system,
    'Unknown' AS browser,
    'Unknown Model' AS device_model

UNION ALL

-- 2. Conformed Device Combinations
SELECT
    ROW_NUMBER() OVER (ORDER BY device_type, operating_system, browser) AS device_key,
    device_type,
    operating_system,
    browser,
    device_model
FROM all_combos
ORDER BY device_key;

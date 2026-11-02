-- ============================================================================
-- PulseTrack: Gold Layer - Incremental Load SQL for bridge_conversion_touchpoint
-- ============================================================================
-- Appends new touchpoints for newly arrived conversions and clicks.
-- ============================================================================

WITH touchpoints AS (
    SELECT
        NULLIF(TRIM(tp.conversion_id), '') AS conversion_id,
        NULLIF(TRIM(tp.click_id), '') AS click_id,
        NULLIF(TRIM(tp.touch_position), '') AS touch_position,
        tp.weight AS attribution_weight
    FROM pulsetrack_silver.conversion_touchpoints tp
    WHERE NULLIF(TRIM(tp.conversion_id), '') IS NOT NULL
      AND NULLIF(TRIM(tp.click_id), '') IS NOT NULL
)

SELECT DISTINCT
    fc.conversion_key,
    fk.click_key,
    t.touch_position,
    t.attribution_weight
FROM touchpoints t
JOIN pulsetrack_gold.fact_conversion fc
    ON t.conversion_id = fc.conversion_id
JOIN pulsetrack_gold.fact_click fk
    ON t.click_id = fk.click_id
ON CONFLICT (conversion_key, click_key) DO UPDATE SET
    touch_position = EXCLUDED.touch_position,
    attribution_weight = EXCLUDED.attribution_weight;

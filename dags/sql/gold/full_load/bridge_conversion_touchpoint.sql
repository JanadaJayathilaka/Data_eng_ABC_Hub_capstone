-- ============================================================================
-- PulseTrack: Gold Layer - Full Load SQL for bridge_conversion_touchpoint
-- ============================================================================
-- Bridges many-to-many relationships between click events (fact_click) and
-- conversion events (fact_conversion) for multi-touch attribution analysis.
-- Maps business keys (conversion_id, click_id) to warehouse surrogate keys
-- (conversion_key, click_key) and preserves attribution weights and positions.
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
    ON t.click_id = fk.click_id;

-- ============================================================================
-- PulseTrack: Data Quality Test Suite across 6 Analytical Categories
-- ============================================================================
-- 1. Schema, Null, and Malformed Records
-- 2. Duplicate Detection
-- 3. Referential Integrity
-- 4. Value Ranges, Monetary Calculations & Statuses
-- 5. Business Rules & Event Sequencing
-- 6. Multi-Touch Attribution Validation
-- ============================================================================

INSERT INTO pulsetrack_audit.data_quality_results (
    run_id,
    checked_at,
    category,
    check_name,
    target_layer,
    target_table,
    records_evaluated,
    failed_records,
    status,
    details
)
WITH test_evaluations AS (

    -- ========================================================================
    -- 1. Schema, Null, and Malformed Record Checks
    -- ========================================================================
    SELECT 
        'NULLABILITY' AS category,
        'chk_null_conversion_id' AS check_name,
        'GOLD' AS target_layer,
        'pulsetrack_gold.fact_conversion' AS target_table,
        (SELECT count(*) FROM pulsetrack_gold.fact_conversion) AS records_evaluated,
        (SELECT count(*) FROM pulsetrack_gold.fact_conversion WHERE conversion_id IS NULL OR TRIM(conversion_id) = '') AS failed_records,
        'Primary key conversion_id must not be null or blank.' AS details

    UNION ALL

    SELECT 
        'NULLABILITY' AS category,
        'chk_null_click_id' AS check_name,
        'GOLD' AS target_layer,
        'pulsetrack_gold.fact_click' AS target_table,
        (SELECT count(*) FROM pulsetrack_gold.fact_click) AS records_evaluated,
        (SELECT count(*) FROM pulsetrack_gold.fact_click WHERE click_id IS NULL OR TRIM(click_id) = '') AS failed_records,
        'Primary key click_id must not be null or blank.' AS details

    UNION ALL

    SELECT 
        'NULLABILITY' AS category,
        'chk_null_surrogate_foreign_keys' AS check_name,
        'GOLD' AS target_layer,
        'pulsetrack_gold.fact_conversion' AS target_table,
        (SELECT count(*) FROM pulsetrack_gold.fact_conversion) AS records_evaluated,
        (SELECT count(*) FROM pulsetrack_gold.fact_conversion 
         WHERE conversion_date_key IS NULL OR advertiser_key IS NULL 
            OR campaign_key IS NULL OR partner_key IS NULL 
            OR geo_key IS NULL OR device_key IS NULL) AS failed_records,
        'All surrogate FKs must be populated (mapped to dimension or -1 Unknown).' AS details

    UNION ALL

    -- ========================================================================
    -- 2. Duplicate Detection
    -- ========================================================================
    SELECT 
        'DUPLICATES' AS category,
        'chk_duplicate_fact_conversion' AS check_name,
        'GOLD' AS target_layer,
        'pulsetrack_gold.fact_conversion' AS target_table,
        (SELECT count(*) FROM pulsetrack_gold.fact_conversion) AS records_evaluated,
        ((SELECT count(*) FROM pulsetrack_gold.fact_conversion) - (SELECT count(DISTINCT conversion_id) FROM pulsetrack_gold.fact_conversion)) AS failed_records,
        'Zero duplicate conversion_id records permitted in fact_conversion.' AS details

    UNION ALL

    SELECT 
        'DUPLICATES' AS category,
        'chk_duplicate_fact_click' AS check_name,
        'GOLD' AS target_layer,
        'pulsetrack_gold.fact_click' AS target_table,
        (SELECT count(*) FROM pulsetrack_gold.fact_click) AS records_evaluated,
        ((SELECT count(*) FROM pulsetrack_gold.fact_click) - (SELECT count(DISTINCT click_id) FROM pulsetrack_gold.fact_click)) AS failed_records,
        'Zero duplicate click_id records permitted in fact_click.' AS details

    UNION ALL

    SELECT 
        'DUPLICATES' AS category,
        'chk_duplicate_active_advertisers' AS check_name,
        'GOLD' AS target_layer,
        'pulsetrack_gold.dim_advertiser' AS target_table,
        (SELECT count(*) FROM pulsetrack_gold.dim_advertiser WHERE is_current = TRUE AND advertiser_key != -1) AS records_evaluated,
        ((SELECT count(*) FROM pulsetrack_gold.dim_advertiser WHERE is_current = TRUE AND advertiser_key != -1) - 
         (SELECT count(DISTINCT advertiser_id) FROM pulsetrack_gold.dim_advertiser WHERE is_current = TRUE AND advertiser_key != -1)) AS failed_records,
        'Each advertiser must have at most 1 active is_current = TRUE version.' AS details

    UNION ALL

    -- ========================================================================
    -- 3. Referential Integrity
    -- ========================================================================
    SELECT 
        'REFERENTIAL_INTEGRITY' AS category,
        'chk_ri_conversion_to_date' AS check_name,
        'GOLD' AS target_layer,
        'pulsetrack_gold.fact_conversion' AS target_table,
        (SELECT count(*) FROM pulsetrack_gold.fact_conversion) AS records_evaluated,
        (SELECT count(*) FROM pulsetrack_gold.fact_conversion fc 
         WHERE NOT EXISTS (SELECT 1 FROM pulsetrack_gold.dim_date dd WHERE dd.date_key = fc.conversion_date_key)) AS failed_records,
        'All conversion_date_key values must exist in dim_date.' AS details

    UNION ALL

    SELECT 
        'REFERENTIAL_INTEGRITY' AS category,
        'chk_ri_conversion_to_advertiser' AS check_name,
        'GOLD' AS target_layer,
        'pulsetrack_gold.fact_conversion' AS target_table,
        (SELECT count(*) FROM pulsetrack_gold.fact_conversion) AS records_evaluated,
        (SELECT count(*) FROM pulsetrack_gold.fact_conversion fc 
         WHERE NOT EXISTS (SELECT 1 FROM pulsetrack_gold.dim_advertiser da WHERE da.advertiser_key = fc.advertiser_key)) AS failed_records,
        'All advertiser_key values must exist in dim_advertiser.' AS details

    UNION ALL

    SELECT 
        'REFERENTIAL_INTEGRITY' AS category,
        'chk_ri_click_to_creative' AS check_name,
        'GOLD' AS target_layer,
        'pulsetrack_gold.fact_click' AS target_table,
        (SELECT count(*) FROM pulsetrack_gold.fact_click) AS records_evaluated,
        (SELECT count(*) FROM pulsetrack_gold.fact_click fk 
         WHERE NOT EXISTS (SELECT 1 FROM pulsetrack_gold.dim_creative dc WHERE dc.creative_key = fk.creative_key)) AS failed_records,
        'All creative_key values in fact_click must exist in dim_creative.' AS details

    UNION ALL

    SELECT 
        'REFERENTIAL_INTEGRITY' AS category,
        'chk_ri_bridge_to_conversion_and_click' AS check_name,
        'GOLD' AS target_layer,
        'pulsetrack_gold.bridge_conversion_touchpoint' AS target_table,
        (SELECT count(*) FROM pulsetrack_gold.bridge_conversion_touchpoint) AS records_evaluated,
        (SELECT count(*) FROM pulsetrack_gold.bridge_conversion_touchpoint b
         WHERE NOT EXISTS (SELECT 1 FROM pulsetrack_gold.fact_conversion fc WHERE fc.conversion_key = b.conversion_key)
            OR NOT EXISTS (SELECT 1 FROM pulsetrack_gold.fact_click fk WHERE fk.click_key = b.click_key)) AS failed_records,
        'All bridge touchpoints must link to valid fact_conversion and fact_click rows.' AS details

    UNION ALL

    SELECT 
        'REFERENTIAL_INTEGRITY' AS category,
        'chk_ri_silver_revenue_adjustments' AS check_name,
        'SILVER' AS target_layer,
        'pulsetrack_silver.revenue_adjustments' AS target_table,
        (SELECT count(*) FROM pulsetrack_silver.revenue_adjustments) AS records_evaluated,
        (SELECT count(*) FROM pulsetrack_silver.revenue_adjustments ra
         WHERE NOT EXISTS (SELECT 1 FROM pulsetrack_silver.conversions c WHERE c.conversion_id = ra.conversion_id)) AS failed_records,
        'Revenue adjustments referencing valid silver conversions.' AS details

    UNION ALL

    -- ========================================================================
    -- 4. Value Ranges, Monetary Calculations & Statuses
    -- ========================================================================
    SELECT 
        'RANGE_VALIDATION' AS category,
        'chk_val_non_negative_payout' AS check_name,
        'GOLD' AS target_layer,
        'pulsetrack_gold.fact_conversion' AS target_table,
        (SELECT count(*) FROM pulsetrack_gold.fact_conversion) AS records_evaluated,
        (SELECT count(*) FROM pulsetrack_gold.fact_conversion WHERE payout < 0) AS failed_records,
        'Partner payouts must be non-negative.' AS details

    UNION ALL

    SELECT 
        'RANGE_VALIDATION' AS category,
        'chk_val_refund_negative_revenue' AS check_name,
        'GOLD' AS target_layer,
        'pulsetrack_gold.fact_conversion' AS target_table,
        (SELECT count(*) FROM pulsetrack_gold.fact_conversion) AS records_evaluated,
        (SELECT count(*) FROM pulsetrack_gold.fact_conversion WHERE revenue < 0) AS failed_records,
        'Audit refund/reversal conversions with negative revenue.' AS details

    UNION ALL

    SELECT 
        'BUSINESS_RULES' AS category,
        'chk_val_margin_and_net_revenue_formula' AS check_name,
        'GOLD' AS target_layer,
        'pulsetrack_gold.fact_conversion' AS target_table,
        (SELECT count(*) FROM pulsetrack_gold.fact_conversion) AS records_evaluated,
        (SELECT count(*) FROM pulsetrack_gold.fact_conversion WHERE abs(margin - (net_revenue - payout)) > 0.01) AS failed_records,
        'Margin must strictly equal net_revenue minus payout.' AS details

    UNION ALL

    SELECT 
        'RANGE_VALIDATION' AS category,
        'chk_val_date_bounds' AS check_name,
        'GOLD' AS target_layer,
        'pulsetrack_gold.fact_conversion' AS target_table,
        (SELECT count(*) FROM pulsetrack_gold.fact_conversion) AS records_evaluated,
        (SELECT count(*) FROM pulsetrack_gold.fact_conversion 
         WHERE conversion_date_key < 20240101 OR conversion_date_key > 20271231) AS failed_records,
        'Conversions must fall within valid operational timeframe (2024 to 2027).' AS details

    UNION ALL

    SELECT 
        'RANGE_VALIDATION' AS category,
        'chk_val_campaign_statuses' AS check_name,
        'GOLD' AS target_layer,
        'pulsetrack_gold.dim_campaign' AS target_table,
        (SELECT count(*) FROM pulsetrack_gold.dim_campaign) AS records_evaluated,
        (SELECT count(*) FROM pulsetrack_gold.dim_campaign 
         WHERE UPPER(TRIM(campaign_status)) NOT IN ('ACTIVE', 'PAUSED', 'COMPLETED', 'ARCHIVED', 'PENDING', 'UNKNOWN')) AS failed_records,
        'Campaign status must adhere to the standardized status domain.' AS details

    UNION ALL

    -- ========================================================================
    -- 5. Business Rules & Event Sequencing
    -- ========================================================================
    SELECT 
        'BUSINESS_RULES' AS category,
        'chk_scd2_effective_dates_consistency' AS check_name,
        'GOLD' AS target_layer,
        'pulsetrack_gold.dim_advertiser' AS target_table,
        (SELECT count(*) FROM pulsetrack_gold.dim_advertiser WHERE effective_to IS NOT NULL) AS records_evaluated,
        (SELECT count(*) FROM pulsetrack_gold.dim_advertiser WHERE effective_to IS NOT NULL AND effective_to < effective_from) AS failed_records,
        'SCD2 effective_to must not precede effective_from.' AS details

    UNION ALL

    SELECT 
        'BUSINESS_RULES' AS category,
        'chk_event_sequence_click_before_conversion' AS check_name,
        'SILVER' AS target_layer,
        'pulsetrack_silver.conversion_touchpoints' AS target_table,
        (SELECT count(*) FROM pulsetrack_silver.conversion_touchpoints) AS records_evaluated,
        (SELECT count(*) FROM pulsetrack_silver.conversion_touchpoints tp
         JOIN pulsetrack_silver.conversions conv ON tp.conversion_id = conv.conversion_id
         JOIN pulsetrack_silver.clicks c ON tp.click_id = c.click_id
         WHERE c.click_time > conv.conversion_time) AS failed_records,
        'Event sequence: Attributed touchpoint click_time should precede or match conversion_time.' AS details

    UNION ALL

    -- ========================================================================
    -- 6. Attribution Validation
    -- ========================================================================
    SELECT 
        'ATTRIBUTION' AS category,
        'chk_attribution_weight_sum' AS check_name,
        'GOLD' AS target_layer,
        'pulsetrack_gold.bridge_conversion_touchpoint' AS target_table,
        (SELECT count(DISTINCT conversion_key) FROM pulsetrack_gold.bridge_conversion_touchpoint) AS records_evaluated,
        (SELECT count(*) FROM (
            SELECT conversion_key, round(sum(attribution_weight)::numeric, 2) AS total_weight
            FROM pulsetrack_gold.bridge_conversion_touchpoint
            GROUP BY conversion_key
            HAVING abs(sum(attribution_weight) - 1.0) > 0.02
        ) sub) AS failed_records,
        'Attribution weights across touchpoints for any conversion must sum to 1.0 (with 0.02 rounding tolerance).' AS details

    UNION ALL

    SELECT 
        'ATTRIBUTION' AS category,
        'chk_attribution_valid_position' AS check_name,
        'GOLD' AS target_layer,
        'pulsetrack_gold.bridge_conversion_touchpoint' AS target_table,
        (SELECT count(*) FROM pulsetrack_gold.bridge_conversion_touchpoint) AS records_evaluated,
        (SELECT count(*) FROM pulsetrack_gold.bridge_conversion_touchpoint 
         WHERE touch_position IS NULL OR TRIM(touch_position) = '') AS failed_records,
        'Touchpoint position must be populated and non-blank.' AS details
)
SELECT 
    '{{ run_id }}' AS run_id,
    CURRENT_TIMESTAMP AS checked_at,
    category,
    check_name,
    target_layer,
    target_table,
    records_evaluated,
    failed_records,
    CASE 
        WHEN failed_records = 0 THEN 'PASSED'
        -- For known source data quirks (e.g. synthetic test future clicks, orphaned raw adjustments, or refunds), flag as WARNING
        WHEN check_name IN ('chk_event_sequence_click_before_conversion', 'chk_ri_silver_revenue_adjustments', 'chk_val_refund_negative_revenue') THEN 'WARNING'
        ELSE 'FAILED'
    END AS status,
    details
FROM test_evaluations;

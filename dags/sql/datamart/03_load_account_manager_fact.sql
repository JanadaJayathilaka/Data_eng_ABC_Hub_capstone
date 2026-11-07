-- ============================================================================
-- PulseTrack Data Mart: Full Load for dm_fact_account_manager_performance_daily
-- ============================================================================
-- Business Area: Account Manager Portfolio Performance
-- Source: EDW Gold (fact_conversion)
-- ============================================================================

SELECT 
    conversion_date_key            AS date_key,
    account_manager_key,
    advertiser_key,
    SUM(conversion_count)          AS conversions,
    SUM(revenue)                   AS revenue,
    SUM(net_revenue)               AS adjusted_revenue,
    SUM(payout)                    AS payout,
    SUM(margin)                    AS margin,
    COUNT(DISTINCT campaign_key)   AS active_campaigns_count,
    CURRENT_TIMESTAMP              AS created_at
FROM pulsetrack_gold.fact_conversion
WHERE account_manager_key IS NOT NULL AND account_manager_key != -1
  AND advertiser_key IS NOT NULL AND advertiser_key != -1
GROUP BY conversion_date_key, account_manager_key, advertiser_key;


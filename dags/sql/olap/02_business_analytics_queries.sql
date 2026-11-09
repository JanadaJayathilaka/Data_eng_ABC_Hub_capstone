-- ============================================================================
-- PulseTrack: Question 9 - Business Analytics Using the Final Platform
-- ============================================================================
-- All 10 complex business questions answered strictly using the final
-- analytical platform (pulsetrack_olap and pulsetrack_dm).
-- NEVER bypasses the warehouse/mart by querying raw source tables.
-- ============================================================================


-- ============================================================================
-- 1. Which advertisers generated the highest revenue each quarter?
-- ============================================================================
WITH quarterly_adv_rev AS (
    SELECT 
        d.year,
        d.quarter,
        a.advertiser_id,
        a.advertiser_name,
        SUM(f.revenue) AS total_revenue,
        DENSE_RANK() OVER (PARTITION BY d.year, d.quarter ORDER BY SUM(f.revenue) DESC) AS rank
    FROM pulsetrack_olap.vw_semantic_marketing_model f
    JOIN pulsetrack_dm.dim_date d ON f.date_key = d.date_key
    JOIN pulsetrack_dm.dim_advertiser a ON f.advertiser_key = a.advertiser_key
    GROUP BY d.year, d.quarter, a.advertiser_id, a.advertiser_name
)
SELECT year, quarter, advertiser_id, advertiser_name, total_revenue
FROM quarterly_adv_rev
WHERE rank = 1
ORDER BY year, quarter;


-- ============================================================================
-- 2. Which campaigns have the highest CTR but the lowest conversion rate?
-- ============================================================================
WITH campaign_metrics AS (
    SELECT 
        campaign_id,
        campaign_name,
        SUM(impressions) AS total_impressions,
        SUM(clicks)      AS total_clicks,
        SUM(conversions) AS total_conversions,
        ROUND((SUM(clicks)::numeric / NULLIF(SUM(impressions), 0)) * 100, 2) AS ctr,
        ROUND((SUM(conversions)::numeric / NULLIF(SUM(clicks), 0)) * 100, 2) AS conversion_rate
    FROM pulsetrack_olap.vw_semantic_marketing_model
    GROUP BY campaign_id, campaign_name
    HAVING SUM(impressions) >= 500 AND SUM(clicks) >= 20
),
ranked AS (
    SELECT 
        *,
        DENSE_RANK() OVER (ORDER BY ctr DESC) AS ctr_rank_desc,
        DENSE_RANK() OVER (ORDER BY conversion_rate ASC) AS cvr_rank_asc
    FROM campaign_metrics
)
SELECT 
    campaign_id, campaign_name, total_impressions, total_clicks, total_conversions,
    ctr, conversion_rate,
    (ctr_rank_desc + cvr_rank_asc) AS combined_rank_score
FROM ranked
ORDER BY combined_rank_score ASC, ctr DESC;


-- ============================================================================
-- 3. Which partners generated the highest attributed revenue?
-- ============================================================================
SELECT 
    p.partner_id,
    p.partner_name,
    p.partner_type,
    SUM(pa.attributed_revenue)     AS total_attributed_revenue,
    SUM(pa.attributed_conversions) AS total_attributed_conversions,
    DENSE_RANK() OVER (ORDER BY SUM(pa.attributed_revenue) DESC) AS partner_rank
FROM pulsetrack_dm.dm_fact_partner_attribution_daily pa
JOIN pulsetrack_dm.dim_partner p ON pa.partner_key = p.partner_key
GROUP BY p.partner_id, p.partner_name, p.partner_type
ORDER BY total_attributed_revenue DESC;


-- ============================================================================
-- 4. How did revenue change compared with the previous month?
-- ============================================================================
WITH monthly_rev AS (
    SELECT 
        year,
        month,
        month_name,
        SUM(total_revenue) AS current_month_revenue
    FROM pulsetrack_olap.cube_executive_performance_summary
    GROUP BY year, month, month_name
)
SELECT 
    year,
    month,
    month_name,
    current_month_revenue,
    LAG(current_month_revenue, 1) OVER (ORDER BY year, month) AS previous_month_revenue,
    ROUND(current_month_revenue - LAG(current_month_revenue, 1) OVER (ORDER BY year, month), 2) AS revenue_dollar_change,
    ROUND(((current_month_revenue - LAG(current_month_revenue, 1) OVER (ORDER BY year, month)) 
           / NULLIF(LAG(current_month_revenue, 1) OVER (ORDER BY year, month), 0)) * 100, 2) AS mom_growth_pct
FROM monthly_rev
ORDER BY year, month;


-- ============================================================================
-- 5. Which campaigns contributed more than 10% of total revenue?
-- ============================================================================
WITH campaign_totals AS (
    SELECT 
        campaign_id,
        campaign_name,
        SUM(revenue) AS campaign_revenue
    FROM pulsetrack_olap.vw_semantic_marketing_model
    GROUP BY campaign_id, campaign_name
),
platform_total AS (
    SELECT SUM(campaign_revenue) AS total_platform_revenue FROM campaign_totals
)
SELECT 
    c.campaign_id,
    c.campaign_name,
    c.campaign_revenue,
    p.total_platform_revenue,
    ROUND((c.campaign_revenue / NULLIF(p.total_platform_revenue, 0)) * 100, 2) AS revenue_contribution_pct
FROM campaign_totals c
CROSS JOIN platform_total p
WHERE (c.campaign_revenue / NULLIF(p.total_platform_revenue, 0)) * 100 > 10.00
ORDER BY revenue_contribution_pct DESC;


-- ============================================================================
-- 6. Which creatives underperformed relative to their campaign?
-- ============================================================================
WITH creative_perf AS (
    SELECT 
        campaign_id,
        campaign_name,
        creative_id,
        creative_name,
        creative_type,
        SUM(impressions) AS creative_impressions,
        SUM(clicks)      AS creative_clicks,
        ROUND((SUM(clicks)::numeric / NULLIF(SUM(impressions), 0)) * 100, 2) AS creative_ctr
    FROM pulsetrack_olap.vw_semantic_marketing_model
    WHERE creative_id IS NOT NULL AND creative_id != 'Unknown'
    GROUP BY campaign_id, campaign_name, creative_id, creative_name, creative_type
    HAVING SUM(impressions) >= 500
),
campaign_benchmark AS (
    SELECT 
        campaign_id,
        ROUND((SUM(clicks)::numeric / NULLIF(SUM(impressions), 0)) * 100, 2) AS avg_campaign_ctr
    FROM pulsetrack_olap.vw_semantic_marketing_model
    GROUP BY campaign_id
)
SELECT 
    cp.campaign_name,
    cp.creative_name,
    cp.creative_type,
    cp.creative_impressions,
    cp.creative_clicks,
    cp.creative_ctr,
    cb.avg_campaign_ctr,
    ROUND(cp.creative_ctr - cb.avg_campaign_ctr, 2) AS ctr_variance_to_campaign_avg
FROM creative_perf cp
JOIN campaign_benchmark cb ON cp.campaign_id = cb.campaign_id
WHERE cp.creative_ctr < cb.avg_campaign_ctr
ORDER BY ctr_variance_to_campaign_avg ASC;


-- ============================================================================
-- 7. How did an advertiser's performance change before and after an account-manager change?
-- ============================================================================
WITH am_transitions AS (
    SELECT 
        advertiser_id,
        advertiser_name,
        account_manager_key,
        effective_from,
        effective_to,
        is_current
    FROM pulsetrack_dm.dim_advertiser
    WHERE advertiser_id IN (
        SELECT advertiser_id 
        FROM pulsetrack_dm.dim_advertiser 
        GROUP BY advertiser_id 
        HAVING COUNT(*) > 1 -- SCD Type 2 history
    )
)
SELECT 
    t.advertiser_name,
    am.am_name AS assigned_account_manager,
    t.effective_from,
    COALESCE(t.effective_to, '9999-12-31'::date) AS effective_to,
    t.is_current,
    SUM(f.conversions) AS conversions_during_tenure,
    SUM(f.revenue)     AS revenue_during_tenure,
    SUM(f.adjusted_revenue) AS adjusted_revenue_during_tenure,
    ROUND(SUM(f.revenue) / NULLIF(COUNT(DISTINCT f.date_key), 0), 2) AS avg_daily_revenue_during_tenure
FROM am_transitions t
JOIN pulsetrack_dm.dim_account_manager am ON t.account_manager_key = am.account_manager_key
JOIN pulsetrack_dm.dm_fact_marketing_performance_daily f 
    ON f.advertiser_key IN (SELECT advertiser_key FROM pulsetrack_dm.dim_advertiser WHERE advertiser_id = t.advertiser_id)
   AND f.account_manager_key = t.account_manager_key
GROUP BY t.advertiser_name, am.am_name, t.effective_from, t.effective_to, t.is_current
ORDER BY t.advertiser_name, t.effective_from ASC;


-- ============================================================================
-- 8. What percentage of total revenue is attributable to each partner?
-- ============================================================================
WITH partner_attr AS (
    SELECT 
        p.partner_id,
        p.partner_name,
        p.partner_type,
        SUM(pa.attributed_revenue) AS partner_attributed_rev
    FROM pulsetrack_dm.dm_fact_partner_attribution_daily pa
    JOIN pulsetrack_dm.dim_partner p ON pa.partner_key = p.partner_key
    GROUP BY p.partner_id, p.partner_name, p.partner_type
),
total_rev AS (
    SELECT SUM(revenue) AS platform_gross_revenue FROM pulsetrack_dm.dm_fact_marketing_performance_daily
)
SELECT 
    pa.partner_id,
    pa.partner_name,
    pa.partner_type,
    pa.partner_attributed_rev,
    ROUND((pa.partner_attributed_rev / NULLIF(tr.platform_gross_revenue, 0)) * 100, 2) AS attribution_percentage_of_total_revenue
FROM partner_attr pa
CROSS JOIN total_rev tr
ORDER BY attribution_percentage_of_total_revenue DESC;


-- ============================================================================
-- 9. Which campaigns show increasing impressions but declining conversions?
-- ============================================================================
WITH monthly_campaign_trends AS (
    SELECT 
        d.year,
        d.month,
        c.campaign_id,
        c.campaign_name,
        SUM(f.impressions) AS impressions,
        SUM(f.conversions) AS conversions,
        LAG(SUM(f.impressions), 1) OVER (PARTITION BY c.campaign_id ORDER BY d.year, d.month) AS prior_month_impressions,
        LAG(SUM(f.conversions), 1) OVER (PARTITION BY c.campaign_id ORDER BY d.year, d.month) AS prior_month_conversions
    FROM pulsetrack_dm.dm_fact_marketing_performance_daily f
    JOIN pulsetrack_dm.dim_date d ON f.date_key = d.date_key
    JOIN pulsetrack_dm.dim_campaign c ON f.campaign_key = c.campaign_key
    GROUP BY d.year, d.month, c.campaign_id, c.campaign_name
)
SELECT 
    year,
    month,
    campaign_id,
    campaign_name,
    prior_month_impressions,
    impressions AS current_impressions,
    (impressions - prior_month_impressions) AS impression_increase,
    prior_month_conversions,
    conversions AS current_conversions,
    (conversions - prior_month_conversions) AS conversion_decrease
FROM monthly_campaign_trends
WHERE impressions > prior_month_impressions   -- Increasing impressions
  AND conversions < prior_month_conversions -- Declining conversions
  AND prior_month_impressions IS NOT NULL
ORDER BY impression_increase DESC, conversion_decrease ASC;


-- ============================================================================
-- 10. What is the revenue impact of late adjustments?
-- ============================================================================
SELECT 
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    SUM(f.revenue)          AS original_gross_revenue,
    SUM(f.adjusted_revenue) AS net_realized_revenue,
    ROUND(SUM(f.adjusted_revenue) - SUM(f.revenue), 2) AS total_adjustment_impact_dollar,
    ROUND(((SUM(f.adjusted_revenue) - SUM(f.revenue)) / NULLIF(SUM(f.revenue), 0)) * 100, 2) AS adjustment_impact_percentage
FROM pulsetrack_dm.dm_fact_marketing_performance_daily f
JOIN pulsetrack_dm.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.quarter, d.month, d.month_name
ORDER BY d.year, d.month;


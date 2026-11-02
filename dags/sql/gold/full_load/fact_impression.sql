with cleaned_impression AS (
    SELECT 
        imp.impression_id,
        imp.impression_time,
        imp.creative_id,
        imp.campaign_id,
        imp.partner_id,
        imp.country, 
        imp.device
    from pulsetrack_silver.impressions imp
    where imp.impression_id is not null and trim(imp.impression_id) != ''

)

SELECT 
    ROW_NUMBER() OVER(ORDER BY imp.impression_time, imp.impression_id) AS impression_key,
    imp.impression_id,
    COALESCE(dd.date_key, -1) AS date_key,
    COALESCE(da.advertiser_key, -1) AS advertiser_key,
    COALESCE(d_off.offer_key, -1) AS offer_key,
    COALESCE(dc.campaign_key, -1) AS campaign_key,
    COALESCE(dcr.creative_key, -1) AS creative_key,
    COALESCE(dp.partner_key, -1) AS partner_key,
    COALESCE(dg.geo_key, -1) AS geo_key,
    COALESCE(ddv.device_key, -1) AS device_key,
    1 AS impression_count
FROM cleaned_impression imp
LEFT JOIN pulsetrack_silver.campaigns c
    ON imp.campaign_id = c.campaign_id
LEFT JOIN pulsetrack_gold.dim_date dd
    ON TO_CHAR(imp.impression_time, 'YYYYMMDD')::INTEGER = dd.date_key
LEFT JOIN pulsetrack_gold.dim_campaign dc
    ON imp.campaign_id = dc.campaign_id AND dc.is_current = TRUE
LEFT JOIN pulsetrack_gold.dim_creative dcr
    ON imp.creative_id = dcr.creative_id AND dcr.is_current = TRUE
LEFT JOIN pulsetrack_gold.dim_offer d_off
    ON c.offer_id = d_off.offer_id AND d_off.is_current = TRUE
LEFT JOIN pulsetrack_gold.dim_advertiser da
    ON c.advertiser_id = da.advertiser_id AND da.is_current = TRUE
LEFT JOIN pulsetrack_gold.dim_partner dp
    ON imp.partner_id = dp.partner_id AND dp.is_current = TRUE
LEFT JOIN pulsetrack_gold.dim_geo dg
    ON COALESCE(imp.country, 'Unknown') = dg.country
LEFT JOIN pulsetrack_gold.dim_device ddv
    ON imp.device = ddv.device_type
   AND ddv.operating_system = 'Unknown'
   AND ddv.browser = 'Unknown';
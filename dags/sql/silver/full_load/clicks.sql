WITH bronze_impressions AS (
    SELECT
        NULLIF(TRIM(impression_id), '') AS impression_id,
        NULLIF(TRIM(campaign_id), '') AS campaign_id,
        NULLIF(TRIM(creative_id), '') AS creative_id,
        pulsetrack_silver.parse_partner_id(partner_id) AS partner_id,
        pulsetrack_silver.parse_timestamp(impression_time) AS impression_time
    FROM pulsetrack_bronze.impressions
    WHERE NULLIF(TRIM(impression_id), '') IS NOT NULL
),

bronze_campaigns AS (
    SELECT DISTINCT NULLIF(TRIM(campaign_id), '') AS campaign_id
    FROM pulsetrack_bronze.campaigns
    WHERE NULLIF(TRIM(campaign_id), '') IS NOT NULL
),

bronze_creatives AS (
    SELECT DISTINCT NULLIF(TRIM(creative_id), '') AS creative_id
    FROM pulsetrack_bronze.creatives
    WHERE NULLIF(TRIM(creative_id), '') IS NOT NULL
),

bronze_partners AS (
    SELECT DISTINCT pulsetrack_silver.parse_partner_id(partner_id) AS partner_id
    FROM pulsetrack_bronze.partners
    WHERE pulsetrack_silver.parse_partner_id(partner_id) IS NOT NULL
    UNION
    SELECT DISTINCT pulsetrack_silver.parse_partner_id(document->>'partner_id') AS partner_id
    FROM pulsetrack_bronze.mongo_partner_profiles
    WHERE pulsetrack_silver.parse_partner_id(document->>'partner_id') IS NOT NULL
),

cleaned AS (
    SELECT
        NULLIF(TRIM(c.click_id), '') AS click_id,

        -- 1. impression_id: Keep if valid in impressions, otherwise NULL (~15% direct clicks)
        CASE
            WHEN imp.impression_id IS NOT NULL THEN imp.impression_id
            ELSE NULL
        END AS impression_id,

        -- 2. campaign_id: Recover from impression if missing, then validate against bronze_campaigns
        CASE
            WHEN camp.campaign_id IS NOT NULL THEN camp.campaign_id
            ELSE NULL
        END AS campaign_id,

        -- 3. creative_id: Recover from impression if missing, then validate against bronze_creatives
        CASE
            WHEN cr.creative_id IS NOT NULL THEN cr.creative_id
            ELSE NULL
        END AS creative_id,

        -- 4. partner_id: Recover from impression if missing, parse canonical ID, then validate against partners
        CASE
            WHEN p.partner_id IS NOT NULL THEN p.partner_id
            ELSE NULL
        END AS partner_id,

        -- 5. click_time: Handle nulls by falling back to impression_time, created_at, or updated_at
        COALESCE(
            pulsetrack_silver.parse_timestamp(c.click_time),
            imp.impression_time,
            pulsetrack_silver.parse_timestamp(c.created_at),
            pulsetrack_silver.parse_timestamp(c.updated_at)
        ) AS click_time,

        pulsetrack_silver.parse_country(c.country) AS country,
        pulsetrack_silver.parse_device(c.device) AS device,
        pulsetrack_silver.parse_boolean(c.is_unique) AS is_unique,
        pulsetrack_silver.parse_timestamp(c.created_at) AS created_at,
        pulsetrack_silver.parse_timestamp(c.updated_at) AS updated_at

    FROM pulsetrack_bronze.clicks c
    LEFT JOIN bronze_impressions imp
           ON NULLIF(TRIM(c.impression_id), '') = imp.impression_id
    LEFT JOIN bronze_campaigns camp
           ON COALESCE(NULLIF(TRIM(c.campaign_id), ''), imp.campaign_id) = camp.campaign_id
    LEFT JOIN bronze_creatives cr
           ON COALESCE(NULLIF(TRIM(c.creative_id), ''), imp.creative_id) = cr.creative_id
    LEFT JOIN bronze_partners p
           ON COALESCE(
               pulsetrack_silver.parse_partner_id(c.partner_id),
               imp.partner_id
           ) = p.partner_id
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY click_id
            ORDER BY
                updated_at DESC NULLS LAST,
                created_at DESC NULLS LAST
        ) AS rn
    FROM cleaned
    WHERE click_id IS NOT NULL
)

SELECT
    click_id,
    impression_id,
    campaign_id,
    creative_id,
    partner_id,
    click_time,
    country,
    device,
    is_unique,
    created_at,
    updated_at
FROM deduplicated
WHERE rn = 1;

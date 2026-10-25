-- ============================================================================
-- PulseTrack: Bronze Schema & Tables DDL
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS pulsetrack_bronze;

-- 1. account_managers
CREATE TABLE IF NOT EXISTS pulsetrack_bronze.account_managers (
    LIKE pulsetrack_raw.account_managers_raw
);

-- 2. advertisers
CREATE TABLE IF NOT EXISTS pulsetrack_bronze.advertisers (
    LIKE pulsetrack_raw.advertisers_raw
);

-- 3. partners
CREATE TABLE IF NOT EXISTS pulsetrack_bronze.partners (
    LIKE pulsetrack_raw.partners_raw
);

-- 4. offers
CREATE TABLE IF NOT EXISTS pulsetrack_bronze.offers (
    LIKE pulsetrack_raw.offers_raw
);

-- 5. campaigns
CREATE TABLE IF NOT EXISTS pulsetrack_bronze.campaigns (
    LIKE pulsetrack_raw.campaigns_raw
);

-- 6. creatives
CREATE TABLE IF NOT EXISTS pulsetrack_bronze.creatives (
    LIKE pulsetrack_raw.creatives_raw
);

-- 7. impressions
CREATE TABLE IF NOT EXISTS pulsetrack_bronze.impressions (
    LIKE pulsetrack_raw.impressions_raw
);

-- 8. clicks
CREATE TABLE IF NOT EXISTS pulsetrack_bronze.clicks (
    LIKE pulsetrack_raw.clicks_raw
);

-- 9. conversions
CREATE TABLE IF NOT EXISTS pulsetrack_bronze.conversions (
    LIKE pulsetrack_raw.conversions_raw
);

-- 10. conversion_touchpoints
CREATE TABLE IF NOT EXISTS pulsetrack_bronze.conversion_touchpoints (
    LIKE pulsetrack_raw.conversion_touchpoints_raw
);

-- 11. revenue_adjustments
CREATE TABLE IF NOT EXISTS pulsetrack_bronze.revenue_adjustments (
    LIKE pulsetrack_raw.revenue_adjustments_raw
);

-- 12. mongo_advertiser_accounts
CREATE TABLE IF NOT EXISTS pulsetrack_bronze.mongo_advertiser_accounts (
    LIKE pulsetrack_raw.mongo_advertiser_accounts
);

-- 13. mongo_partner_profiles
CREATE TABLE IF NOT EXISTS pulsetrack_bronze.mongo_partner_profiles (
    LIKE pulsetrack_raw.mongo_partner_profiles
);

-- 14. kafka_pulsetrack_conversions
CREATE TABLE IF NOT EXISTS pulsetrack_bronze.kafka_pulsetrack_conversions (
    LIKE pulsetrack_raw.kafka_pulsetrack_conversions
);

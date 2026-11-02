-- ============================================================================
-- PulseTrack: Gold Schema, Dimensions, Facts & Bridge Tables DDL
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS pulsetrack_gold;

-- ----------------------------------------------------------------------------
-- 1. Dimensions
-- ----------------------------------------------------------------------------

-- 1. dim_date (Surrogate Date Dimension)
CREATE TABLE IF NOT EXISTS pulsetrack_gold.dim_date (
    date_key        INTEGER PRIMARY KEY,         -- e.g. 20250801
    full_date       DATE NOT NULL UNIQUE,
    day             INTEGER NOT NULL,
    day_name        VARCHAR(20) NOT NULL,
    week            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    quarter         INTEGER NOT NULL,
    year            INTEGER NOT NULL
);

-- 2. dim_geo (Geography Dimension)
CREATE TABLE IF NOT EXISTS pulsetrack_gold.dim_geo (
    geo_key         SERIAL PRIMARY KEY,
    country         VARCHAR(50),
    region          VARCHAR(50),
    city            VARCHAR(50),
    postal_code     VARCHAR(20)
);

-- 3. dim_device (Device Dimension)
CREATE TABLE IF NOT EXISTS pulsetrack_gold.dim_device (
    device_key       SERIAL PRIMARY KEY,
    device_type      VARCHAR(50),                -- Mobile / Desktop / Tablet etc.
    operating_system VARCHAR(50),
    browser          VARCHAR(50),
    device_model     VARCHAR(50)
);

-- 4. dim_account_manager (Account Manager Dimension - SCD2 versioned)
CREATE TABLE IF NOT EXISTS pulsetrack_gold.dim_account_manager (
    account_manager_key SERIAL PRIMARY KEY,
    am_id               VARCHAR(10) NOT NULL,
    am_name             VARCHAR(100),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    effective_from      DATE,
    effective_to        DATE,
    is_current          BOOLEAN DEFAULT TRUE
);

-- 5. dim_advertiser (Advertiser Dimension - SCD2 versioned from Mongo CRM)
CREATE TABLE IF NOT EXISTS pulsetrack_gold.dim_advertiser (
    advertiser_key      SERIAL PRIMARY KEY,
    advertiser_id       VARCHAR(10) NOT NULL,
    advertiser_name     VARCHAR(150) NOT NULL,
    account_manager_key INTEGER REFERENCES pulsetrack_gold.dim_account_manager(account_manager_key),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    effective_from      DATE,
    effective_to        DATE,
    is_current          BOOLEAN DEFAULT TRUE
);

-- 6. dim_partner (Partner Dimension - SCD2 versioned)
CREATE TABLE IF NOT EXISTS pulsetrack_gold.dim_partner (
    partner_key         SERIAL PRIMARY KEY,
    partner_id          VARCHAR(10) NOT NULL,
    partner_name        VARCHAR(150),
    partner_type        VARCHAR(50),
    status              VARCHAR(20),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    effective_from      DATE,
    effective_to        DATE,
    is_current          BOOLEAN DEFAULT TRUE
);

-- 7. dim_offer (Offer Dimension - SCD2 versioned)
CREATE TABLE IF NOT EXISTS pulsetrack_gold.dim_offer (
    offer_key           SERIAL PRIMARY KEY,
    offer_id            VARCHAR(10) NOT NULL,
    offer_name          VARCHAR(150),
    advertiser_key      INTEGER REFERENCES pulsetrack_gold.dim_advertiser(advertiser_key),
    offer_status        VARCHAR(20),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    effective_from      DATE,
    effective_to        DATE,
    is_current          BOOLEAN DEFAULT TRUE
);

-- 8. dim_campaign (Campaign Dimension - SCD2 versioned)
CREATE TABLE IF NOT EXISTS pulsetrack_gold.dim_campaign (
    campaign_key        SERIAL PRIMARY KEY,
    campaign_id         VARCHAR(10) NOT NULL,
    campaign_name       VARCHAR(150),
    offer_key           INTEGER REFERENCES pulsetrack_gold.dim_offer(offer_key),
    advertiser_key      INTEGER REFERENCES pulsetrack_gold.dim_advertiser(advertiser_key),
    campaign_status     VARCHAR(20),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    effective_from      DATE,
    effective_to        DATE,
    is_current          BOOLEAN DEFAULT TRUE
);

-- 9. dim_creative (Creative Dimension - SCD2 versioned)
CREATE TABLE IF NOT EXISTS pulsetrack_gold.dim_creative (
    creative_key        SERIAL PRIMARY KEY,
    creative_id         VARCHAR(10) NOT NULL,
    creative_name       VARCHAR(150),
    campaign_key        INTEGER REFERENCES pulsetrack_gold.dim_campaign(campaign_key),
    creative_type       VARCHAR(50),
    status              VARCHAR(20),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    effective_from      DATE,
    effective_to        DATE,
    is_current          BOOLEAN DEFAULT TRUE
);


-- ----------------------------------------------------------------------------
-- 2. Fact Tables
-- ----------------------------------------------------------------------------

-- 10. fact_impression (Impression Fact)
CREATE TABLE IF NOT EXISTS pulsetrack_gold.fact_impression (
    impression_key      BIGSERIAL PRIMARY KEY,
    impression_id       VARCHAR(20) NOT NULL,
    date_key            INTEGER REFERENCES pulsetrack_gold.dim_date(date_key),
    advertiser_key      INTEGER REFERENCES pulsetrack_gold.dim_advertiser(advertiser_key),
    offer_key           INTEGER REFERENCES pulsetrack_gold.dim_offer(offer_key),
    campaign_key        INTEGER REFERENCES pulsetrack_gold.dim_campaign(campaign_key),
    creative_key        INTEGER REFERENCES pulsetrack_gold.dim_creative(creative_key),
    partner_key         INTEGER REFERENCES pulsetrack_gold.dim_partner(partner_key),
    geo_key             INTEGER REFERENCES pulsetrack_gold.dim_geo(geo_key),
    device_key          INTEGER REFERENCES pulsetrack_gold.dim_device(device_key),
    impression_count    INTEGER NOT NULL DEFAULT 1
);

-- 11. fact_click (Click Fact)
CREATE TABLE IF NOT EXISTS pulsetrack_gold.fact_click (
    click_key           BIGSERIAL PRIMARY KEY,
    click_id            VARCHAR(20) NOT NULL,
    date_key            INTEGER REFERENCES pulsetrack_gold.dim_date(date_key),
    advertiser_key      INTEGER REFERENCES pulsetrack_gold.dim_advertiser(advertiser_key),
    offer_key           INTEGER REFERENCES pulsetrack_gold.dim_offer(offer_key),
    campaign_key        INTEGER REFERENCES pulsetrack_gold.dim_campaign(campaign_key),
    creative_key        INTEGER REFERENCES pulsetrack_gold.dim_creative(creative_key),
    partner_key         INTEGER REFERENCES pulsetrack_gold.dim_partner(partner_key),
    geo_key             INTEGER REFERENCES pulsetrack_gold.dim_geo(geo_key),
    device_key          INTEGER REFERENCES pulsetrack_gold.dim_device(device_key),
    click_count         INTEGER NOT NULL DEFAULT 1
);

-- 12. fact_conversion (Conversion Fact)
CREATE TABLE IF NOT EXISTS pulsetrack_gold.fact_conversion (
    conversion_key      BIGSERIAL PRIMARY KEY,
    conversion_id       VARCHAR(20) NOT NULL,
    conversion_date_key INTEGER REFERENCES pulsetrack_gold.dim_date(date_key),
    advertiser_key      INTEGER REFERENCES pulsetrack_gold.dim_advertiser(advertiser_key),
    offer_key           INTEGER REFERENCES pulsetrack_gold.dim_offer(offer_key),
    campaign_key        INTEGER REFERENCES pulsetrack_gold.dim_campaign(campaign_key),
    partner_key         INTEGER REFERENCES pulsetrack_gold.dim_partner(partner_key),
    account_manager_key INTEGER REFERENCES pulsetrack_gold.dim_account_manager(account_manager_key),
    geo_key             INTEGER REFERENCES pulsetrack_gold.dim_geo(geo_key),
    device_key          INTEGER REFERENCES pulsetrack_gold.dim_device(device_key),
    conversion_count    INTEGER NOT NULL DEFAULT 1,
    revenue             NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    payout              NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    net_revenue         NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    margin              NUMERIC(12, 2) NOT NULL DEFAULT 0.00
);

-- 13. bridge_conversion_touchpoint (Bridge table for multi-touch attribution)
CREATE TABLE IF NOT EXISTS pulsetrack_gold.bridge_conversion_touchpoint (
    conversion_key      BIGINT REFERENCES pulsetrack_gold.fact_conversion(conversion_key),
    click_key           BIGINT REFERENCES pulsetrack_gold.fact_click(click_key),
    touch_position      VARCHAR(20),
    attribution_weight  NUMERIC(6, 4),
    PRIMARY KEY (conversion_key, click_key)
);

-- ----------------------------------------------------------------------------
-- 3. Performance Indexes
-- ----------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_fact_impression_date ON pulsetrack_gold.fact_impression(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_impression_campaign ON pulsetrack_gold.fact_impression(campaign_key);
CREATE INDEX IF NOT EXISTS idx_fact_impression_partner ON pulsetrack_gold.fact_impression(partner_key);

CREATE INDEX IF NOT EXISTS idx_fact_click_date ON pulsetrack_gold.fact_click(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_click_campaign ON pulsetrack_gold.fact_click(campaign_key);
CREATE INDEX IF NOT EXISTS idx_fact_click_partner ON pulsetrack_gold.fact_click(partner_key);

CREATE INDEX IF NOT EXISTS idx_fact_conversion_date ON pulsetrack_gold.fact_conversion(conversion_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_conversion_campaign ON pulsetrack_gold.fact_conversion(campaign_key);
CREATE INDEX IF NOT EXISTS idx_fact_conversion_advertiser ON pulsetrack_gold.fact_conversion(advertiser_key);
CREATE INDEX IF NOT EXISTS idx_fact_conversion_partner ON pulsetrack_gold.fact_conversion(partner_key);

CREATE INDEX IF NOT EXISTS idx_bridge_click ON pulsetrack_gold.bridge_conversion_touchpoint(click_key);

-- Unique business keys for idempotent incremental upserts
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_geo_country ON pulsetrack_gold.dim_geo(country);
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_device_type_os_browser ON pulsetrack_gold.dim_device(device_type, operating_system, browser);
CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_impression_id ON pulsetrack_gold.fact_impression(impression_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_click_id ON pulsetrack_gold.fact_click(click_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_conversion_id ON pulsetrack_gold.fact_conversion(conversion_id);

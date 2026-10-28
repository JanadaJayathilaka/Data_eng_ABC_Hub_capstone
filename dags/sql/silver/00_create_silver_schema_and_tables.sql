-- ============================================================================
-- PulseTrack: Silver Schema, Helper Functions & Clean Tables DDL
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS pulsetrack_silver;

-- ----------------------------------------------------------------------------
-- 1. Helper Parsing Functions (Type Conversion & Cleaning)
-- ----------------------------------------------------------------------------

-- A. parse_timestamp: Parses mixed dates, epochs, ISO tz, and noise suffixes
CREATE OR REPLACE FUNCTION pulsetrack_silver.parse_timestamp(val TEXT)
RETURNS TIMESTAMP AS $$
DECLARE
    cleaned TEXT;
BEGIN
    IF val IS NULL OR TRIM(val) IN ('', 'None', 'N/A', 'null') THEN
        RETURN NULL;
    END IF;
    
    cleaned := TRIM(val);
    
    -- Epoch seconds (10 digits)
    IF cleaned ~ '^[0-9]{10}$' THEN
        RETURN to_timestamp(cleaned::BIGINT) AT TIME ZONE 'UTC';
    END IF;

    -- Epoch milliseconds (13 digits)
    IF cleaned ~ '^[0-9]{13}$' THEN
        RETURN to_timestamp((cleaned::BIGINT) / 1000) AT TIME ZONE 'UTC';
    END IF;

    -- Remove ' EST' noise suffix
    cleaned := REGEXP_REPLACE(cleaned, '\s+EST$', '', 'i');

    -- Slash dates: MM/DD/YYYY [HH:MI[:SS]]
    IF cleaned ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}' THEN
        IF cleaned ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}\s+[0-9]{1,2}:[0-9]{2}:[0-9]{2}' THEN
            RETURN to_timestamp(cleaned, 'MM/DD/YYYY HH24:MI:SS');
        ELSIF cleaned ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}\s+[0-9]{1,2}:[0-9]{2}' THEN
            RETURN to_timestamp(cleaned, 'MM/DD/YYYY HH24:MI');
        ELSE
            RETURN to_timestamp(cleaned, 'MM/DD/YYYY');
        END IF;
    END IF;

    -- Dash dates starting with 2-digit day: DD-MM-YYYY [HH:MI[:SS]]
    IF cleaned ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}' THEN
        IF cleaned ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}\s+[0-9]{1,2}:[0-9]{2}:[0-9]{2}' THEN
            RETURN to_timestamp(cleaned, 'DD-MM-YYYY HH24:MI:SS');
        ELSE
            RETURN to_timestamp(cleaned, 'DD-MM-YYYY');
        END IF;
    END IF;

    -- Text month: 'Mon DD, YYYY' (e.g. 'Jul 24, 2025')
    IF cleaned ~ '^[A-Za-z]{3}\s+[0-9]{1,2},\s+[0-9]{4}' THEN
        RETURN to_timestamp(cleaned, 'Mon DD, YYYY');
    END IF;

    -- Standard ISO format or YYYY-MM-DD
    BEGIN
        RETURN cleaned::TIMESTAMP;
    EXCEPTION WHEN OTHERS THEN
        BEGIN
            RETURN (cleaned::TIMESTAMPTZ) AT TIME ZONE 'UTC';
        EXCEPTION WHEN OTHERS THEN
            RETURN NULL;
        END;
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- B. parse_date: Parses any messy string into DATE
CREATE OR REPLACE FUNCTION pulsetrack_silver.parse_date(val TEXT)
RETURNS DATE AS $$
BEGIN
    RETURN (pulsetrack_silver.parse_timestamp(val))::DATE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- C. parse_money: Strips currency symbols ($, USD), commas, keeps negatives
CREATE OR REPLACE FUNCTION pulsetrack_silver.parse_money(val TEXT)
RETURNS NUMERIC AS $$
DECLARE
    cleaned TEXT;
    is_neg BOOLEAN := FALSE;
BEGIN
    IF val IS NULL OR TRIM(val) IN ('', 'None', 'N/A', 'null') THEN
        RETURN NULL;
    END IF;

    cleaned := TRIM(val);

    -- Check for negative sign or parenthesized negative: -24.75, $-28.28, ($25)
    IF cleaned LIKE '%-%' OR cleaned LIKE '(%)' THEN
        is_neg := TRUE;
    END IF;

    -- Strip everything except digits and decimal point
    cleaned := REGEXP_REPLACE(cleaned, '[^0-9.]', '', 'g');

    IF cleaned = '' THEN
        RETURN NULL;
    END IF;

    IF is_neg THEN
        RETURN -(cleaned::NUMERIC(12, 2));
    ELSE
        RETURN cleaned::NUMERIC(12, 2);
    END IF;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- D. parse_boolean: Normalizes 1/0, true/false, Y/N, blank -> TRUE/FALSE/NULL
CREATE OR REPLACE FUNCTION pulsetrack_silver.parse_boolean(val TEXT)
RETURNS BOOLEAN AS $$
BEGIN
    IF val IS NULL OR TRIM(val) = '' THEN
        RETURN NULL;
    END IF;
    IF LOWER(TRIM(val)) IN ('1', 'true', 't', 'y', 'yes') THEN
        RETURN TRUE;
    ELSIF LOWER(TRIM(val)) IN ('0', 'false', 'f', 'n', 'no') THEN
        RETURN FALSE;
    ELSE
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- E. parse_partner_id: Resolves canonical 5-digit numeric prefix
CREATE OR REPLACE FUNCTION pulsetrack_silver.parse_partner_id(val TEXT)
RETURNS TEXT AS $$
BEGIN
    IF val IS NULL OR TRIM(val) = '' THEN
        RETURN NULL;
    END IF;
    IF val ~ '^[0-9]{5}' THEN
        RETURN SUBSTRING(val FROM '^[0-9]{5}');
    ELSE
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- F. parse_status: Standardizes status to 'approved', 'pending', 'reversed'
CREATE OR REPLACE FUNCTION pulsetrack_silver.parse_status(val TEXT)
RETURNS TEXT AS $$
DECLARE
    cleaned TEXT;
BEGIN
    IF val IS NULL OR TRIM(val) = '' THEN
        RETURN 'unknown';
    END IF;
    cleaned := LOWER(TRIM(val));
    IF cleaned IN ('approved', 'app', '1') OR cleaned LIKE '%approv%' THEN
        RETURN 'approved';
    ELSIF cleaned LIKE '%pend%' THEN
        RETURN 'pending';
    ELSIF cleaned LIKE '%rev%' OR cleaned LIKE '%return%' THEN
        RETURN 'reversed';
    ELSE
        RETURN cleaned;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- G. parse_device: Standardizes device to 'desktop', 'mobile', 'tablet'
CREATE OR REPLACE FUNCTION pulsetrack_silver.parse_device(val TEXT)
RETURNS TEXT AS $$
DECLARE
    cleaned TEXT;
BEGIN
    IF val IS NULL OR TRIM(val) = '' THEN
        RETURN 'unknown';
    END IF;
    cleaned := LOWER(TRIM(val));
    IF cleaned IN ('desktop', 'desk', 'windows', 'mac', 'macintosh', 'linux') THEN
        RETURN 'desktop';
    ELSIF cleaned IN ('mobile', 'mob', 'android', 'ios', 'iphone') THEN
        RETURN 'mobile';
    ELSIF cleaned IN ('tablet', 'tab', 'ipad') THEN
        RETURN 'tablet';
    ELSE
        RETURN cleaned;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- H. parse_country: Standardizes 2-letter ISO country codes
CREATE OR REPLACE FUNCTION pulsetrack_silver.parse_country(val TEXT)
RETURNS TEXT AS $$
DECLARE
    cleaned TEXT;
BEGIN
    IF val IS NULL OR TRIM(val) = '' THEN
        RETURN NULL;
    END IF;
    cleaned := LOWER(TRIM(val));
    IF cleaned IN ('us', 'usa', 'united states', 'u.s.') THEN
        RETURN 'US';
    ELSIF cleaned IN ('gb', 'uk', 'united kingdom') THEN
        RETURN 'GB';
    ELSIF cleaned IN ('ca', 'canada') THEN
        RETURN 'CA';
    ELSIF cleaned IN ('au', 'australia') THEN
        RETURN 'AU';
    ELSE
        RETURN UPPER(cleaned);
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- ----------------------------------------------------------------------------
-- 2. Silver Tables DDL (Strictly Typed, Validated, Keyed)
-- ----------------------------------------------------------------------------

-- 1. account_managers
CREATE TABLE IF NOT EXISTS pulsetrack_silver.account_managers (
    am_id       VARCHAR(10) PRIMARY KEY,
    am_name     VARCHAR(100),
    team        VARCHAR(50),
    region      VARCHAR(50),
    hire_date   DATE,
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP
);

-- 2. advertisers
CREATE TABLE IF NOT EXISTS pulsetrack_silver.advertisers (
    advertiser_id       VARCHAR(10) PRIMARY KEY,
    advertiser_name     VARCHAR(150) NOT NULL,
    vertical            VARCHAR(50),
    account_manager_id  VARCHAR(10),
    status              VARCHAR(20),
    signup_date         DATE,
    billing_country     VARCHAR(10),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
);

-- 3. partners (Enriched with MongoDB portal profiles to resolve the 40 missing partners)
CREATE TABLE IF NOT EXISTS pulsetrack_silver.partners (
    partner_id      VARCHAR(10) PRIMARY KEY,
    partner_name    VARCHAR(150),
    channel         VARCHAR(50),
    media_buyer     VARCHAR(100),
    join_date       DATE,
    payment_terms   VARCHAR(50),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

-- 4. offers
CREATE TABLE IF NOT EXISTS pulsetrack_silver.offers (
    offer_id        VARCHAR(10) PRIMARY KEY,
    offer_name      VARCHAR(150),
    advertiser_id   VARCHAR(10),
    vertical        VARCHAR(50),
    default_payout  NUMERIC(10, 2),
    payout_type     VARCHAR(20),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

-- 5. campaigns
CREATE TABLE IF NOT EXISTS pulsetrack_silver.campaigns (
    campaign_id     VARCHAR(10) PRIMARY KEY,
    campaign_name   VARCHAR(150),
    offer_id        VARCHAR(10),
    advertiser_id   VARCHAR(10),
    price_type      VARCHAR(20),
    start_date      DATE,
    end_date        DATE,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

-- 6. creatives
CREATE TABLE IF NOT EXISTS pulsetrack_silver.creatives (
    creative_id     VARCHAR(10) PRIMARY KEY,
    campaign_id     VARCHAR(10),
    creative_name   VARCHAR(150),
    format          VARCHAR(50),
    size            VARCHAR(50),
    landing_url     TEXT,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

-- 7. impressions (Factless fact, funnel top; junk dropped)
CREATE TABLE IF NOT EXISTS pulsetrack_silver.impressions (
    impression_id   VARCHAR(20) PRIMARY KEY,
    campaign_id     VARCHAR(10),
    creative_id     VARCHAR(10),
    partner_id      VARCHAR(10),
    impression_time TIMESTAMP,
    country         VARCHAR(10),
    device          VARCHAR(20),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

-- 8. clicks (Middle of funnel; PII/junk dropped; boolean parsed)
CREATE TABLE IF NOT EXISTS pulsetrack_silver.clicks (
    click_id        VARCHAR(20) PRIMARY KEY,
    impression_id   VARCHAR(20),
    campaign_id     VARCHAR(10),
    creative_id     VARCHAR(10),
    partner_id      VARCHAR(10),
    click_time      TIMESTAMP,
    country         VARCHAR(10),
    device          VARCHAR(20),
    is_unique       BOOLEAN,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

-- 9. conversions (Deduplicated on MAX(updated_at); is_test filtered; money parsed)
CREATE TABLE IF NOT EXISTS pulsetrack_silver.conversions (
    conversion_id   VARCHAR(20) PRIMARY KEY,
    click_id        VARCHAR(20),
    campaign_id     VARCHAR(10),
    partner_id      VARCHAR(10),
    conversion_time TIMESTAMP,
    revenue         NUMERIC(12, 2),
    payout          NUMERIC(12, 2),
    currency        VARCHAR(10),
    status          VARCHAR(20),
    country         VARCHAR(10),
    device          VARCHAR(20),
    sub_id          VARCHAR(50),
    account_manager VARCHAR(100),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

-- 10. conversion_touchpoints (Bridge table for multi-touch attribution)
CREATE TABLE IF NOT EXISTS pulsetrack_silver.conversion_touchpoints (
    touchpoint_id   VARCHAR(20) PRIMARY KEY,
    conversion_id   VARCHAR(20),
    click_id        VARCHAR(20),
    touch_position  VARCHAR(20),
    weight          NUMERIC(6, 4),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

-- 11. revenue_adjustments (Late-arriving financial adjustments)
CREATE TABLE IF NOT EXISTS pulsetrack_silver.revenue_adjustments (
    adjustment_id   VARCHAR(20) PRIMARY KEY,
    conversion_id   VARCHAR(20),
    delta_amount    NUMERIC(12, 2),
    reason          VARCHAR(255),
    adjusted_at     TIMESTAMP,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

-- 12. mongo_advertiser_accounts (SCD2 truth & accounts extracted from MongoDB)
CREATE TABLE IF NOT EXISTS pulsetrack_silver.mongo_advertiser_accounts (
    advertiser_id   VARCHAR(10),
    legal_name      VARCHAR(150),
    display_name    VARCHAR(150),
    vertical        VARCHAR(50),
    status          VARCHAR(20),
    billing_country VARCHAR(10),
    am_id           VARCHAR(10),
    am_name         VARCHAR(100),
    valid_from      DATE,
    valid_to        DATE,
    is_current      BOOLEAN,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    PRIMARY KEY (advertiser_id, am_id, valid_from)
);

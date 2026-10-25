-- ============================================================================
-- PulseTrack: Raw Schema & Tables DDL
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS pulsetrack_raw;

-- 1. account_managers_raw
CREATE TABLE IF NOT EXISTS pulsetrack_raw.account_managers_raw
(
    am_id      TEXT,
    am_name    TEXT,
    team       TEXT,
    region     TEXT,
    hire_date  TEXT,
    created_at TEXT,
    updated_at TEXT
);

-- 2. advertisers_raw
CREATE TABLE IF NOT EXISTS pulsetrack_raw.advertisers_raw
(
    advertiser_id      TEXT,
    advertiser_name    TEXT,
    vertical           TEXT,
    account_manager_id TEXT,
    status             TEXT,
    signup_date        TEXT,
    billing_country    TEXT,
    created_at         TEXT,
    updated_at         TEXT
);

-- 3. partners_raw
CREATE TABLE IF NOT EXISTS pulsetrack_raw.partners_raw
(
    partner_id    TEXT,
    partner_name  TEXT,
    channel       TEXT,
    media_buyer   TEXT,
    join_date     TEXT,
    payment_terms TEXT,
    created_at    TEXT,
    updated_at    TEXT
);

-- 4. offers_raw
CREATE TABLE IF NOT EXISTS pulsetrack_raw.offers_raw
(
    offer_id       TEXT,
    offer_name     TEXT,
    advertiser_id  TEXT,
    vertical       TEXT,
    default_payout TEXT,
    payout_type    TEXT,
    created_at     TEXT,
    updated_at     TEXT
);

-- 5. campaigns_raw
CREATE TABLE IF NOT EXISTS pulsetrack_raw.campaigns_raw
(
    campaign_id   TEXT,
    campaign_name TEXT,
    offer_id      TEXT,
    advertiser_id TEXT,
    price_type    TEXT,
    start_date    TEXT,
    end_date      TEXT,
    created_at    TEXT,
    updated_at    TEXT
);

-- 6. creatives_raw
CREATE TABLE IF NOT EXISTS pulsetrack_raw.creatives_raw
(
    creative_id   TEXT,
    campaign_id   TEXT,
    creative_name TEXT,
    format        TEXT,
    size          TEXT,
    landing_url   TEXT,
    created_at    TEXT,
    updated_at    TEXT
);

-- 7. impressions_raw
CREATE TABLE IF NOT EXISTS pulsetrack_raw.impressions_raw
(
    impression_id   TEXT,
    campaign_id     TEXT,
    creative_id     TEXT,
    partner_id      TEXT,
    impression_time TEXT,
    placement_raw   TEXT,
    country         TEXT,
    device          TEXT,
    bot_score       TEXT,
    created_at      TEXT,
    updated_at      TEXT
);

-- 8. clicks_raw
CREATE TABLE IF NOT EXISTS pulsetrack_raw.clicks_raw
(
    click_id      TEXT,
    impression_id TEXT,
    campaign_id   TEXT,
    creative_id   TEXT,
    partner_id    TEXT,
    click_time    TEXT,
    ip            TEXT,
    user_agent    TEXT,
    country       TEXT,
    device        TEXT,
    is_unique     TEXT,
    referrer_raw  TEXT,
    created_at    TEXT,
    updated_at    TEXT
);

-- 9. conversions_raw
CREATE TABLE IF NOT EXISTS pulsetrack_raw.conversions_raw
(
    conversion_id   TEXT,
    click_id        TEXT,
    campaign_id     TEXT,
    advertiser_name TEXT,
    account_manager TEXT,
    campaign_name   TEXT,
    partner_id      TEXT,
    partner_name    TEXT,
    offer_name      TEXT,
    price_type      TEXT,
    conversion_time TEXT,
    revenue         TEXT,
    payout          TEXT,
    currency        TEXT,
    status          TEXT,
    country         TEXT,
    device          TEXT,
    sub_id          TEXT,
    is_test         TEXT,
    legacy_flag     TEXT,
    pixel_fired     TEXT,
    internal_note   TEXT,
    reserved_1      TEXT,
    created_at      TEXT,
    updated_at      TEXT
);

-- 10. conversion_touchpoints_raw
CREATE TABLE IF NOT EXISTS pulsetrack_raw.conversion_touchpoints_raw
(
    touchpoint_id  TEXT,
    conversion_id  TEXT,
    click_id       TEXT,
    touch_position TEXT,
    weight         TEXT,
    created_at     TEXT,
    updated_at     TEXT
);

-- 11. revenue_adjustments_raw
CREATE TABLE IF NOT EXISTS pulsetrack_raw.revenue_adjustments_raw
(
    adjustment_id TEXT,
    conversion_id TEXT,
    delta_amount  TEXT,
    reason        TEXT,
    adjusted_at   TEXT,
    created_at    TEXT,
    updated_at    TEXT
);

-- 12. mongo_partner_profiles
CREATE TABLE IF NOT EXISTS pulsetrack_raw.mongo_partner_profiles
(
    raw_id            BIGSERIAL PRIMARY KEY,
    mongo_document_id TEXT                                NOT NULL,
    partner_id        TEXT,
    document          JSONB                               NOT NULL,
    extracted_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- 13. kafka_pulsetrack_conversions
CREATE TABLE IF NOT EXISTS pulsetrack_raw.kafka_pulsetrack_conversions
(
    raw_id            BIGSERIAL PRIMARY KEY,
    topic             VARCHAR(255)                        NOT NULL,
    partition_id      INTEGER                             NOT NULL,
    offset_id         BIGINT                              NOT NULL,
    message_timestamp BIGINT,
    message_key       VARCHAR(255),
    event_id          VARCHAR(255),
    conversion_id     VARCHAR(255),
    event_payload     JSONB                               NOT NULL,
    extracted_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- 14. mongo_advertiser_accounts
CREATE TABLE IF NOT EXISTS pulsetrack_raw.mongo_advertiser_accounts
(
    raw_id            BIGSERIAL PRIMARY KEY,
    mongo_document_id VARCHAR(50)                         NOT NULL,
    advertiser_id     VARCHAR(50),
    document          JSONB                               NOT NULL,
    extracted_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

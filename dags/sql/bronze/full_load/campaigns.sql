SELECT
    *,
    CURRENT_TIMESTAMP AS load_timestamp
FROM pulsetrack_raw.campaigns_raw;

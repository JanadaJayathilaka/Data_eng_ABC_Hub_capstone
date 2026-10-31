SELECT *
FROM pulsetrack_raw.impressions_raw
WHERE COALESCE(pulsetrack_silver.parse_timestamp(updated_at), pulsetrack_silver.parse_timestamp(created_at)) > '{{ last_watermark }}'::timestamp;

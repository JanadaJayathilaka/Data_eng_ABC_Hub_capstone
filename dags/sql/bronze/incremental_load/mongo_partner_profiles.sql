SELECT *
FROM pulsetrack_raw.mongo_partner_profiles
WHERE extracted_at > '{{ last_watermark }}'::timestamp;

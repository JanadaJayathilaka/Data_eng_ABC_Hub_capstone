SELECT *
FROM pulsetrack_raw.mongo_advertiser_accounts
WHERE extracted_at > '{{ last_watermark }}'::timestamp;

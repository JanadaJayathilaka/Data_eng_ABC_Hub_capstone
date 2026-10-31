SELECT *
FROM pulsetrack_raw.kafka_pulsetrack_conversions
WHERE extracted_at > '{{ last_watermark }}'::timestamp;

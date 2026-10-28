SELECT *
FROM pulsetrack_raw.advertisers_raw
WHERE created_at > '{{ last_watermark }}'
   OR updated_at > '{{ last_watermark }}';

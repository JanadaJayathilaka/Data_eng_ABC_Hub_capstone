SELECT *
FROM pulsetrack_raw.account_managers_raw
WHERE created_at > '{{ last_watermark }}'
   OR updated_at > '{{ last_watermark }}';

SELECT *
FROM pulsetrack_raw.conversion_touchpoints_raw
WHERE created_at > '{{ last_watermark }}'
   OR updated_at > '{{ last_watermark }}';

SELECT *
FROM pulsetrack_raw.revenue_adjustments_raw
WHERE created_at > '{{ last_watermark }}'
   OR updated_at > '{{ last_watermark }}';

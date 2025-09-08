{{ config(
    materialized='table'
) }}
WITH source AS (
    select
        video_id,
        channel_id,
        lower(video_title) as video_title,
        lower(channel_title) as channel_title,
        published_at::timestamptz AT TIME ZONE 'UTC' as published_at,
        engagement_rate
    from {{ source('bronze', 'youtube_videos_clean') }}
    WHERE video_id IS NOT NULL
        AND channel_id IS NOT NULL
)
SELECT
    video_id,
    channel_id,
    video_title,
    channel_title,
    published_at,
    engagement_rate,
    CURRENT_TIMESTAMP AS ingested_at
FROM source
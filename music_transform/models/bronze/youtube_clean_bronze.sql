{{ config(
    materialized='table'
) }}

select
    video_id,
    channel_id,
    lower(video_title) as video_title,
    lower(channel_title) as channel_title,
    published_at::timestamptz AT TIME ZONE 'UTC' as published_at,
    engagement_rate
from {{ source('bronze', 'youtube_videos_clean') }}

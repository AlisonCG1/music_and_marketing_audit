{{ config(
    materialized='table'
) }}

select
    id,
    lower(title) as video_title,
    lower(channel_title) as channel_title,
    published_at::timestamptz AT TIME ZONE 'UTC' as published_at,
    engagement_rate,
    thumbnail_url
from {{ source('bronze', 'youtube_videos_clean') }}

{{ config(
    materialized='table'
) }}

select
    *
from {{ source('bronze', 'youtube_search') }}

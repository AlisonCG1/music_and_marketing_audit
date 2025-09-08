{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT
        video_id,
        channel_id,
        lower(title) AS video_title,
        lower(channel_title) AS channel_title,
        published_at::timestamptz AT TIME ZONE 'UTC' AS published_at,
        duration_seconds,
        views,
        likes,
        dislikes,
        favorite_count,
        comment_count,
        tags,
        engagement_rate
    FROM {{ source('bronze', 'youtube_videos_clean') }}
    WHERE video_id IS NOT NULL
        AND channel_id IS NOT NULL
)
SELECT
    video_id,
    channel_id,
    video_title,
    channel_title,
    published_at,
    duration_seconds,
    views,
    likes,
    dislikes,
    favorite_count,
    comment_count,
    tags,
    engagement_rate,
    CURRENT_TIMESTAMP AS ingested_at
FROM source

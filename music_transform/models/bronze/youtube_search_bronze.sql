{{ config(materialized='table') }}

WITH source AS (
    SELECT
        video_id,
        TRIM(LOWER(REGEXP_REPLACE(genre, '[\(\)"-]', '', 'g'))) AS genre
    FROM {{ source('bronze', 'youtube_search') }}
    WHERE video_id IS NOT NULL
        AND genre IS NOT NULL
)
SELECT
    video_id,
    genre,
    CURRENT_TIMESTAMP AS ingested_at
FROM source
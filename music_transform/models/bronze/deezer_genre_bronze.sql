{{ config(materialized='table') }}
WITH source AS (
    select
        id,
        lower(regexp_replace(title, '[\(\)"-]', '', 'g')) as track_title,
        lower(regexp_replace(artist, '[\(\)"-]', '', 'g')) as artist_name,
        lower(regexp_replace(album, '[\(\)"-]', '', 'g')) as album_name,
        genre_id,
        CAST(duration AS INT) AS duration_seconds
from {{ source('bronze', 'deezer_genres') }}
    WHERE id IS NOT NULL
        AND title IS NOT NULL
        AND artist IS NOT NULL
)
SELECT
    id AS deezer_genre_id,
    track_title,
    artist_name,
    album_name,
    genre_id,
    duration_seconds,
    CURRENT_TIMESTAMP AS ingested_at
FROM source
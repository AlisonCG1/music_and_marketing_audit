-- Active: 1749228330621@@127.0.0.1@5434

{{ config(materialized='table') }}
WITH source AS (
    select
        id,
        lower(regexp_replace(title, '[\(\)"-]', '', 'g')) as track_title,
        lower(regexp_replace(artist, '[\(\)"-]', '', 'g')) as artist_name,
        lower(regexp_replace(album, '[\(\)"-]', '', 'g')) as album_name,
        CAST(duration AS INT) AS duration_seconds
    from {{ source('bronze', 'deezer_charts') }}
)
SELECT
    id AS deezer_chart_id,
    track_title,
    artist_name,
    album_name,
    duration_seconds,
    CURRENT_TIMESTAMP AS ingested_at
FROM source
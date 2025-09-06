{{ config(materialized='table') }}

select
    track_id,
    trim(regexp_replace(lower(track_name), '[\(\)"-]', '', 'g')) as track_name,
    trim(regexp_replace(lower(artist), '[\(\)"-]', '', 'g')) as artist,
    trim(regexp_replace(lower(album), '[\(\)"-]', '', 'g')) as album,
    release_date,
    duration_ms,
    popularity,
    query
from {{ source('bronze', 'spotify_tracks') }}

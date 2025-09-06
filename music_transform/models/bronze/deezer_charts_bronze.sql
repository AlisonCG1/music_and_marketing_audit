{{ config(materialized='table') }}

select
    id,
    lower(regexp_replace(title, '[\(\)"-]', '', 'g')) as title,
    lower(regexp_replace(artist, '[\(\)"-]', '', 'g')) as artist,
    lower(regexp_replace(album, '[\(\)"-]', '', 'g')) as album,
    link,
    duration
from {{ source('bronze', 'deezer_charts') }}

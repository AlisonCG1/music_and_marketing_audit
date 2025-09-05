{{ config(materialized='table', schema='bronze') }}

select
    lower(regexp_replace(title, '[\(\)"-]', '', 'g')) as title,
    lower(regexp_replace(artist, '[\(\)"-]', '', 'g')) as artist,
    lower(regexp_replace(album, '[\(\)"-]', '', 'g')) as album,
    genre_id
from {{ source('bronze', 'deezer_genres') }};

{{ config(materialized='table', schema='bronze') }}

select
    lower(regexp_replace(title, '[\(\)"-]', '', 'g')) as title,
    lower(regexp_replace(artist, '[\(\)"-]', '', 'g')) as artist,
    lower(regexp_replace(album, '[\(\)"-]', '', 'g')) as album,
    chart_position,
    streams,
    genre_id
from {{ source('bronze', 'deezer_charts') }};

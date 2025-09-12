{{ config(materialized='table') }}

WITH source AS (
    select
        track_id,
        trim(regexp_replace(lower(name), '[\(\)"-]', '', 'g')) as track_title,
        trim(regexp_replace(lower(artist), '[\(\)"-]', '', 'g')) as artist_name,
        trim(regexp_replace(lower(album), '[\(\)"-]', '', 'g')) as album_name,
        case
            when release_date ~ '^[0-9]{4}$' then to_date(release_date || '-01-01', 'YYYY-MM-DD')
            when release_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then to_date(release_date, 'YYYY-MM-DD')
            else null
        end as release_date,
        (duration_ms / 1000)::int as duration_seconds,
        trim(lower(query)) AS query
    from {{ source('bronze', 'spotify_tracks') }}
    where track_id IS NOT NULL
      and name IS NOT NULL
      and artist IS NOT NULL
)
select
    track_id,
    track_title,
    artist_name,
    album_name,
    release_date,
    duration_seconds,
    query,
    current_timestamp AS ingested_at
from source

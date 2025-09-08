{{ config(materialized='table') }}

WITH source AS (
    select
        album_id,
        trim(regexp_replace(lower(search_query), '[\(\)"-]', '', 'g')) as search_query,
        trim(regexp_replace(lower(artist), '[\(\)"-]', '', 'g')) as artist_name,
        case 
            when release_date ~ '^[0-9]{4}$' then to_date(release_date || '-01-01', 'YYYY-MM-DD')
            when release_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then to_date(release_date, 'YYYY-MM-DD')
            else null
        end as release_date,
        CAST(total_tracks AS INT) AS total_tracks
    from {{ source('bronze', 'spotify_search') }}
    WHERE album_id IS NOT NULL
      AND artist IS NOT NULL
)
SELECT
    album_id,
    search_query,
    artist_name,
    release_date,
    total_tracks,
    CURRENT_TIMESTAMP AS ingested_at
FROM source

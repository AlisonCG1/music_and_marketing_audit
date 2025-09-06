{{ config(materialized='table') }}

select
    album_id,
    trim(regexp_replace(lower(search_query), '[\(\)"-]', '', 'g')) as search_query,
    trim(regexp_replace(lower(artist), '[\(\)"-]', '', 'g')) as artist,
    case 
        when release_date ~ '^[0-9]{4}$' then to_date(release_date || '-01-01', 'YYYY-MM-DD')
        when release_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then to_date(release_date, 'YYYY-MM-DD')
        else null
    end as release_date,
    total_tracks,
    query
from {{ source('bronze', 'spotify_search') }}

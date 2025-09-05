{{ config(materialized='table') }}

select
    id,
    trim(regexp_replace(lower(name), '[\(\)"-]', '', 'g')) as track_name,
    trim(regexp_replace(lower(artist), '[\(\)"-]', '', 'g')) as artist,
    trim(regexp_replace(lower(album), '[\(\)"-]', '', 'g')) as album,
    release_date
from {{ source('bronze', 'spotify_tracks') }}


{{ config(materialized='table') }}

select
    id,
    trim(regexp_replace(lower(name), '[\(\)"-]', '', 'g')) as search_query,
    trim(regexp_replace(lower(artist), '[\(\)"-]', '', 'g')) as artist,
    case 
        when release_date ~ '^[0-9]{4}$' then to_date(release_date || '-01-01', 'YYYY-MM-DD')
        when release_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then to_date(release_date, 'YYYY-MM-DD')
        else null
    end as release_date
from {{ source('bronze', 'spotify_search') }}

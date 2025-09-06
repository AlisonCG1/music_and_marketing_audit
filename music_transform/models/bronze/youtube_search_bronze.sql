{{ config(materialized='table') }}

select
    *
from {{ source('bronze', 'youtube_search') }}

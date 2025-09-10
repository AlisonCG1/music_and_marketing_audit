{{ config(
    schema = 'gold',
    materialized = 'table'
) }}

select
    artist_name,
    count(distinct track_id) as track_count,
    avg(youtube_engagement_rate) as avg_engagement,
    avg(trend_score) as avg_trend_score
from {{ ref('alldata_silver') }}
group by artist_name
order by avg_trend_score desc

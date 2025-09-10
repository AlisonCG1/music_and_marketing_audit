{{ config(
    schema = 'gold',
    materialized = 'table'
) }}

select
    youtube_genre as genre,
    date_trunc('month', analysis_date) as month,
    avg(youtube_engagement_rate) as avg_engagement,
    avg(trend_score) as avg_trend_score
from {{ ref('alldata_silver') }}
group by 1, 2
order by 2 desc, 1

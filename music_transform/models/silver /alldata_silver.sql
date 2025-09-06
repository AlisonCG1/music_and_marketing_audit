{{ config(materialized='table') }}

with unified_tracks as (
    -- Spotify tracks
    select
        track_id as source_track_id,
        track_name,
        artist as artist_name
    from {{ ref('spotify_tracks_bronze') }}

    union

    -- Deezer charts
    select
        id as source_track_id,
        title as track_name,
        artist as artist_name
    from {{ ref('deezer_charts_bronze') }}

    union

    -- Deezer genres
    select
        id as source_track_id,
        title as track_name,
        artist as artist_name
    from {{ ref('deezer_genre_bronze') }}

    union

    -- YouTube videos
    select
        video_id as source_track_id,
        video_title as track_name,
        channel_title as artist_name
    from {{ ref('youtube_clean_bronze') }}

    union


    select
        album_id as source_track_id,
        search_query as track_name,
        artist as artist_name
    from {{ ref('spotify_search_bronze') }}

    union

    select
        id as source_track_id,
        search_query as track_name,
        artist as artist_name
    from {{ ref('youtube_search_bronze') }}
)

select
    row_number() over () as track_id,
    track_name,
    row_number() over (partition by artist_name order by track_name) as artist_id,
    artist_name
from unified_tracks
group by track_name, artist_name

{{ config(materialized='table') }}

WITH unified_tracks AS (
    SELECT
    COALESCE(st.track_id::text, dc.deezer_chart_id::text, dg.deezer_genre_id::text) AS track_id,
    COALESCE(st.track_title, dc.track_title, dg.track_title) AS track_title,
    COALESCE(st.artist_name, dc.artist_name, dg.artist_name) AS artist_name,
    COALESCE(st.album_name, dc.album_name, dg.album_name) AS album_name,
    COALESCE(st.release_date, ss.release_date) AS release_date,
    COALESCE(st.duration_seconds, dc.duration_seconds, dg.duration_seconds) AS duration_seconds,
    yv.video_id AS youtube_video_id,
    yv.engagement_rate AS youtube_engagement_rate,
    ys.genre AS youtube_genre,
    CASE
        WHEN dc.deezer_chart_id IS NOT NULL THEN 1
        ELSE 0
    END AS is_on_deezer_charts,
    CURRENT_DATE AS analysis_date
    FROM {{ ref('spotify_tracks_bronze') }} st
    FULL OUTER JOIN {{ ref('deezer_charts_bronze') }} dc
        ON st.track_title = dc.track_title
        AND st.artist_name = dc.artist_name
    FULL OUTER JOIN {{ ref('deezer_genre_bronze') }} dg
        ON COALESCE(st.track_title, dc.track_title) = dg.track_title
        AND COALESCE(st.artist_name, dc.artist_name) = dg.artist_name
    LEFT JOIN {{ ref('spotify_search_bronze') }} ss
        ON COALESCE(st.album_name, dc.album_name, dg.album_name) = ss.search_query
        AND COALESCE(st.artist_name, dc.artist_name, dg.artist_name) = ss.artist_name
    LEFT JOIN {{ ref('youtube_clean_bronze') }} yv
        ON COALESCE(st.track_title, dc.track_title, dg.track_title) = yv.video_title
        OR (COALESCE(st.artist_name, dc.artist_name, dg.artist_name) = yv.channel_title
            AND COALESCE(st.release_date, ss.release_date) >= yv.published_at - INTERVAL '7 days')
    LEFT JOIN {{ ref('youtube_search_bronze') }} ys
        ON yv.video_id = ys.video_id
    WHERE COALESCE(st.track_title, dc.track_title, dg.track_title) IS NOT NULL
),

trend_metrics AS (
    SELECT
        track_id,
        track_title,
        artist_name,
        album_name,
        release_date,
        duration_seconds,
        youtube_engagement_rate,
        youtube_genre,
       COALESCE(youtube_engagement_rate, 0) * 100 AS trend_score,
        CASE
            WHEN youtube_engagement_rate > 0.05
                AND release_date >= CURRENT_DATE - INTERVAL '30 days'
                THEN 'High'
            WHEN youtube_engagement_rate > 0.02
                THEN 'Medium'
            ELSE 'Low'
        END AS virality_potential,
        analysis_date
    FROM unified_tracks
    WHERE track_title NOT LIKE '%drop t%'
        AND track_title NOT LIKE '$%'
        AND artist_name NOT LIKE '%electronic%'
)

SELECT
    track_id,
    track_title,
    artist_name,
    album_name,
    release_date,
    duration_seconds,
    youtube_engagement_rate,
    youtube_genre,
    trend_score,
    virality_potential,
    analysis_date
FROM trend_metrics
WHERE trend_score IS NOT NULL
ORDER BY trend_score DESC, analysis_date
from dagster import job, ScheduleDefinition, Definitions
from src.ingestion.youtubeapi import youtube_videos_op, youtube_search_op
from src.ingestion.spotifyapi import spotify_search_op
from src.ingestion.deezerapi import deezer_charts_op, deezer_genres_op, deezer_albums_op
from src.minioclient import minio_resource


@job(resource_defs={"minio": minio_resource})
def youtube_job():
    videos_df = youtube_videos_op()
    youtube_search_op(videos_df=videos_df)


@job(resource_defs={"minio": minio_resource})
def spotify_job():
    spotify_search_op()


@job(resource_defs={"minio": minio_resource})
def deezer_job():
    charts_df = deezer_charts_op()
    deezer_genres_op()
    deezer_albums_op(charts_df=charts_df)


youtube_schedule = ScheduleDefinition(
    job=youtube_job,
    cron_schedule="5 7 * * *", 
    execution_timezone="America/Chicago",
)

spotify_schedule = ScheduleDefinition(
    job=spotify_job,
    cron_schedule="0 8 * * *",  
    execution_timezone="America/Chicago",
)

deezer_schedule = ScheduleDefinition(
    job=deezer_job,
    cron_schedule="30 8 * * *",  # 3:30 AM CDT
    execution_timezone="America/Chicago",
)


defs = Definitions(
    jobs=[youtube_job, spotify_job, deezer_job],
    schedules=[youtube_schedule, spotify_schedule, deezer_schedule],
    resources={"minio": minio_resource},
)
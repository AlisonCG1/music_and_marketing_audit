from dagster import job, ScheduleDefinition, Definitions
from src.ingestion.youtubeapi import youtube_videos_op, youtube_search_op
from src.ingestion.spotifyapi import spotify_search_op
from src.ingestion.deezerapi import deezer_charts_op, deezer_genres_op, deezer_albums_op
from src.minioclient import minio_resource
from src.duckdb.minio_to_duckdb import load_and_update_duckdb_to_postgres
from src.silver.silver import youtube_videos_clean_op
from src.dbt.dbt_job import dbt_job



@job(resource_defs={"minio": minio_resource})
def youtube_job():
    videos_df = youtube_videos_op()
    youtube_search_op(videos_df=videos_df)

@job(resource_defs={"minio": minio_resource})
def youtube_clean_job():
    youtube_videos_clean_op()

@job(resource_defs={"minio": minio_resource})
def spotify_job():
    spotify_search_op()


@job(resource_defs={"minio": minio_resource})
def deezer_job():
    charts_df = deezer_charts_op()
    deezer_genres_op()
    deezer_albums_op(charts_df=charts_df)

@job(resource_defs={"minio": minio_resource})
def duckdb_to_postgres_job():
    load_and_update_duckdb_to_postgres()


youtube_schedule = ScheduleDefinition(
    job=youtube_job,
    cron_schedule="5 7 * * *", 
    execution_timezone="America/Chicago",
)

youtube_clean_schedule = ScheduleDefinition(
    job=youtube_clean_job,
    cron_schedule="0 6 * * *",  # 6:00 AM CDT
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

duckdb_schedule = ScheduleDefinition(
    job=duckdb_to_postgres_job,
    cron_schedule="0 10 * * *",  # 10:00 AM CDT
    execution_timezone="America/Chicago",
)

dbt_schedule = ScheduleDefinition(
    job=dbt_job,
    cron_schedule="30 10 * * *",  # runs daily at 10:30 AM CDT
    execution_timezone="America/Chicago",
)


defs = Definitions(
    jobs=[youtube_job, spotify_job, deezer_job, duckdb_to_postgres_job, youtube_clean_job,  dbt_job],
    schedules=[youtube_schedule, spotify_schedule, deezer_schedule, duckdb_schedule, youtube_clean_schedule, dbt_schedule],
    resources={"minio": minio_resource},
)

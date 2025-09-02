from dagster import Definitions
from src.ingestion.youtubeapi import youtube_videos, youtube_search
from src.minioclient import minio_resource
from dagster import define_asset_job, ScheduleDefinition

# Define job
youtube_job = define_asset_job(
    name="youtube_job",
    selection=["youtube_videos", "youtube_search"]
)

# Define schedule
daily_schedule = ScheduleDefinition(
    job=youtube_job,
    cron_schedule="5 2 * * *",
    execution_timezone="America/Chicago"
)

# Definitions
defs = Definitions(
    assets=[youtube_videos, youtube_search],
    jobs=[youtube_job],
    schedules=[daily_schedule],
    resources={"minio": minio_resource}
)

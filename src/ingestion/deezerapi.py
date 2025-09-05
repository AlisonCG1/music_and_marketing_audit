import requests
import pandas as pd
import os
from io import BytesIO
import time
from dagster import op, Out, Output, resource
from minio import Minio
import logging
from dotenv import load_dotenv


load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

@resource
def minio_resource(context):
    import os
    client = Minio(
        endpoint=os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=os.getenv("MINIO_SECURE", "False").lower() == "true"
    )
    return client


def load_from_minio(minio_client, filename):
    bucket = os.getenv("BUCKET_NAME")
    try:
        obj = minio_client.get_object(bucket, filename)
        return pd.read_parquet(BytesIO(obj.read()))
    except:
        return pd.DataFrame()

def upload_to_minio(minio_client, df, filename):
    if df.empty:
        logger.warning(f"Nothing to upload for {filename}, DataFrame is empty")
        return
    
    bucket = os.getenv("BUCKET_NAME")
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    minio_client.put_object(bucket, filename, buffer, length=len(buffer.getvalue()))
    logger.info(f"Uploaded {filename} successfully")

def deezer_request(url, retries=3, delay=1):
    for _ in range(retries):
        resp = requests.get(url)
        if resp.status_code == 200:
            return resp.json()
        time.sleep(delay)
    logger.warning(f"Failed request: {url}")
    return None

@op(out={"charts_df": Out()}, required_resource_keys={"minio"})
def deezer_charts_op(context):
    minio_client = context.resources.minio
    chart_data = deezer_request("https://api.deezer.com/chart")
    chart_tracks = chart_data.get("tracks", {}).get("data", []) if chart_data else []
    
    rows = [{"id": t["id"], "title": t["title"], "artist": t["artist"]["name"],
             "album": t["album"]["title"], "link": t["link"], "duration": t["duration"]}
            for t in chart_tracks]
    
    df = pd.DataFrame(rows)
    existing_df = load_from_minio(minio_client, "deezer_charts.parquet")
    if not existing_df.empty:
        df = pd.concat([existing_df, df]).drop_duplicates(subset=["id"], keep="last")
    
    upload_to_minio(minio_client, df, "deezer_charts.parquet")
    context.log.info(f"Charts DF rows: {len(df)}")
    yield Output(df, output_name="charts_df", metadata={"rows": len(df)})


@op(out={"genre_df": Out()}, required_resource_keys={"minio"})
def deezer_genres_op(context):
    minio_client = context.resources.minio
    genre_ids = [132, 116, 152, 113, 106]  
    rows = []

    for gid in genre_ids:
        artists = deezer_request(f"https://api.deezer.com/genre/{gid}/artists").get("data", [])
        for artist in artists:
            top_tracks = deezer_request(f"https://api.deezer.com/artist/{artist['id']}/top?limit=50").get("data", [])
            for t in top_tracks:
                rows.append({
                    "id": t["id"], "title": t["title"], "artist": t["artist"]["name"],
                    "album": t["album"]["title"], "link": t["link"], "duration": t["duration"], "genre_id": gid
                })
            time.sleep(0.2)
    
    df = pd.DataFrame(rows)
    existing_df = load_from_minio(minio_client, "deezer_genres.parquet")
    if not existing_df.empty:
        df = pd.concat([existing_df, df]).drop_duplicates(subset=["id"], keep="last")
    
    upload_to_minio(minio_client, df, "deezer_genres.parquet")
    context.log.info(f"Genre DF rows: {len(df)}")
    yield Output(df, output_name="genre_df", metadata={"rows": len(df)})

def get_album_tracks(album_id):
    data = deezer_request(f"https://api.deezer.com/album/{album_id}/tracks")
    return data.get("data", []) if data else []

@op(out={"albums_df": Out()}, required_resource_keys={"minio"})
def deezer_albums_op(context, charts_df: pd.DataFrame):
    minio_client = context.resources.minio
    album_ids = charts_df["album"].unique().tolist()
    rows = []

    for aid in album_ids:
        tracks = get_album_tracks(aid)
        for t in tracks:
            rows.append({
                "id": t["id"], "title": t["title"], "artist": t["artist"]["name"],
                "album": t["album"]["title"], "link": t["link"], "duration": t["duration"]
            })
        time.sleep(0.2)
    
    df = pd.DataFrame(rows)
    existing_df = load_from_minio(minio_client, "deezer_albums.parquet")
    if not existing_df.empty:
        df = pd.concat([existing_df, df]).drop_duplicates(subset=["id"], keep="last")
    
    upload_to_minio(minio_client, df, "deezer_albums.parquet")
    context.log.info(f"Albums DF rows: {len(df)}")
    yield Output(df, output_name="albums_df", metadata={"rows": len(df)})
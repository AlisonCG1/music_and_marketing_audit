import os
import requests
import pandas as pd
from io import BytesIO
import logging
from dotenv import load_dotenv
import time
from dagster import op, Out, Output, resource
from minio import Minio
from minio.error import S3Error

# Setup logging with console and file output
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
file_handler = logging.FileHandler("spotify_etl.log")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Load environment variables
load_dotenv()

# Validate environment variables
required_vars = ["SPOTIFY_CLIENT_ID", "SPOTIFY_CLIENT_SECRET", "MINIO_ENDPOINT", 
                 "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "BUCKET_NAME"]
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    logger.error(f"Missing environment variables: {missing_vars}")
    raise ValueError(f"Missing environment variables: {missing_vars}")

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("BUCKET_NAME")
MINIO_SECURE = os.getenv("MINIO_SECURE", "False").lower() == "true"

@resource
def minio_resource(context):
    try:
        client = Minio(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )
        # Check if bucket exists, create if it doesn't
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
            context.log.info(f"Created MinIO bucket: {MINIO_BUCKET}")
        else:
            context.log.info(f"MinIO bucket {MINIO_BUCKET} already exists")
        client.list_buckets()  # Test connectivity
        context.log.info("Connected to MinIO successfully")
        logger.info("Connected to MinIO successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize MinIO client: {e}")
        context.log.error(f"Failed to initialize MinIO client: {e}")
        raise ValueError(f"Failed to initialize MinIO client: {e}")

def load_from_minio(minio_client, filename, context):
    try:
        context.log.info(f"Attempting to load {filename} from MinIO bucket {MINIO_BUCKET}")
        logger.info(f"Attempting to load {filename} from MinIO bucket {MINIO_BUCKET}")
        obj = minio_client.get_object(MINIO_BUCKET, filename)
        df = pd.read_parquet(BytesIO(obj.read()))
        context.log.info(f"Loaded {filename} from MinIO: shape={df.shape}")
        logger.info(f"Loaded {filename} from MinIO: shape={df.shape}")
        return df
    except S3Error as e:
        if e.code == "NoSuchKey":
            context.log.info(f"No existing {filename} found in MinIO; starting fresh")
            logger.info(f"No existing {filename} found in MinIO; starting fresh")
            return pd.DataFrame()
        else:
            context.log.error(f"MinIO S3Error while loading {filename}: {e}")
            logger.error(f"MinIO S3Error while loading {filename}: {e}")
            raise
    except Exception as e:
        context.log.error(f"Error loading {filename} from MinIO: {e}")
        logger.error(f"Error loading {filename} from MinIO: {e}")
        raise

def upload_to_minio(minio_client, df, filename, context, format="parquet"):
    if df is None or df.empty:
        context.log.warning(f"Nothing to upload for {filename}, DataFrame is empty")
        logger.warning(f"Nothing to upload for {filename}, DataFrame is empty")
        return

    buffer = BytesIO()
    if format == "parquet":
        df.to_parquet(buffer, index=False)
    else:
        df.to_csv(buffer, index=False)
    buffer.seek(0)
    size = len(buffer.getvalue())
    
    context.log.info(f"Uploading {filename} ({size} bytes) to MinIO bucket {MINIO_BUCKET}")
    logger.info(f"Uploading {filename} ({size} bytes) to MinIO bucket {MINIO_BUCKET}")

    try:
        minio_client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=filename,
            data=buffer,
            length=size,
            content_type="application/octet-stream"
        )
        context.log.info(f"Uploaded {filename} successfully")
        logger.info(f"Uploaded {filename} successfully")
    except S3Error as e:
        context.log.error(f"MinIO S3Error while uploading {filename}: {e}")
        logger.error(f"MinIO S3Error while uploading {filename}: {e}")
        raise
    except Exception as e:
        context.log.error(f"Failed to upload {filename}: {e}")
        logger.error(f"Failed to upload {filename}: {e}")
        raise

def get_spotify_token():
    url = "https://accounts.spotify.com/api/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "client_id": SPOTIFY_CLIENT_ID,
        "client_secret": SPOTIFY_CLIENT_SECRET,
    }
    response = requests.post(url, headers=headers, data=data)
    if response.status_code != 200:
        logger.error(f"Failed to get token: {response.status_code} - {response.text}")
        raise Exception(f"Token retrieval failed: {response.text}")
    token = response.json()["access_token"]
    logger.info("Spotify token retrieved successfully")
    return token

def spotify_request(url, token, retries=3):
    headers = {"Authorization": f"Bearer {token}"}
    for attempt in range(retries):
        response = requests.get(url, headers=headers)
        if response.status_code == 429:
            wait = int(response.headers.get("Retry-After", 1))
            logger.warning(f"Rate limit exceeded, retrying in {wait} seconds...")
            time.sleep(wait)
            continue
        elif response.status_code != 200:
            logger.error(f"Spotify API request failed: {response.status_code} - {response.text}")
            raise Exception(f"Spotify API request failed: {response.text}")
        return response.json()
    logger.error(f"Failed after {retries} retries: {url}")
    raise Exception(f"Spotify API request failed after {retries} retries")

def paginate_search_by_genre(query, token, limit_per_page=50, max_results=2000):
    import math
    results = []
    total_fetched = 0
    per_request_limit = min(limit_per_page, 100)
    num_requests = math.ceil(max_results / per_request_limit)

    for i in range(num_requests):
        url = (
            f"https://api.spotify.com/v1/recommendations"
            f"?seed_genres={query}"
            f"&limit={per_request_limit}"
        )
        try:
            data = spotify_request(url, token)
            if not data or "tracks" not in data:
                logger.warning(f"No data returned for genre '{query}' on request {i+1}")
                break
            tracks = data["tracks"]
            results.extend(tracks)
            total_fetched += len(tracks)
            logger.info(f"Fetched {len(tracks)} tracks for genre '{query}' (total: {total_fetched})")
            if total_fetched >= max_results:
                break
        except Exception as e:
            logger.error(f"Error fetching tracks for genre '{query}' on request {i+1}: {e}")
            break
    logger.info(f"Total tracks retrieved for genre '{query}': {len(results)}")
    return results

def get_album_tracks(album_id, token, market="US"):
    tracks = []
    limit = 50
    offset = 0
    while True:
        url = f"https://api.spotify.com/v1/albums/{album_id}/tracks?market={market}&limit={limit}&offset={offset}"
        try:
            data = spotify_request(url, token)
            if not data:
                logger.warning(f"No data returned for album {album_id}")
                break
            items = data.get("items")
            if items is None:
                logger.warning(f"No 'items' key in response for album {album_id}: {data}")
                break
            if len(items) == 0:
                logger.info(f"No tracks found for album {album_id} at offset {offset}")
                break
            tracks.extend(items)
            logger.debug(f"Retrieved {len(items)} tracks for album {album_id} at offset {offset}")
            if len(items) < limit:
                break
            offset += len(items)
        except Exception as e:
            logger.error(f"Error fetching tracks for album {album_id}: {e}")
            break
    logger.info(f"Retrieved total {len(tracks)} tracks for album {album_id}")
    return tracks

@op(out={"search_df": Out(), "tracks_df": Out()}, required_resource_keys={"minio"})
def spotify_search_op(context):
    minio_client = context.resources.minio
    context.log.info("Starting Spotify ETL")
    logger.info("Starting Spotify ETL")

    token = get_spotify_token()
    context.log.debug(f"Spotify token (first 10 chars): {token[:10]}...")

    # Load existing parquet data from MinIO
    existing_search_df = load_from_minio(minio_client, "spotify_search.parquet", context)
    existing_tracks_df = load_from_minio(minio_client, "spotify_tracks.parquet", context)

    # List of queries/genres to search
    queries = ["pop", "electronic", "heavy metal", "country", "jazz",
               "hip hop", "classical", "folk", "rock", "reggae", "blues", "r&b"]
    
    search_rows = []
    track_rows = []
    album_map = {}

    # Search albums by query
    for q in queries:
        search_url = f"https://api.spotify.com/v1/search?q={q}&type=album&limit=50"
        data = spotify_request(search_url, token)
        albums = data.get("albums", {}).get("items", [])
        context.log.info(f"Query '{q}' returned {len(albums)} albums")

        for album in albums:
            album_id = album["id"]
            album_map[album_id] = q  # store query/genre mapping
            search_rows.append({
                "album_id": album_id,
                "name": album["name"],
                "artist": album["artists"][0]["name"],
                "release_date": album["release_date"],
                "total_tracks": album["total_tracks"],
                "query": q
            })

            # Get tracks for the album
            album_tracks = get_album_tracks(album_id, token)
            for t in album_tracks:
                track_rows.append({
                    "track_id": t["id"],
                    "name": t["name"],
                    "artist": t["artists"][0]["name"],
                    "album": album["name"],
                    "release_date": album["release_date"],
                    "duration_ms": t["duration_ms"],
                    "popularity": t.get("popularity"),
                    "query": q
                })
            time.sleep(0.5)

    # Convert to DataFrames
    search_df = pd.DataFrame(search_rows)
    tracks_df = pd.DataFrame(track_rows)

    # Merge with existing data
    if not existing_search_df.empty:
        search_df = pd.concat([existing_search_df, search_df]).drop_duplicates(subset=["album_id"])
    if not existing_tracks_df.empty:
        tracks_df = pd.concat([existing_tracks_df, tracks_df]).drop_duplicates(subset=["track_id"])

    # Upload updated parquet files to MinIO
    upload_to_minio(minio_client, search_df, "spotify_search.parquet", context)
    upload_to_minio(minio_client, tracks_df, "spotify_tracks.parquet", context)

    context.log.info(f"Spotify ETL complete: {len(search_df)} albums, {len(tracks_df)} tracks")
    logger.info(f"Spotify ETL complete: {len(search_df)} albums, {len(tracks_df)} tracks")

    yield Output(search_df, output_name="search_df", metadata={"rows": len(search_df)})
    yield Output(tracks_df, output_name="tracks_df", metadata={"rows": len(tracks_df)})
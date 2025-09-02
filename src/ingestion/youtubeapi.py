import os
import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
from datetime import datetime
import isodate
from dotenv import load_dotenv
import boto3
from io import BytesIO
import logging
import time
from googleapiclient.errors import HttpError
from dagster import asset, Output

# Set logging to DEBUG
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")


# Initialize YouTube client
try:
    youtube = googleapiclient.discovery.build(
        "youtube", "v3", developerKey=API_KEY, cache_discovery=False
    )
    logger.info("YouTube API client initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize YouTube API client: {e}")
    raise

def fetch_videos(video_ids, retries=3, backoff_factor=2):
    video_data = []
    for attempt in range(retries):
        try:
            logger.debug(f"Fetching videos for IDs: {video_ids}, attempt {attempt + 1}")
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=",".join(video_ids)
            )
            response = request.execute()
            logger.debug(f"videos.list response: {response}")
            for item in response.get("items", []):
                video_data.append({
                    "video_id": item["id"],
                    "title": item["snippet"]["title"],
                    "channel_id": item["snippet"]["channelId"],
                    "channel_title": item["snippet"]["channelTitle"],
                    "published_at": item["snippet"]["publishedAt"],
                    "duration_seconds": isodate.parse_duration(item["contentDetails"]["duration"]).total_seconds(),
                    "views": int(item["statistics"].get("viewCount", 0)),
                    "likes": int(item["statistics"].get("likeCount", 0)),
                    "dislikes": int(item["statistics"].get("dislikeCount", 0)),
                    "favorite_count": int(item["statistics"].get("favoriteCount", 0)),
                    "comment_count": int(item["statistics"].get("commentCount", 0)),
                    "tags": ",".join(item["snippet"].get("tags", [])),
                    "thumbnail_url": item["snippet"]["thumbnails"].get("maxres", {}).get("url")
                })
            logger.info(f"Fetched {len(response.get('items', []))} videos")
            return video_data
        except HttpError as e:
            logger.error(f"Error fetching videos: {e}")
            if "quotaExceeded" in str(e):
                logger.warning(f"YouTube API quota exceeded. Retry {attempt + 1}/{retries} after {backoff_factor ** attempt} seconds.")
                if attempt < retries - 1:
                    time.sleep(backoff_factor ** attempt)
                else:
                    logger.error("Max retries reached.")
                    return []
            else:
                raise
    return []

def search_videos(queries, max_results_per_query=10):
    all_video_ids = []
    search_data = []
    for query in queries:
        next_page_token = None
        total_results = 0
        while total_results < max_results_per_query:
            try:
                logger.debug(f"Searching videos for query: {query}, pageToken: {next_page_token}")
                request = youtube.search().list(
                    part="snippet",
                    q=query,
                    type="video",
                    regionCode="US",
                    maxResults=min(max_results_per_query - total_results, 50),
                    order="viewCount",
                    pageToken=next_page_token
                )
                response = request.execute()
                logger.debug(f"search.list response for query {query}: {response}")
                items = response.get("items", [])
                for item in items:
                    video_id = item["id"]["videoId"]
                    search_data.append({
                        "video_id": video_id,
                        "title": item["snippet"]["title"],
                        "channel_title": item["snippet"]["channelTitle"],
                        "published_at": item["snippet"]["publishedAt"],
                        "genre": query
                    })
                    all_video_ids.append(video_id)
                total_results += len(items)
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break
            except HttpError as e:
                logger.error(f"Error searching videos for query '{query}': {e}")
                if "quotaExceeded" in str(e):
                    logger.warning("YouTube API quota exceeded for search.")
                    return all_video_ids, search_data
                break
    logger.info(f"Collected {len(all_video_ids)} video IDs from search")
    return all_video_ids, search_data

def load_from_minio(filename):
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        obj = s3_client.get_object(Bucket=MINIO_BUCKET, Key=filename)
        df = pd.read_parquet(BytesIO(obj["Body"].read()))
        logger.info(f"Loaded {filename} from MinIO: shape={df.shape}")
        return df
    except s3_client.exceptions.NoSuchKey:
        logger.info(f"No existing {filename} found in MinIO; starting fresh")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error loading {filename} from MinIO: {e}")
        return pd.DataFrame()

def upload_to_minio(tabletominio, filename, format="parquet"):
    try:
        if tabletominio is None or (isinstance(tabletominio, pd.DataFrame) and tabletominio.empty):
            logger.error(f"Cannot upload {filename}: DataFrame is None or empty")
            return
        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        buffer = BytesIO()
        if format == "parquet":
            tabletominio.to_parquet(buffer, index=False)
        else:
            tabletominio.to_csv(buffer, index=False)
        buffer.seek(0)
        s3_client.put_object(Bucket=MINIO_BUCKET, Key=filename, Body=buffer.getvalue())
        logger.info(f"Uploaded {filename} to MinIO bucket {MINIO_BUCKET}")
    except Exception as e:
        logger.error(f"Error uploading {filename} to MinIO: {e}")

@asset
def youtube_videos():
    genres = ["pop", "electronic", "heavy metal", "country", "jazz", "hip hop", "classical", "folk", "rock", "reggae", "blues", "r&b"]
    search_queries = genres 
    
    video_ids, search_data_list = search_videos(search_queries, max_results_per_query=10)
    video_ids.append("Eb8rXCzJMUc")
    video_ids = list(set(video_ids))
    logger.info(f"Total unique video IDs after deduplication: {len(video_ids)}")

    video_data_list = []
    if video_ids:
        for i in range(0, len(video_ids), 50):
            batch_ids = video_ids[i:i+50]
            batch_data = fetch_videos(batch_ids)
            video_data_list.extend(batch_data)
    
    new_videos_df = pd.DataFrame(video_data_list)
    existing_videos_df = load_from_minio("youtube_videos.parquet")
    
    if not new_videos_df.empty and not existing_videos_df.empty:
        videos_df = pd.concat([existing_videos_df, new_videos_df]).drop_duplicates(subset=["video_id"], keep="last")
    elif not new_videos_df.empty:
        videos_df = new_videos_df
    else:
        videos_df = existing_videos_df
        logger.warning("No new video data collected; using existing videos_df")

    if not videos_df.empty:
        videos_df["published_at"] = pd.to_datetime(videos_df["published_at"])
        videos_df["engagement_rate"] = (
            videos_df["likes"] + videos_df["dislikes"] + 
            videos_df["favorite_count"] + videos_df["comment_count"]
        ) / videos_df["views"].replace(0, 1)
    
    logger.debug(f"videos_df after merge: shape={videos_df.shape}, empty={videos_df.empty}, data=\n{videos_df.to_string()}")
    upload_to_minio(videos_df, "youtube_videos.parquet")
    
    logger.info("Video Metrics:")
    if not videos_df.empty and all(col in videos_df.columns for col in ["title", "views", "likes", "engagement_rate"]):
        print(videos_df[["title", "views", "likes", "engagement_rate"]].head())
    
    return Output(videos_df, metadata={"num_rows": len(videos_df)})

@asset
def youtube_search(youtube_videos):
    genres = ["pop", "electronic", "heavy metal", "country", "jazz", "hip hop", "classical", "folk", "rock", "reggae", "blues", "r&b"]
    search_queries = genres  # Only genres, no artists
    
    _, search_data_list = search_videos(search_queries, max_results_per_query=10)
    new_search_df = pd.DataFrame(search_data_list)
    existing_search_df = load_from_minio("youtube_search.parquet")
    
    if not new_search_df.empty and not existing_search_df.empty:
        search_df = pd.concat([existing_search_df, new_search_df]).drop_duplicates(subset=["video_id"], keep="last")
    elif not new_search_df.empty:
        search_df = new_search_df
    else:
        search_df = existing_search_df
        logger.warning("No new search data collected; using existing search_df")
    
    logger.debug(f"search_df after merge: shape={search_df.shape}, empty={search_df.empty}, data=\n{search_df.to_string()}")
    upload_to_minio(search_df, "youtube_search.parquet")
    
    logger.info("Videos Found by Genre Query:")
    if not search_df.empty and all(col in search_df.columns for col in ["title", "video_id", "genre"]):
        print(search_df[["title", "video_id", "genre"]].head())
    
    return Output(search_df, metadata={"num_rows": len(search_df)})
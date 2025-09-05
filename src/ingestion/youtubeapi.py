import os
import pandas as pd
from datetime import datetime
import isodate
from io import BytesIO
from dotenv import load_dotenv
import logging
import time
from googleapiclient.errors import HttpError
from dagster import op, Out, In, Output, resource
from minio import Minio
from minio.error import S3Error
from googleapiclient.discovery import build

# Set up logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")
MINIO_BUCKET = os.getenv("BUCKET_NAME", "bronze-layer")
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

# MinIO resource
@resource
def minio_resource(context):
    return Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )

# Initialize YouTube client
def init_youtube_client(api_key):
    try:
        youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=api_key, cache_discovery=False)
        logger.info("YouTube API client initialized successfully.")
        return youtube
    except Exception as e:
        logger.error(f"Failed to initialize YouTube API client: {e}")
        raise

# Fetch video details
def fetch_videos(youtube, video_ids, retries=3, backoff_factor=2):
    video_data = []
    quota_cost = len(video_ids) * 3  # 3 units per video (snippet, contentDetails, statistics)
    logger.info(f"Estimated quota cost for videos.list: {quota_cost} units")
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
                try:
                    video_data.append({
                        "video_id": item["id"],
                        "title": item["snippet"]["title"],
                        "channel_id": item["snippet"]["channelId"],
                        "channel_title": item["snippet"]["channelTitle"],
                        "published_at": item["snippet"]["publishedAt"],
                        "duration_seconds": isodate.parse_duration(item["contentDetails"]["duration"]).total_seconds(),
                        "views": int(item["statistics"].get("viewCount", 0)),
                        "likes": int(item["statistics"].get("likeCount", 0)),
                        "favorite_count": int(item["statistics"].get("favoriteCount", 0)),
                        "comment_count": int(item["statistics"].get("commentCount", 0)),
                        "tags": ",".join(item["snippet"].get("tags", [])),
                        "thumbnail_url": item["snippet"]["thumbnails"].get("maxres", {}).get("url", "")
                    })
                except KeyError as e:
                    logger.error(f"KeyError processing video {item.get('id')}: {e}")
                    continue
            logger.info(f"Fetched {len(video_data)} videos")
            return video_data
        except HttpError as e:
            logger.error(f"Error fetching videos: {e}")
            if "quotaExceeded" in str(e):
                logger.warning(f"YouTube API quota exceeded on attempt {attempt + 1}/{retries}.")
                raise Exception("Quota exceeded. Wait until midnight PT for reset.")
            if attempt < retries - 1:
                time.sleep(backoff_factor ** attempt)
            else:
                logger.error("Max retries reached for video fetch.")
                return []
    return []

# Search videos and cache results
def search_videos(youtube, search_queries, max_results_per_query=50, minio_client=None):
    all_data_list = []
    all_video_ids = []

    for query in search_queries:
        filename = f"youtube_search_{query}.parquet"
        try:
            obj = minio_client.get_object(MINIO_BUCKET, filename)
            cached_df = pd.read_parquet(BytesIO(obj.read()))
            cached_df["genre"] = query
            all_data_list.append(cached_df)
            all_video_ids.extend(cached_df["video_id"].tolist())
            logger.info(f"[CACHE HIT] Loaded {len(cached_df)} videos for '{query}' from MinIO.")
            continue
        except S3Error as e:
            if e.code == "NoSuchKey":
                logger.info(f"[CACHE MISS] No cached data for '{query}', fetching from YouTube...")
            else:
                logger.error(f"Error accessing {filename} in MinIO: {e}")
                continue

        try:
            request = youtube.search().list(
                part="snippet",
                q=query,
                type="video",
                regionCode="US",
                maxResults=min(max_results_per_query, 50),
                order="viewCount"
            )
            response = request.execute()
            video_ids = [item["id"]["videoId"] for item in response.get("items", [])]
            df = pd.DataFrame({"video_id": video_ids, "genre": query})
            all_data_list.append(df)
            all_video_ids.extend(video_ids)

            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            minio_client.put_object(
                MINIO_BUCKET,
                filename,
                buffer,
                length=buffer.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            logger.info(f"[API CALL] Saved {len(video_ids)} videos for '{query}' to MinIO.")
        except HttpError as e:
            if e.resp.status == 403 and "quotaExceeded" in str(e):
                logger.error("Quota exceeded. Wait until midnight PT for reset.")
                raise
            else:
                logger.error(f"Error fetching search results for '{query}': {e}")
                continue

    if all_data_list:
        merged_df = pd.concat(all_data_list, ignore_index=True).drop_duplicates(subset=["video_id"])
    else:
        merged_df = pd.DataFrame()

    return merged_df, all_video_ids, all_data_list

# Merge search tables
def merge_search_tables(all_data_list, minio_client, merged_filename="youtube_search.parquet"):
    if not all_data_list:
        logger.warning("No search tables to merge")
        return pd.DataFrame()
    merged_df = pd.concat(all_data_list, ignore_index=True).drop_duplicates(subset=["video_id"])
    try:
        upload_to_minio(minio_client, merged_df, merged_filename)
        logger.info(f"Merged search table saved: {merged_filename} with {len(merged_df)} rows")
    except Exception as e:
        logger.error(f"Error uploading merged search table: {e}")
        raise
    return merged_df

# Load from MinIO
def load_from_minio(minio_client, filename):
    try:
        obj = minio_client.get_object(bucket_name=MINIO_BUCKET, object_name=filename)
        df = pd.read_parquet(BytesIO(obj.read()))
        logger.info(f"Loaded {filename} from MinIO: shape={df.shape}")
        return df
    except S3Error as e:
        if e.code == "NoSuchKey":
            logger.info(f"No existing {filename} found in MinIO; starting fresh")
            return pd.DataFrame()
        logger.error(f"Error loading {filename} from MinIO: {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading {filename} from MinIO: {e}")
        raise

# Upload to MinIO
def upload_to_minio(minio_client, df, filename, format="parquet"):
    try:
        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            logger.warning(f"Cannot upload {filename}: DataFrame is None or empty")
            return
        buffer = BytesIO()
        if format == "parquet":
            df.to_parquet(buffer, index=False)
        else:
            df.to_csv(buffer, index=False)
        buffer.seek(0)
        minio_client.put_object(bucket_name=MINIO_BUCKET, object_name=filename, data=buffer, length=len(buffer.getvalue()))
        logger.info(f"Uploaded {filename} to MinIO bucket {MINIO_BUCKET}")
    except Exception as e:
        logger.error(f"Error uploading {filename} to MinIO: {e}")
        raise

@op(out={"videos_df": Out()}, required_resource_keys={"minio"})
def youtube_videos_op(context):
    minio_client = context.resources.minio
    youtube = init_youtube_client(API_KEY)
    genres = ["pop", "electronic", "heavy metal", "country", "jazz", "hip hop",
              "classical", "folk", "rock", "reggae", "blues", "r&b"]
    
    try:
        # Get search results (cached + new)
        search_df, all_video_ids, _ = search_videos(youtube, genres, max_results_per_query=100, minio_client=minio_client)
        
        # Load existing video details
        existing_videos_df = load_from_minio(minio_client, "youtube_videos.parquet")
        existing_ids = set(existing_videos_df["video_id"].tolist()) if not existing_videos_df.empty else set()

        # Fetch missing videos
        missing_ids = list(set(all_video_ids) - existing_ids)
        logger.info(f"Missing videos to fetch: {len(missing_ids)}")

        video_data_list = []
        for i in range(0, len(missing_ids), 50):
            batch_ids = missing_ids[i:i+50]
            video_data_list.extend(fetch_videos(youtube, batch_ids))
            time.sleep(0.5)

        new_videos_df = pd.DataFrame(video_data_list)
        videos_df = pd.concat([existing_videos_df, new_videos_df]).drop_duplicates(subset=["video_id"], keep="last") if not new_videos_df.empty else existing_videos_df

        if not videos_df.empty:
            videos_df["published_at"] = pd.to_datetime(videos_df["published_at"])
            videos_df["engagement_rate"] = (
                videos_df["likes"] +
                videos_df["favorite_count"] + videos_df["comment_count"]
            ) / videos_df["views"].replace(0, 1)

            upload_to_minio(minio_client, videos_df, "youtube_videos.parquet")
            logger.info("Video Metrics (first 5 rows):")
            logger.info(f"\n{videos_df[['title', 'views', 'likes', 'engagement_rate']].head().to_string()}")

        yield Output(videos_df, output_name="videos_df", metadata={"rows": len(videos_df)})

    except Exception as e:
        if "Quota exceeded" in str(e):
            logger.error("Pipeline stopped due to YouTube API quota limit. Schedule retry after midnight PT.")
        raise

@op(out={"search_df": Out()}, ins={"videos_df": In()}, required_resource_keys={"minio"})
def youtube_search_op(context, videos_df):
    minio_client = context.resources.minio
    youtube = init_youtube_client(API_KEY)
    genres = ["pop", "electronic", "heavy metal", "country", "jazz", "hip hop",
              "classical", "folk", "rock", "reggae", "blues", "r&b"]
    
    try:
        # Fetch search results (prioritize cache)
        _, _, all_data_list = search_videos(youtube, genres, max_results_per_query=100, minio_client=minio_client)
        
        # Merge search tables
        search_df = merge_search_tables(all_data_list, minio_client)
        
        if not search_df.empty:
            logger.info("Videos Found by Genre Query (first 5 rows):")
            logger.info(f"\n{search_df[['video_id', 'genre']].head().to_string()}")
        else:
            logger.warning("No search data available")
        
        yield Output(search_df, output_name="search_df", metadata={"rows": len(search_df)})
    except Exception as e:
        logger.error(f"Error in youtube_search_op: {e}")
        raise
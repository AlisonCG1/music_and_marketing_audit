# -*- coding: utf-8 -*-
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

# Setup logging for better error debugging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# YouTube API configuration
API_KEY = os.getenv("YOUTUBE_API_KEY")
API_SERVICE_NAME = "youtube"
API_VERSION = "v3"

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")  # Must include http:// or https://
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

# Validate API key
if not API_KEY or API_KEY == "your_youtube_api_key":
    logger.error("Invalid or missing YOUTUBE_API_KEY in .env file")
    raise ValueError("Please set a valid YOUTUBE_API_KEY in the .env file")

# Initialize YouTube API client
try:
    youtube = googleapiclient.discovery.build(
        API_SERVICE_NAME, API_VERSION, developerKey=API_KEY
    )
    logger.info("YouTube API client initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize YouTube API client: {e}")
    raise

# Data storage
video_data = []
search_data = []

def fetch_videos(video_ids):
    """Fetch video metadata and engagement metrics."""
    try:
        logger.debug(f"Fetching videos for IDs: {video_ids}")
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
    except googleapiclient.errors.HttpError as e:
        logger.error(f"Error fetching videos: {e}")
        if "quotaExceeded" in str(e):
            logger.warning("YouTube API quota exceeded. Try again after quota reset (2:00 AM CDT) or use a new API key.")

def search_videos(queries, max_results_per_query=50):
    """Search for videos by multiple keywords."""
    all_video_ids = []
    for query in queries:
        next_page_token = None
        while True:
            try:
                logger.debug(f"Searching videos for query: {query}, pageToken: {next_page_token}")
                request = youtube.search().list(
                    part="snippet",
                    q=query,
                    type="video",
                    regionCode="US",
                    maxResults=max_results_per_query,
                    order="viewCount",
                    pageToken=next_page_token
                )
                response = request.execute()
                logger.debug(f"search.list response for query {query}: {response}")
                for item in response.get("items", []):
                    video_id = item["id"]["videoId"]
                    search_data.append({
                        "video_id": video_id,
                        "title": item["snippet"]["title"],
                        "channel_title": item["snippet"]["channelTitle"],
                        "published_at": item["snippet"]["publishedAt"],
                        "genre": query
                    })
                    all_video_ids.append(video_id)
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break
            except googleapiclient.errors.HttpError as e:
                logger.error(f"Error searching videos for query '{query}': {e}")
                if "quotaExceeded" in str(e):
                    logger.warning("YouTube API quota exceeded for search. Try again after quota reset (2:00 AM CDT) or use a new API key.")
                break
    logger.info(f"Collected {len(all_video_ids)} video IDs from search")
    return all_video_ids

def upload_to_minio(df, filename, format="parquet"):
    """Upload DataFrame to MinIO as CSV or Parquet."""
    try:
        # Validate endpoint
        if not MINIO_ENDPOINT or not MINIO_ENDPOINT.startswith(("http://", "https://")):
            logger.error(f"Invalid MINIO_ENDPOINT: {MINIO_ENDPOINT}")
            raise ValueError("MINIO_ENDPOINT must include http:// or https://")
        
        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        buffer = BytesIO()
        if format == "parquet":
            df.to_parquet(buffer, index=False)
        else:
            df.to_csv(buffer, index=False)
        buffer.seek(0)
        s3_client.put_object(Bucket=MINIO_BUCKET, Key=filename, Body=buffer.getvalue())
        logger.info(f"Uploaded {filename} to MinIO bucket {MINIO_BUCKET}")
    except Exception as e:
        logger.error(f"Error uploading {filename} to MinIO: {e}")

def main():
    # Step 1: Search YouTube videos by genre
    genres = ["pop", "heavy metal", "electronic", "country"]
    video_ids = search_videos(genres, max_results_per_query=50)
    logger.info(f"Collected {len(video_ids)} video IDs: {video_ids}")

    # Add a specific video ID from prior JSON
    video_ids.append("Eb8rXCzJMUc")
    video_ids = list(set(video_ids))  # Remove duplicates
    logger.info(f"Total video IDs after adding Eb8rXCzJMUc: {len(video_ids)}")

    # Step 2: Fetch video data in batches of 50
    if video_ids:
        for i in range(0, len(video_ids), 50):
            batch_ids = video_ids[i:i+50]
            fetch_videos(batch_ids)
    else:
        logger.warning("No video IDs collected; skipping fetch_videos")

    # Step 3: Create DataFrames
    videos_df = pd.DataFrame(video_data)
    search_df = pd.DataFrame(search_data)
    
    # Step 4: Process data
    if not videos_df.empty:
        videos_df["published_at"] = pd.to_datetime(videos_df["published_at"])
        videos_df["engagement_rate"] = (
            videos_df["likes"] + videos_df["dislikes"] + 
            videos_df["favorite_count"] + videos_df["comment_count"]
        ) / videos_df["views"].replace(0, 1)  # Avoid division by zero
    else:
        logger.warning("videos_df is empty; no video data collected")

    # Step 5: Upload to MinIO
    if not videos_df.empty:
        upload_to_minio(videos_df, "youtube_videos.parquet")
    else:
        logger.warning("Skipping MinIO upload for youtube_videos.parquet due to empty DataFrame")
    if not search_df.empty:
        upload_to_minio(search_df, "youtube_search.parquet")
    else:
        logger.warning("Skipping MinIO upload for youtube_search.parquet due to empty DataFrame")

    # Step 6: Log sample insights
    logger.info("Video Metrics:")
    if not videos_df.empty and all(col in videos_df.columns for col in ["title", "views", "likes", "engagement_rate"]):
        print(videos_df[["title", "views", "likes", "engagement_rate"]].head())
    else:
        logger.warning("Cannot print video metrics: videos_df is empty or missing required columns")

    logger.info("Videos Found by Genre:")
    if not search_df.empty and all(col in search_df.columns for col in ["title", "video_id", "genre"]):
        print(search_df[["title", "video_id", "genre"]].head())
    else:
        logger.warning("Cannot print search results: search_df is empty or missing required columns")

if __name__ == "__main__":
    main()
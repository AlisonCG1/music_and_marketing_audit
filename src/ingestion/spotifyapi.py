import os
import requests
import pandas as pd
from io import BytesIO
import boto3
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load env variables
load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("BUCKET_NAME")

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")


def upload_to_minio(spotifydf, filename, format="parquet"):
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )
        buffer = BytesIO()
        if format == "parquet":
            spotifydf.to_parquet(buffer, index=False)
        else:
            spotifydf.to_csv(buffer, index=False)
        buffer.seek(0)
        s3_client.put_object(Bucket=MINIO_BUCKET, Key=filename, Body=buffer.getvalue())
        logger.info(f"Uploaded {filename} to MinIO bucket {MINIO_BUCKET}")
    except Exception as e:
        logger.error(f"Error uploading {filename} to MinIO: {e}")

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
        return None
    token = response.json()["access_token"]
    return token

def spotify_request(url, token):
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        logger.error(f"Spotify API request failed: {response.status_code} - {response.text}")
        return {}
    return response.json()


def main():
    # Get token
    token = get_spotify_token()
    if not token:
        logger.error("No Spotify token retrieved, exiting.")
        return

    # 1) Search query (albums, playlists, tracks)
    search_url = "https://api.spotify.com/v1/search?q=remaster%20track:Doxy%20artist:Miles%20Davis&type=album,playlist,track&market=US"
    search_data = spotify_request(search_url, token)

    search_results = []
    for section in ["albums", "playlists", "tracks"]:
        for item in search_data.get(section, {}).get("items", []):
            search_results.append({
                "type": section,
                "id": item.get("id"),
                "name": item.get("name"),
                "artist": item["artists"][0]["name"] if section != "playlists" else None,
                "owner": item.get("owner", {}).get("display_name") if section == "playlists" else None,
                "release_date": item.get("release_date"),
                "total_tracks": item.get("total_tracks"),
                "popularity": item.get("popularity"),
            })

    search_df = pd.DataFrame(search_results)
    if not search_df.empty:
        upload_to_minio(search_df, "spotify_search.parquet")
    else:
        logger.warning("No search results found.")

    tracks_url = "https://api.spotify.com/v1/tracks?market=US&ids=7ouMYWpwJ422jRcDASZB7P,4VqPOruhp5EdPBeR92t6lQ,2takcwOaAZWiXQijPHIx7B"
    tracks_data = spotify_request(tracks_url, token)

    track_results = []
    for item in tracks_data.get("tracks", []):
        track_results.append({
            "id": item.get("id"),
            "name": item.get("name"),
            "artist": item["artists"][0]["name"],
            "album": item["album"]["name"],
            "release_date": item["album"]["release_date"],
            "duration_ms": item["duration_ms"],
            "popularity": item["popularity"],
            "preview_url": item.get("preview_url"),
        })

    tracks_df = pd.DataFrame(track_results)
    if not tracks_df.empty:
        upload_to_minio(tracks_df, "spotify_tracks.parquet")
    else:
        logger.warning("No track results found.")

if __name__ == "__main__":
    main()

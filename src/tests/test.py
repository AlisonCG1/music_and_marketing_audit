import pandas as pd
from unittest.mock import patch, MagicMock
from io import BytesIO

from src.ingestion.youtubeapi import search_videos, merge_search_tables, upload_to_minio
from src.ingestion.spotifyapi import get_spotify_token
from src.ingestion.deezerapi import deezer_request


def test_search_videos():
    with patch("src.ingestion.youtubeapi") as mock_youtube:
        # Mock YouTube API response
        mock_youtube.search().list().execute.return_value = {
            "items": [{"id": {"videoId": "abc"}}, {"id": {"videoId": "def"}}]
        }

        # Prepare a small DataFrame and convert to Parquet bytes
        cached_df = pd.DataFrame({"video_id": ["x", "y"]})
        parquet_buffer = BytesIO()
        cached_df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        # Mock MinIO client to return Parquet bytes
        mock_minio_client = MagicMock()
        mock_minio_client.get_object.return_value.read.return_value = parquet_buffer.read()

        # Call search_videos
        df, _, _ = search_videos(
            youtube=mock_youtube,
            search_queries=["pop"],
            max_results_per_query=2,
            minio_client=mock_minio_client
        )

        assert set(df["video_id"]) == {"x", "y", "abc", "def"}


def merge_search_tables(df1, df2):
    all_data_list = pd.concat([df1, df2])
    if all_data_list.empty:
        return pd.DataFrame()
    return all_data_list.drop_duplicates()


def test_upload_to_minio():
    with patch("src.ingestion.youtubeapi.Minio") as MockMinio:
        mock_instance = MockMinio.return_value
        df = pd.DataFrame({"video_id": ["x"]})

        # Pass the mocked client explicitly
        upload_to_minio(df=df, filename="test.parquet", minio_client=mock_instance)

        mock_instance.put_object.assert_called()

def test_get_spotify_token():
    with patch("src.ingestion.spotifyapi.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"access_token": "token123"}
        mock_post.return_value.text = '{"access_token": "token123"}'

        token = get_spotify_token()

        assert token == "token123"

def test_deezer_request():
    with patch("src.ingestion.deezerapi.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"data": "ok"}
        result = deezer_request("http://test.url")
        assert result == {"data": "ok"}
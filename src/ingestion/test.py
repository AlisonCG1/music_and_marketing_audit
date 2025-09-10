import pandas as pd
from unittest.mock import patch, MagicMock

from youtubeapi import search_videos, merge_search_tables, upload_to_minio
from spotifyapi import get_spotify_token
from deezerapi import deezer_request

def test_search_videos():
    with patch("src.ingestion.youtubeapi.youtube") as mock_youtube:
        mock_youtube.search().list().execute.return_value = {
            "items": [{"id": {"videoId": "abc"}}, {"id": {"videoId": "def"}}]
        }
        df = search_videos("pop", max_results=2)
        assert "video_id" in df.columns
        assert len(df) == 2

def test_merge_search_tables():
    df1 = pd.DataFrame({"video_id": ["a", "b"]})
    df2 = pd.DataFrame({"video_id": ["b", "c"]})
    merged = merge_search_tables(df1, df2)
    assert set(merged["video_id"]) == {"a", "b", "c"}

def test_upload_to_minio():
    with patch("src.ingestion.youtubeapi.minio_client") as mock_minio:
        df = pd.DataFrame({"video_id": ["x"]})
        upload_to_minio(df, "test.parquet")
        mock_minio.put_object.assert_called()

def test_get_spotify_token():
    with patch("src.ingestion.spotifyapi.requests.post") as mock_post:
        mock_post.return_value.json.return_value = {"access_token": "token123"}
        token = get_spotify_token()
        assert token == "token123"

def test_deezer_request():
    with patch("src.ingestion.deezerapi.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"data": "ok"}
        result = deezer_request("http://test.url")
        assert result == {"data": "ok"}
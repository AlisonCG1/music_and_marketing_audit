from dagster import op, OpExecutionContext
from minio import Minio
import pandas as pd
from io import BytesIO
import os
import re
import logging
from dotenv import load_dotenv


load_dotenv()


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


def clean_text(text: str) -> str:
    if pd.isna(text):
        return text

    text = re.sub(r'[^\w\s.,!?-]', '', text)

    text = re.sub(r'#\w+', '', text)

    text = re.sub(r'\s+', ' ', text).strip()
    return text

@op
def youtube_videos_clean_op(context: OpExecutionContext):
   
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )


    response = client.get_object("bronze-layer", "youtube_videos.parquet")
    data = BytesIO(response.read())
    response.close()
    response.release_conn()

    df = pd.read_parquet(data)
    logger.info(f"Loaded raw YouTube videos: {df.shape[0]} rows")


    if "title" in df.columns:
        df["title"] = df["title"].astype(str).apply(clean_text)
    if "description" in df.columns:
        df["description"] = df["description"].astype(str).apply(clean_text)

    logger.info("Sample of cleaned data:\n" + str(df.head()))

    out_data = BytesIO()
    df.to_parquet(out_data, index=False)
    out_data.seek(0)

    client.put_object(
        "bronze-layer",
        "youtube_videos_cleaned.parquet",
        out_data,
        length=len(out_data.getvalue()),
        content_type="application/parquet"
    )

    logger.info("✅ Cleaned data uploaded back to MinIO as youtube_videos_cleaned.parquet")

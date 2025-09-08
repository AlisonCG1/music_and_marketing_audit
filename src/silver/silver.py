from minio import Minio
import pandas as pd
from io import BytesIO
import os
import re
from dotenv import load_dotenv

load_dotenv()


#-- ADD FUNCTIONS HERE --
BUCKET_NAME = os.getenv("BUCKET_NAME")

client = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False
)

response = client.get_object(BUCKET_NAME, "youtube_videos.parquet")
data = BytesIO(response.read())
response.close()
response.release_conn()

to_clean = pd.read_parquet(data)

#print(to_clean.head())


def clean_text(text):
    text = str(text)
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text) 
    text = ' '.join(text.split())
    return text

try:
    to_clean['title'] = to_clean['title'].apply(clean_text)
except Exception as e:
    print(f"Error applying clean_text: {e}")
    raise

to_clean = to_clean.drop(columns=['thumbnail_url'], errors='ignore')

to_clean['engagement_rate'] = pd.to_numeric(to_clean['engagement_rate'], errors='coerce').round(6)


print("Cleaned DataFrame:")
print(to_clean)


output = BytesIO()
to_clean.to_parquet(output, index=False, engine="pyarrow")
output.seek(0)

# Upload back to MinIO (same or new bucket/key)
client.put_object(
    bucket_name=BUCKET_NAME,    
    object_name="youtube_videos_clean.parquet",
    data=output,
    length=output.getbuffer().nbytes,
    content_type="application/octet-stream",
)

print("Cleaned file uploaded to MinIO (silver-layer/youtube_videos_clean.parquet)")



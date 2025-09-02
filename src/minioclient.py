from dagster import resource
from minio import Minio
from dotenv import load_dotenv
import os

load_dotenv()

@resource
def minio_resource(init_context):
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"

    if not all([endpoint, access_key, secret_key]):
        raise ValueError("Incomplete MinIO configuration. Please set MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY")
    
    return Minio(endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

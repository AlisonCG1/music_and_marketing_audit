from fastapi import FastAPI
import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv

# Load env vars
load_dotenv()


DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DBNAME")

# Build connection string
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = sqlalchemy.create_engine(DATABASE_URL)

# FastAPI app
app = FastAPI()

@app.get("/")
def root():
    return {"message": "FastAPI + dbt is working!"}

@app.get("/music_data")
def get_alldata_silver(limit: int = 10):
    """Query dbt model from schema bronze_bronze"""
    query = f'SELECT * FROM bronze.alldata_silver LIMIT {limit};'
    df = pd.read_sql(query, engine)
    return df.to_dict(orient="records")

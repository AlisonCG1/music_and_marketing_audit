import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import altair as alt
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- Streamlit UI ---
st.set_page_config(page_title="Music & Marketing Trends", layout="wide")
st.title("🎶 Music & Marketing Trends Dashboard")

# --- Database connection ---
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DBNAME")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# --- Load Data ---
@st.cache_data
def load_data():
    artist_trends = pd.read_sql("SELECT * FROM gold.artist_trends", engine)
    genre_monthly = pd.read_sql("SELECT * FROM gold.genre_monthly_trends", engine)
    return artist_trends, genre_monthly

artist_trends, genre_monthly = load_data()

# --- Artist Trends Plot ---
st.subheader("📊 Artist Popularity & Engagement")

artist_chart = (
    alt.Chart(artist_trends)
    .mark_circle(size=80)
    .encode(
        x="avg_spotify_popularity:Q",
        y="avg_engagement:Q",
        color="avg_trend_score:Q",
        tooltip=["artist_name", "track_count", "avg_spotify_popularity", "avg_engagement", "current_rank", "avg_trend_score"]
    )
    .interactive()
)

st.altair_chart(artist_chart, use_container_width=True)

# --- Genre Monthly Trends Plot ---
st.subheader("📈 Genre Trends Over Time")

genre_chart = (
    alt.Chart(genre_monthly)
    .mark_line(point=True)
    .encode(
        x="month:T",
        y="avg_trend_score:Q",
        color="genre:N",
        tooltip=["genre", "month", "avg_trend_score", "avg_spotify_popularity", "avg_engagement"]
    )
    .interactive()
)

st.altair_chart(genre_chart, use_container_width=True)

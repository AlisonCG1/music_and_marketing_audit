
# Hi, I'm Alison! 👋


# Music & Marketing Audit Dashboard

An end-to-end data pipeline and dashboard for analyzing Spotify, Deezer, and YouTube music trends.
Built with DBT, PostgreSQL, and Streamlit for marketing + music insights.

The goal of the project is to create a database that can be used to analyze the intersection of music and marketing through data collected from YouTube, Spotify, and Deezer. These platforms are the main place where people discover music, how artists get noticed, and how viral campaigns take off. By pulling together and making sense of data from these sources, I’m aiming to uncover insights that’ll help artists, marketers, and record labels spot trends, connect with audiences, and boost their promotional game.

This capstone will demonstrate how modern data engineering tools and workflows can be applied to real-world datasets. The final product will include a set of dashboards and reports that allow stakeholders (e.g., artists, music marketers, record labels) to understand the marketing performance of songs, identify high-engagement content, and forecast potential hits.

## Project Overview
The goal of this project is to:
Collect music data from Spotify, Deezer, and YouTube.
Clean and transform raw data into structured tables (bronze, silver, gold).
Calculate trend metrics such as engagement, virality potential, and trend score.
Provide a visual analytics dashboard for tracking top artists, genres, and music trends over time.

## Tech Stack
Python 3.13
PostgreSQL for storing music data
DBT for data transformation (bronze → silver → gold layers)
Streamlit for interactive dashboard visualizations
Pandas for data manipulation
Plotly / Seaborn for interactive plots
SQLAlchemy for database connections
pytest for testing

## Data Sources
Spotify: track metadata, search results
Deezer: charts and genres
YouTube: video engagement and search data
All raw data is ingested into the bronze layer in PostgreSQL.



## Installation

git clone <repo_url>
cd music_and_marketing_audit

# Setup environment
python -m venv .venv
source .venv/bin/activate   # macOS/Linux
.venv\Scripts\activate      # Windows
pip install -r requirements.txt

# Run dbt models
dbt run --select silver
dbt run --select gold

# Launch dashboard
streamlit run mydataviz/app.py

## API Reference

#### Get all items

```http
  GET /api/items
```

| Parameter | Type     | Description                |
| :-------- | :------- | :------------------------- |
| `api_key` | `string` | **Required**. Your API key |

#### References: 

- https://developer.spotify.com/documentation/web-api/reference/get-an-albums-tracks

- https://developer.spotify.com/documentation/web-api/reference/search

- https://developers.deezer.com/api 

- https://developers.google.com/youtube/v3
## Documentation

[youtube documentation](https://developers.google.com/youtube/v3/getting-started)


[seaborn](https://seaborn.pydata.org/examples/index.html)

[Streamlit](https://www.youtube.com/watch?v=7yAw1nPareM&list=PLWuFHho1zKhWN-Qp5hrR0e9RZIo7QO7z6&index=2)


[Streamlit](https://docs.streamlit.io)


## Features
ETL pipeline: Bronze → Silver → Gold layers
Interactive dashboard:
Genre filters
Top artists by virality potential
Engagement trends over time
Gold-layer insights on artist + genre performance
Tests & CI with GitHub Actions



## Architecture
- Python
- Docker
- Postgress
- DBT
- Duckdb
- MinIO
- Streamlit





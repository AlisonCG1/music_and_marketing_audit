import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import seaborn as sns
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv


load_dotenv()

db_params = {
    'host': 'localhost', 
    'database': 'music_db', 
    'user': 'postgres', 
    'password': 'mysecretpassword', 
    'port': '5432'  
}


engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")


BRONZE_TABLES = ['alldata_silver']
SILVER_TABLES = ['deezer_charts_bronze', 'deezer_genre_bronze', 'spotify_search_bronze',
                 'spotify_tracks_bronze', 'youtube_clean_bronze']
GOLD_TABLES = ['artist_performance_gold', 'genre_trends_gold']

BRONZE_SCHEMA = 'bronze'
SILVER_SCHEMA = 'bronze_bronze'
GOLD_SCHEMA = 'bronze_gold'

def load_tables(schema, table_names):

    dataframes = {}
    for table in table_names:
        query = f'SELECT * FROM {schema}.{table}'
        dataframes[table] = pd.read_sql(query, engine)
    return dataframes

def load_all_data():

    bronze_dfs = load_tables(BRONZE_SCHEMA, BRONZE_TABLES)
    silver_dfs = load_tables(SILVER_SCHEMA, SILVER_TABLES)
    gold_dfs = load_tables(GOLD_SCHEMA, GOLD_TABLES)
    return bronze_dfs, silver_dfs, gold_dfs



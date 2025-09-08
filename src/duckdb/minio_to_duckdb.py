import duckdb
import os
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

load_dotenv()

def load_minio_to_duckdb(filenames, duckdb_path="music_data.duckdb"):
    con = duckdb.connect(database=duckdb_path)
    try:
        con.execute("INSTALL httpfs; LOAD httpfs; INSTALL parquet; LOAD parquet;")
        con.execute(f"""
            SET s3_endpoint='{os.getenv("MINIO_ENDPOINT", "localhost:9000")}';
            SET s3_access_key_id='{os.getenv("MINIO_ACCESS_KEY")}';
            SET s3_secret_access_key='{os.getenv("MINIO_SECRET_KEY")}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)
        logger.info("Configured MinIO access in DuckDB")

    except Exception as e:
        logger.error(f"Error loading tables to DuckDB: {e}")
        con.close()
        raise
    return con

def export_duckdb_to_postgres(duckdb_con, tables, pg_config):
    try:
        duckdb_con.execute("INSTALL postgres; LOAD postgres;")
        duckdb_con.execute(f"""
            ATTACH 'dbname={pg_config['dbname']} host={pg_config['host']}
            port={pg_config['port']} user={pg_config['user']}
            password={pg_config['password']}' AS pg (TYPE postgres)
        """)
        logger.info("Attached PostgreSQL database")
        
      #  for table in tables:
     #       duckdb_con.execute("CREATE SCHEMA IF NOT EXISTS pg.bronze")
     #       duckdb_con.execute(f"""
     #           CREATE TABLE IF NOT EXISTS pg.bronze.{table} AS
     #           SELECT * FROM {table}
     #       """)
      #      count = duckdb_con.execute(f"SELECT COUNT(*) FROM pg.bronze.{table}").fetchone()[0]
          #  logger.info(f"Copied {table} to PostgreSQL with {count} rows")
    except Exception as e:
        logger.error(f"Error copying to PostgreSQL: {e}")
        raise

if __name__ == "__main__":
    files = [
        "deezer_charts.parquet",
        "deezer_genres.parquet",
        "spotify_search.parquet",
        "spotify_tracks.parquet",
        "youtube_search.parquet",
        "youtube_videos_clean.parquet"
    ]
    pg_config = {
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "dbname": os.getenv("POSTGRES_DBNAME")
    }

    try:
        con = load_minio_to_duckdb(files)
        tables = [f.replace(".parquet", "") for f in files]
        export_duckdb_to_postgres(con, tables, pg_config)
        con.close()
        logger.info("Database connection closed")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        if 'con' in locals():
            con.close()
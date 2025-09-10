from dagster import op, OpExecutionContext
import duckdb
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

@op
def youtube_videos_clean_op(context: OpExecutionContext):
    
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

   
    if not all(pg_config.values()):
        logger.error("Missing PostgreSQL configuration in environment variables")
        raise ValueError("Incomplete PostgreSQL configuration")

    duckdb_path = "music_data.duckdb"
    con = duckdb.connect(database=duckdb_path)

    try:
        
        con.execute("INSTALL httpfs; LOAD httpfs; INSTALL parquet; LOAD parquet;")
        con.execute(f"""
            SET s3_endpoint='{os.getenv("MINIO_ENDPOINT")}';
            SET s3_access_key_id='{os.getenv("MINIO_ACCESS_KEY")}';
            SET s3_secret_access_key='{os.getenv("MINIO_SECRET_KEY")}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
            SET s3_region='';
        """)
        logger.info("Configured MinIO access in DuckDB")

        bucket_name = os.getenv("BUCKET_NAME")  
        for filename in files:
            table_name = filename.replace(".parquet", "")
            s3_path = f"s3://{bucket_name}/bronze-layer/{filename}"
            try:
                con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{s3_path}')")
                count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                logger.info(f"Loaded {count} rows from {s3_path} into DuckDB table {table_name}")
               
                columns = [col[0] for col in con.execute(f"DESCRIBE {table_name}").fetchall()]
                logger.info(f"DuckDB table {table_name} has columns: {columns}")
            except Exception as e:
                logger.error(f"Failed to load {s3_path}: {str(e)}")
                raise

        
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"""
            ATTACH 'dbname={pg_config['dbname']} host={pg_config['host']}
            port={pg_config['port']} user={pg_config['user']}
            password={pg_config['password']}' AS pg (TYPE postgres)
        """)
        logger.info("Attached PostgreSQL database")

      
        schema = "bronze"  
        for table in [f.replace(".parquet", "") for f in files]:
            try:
                
                columns = [col[0] for col in con.execute(f"DESCRIBE {table}").fetchall()]
                columns_str = ", ".join(columns)
                logger.info(f"PostgreSQL table {schema}.{table} will be created with columns: {columns}")

                
                con.execute(f"DROP TABLE IF EXISTS pg.{schema}.{table}")
                con.execute(f"CREATE TABLE pg.{schema}.{table} AS SELECT * FROM {table} WITH NO DATA")
                logger.info(f"Created PostgreSQL table {schema}.{table}")

               
                con.execute(f"""
                    INSERT INTO pg.{schema}.{table} ({columns_str})
                    SELECT {columns_str} FROM {table}
                """)
                count = con.execute(f"SELECT COUNT(*) FROM pg.{schema}.{table}").fetchone()[0]
                logger.info(f"Updated pg.{schema}.{table}, now has {count} rows")
            except Exception as e:
                logger.error(f"Failed to update pg.{schema}.{table}: {str(e)}")
                raise e  

    except Exception as e:
        logger.error(f"Error in DuckDB to Postgres operation: {str(e)}")
        raise
    finally:
        con.close()
        logger.info("DuckDB connection closed")

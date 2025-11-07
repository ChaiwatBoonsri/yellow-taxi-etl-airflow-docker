"""
Dependencies: polars, sqlalchemy, python-dotenv, pyarrow, pandas
"""
import polars as pl
import sqlalchemy
import os
import time
from dotenv import load_dotenv
load_dotenv() # load environment variables from .env file

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Establish database connection
try:
    connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}" 
    engine = sqlalchemy.create_engine(connection_string) # create SQLAlchemy engine
    connection = engine.connect() # establish connection
    print("Database connection established")
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit(1)

# Define file path
file_name = f"yellow_tripdata_2025-01.parquet"
input_path = os.path.join("data/raw", file_name)

if not os.path.exists(input_path):
    print(f"File {input_path} does not exist.")
    exit(1)

# Read sample data from parquet file
print(f"Reading data from {input_path}")
start_time = time.time()

try:
    df_sample = (pl.scan_parquet(input_path).head(100).collect())  # read only first 100 rows
    print(f"Read 100 data successfully in {time.time() - start_time:.2f}")
except Exception as e:
    print(f"Error reading parquet file: {e}")
    exit(1)

# Write sample data to database
TABLE_NAME = "sample_raw_yellow_taxi_data"
try:

    df_sample.write_database(
        table_name=TABLE_NAME,
        connection=connection_string,
        if_table_exists="replace"  # replace table if it exists
    )
    print(f"Sample data written to table {TABLE_NAME} successfully")
except Exception as e: 
    print(f"Error writing to database: {e}")
    exit(1)
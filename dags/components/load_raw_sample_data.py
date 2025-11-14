import polars as pl
import os
import time
from connection import connection

def load_raw_sample_data(year, month):
    """Load a small sample of raw taxi trip data into the database."""
    connection_string = connection() # get connection string from connection module

    # Define file path
    month_str = f"{month:02d}" # format month as two digits
    file_name = f"yellow_tripdata_{year}-{month_str}.parquet"
    input_path = os.path.join("data/raw", file_name)

    if not os.path.exists(input_path):
        print(f"File {input_path} does not exist.")
        return

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
    TABLE_NAME = f"{year}_{month_str}_sample_raw_data"
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

if __name__ == "__main__":
    load_raw_sample_data(2025, 3)
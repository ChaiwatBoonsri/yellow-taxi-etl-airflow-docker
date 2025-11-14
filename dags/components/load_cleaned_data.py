import polars as pl
import os
import time
import tqdm
import math
from connection import connection

def load_cleaned_data(year, month):
    """Load cleaned taxi trip data into the database."""
    connection_string = connection() # get connection string from connection module
    
    # Define file path
    month_str = f"{month:02d}" # format month as two digits
    file_name = f"cleaned_yellow_tripdata_{year}-{month_str}.parquet"

    shared_data_path = "/opt/airflow/shared_data"
    # shared_data_windows_path = "data"
    input_path = os.path.join(shared_data_path, "processed", file_name)
    # input_path = os.path.join(shared_data_windows_path, "processed", file_name)

    if not os.path.exists(input_path):
        print(f"File {input_path} does not exist.")
        return

    # Read data from parquet file
    print(f"Reading data from {input_path}")
    start_time = time.time()

    try:
        df_sample = (pl.scan_parquet(input_path).collect())  # read all rows
        print(f"Read all data successfully in {time.time() - start_time:.2f}")
    except Exception as e:
        print(f"Error reading parquet file: {e}")
        exit(1)

    # Write data to database
    TABLE_NAME = f"{year}_{month_str}_cleaned_data"

    chunk_size = 100000  # number of rows per chunk
    df_height = df_sample.height # get total number of rows
    total_chunks = math.ceil(df_height / chunk_size) 
    print(f"Writing {df_height} rows to table {TABLE_NAME} in {total_chunks} chunks...")

    progress_bar = tqdm.tqdm(
        total=total_chunks, 
        desc=f"Loading to table {TABLE_NAME}", 
        unit="chunk"
    )

    try:
        # Write data in chunks
        for i, chunk in enumerate(df_sample.iter_slices(chunk_size)): # iterate over chunks
            write_logic = "replace" if i == 0 else "append" # replace for first chunk, append for others
            chunk.write_database(
                table_name=TABLE_NAME,
                connection=connection_string,
                if_table_exists=write_logic
            )
            progress_bar.update(1)
        progress_bar.close()

        print(f"Data written to table {TABLE_NAME} successfully")
    except Exception as e: 
        if 'progress_bar' in locals(): # ensure progress bar is closed on error
            progress_bar.close()
        print(f"\nError writing to database: {e}")
        exit(1)

if __name__ == "__main__":
    load_cleaned_data(2025, 5)
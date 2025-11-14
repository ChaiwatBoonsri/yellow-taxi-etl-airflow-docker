import polars as pl
import os
import time
import sys

def clean_data(year, month):
    
    # Define file paths
    month_str = f"{month:02d}"
    file_name = f"yellow_tripdata_{year}-{month_str}.parquet"

    shared_data_path = "/opt/airflow/shared_data"
    input_path = os.path.join(shared_data_path, "raw", file_name)
    output_dir = os.path.join(shared_data_path, "processed")
    output_path = os.path.join(shared_data_path, "processed", f"cleaned_{file_name}")

    os.makedirs(output_dir, exist_ok=True) # create directory if not exists
    if not os.path.exists(input_path):
        print(f"Input file {input_path} does not exist.")
        return

    # Start data transformation
    print(f"Transform data from {input_path}...")
    start_time = time.time()

    try:
        # Build lazy query for data cleaning
        lazy_query = (pl.scan_parquet(input_path)
                    .filter(pl.col("passenger_count") > 0)
                    .filter(pl.col("trip_distance") > 0)
                    .filter(pl.col("total_amount") > 0)

                        # Calculate trip duration in minutes
                    .with_columns(
                        (pl.col('tpep_dropoff_datetime') - pl.col('tpep_pickup_datetime'))
                        .dt.total_minutes()
                        .alias('trip_duration_minutes')
                    )

                        # Filter trips with duration between 1 minute and 24 hours
                    .filter(pl.col("trip_duration_minutes") >= 1)
                    .filter(pl.col("trip_duration_minutes") <= 1440)

                        # Select relevant columns
                    .select(
                            pl.col("VendorID"),
                            pl.col("tpep_pickup_datetime"),
                            pl.col("tpep_dropoff_datetime"), 
                            pl.col("passenger_count"),
                            pl.col("trip_distance"),
                            pl.col("total_amount"),
                            pl.col("PULocationID"),
                            pl.col("DOLocationID"),
                            pl.col('fare_amount'),
                            pl.col("payment_type"),
                            pl.col("trip_duration_minutes")
                    )
                    )
        
        # Execute the lazy query
        print("Executing lazy query...")
        cleaned_df = lazy_query.collect(engine="streaming")

        # Write cleaned data to parquet
        print(f"Write cleaned data to {output_path}...")
        cleaned_df.write_parquet(output_path)

        end_time = time.time()
        print(f"Data cleaning completed in {end_time - start_time:.2f} seconds.")
        print(f"raw data rows: {pl.read_parquet(input_path).height}")
        print(f"cleaned data rows: {cleaned_df.height}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__": 
    try:
        year = int(sys.argv[1])
        month = int(sys.argv[2])
    except (IndexError):
        print("Error: Missing arguments.")
        print("Usage: python clean_data.py <year> <month>")
    except ValueError:
        print("Year and month must be integers.")
        sys.exit(1)
    
    print(f"--- Starting Cleaning data for {year}-{month:02d} ---")
    clean_data(year, month)
import requests # for HTTP requests
import os # for file operations
import sys # for command-line arguments
from tqdm import tqdm # for progress bar

def download_file(year, month):
    """Download yellow taxi trip data for a specific year and month with progress bar."""
    month_str = f"{month:02d}" # format month as two digits to match file naming convention

    file_name = f"yellow_tripdata_{year}-{month_str}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"

    shared_data_path = "/opt/airflow/shared_data"
    save_dir = os.path.join(shared_data_path, "raw")
    save_path = os.path.join(shared_data_path, "raw", file_name)

    os.makedirs(save_dir, exist_ok=True) # create directory if not exists

    print(f"Starting download: {file_name}")
    print(f"From: {url}")
    print(f"To: {save_path}")

    try:
        response = requests.get(url, stream=True) # stream=True for downloading large files
        response.raise_for_status()  # check for HTTP errors

        total_size_in_bytes = int(response.headers.get('content-length', 0)) # get total size
        block_size = 1024  # chunk size in bytes to download at a time

        progress_bar = tqdm(
            total = total_size_in_bytes, 
            unit = 'iB', 
            unit_scale = True,
            desc = f"Downloading {file_name}"
        )

        # 'wb' - write binary mode for non-text files like parquet
        with open(save_path, 'wb') as file:
            for data in response.iter_content(block_size): # download in chunks 
                progress_bar.update(len(data))  # update progress bar
                file.write(data)  # write chunk to file
        progress_bar.close()

        if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
            print("ERROR: Something went wrong during the download")
            os.remove(save_path)  # remove incomplete file
        else:
            print(f"Download completed")
            
    except requests.exceptions.HTTPError as errh:
        print(f"Http Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        print(f"Oops: Something Else: {err}")
    except KeyboardInterrupt:
        print("Download interrupted by user")
        if os.path.exists(save_path):
            os.remove(save_path)  # remove incomplete file
            print("Incomplete file removed")

if __name__ == "__main__":
    try:
        year = int(sys.argv[1])
        month = int(sys.argv[2])
    except (IndexError):
        print("Error: Missing arguments.")
        print("Usage: python get_taxi_tripdata.py <year> <month>")
        sys.exit(1)
    except ValueError:
        print("Year and month must be integers.")
        sys.exit(1)
    
    print(f"--- Starting Download Task for {year}-{month:02d} ---")
    download_file(year, month)
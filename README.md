# 🚕 Automated NYC Taxi ETL: A Dockerized Pipeline with Airflow & Polars

This project demonstrates the creation of an automated Data Pipeline (E-T-L) for batch processing NYC Yellow Taxi data, using industry-standard tools (Apache Airflow, Docker, and Polars).

## Description

This project aims to bridge the gap between theoretical study (Theory) and practical application (Practice) in the Data Engineering industry. It focuses on understanding widely-used tools and building a Data Pipeline that is **"Tangible"**, **"Reproducible"**, and **"Automated"**.

This project demonstrates the complete E-T-L (Extract, Transform, Load) process:

-   **Extract:** Pull raw data (Parquet files) of NYC Yellow Taxi from the public data source.
-   **Transform:** Use **Polars** (a high-performance library, faster than Pandas) to clean, filter, and transform the data.
-   **Load:** Load the cleaned data into a **PostgreSQL** database.

This entire process is orchestrated by **Apache Airflow** and runs on **Docker Compose** to simulate a production-like environment that is isolated and scalable.

## Getting Started

These are all the necessary steps to set up and run this Data Pipeline on your local machine.
### Dependencies

You must have the following programs installed on your Host machine (Windows/Mac/Linux) beforehand:
-   **[Docker Desktop](https://www.docker.com/products/docker-desktop/):** Installed and running.
-   **[PostgreSQL](https://www.postgresql.org/):** Used as the destination Database running on your Host machine (not in Docker).
-   **[Git](https://git-scm.com/):** For cloning the project.
### Installing
These are the "first-time" setup steps for the project.

**1. Clone the Project**
```
# Clone a copy of the repository 
git clone https://github.com/ChaiwatBoonsri/yellow-taxi-etl-airflow-docker.git 
# Move into the project directory 
cd yellow-taxi-etl-airflow-docker
```

**2. Create the Destination Database**
-   Open your PostgreSQL client (e.g., DBeaver).
-   Create a new, empty database (e.g., `nyc_taxi_data`).
```sql 
CREATE DATABASE nyc_taxi_data; 
```
**3. Configure Airflow Secrets (Main .env file)**
-   The `docker-compose.yml` file is configured to read secret Keys from an `.env` file in the "main" folder (`yellow-taxi-etl-airflow-docker/`).
-   Create an `.env` file in this folder and generate two new keys as follows:

- **Generate the `FERNET_KEY` (for DB encryption):**
```
# (Run in PowerShell/Terminal) 
docker-compose run --rm airflow-scheduler python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```
- **Generate the `JWT_SECRET` (for API Token):**
```
# (Run in PowerShell/Terminal)
docker-compose run --rm airflow-scheduler python -c "import secrets; print(secrets.token_hex(32))"
```
- **Add the generated keys to the .env file:**
```
# (File: .env in the main folder)
AIRFLOW__CORE__FERNET_KEY=[...Paste the generated FERNET_KEY here...]
AIRFLOW__API_AUTH__JWT_SECRET=[...Paste the generated JWT_SECRET here...]
```

**4. Configure ETL Secrets (Script .env file)**

-   The Airflow Worker (in Docker) needs the password to connect to PostgreSQL (on the Host).
- Go to `dags/components/`
-   Create an `.env` file here and enter your database connection details:
	```
	# (File: dags/components/.env)
	DB_HOST=host.docker.internal
	DB_PORT=5432
	DB_NAME=nyc_taxi_data
	DB_USER=[Your_Postgres_Username]
	DB_PASSWORD=[Your_Postgres_Password]
	 ```
    
-   Note: We use `host.docker.internal` to allow the Container (Airflow) to "see" the PostgreSQL Database running on your Host (Windows/Mac) machine.

### Executing program

**1. Start Airflow containers (in the `yellow-taxi-etl-airflow-docker/` folder)**
```
docker-compose up -d
```

**2. Access the Airflow UI**
-   Open a web browser to: `http://localhost:8080`
-   Log in with the user automatically created by `airflow-init`:
    -   **Username:** `airflow`
    -   **Password:** `airflow`
        
**3. Trigger the DAG (Run the Pipeline)**
-   In the Dashboard, find the `ETL_pipeline` DAG.
-   Click the "Toggle Dag" switch to "Unpause" (turn on) the DAG.
-   Enter this JSON in the "Advanced Options" to specify the desired month:
	```json
	{
        "year": 2025,
        "month": 4
	}
	```
-   Click "Trigger" and watch your pipeline run!
  
## Help

This section covers common problems and issues encountered while setting up this project.

### ⚠️ Problem: Task fails with `Invalid auth token: Signature verification failed`

-   **Cause:** This is the most common and complex issue. It means the Airflow services (Worker, Scheduler, Webserver) are using different secret keys (`FERNET_KEY` or `JWT_SECRET`) and cannot authenticate with each other. This usually happens if the services start and auto-generate different keys, or if a "bad" config is cached in a Docker volume.
    
-   **Solution:** The most reliable fix is to "nuke" the bad state and restart "clean" with the correct keys:
    
    1.  Ensure your `FERNET_KEY` and `JWT_SECRET` are correctly defined in your main `.env` file (and **not** hardcoded in `docker-compose.yml`).
        
    2.  Ensure your `.env` file is being read (the `env_file:` directive is in `docker-compose.yml`).
        
    3.  Run the "nuke" command to delete the bad database (`postgres-db-volume`) that stored the old, bad keys:
        ```
        docker-compose down --volumes
        ```
        
    4.  Restart the entire stack. This forces `airflow-init` to re-create the database using the _correct_ keys from your `.env` file.
        ```
        docker-compose up -d
        ```

### ⚠️ Problem: Task 3 (`load_cleaned_data`) fails with `Connection refused` or `Host not found`
-   **Cause:** The Airflow Worker (inside a Docker container) cannot find your PostgreSQL database (running on your "Host" Windows/Mac machine). The worker doesn't know what `localhost` means.
-   **Solution:**
    1.  **Check `DB_HOST`:** In your _script's_ `.env` file (`dags/components/.env`), ensure `DB_HOST` is set to the special Docker DNS name:
        ```
        DB_HOST=host.docker.internal
        ```
        
    2.  **Check Firewall:** This is the next most common failure. Your Host machine's firewall (e.g., Windows Defender Firewall) is blocking the connection _from_ Docker. You must create a new **Inbound Rule** to **Allow** connections on **TCP Port `5432`**.

### ⚠️ Problem: Task fails with `ModuleNotFoundError: No module named 'polars'` (or 'tqdm', 'psycopg2-binary')
-   **Cause:** The Airflow Worker (inside Docker) does not have the necessary Python libraries installed.
-   **Solution:** Add the missing library to your `docker-compose.yml` file under the `_PIP_ADDITIONAL_REQUIREMENTS` variable.
    ```
    # in docker-compose.yml -> x-airflow-common -> environment
    _PIP_ADDITIONAL_REQUIREMENTS: 'polars sqlalchemy psycopg2-binary python-dotenv pandas pyarrow tqdm'
    ```
    Then, you must **rebuild** the worker container for the change to take effect:
    ```
    docker-compose up -d --build
    ```
    
    (Or, more reliably, use `docker-compose down` and `docker-compose up -d`).
    
### ⚠️ Problem: Task 2 or 3 fails with `FileNotFoundError`

-   **Cause:** You are using `CeleryExecutor`, which means Task 1 (Extract) might run on "Worker A" and Task 2 (Clean) might run on "Worker B". Worker B cannot see the files saved locally by Worker A.
-   **Solution:** All tasks must read and write to a "shared central warehouse" (a shared volume).
    1.  Ensure you have created a `shared_data` folder on your Host.
    2.  Ensure your `docker-compose.yml` mounts this folder into `/opt/airflow/shared_data` (see `x-airflow-common` -> `volumes:`).
    3.  Ensure all three Python scripts (`get_...`, `clean_...`, `load_...`) have their paths updated to use `/opt/airflow/shared_data/raw/` and `/opt/airflow/shared_data/processed/`.
    
## Author

-   **Chaiwat Boonsri**
-   **GitHub:** [@ChaiwatBoonsri](https://www.google.com/url?sa=E&source=gmail&q=https://github.com/ChaiwatBoonsri)
-   **linkedIn:** [Chaiwat Boonsri](https://www.linkedin.com/in/chaiwat-boonsri-0584a4395/)

## Acknowledgments

-   **Data Source:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
-  **Apache Airflow:** [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)

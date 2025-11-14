from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

components_path = "/opt/airflow/dags/components"

with DAG(
    dag_id="ETL_pipeline", 
    default_args={ 
        "owner": "airflow", 
        "depends_on_past": False, # Do not depend on past runs
        "email_on_failure": False, # Disable email on failure for simplicity
        "email_on_retry": False, # Disable email on retry for simplicity
        "retries": 1, # Number of retries on failure
        "retry_delay": timedelta(seconds=20), # Delay between retries
    },
    description="ETL pipeline for getting, processing, and storing Yellow taxi data",
    schedule=None, # Run on demand
    start_date=datetime(2025, 1, 1), # Start date of the DAG
    catchup=False, # Do not perform backfill
    tags=["ETL", "YellowTaxi"], # Tags for categorizing the DAG
) as dag:
    extract_task = BashOperator(
        task_id = "run_extract",
        bash_command = f"""
        cd {components_path}
        export YEAR_TO_PROCESS={{{{ dag_run.conf.get('year', logical_date.year) }}}} 
        export MONTH_TO_PROCESS={{{{ dag_run.conf.get('month', logical_date.month) }}}}
        python get_taxi_tripdata.py $YEAR_TO_PROCESS $MONTH_TO_PROCESS
        """
        # Note: The above bash_command uses Airflow's Jinja templating to get 'year' and 'month' 
        # from dag_run configuration or defaults to logical_date values.
    )

    clean_task = BashOperator(
        task_id = "run_clean_data",
        bash_command = f"""
        cd {components_path}
        export YEAR_TO_PROCESS={{{{ dag_run.conf.get('year', logical_date.year) }}}} 
        export MONTH_TO_PROCESS={{{{ dag_run.conf.get('month', logical_date.month) }}}}
        python clean_data.py $YEAR_TO_PROCESS $MONTH_TO_PROCESS
        """
    )

    load_task = BashOperator(
        task_id = "run_load_clean_data",
        bash_command = f"""
        cd {components_path}
        export YEAR_TO_PROCESS={{{{ dag_run.conf.get('year', logical_date.year) }}}} 
        export MONTH_TO_PROCESS={{{{ dag_run.conf.get('month', logical_date.month) }}}}
        python load_cleaned_data.py $YEAR_TO_PROCESS $MONTH_TO_PROCESS
        """
    )

    extract_task >> clean_task >> load_task # Define task dependencies
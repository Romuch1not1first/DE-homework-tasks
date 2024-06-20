from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta

from ..schemas import user_profiles_schema

default_args = {
    'start_date': datetime(2022, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'process_user_profiles',
    default_args=default_args, 
    schedule_interval=None,
    catchup=True, 
) as dag:
    
    # Task to load data from GCS to BigQuery silver table
    load_to_silver = GCSToBigQueryOperator(
        task_id='load_to_silver',
        bucket='de-final-project-data',  
        source_objects=['user_profiles/user_profiles.json'],  # Source file path
        destination_project_dataset_table='silver.user_profiles', 
        source_format='NEWLINE_DELIMITED_JSON', 
        write_disposition='WRITE_TRUNCATE', 
        schema_fields=user_profiles_schema, 
    )

    load_to_silver 

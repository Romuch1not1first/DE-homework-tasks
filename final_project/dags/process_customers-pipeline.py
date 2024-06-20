from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import logging

from ..schemas import bronze_customers_schema
from ..queries import populate_silver_customers

default_args = {
    'start_date': datetime(2022, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5), 
}

def get_file_list(execution_date, **kwargs):
    # Get the list of files from Google Cloud Storage based on the execution date
    bucket_name = 'de-final-project-data'
    execution_date = pendulum.parse(execution_date).date()
    prefix = f"customers/{execution_date.strftime('%Y-%m')}-{execution_date.day}/"

    logging.info(f"Prefix to check: {prefix}")

    hook = GCSHook(google_cloud_storage_conn_id='google_cloud_default')
    blobs = hook.list(bucket_name=bucket_name, prefix=prefix)

    files = [str(blob) for blob in blobs]

    logging.info(f"Files: {files}")

    return files

with DAG(
    'process_customers_v15',
    default_args=default_args,
    schedule_interval='@daily', 
    catchup=True, 
) as dag:

    list_gcs_files = PythonOperator(
        task_id='list_gcs_files', 
        python_callable=get_file_list,  # Function to call to list files
        op_kwargs={'execution_date': '{{ execution_date }}'},
        provide_context=True,
    )

    load_to_bronze = GCSToBigQueryOperator(
        task_id='load_to_bronze', 
        bucket='de-final-project-data',
        source_objects=list_gcs_files.output,  # Files to load
        destination_project_dataset_table='bronze.customers',
        schema_fields=bronze_customers_schema, 
        source_format='CSV', 
        skip_leading_rows=1, 
        write_disposition='WRITE_TRUNCATE',
    )

    transform_to_silver = BigQueryExecuteQueryOperator(
        task_id='transform_to_silver', 
        sql=populate_silver_customers,
        use_legacy_sql=False,  # Use Standard SQL syntax
    )

    list_gcs_files >> load_to_bronze >> transform_to_silver

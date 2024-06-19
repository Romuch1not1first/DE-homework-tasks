from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import logging

default_args = {
    'start_date': datetime(2022, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_file_list(execution_date, **kwargs):
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

    # Task to list GCS files
    list_gcs_files = PythonOperator(
        task_id='list_gcs_files',
        python_callable=get_file_list,
        op_kwargs={'execution_date': '{{ execution_date }}'},
        provide_context=True,
    )

    # Task to load data from GCS to BigQuery Bronze table
    load_to_bronze = GCSToBigQueryOperator(
        task_id='load_to_bronze',
        bucket='de-final-project-data',
        source_objects="{{ task_instance.xcom_pull(task_ids='list_gcs_files') }}",
        destination_project_dataset_table='bronze.customers',
        schema_fields=[
            {'name': 'ClientId', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'FirstName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'LastName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'RegistrationDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
    )

    # Task to transform data from Bronze to Silver table
    transform_to_silver = BigQueryExecuteQueryOperator(
        task_id='transform_to_silver',
        sql='''
        CREATE OR REPLACE TABLE silver.customers AS
        SELECT
            CAST(ClientId AS STRING) AS client_id,
            CAST(FirstName AS STRING) AS first_name,
            CAST(LastName AS STRING) AS last_name,
            CAST(Email AS STRING) AS email,
            CAST(RegistrationDate AS TIMESTAMP) AS registration_date,
            CAST(State AS STRING) AS state
        FROM bronze.customers
        ''',
        use_legacy_sql=False,
    )

    list_gcs_files >> load_to_bronze >> transform_to_silver

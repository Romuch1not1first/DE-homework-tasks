from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta

from ..schemas import bronze_sales_schema
from ..queries import process_sales_to_silver

default_args = {
    'start_date': datetime(2022, 9, 1), 
    'end_date': datetime(2022, 9, 29), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5), 
}

with DAG(
    'process_sales',
    default_args=default_args, 
    schedule_interval='@daily', 
    catchup=True, 
) as dag:

    # Task to load data from GCS to BigQuery bronze table
    load_to_bronze = GCSToBigQueryOperator(
        task_id='load_to_bronze', 
        bucket='de-final-project-data',
        source_objects=[
            "sales/{{ execution_date.strftime('%Y-%m-') }}{{ execution_date.day }}/{{ execution_date.strftime('%Y-%m-') }}{{ execution_date.day }}__sales.csv"
        ],  # Source file path
        destination_project_dataset_table='bronze.sales', 
        schema_fields=bronze_sales_schema, 
        source_format='CSV',
        skip_leading_rows=1, 
        write_disposition='WRITE_APPEND', 
    )

    # Task to clean data and load to BigQuery silver table
    clean_and_load_to_silver = BigQueryExecuteQueryOperator(
        task_id='transform_to_silver', 
        sql=process_sales_to_silver, 
        use_legacy_sql=False, 
    )

    load_to_bronze >> clean_and_load_to_silver

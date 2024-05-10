from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.dummy import DummyOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 8, 9),
    'end_date': datetime(2022, 8, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

BUCKET_NAME = 'my-unique-data-backet'

with DAG('upload_to_gcs_dag',
         default_args=default_args,
         description='Upload data to GCS',
         schedule_interval='@daily',
         catchup=True) as dag:
    
    start_task = DummyOperator(
        task_id='start'
    )

    upload_file_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_file_to_gcs',
        src='/raw/raw/sales/{{ ds }}/sales_{{ ds }}_page_1.json',
        dst='src1/sales/v1/{{ execution_date.strftime("%Y") }}/{{ execution_date.strftime("%m") }}/{{ execution_date.strftime("%d") }}/sales_{{ ds }}_page_1.json',
        bucket=BUCKET_NAME,
        mime_type='application/json'
    )

    end_task = DummyOperator(
        task_id='end'
    )

    start_task >> upload_file_to_gcs >> end_task

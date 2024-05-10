from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
import os

def build_target_path(**kwargs):
    BASE_DIR = os.getenv("BASE_DIR")
    execution_date = kwargs['execution_date'].strftime("%Y-%m-%d")

    raw_dir = os.path.join(BASE_DIR, "raw", "sales", execution_date)
    stg_dir = os.path.join(BASE_DIR, "stg", "sales", execution_date)

    kwargs['ti'].xcom_push(key='raw_dir', value=raw_dir)
    kwargs['ti'].xcom_push(key='stg_dir', value=stg_dir)

default_args = {
    'start_date': datetime(2022, 8, 9),
    'end_date': datetime(2022, 8, 12),
    'catchup': True
}

with DAG(
    dag_id='process_sales_rewrite',
    default_args=default_args,
    description="Переписанный DAG для обработки данных о продажах",
    schedule_interval="0 1 * * *",
    tags=["process_sales", "robot_dreams", "data_engineering", "home_work"],
    max_active_runs=1
) as dag:

    build_paths = PythonOperator(
        task_id='build_paths',
        python_callable=build_target_path,
        provide_context=True
    )

    extract_data_from_api = SimpleHttpOperator(
        task_id='extract_data_from_api',
        method='POST',
        http_conn_id='flask_job_1_fetch',
        endpoint='/',
        data='''{
             "date": "{{ execution_date }}",
             "raw_dir": "{{ ti.xcom_pull(task_ids=\'build_paths\', key=\'raw_dir\') }}"
             }''',
        headers={"Content-Type": "application/json"},
        log_response=True,
        response_check=lambda response: response.status_code == 201
    )

    convert_to_avro = SimpleHttpOperator(
        task_id='convert_to_avro',
        method='POST',
        http_conn_id='flask_job_2_convert_to_avro',
        data='''{
            "raw_dir": "{{ ti.xcom_pull(task_ids=\'build_paths\', key=\'raw_dir\') }}",
            "stg_dir": "{{ ti.xcom_pull(task_ids=\'build_paths\', key=\'stg_dir\') }}"
        }''',
        headers={"Content-Type": "application/json"},
        log_response=True,
        response_check=lambda response: response.status_code == 201
    )

    build_paths >> extract_data_from_api >> convert_to_avro

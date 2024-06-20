from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2022, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'enrich_user_profiles',
    default_args=default_args,
    schedule_interval=None,
    catchup=True,
) as dag:

    # Task to enrich user profiles by joining customer and user profile data
    enrich_user_profiles = BigQueryExecuteQueryOperator(
        task_id='enrich_user_profiles',
        sql="""
        CREATE OR REPLACE TABLE gold.user_profiles_enriched AS
        SELECT
            c.client_id,
            COALESCE(SPLIT(p.full_name, ' ')[OFFSET(0)], c.first_name) AS first_name,
            COALESCE(SPLIT(p.full_name, ' ')[OFFSET(1)], c.last_name) AS last_name,
            c.email,
            c.registration_date,
            COALESCE(p.state, c.state) AS state,
            p.birth_date,
            p.phone_number
        FROM
            silver.customers c
        LEFT JOIN
            silver.user_profiles p
        ON
            c.email = p.email;
        """,
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
    )

    enrich_user_profiles

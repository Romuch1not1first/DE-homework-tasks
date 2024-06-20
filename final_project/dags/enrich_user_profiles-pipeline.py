from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta

from ..queries import populate_gold_user_profile_enriched


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

    # Define the task to enrich user profiles using a BigQuery operator
    enrich_user_profiles = BigQueryExecuteQueryOperator(
        task_id='enrich_user_profiles',
        sql=populate_gold_user_profile_enriched,
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
    )

    enrich_user_profiles

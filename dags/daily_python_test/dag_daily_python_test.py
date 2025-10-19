from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from utils.airflow_operators import build_tasks

default_args = {
    'start_date': days_ago(1),
    'retries': 6,
    'retry_delay': timedelta(minutes=5)
}

dag_file_path = __file__

with DAG(
    dag_id='daily_python_test',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False,
    tags=['python']
) as dag:

    ## Operator Load : gcs_sensor, bigquery_load, python_operator
    tasks = build_tasks(
        dag_file_path,
        enabled_operators=['python_operator']
    )

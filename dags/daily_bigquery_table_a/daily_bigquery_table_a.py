from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from utils.airflow_operators import build_tasks

dag_file_path = __file__
default_args = {
    'start_date': days_ago(1),
    'retries': 6,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='daily_bigquery_table_a',
    default_args=default_args,
    schedule_interval='30 8 * * *',
    catchup=False,
    tags=['bigquery']
) as dag:

    ## Operator Load : gcs_sensor, bigquery_load, python_operator
    tasks = build_tasks(
        dag_file_path, 
        enabled_operators=['gcs_sensor_operator', 'bigquery_load_operator']
    )

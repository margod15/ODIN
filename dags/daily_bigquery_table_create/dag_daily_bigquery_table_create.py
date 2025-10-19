import yaml
import re
from urllib.parse import urlparse
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta, timezone
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound, BadRequest

BUCKET_NAME = "airflow-data-management"
BASE_PREFIX = "dags/"
DICT_PREFIX = "dictionaries/"
PROJECT_ID = "<project_id>"

log = LoggingMixin().log

def get_column_description(bucket, column_name, table_name):
    col_blob = bucket.blob(f"{DICT_PREFIX}{column_name}.yaml")
    if not col_blob.exists():
        log.warning(f"Column dictionary not found for {column_name}. Description will be empty.")
        return ""

    col_content = yaml.safe_load(col_blob.download_as_bytes())
    descriptions = col_content.get("description", [])

    desc_map = {}
    for item in descriptions:
        if isinstance(item, dict):
            desc_map.update(item)

    if table_name in desc_map and desc_map[table_name]:
        return desc_map[table_name]

    return desc_map.get("default", "")

def process_recent_yaml_files():
    storage_client = storage.Client()
    bq_client = bigquery.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    now = datetime.now(timezone.utc)
    cutoff_time = now - timedelta(days=1)

    blobs = bucket.list_blobs(prefix=BASE_PREFIX)

    for blob in blobs:
        if not blob.name.endswith(".yaml"):
            continue
        if not re.match(r"dags/.+/tables/.+\.yaml", blob.name):
            continue
        if blob.updated < cutoff_time:
            continue

        log.info(f"Processing file: {blob.name}")

        try:
            yaml_bytes = blob.download_as_bytes()
            yaml_content = yaml.safe_load(yaml_bytes)
            if not isinstance(yaml_content, dict):
                log.warning(f"YAML content is not a dictionary for {blob.name}. Skipping process..")
                continue

            dataset = yaml_content.get("dataset")
            table_name = yaml_content.get("name")
            schema_def = yaml_content.get("schema", [])
            partition_field = yaml_content.get("partition")
            table_description = yaml_content.get("description", "")

            if not dataset or not table_name or not schema_def:
                log.warning(f"Missing dataset, table_name, or schema for {blob.name}. Skipping process..")
                continue

            dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{dataset}")
            table_ref = f"{PROJECT_ID}.{dataset}.{table_name}"

            table_exists = True
            try:
                bq_client.get_table(table_ref)
            except NotFound:
                table_exists = False

            schema = []
            for field in schema_def:
                name = field["name"]
                field_type = field.get("type", "STRING").upper()
                description = get_column_description(bucket, name, table_name)
                schema.append(bigquery.SchemaField(name, field_type, description=description))

            table = bigquery.Table(table_ref, schema=schema)
            table.description = table_description

            if partition_field:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partition_field
                )

            if not table_exists:
                bq_client.create_table(table)
                log.info(f"Created table {table_ref}")
            else:
                existing_table = bq_client.get_table(table_ref)
                existing_table.schema = schema
                existing_table.description = table_description
                if partition_field:
                    existing_table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=partition_field
                    )
                    update_fields = ["schema", "time_partitioning", "description"]
                else:
                    update_fields = ["schema", "description"]
                bq_client.update_table(existing_table, update_fields)
                log.info(f"Updated table {table_ref}")

        except BadRequest as e:
            log.error(f"Schema update failed for {blob.name}: {e}")
        except Exception as e:
            log.error(f"Failed to process {blob.name}: {e}")

default_args = {
    "start_date": days_ago(1),
    "retries": 6,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="daily_bigquery_table_create",
    default_args=default_args,
    schedule_interval="00 18 * * *",
    catchup=False,
    tags=["bigquery", "data_management"]
) as dag:

    run_creator = PythonOperator(
        task_id="create_bigquery_table",
        python_callable=process_recent_yaml_files,
    )

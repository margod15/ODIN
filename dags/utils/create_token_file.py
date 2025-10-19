from pathlib import Path
from google.cloud import storage
import logging
import yaml

logger = logging.getLogger(__name__)

def create_token(task_id, execution_date, dag_name, starter_file, custom_variable):
    storage_client = storage.Client()
    bucket = storage_client.bucket("airflow-data-management")

    blob_path = f"data/{task_id}.yaml"
    blob = bucket.blob(blob_path)

    # YAML content
    yaml_content = {
        "dag_name": dag_name,
        "date_run": execution_date,
        "starter_file": starter_file,
        "custom_variable": custom_variable
    }

    # Convert dictionary to YAML string
    yaml_str = yaml.dump(yaml_content, default_flow_style=False)

    # Upload to GCS
    blob.upload_from_string(yaml_str)

    logger.info(f"Token YAML file created at: {blob_path}")
import yaml
import os
import logging
from pathlib import Path
from airflow.exceptions import AirflowFailException
from utils.docker_image_builder import prepare_job
from utils.create_token_file import create_token
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.cloud_build import CloudBuildCreateBuildOperator

logger = logging.getLogger(__name__)

# Load configuration from config.yaml file in the DAG folder
def load_config(dag_file_path):
    config_path = Path(dag_file_path).parent / 'config.yaml'

    logger.info(f"Loading configuration from {config_path}")
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

# Load SQL query file from scripts folder in the DAG directory
def load_sql(dag_file_path, filename):
    sql_folder = Path(dag_file_path).parent / 'scripts'
    if not (sql_folder / filename).exists():
        raise AirflowFailException(f"SQL file {filename} not found in {sql_folder}")

    logger.info(f"Loading SQL from {filename}")
    with open(sql_folder / filename, 'r') as f:
        return f.read()

# Create a sensor to detect the existence of a file in GCS
def create_sensor_task(sensor_conf):
    logger.info(f"Creating GCS sensor task: {sensor_conf['task_id']}")

    return GCSObjectsWithPrefixExistenceSensor(
        task_id = sensor_conf['task_id'],
        bucket = sensor_conf['bucket'],
        prefix = sensor_conf['object'],
        timeout = sensor_conf.get('timeout', 3600),
        poke_interval = sensor_conf.get('check_interval', 60),
        mode = 'reschedule'
    )

# Create a task to load data from GCS to BigQuery
def create_loader_task(sensor_conf):
    gcs_task_id = f"load_gcs_to_bq_{sensor_conf['task_id'].replace('_file_sensor', '')}"
    logger.info(f"Creating loader task: {gcs_task_id}")

    bucket = sensor_conf['bucket']
    prefix = sensor_conf['object']

    # If the prefix contains Jinja, let Airflow operator do templating and loading
    if "{" in prefix or "}" in prefix:
        logger.info("Prefix contains Jinja - passing templated value to operator.")
        operator_kwargs = {
            'task_id': gcs_task_id,
            'bucket': bucket,
            'source_objects': [prefix],
            'destination_project_dataset_table': sensor_conf.get('target_table'),
            'source_format': sensor_conf['file_type'],
            'write_disposition': sensor_conf['load_method'],
            'autodetect': True
        }
        if sensor_conf.get('delimiter') and sensor_conf['file_type'].upper() == 'CSV':
            operator_kwargs['field_delimiter'] = sensor_conf['delimiter']
            logger.info(f"Using delimiter for CSV: {sensor_conf['delimiter']}")
        return gcs_task_id, GCSToBigQueryOperator(**operator_kwargs)

    gcs_hook = GCSHook()
    client = gcs_hook.get_conn()
    bucket_ref = client.bucket(bucket)

    matched_files = gcs_hook.list(bucket_name = bucket, prefix = prefix)
    if not matched_files:
        raise AirflowFailException(f"No files found in bucket {bucket} with name {prefix}")

    blobs = [bucket_ref.get_blob(f) for f in matched_files if bucket_ref.get_blob(f)]
    if not blobs:
        raise AirflowFailException(f"No blobs found in bucket {bucket} for matched files: {matched_files}")

    latest_blob = sorted(blobs, key = lambda b: b.updated, reverse = True)[0]
    latest_file = latest_blob.name
    logger.info(f"Selected latest file: {latest_file}")

    operator_kwargs = {
        'task_id': gcs_task_id,
        'bucket': bucket,
        'source_objects': [latest_file],
        'destination_project_dataset_table': sensor_conf.get('target_table'),
        'source_format': sensor_conf['file_type'],
        'write_disposition': sensor_conf['load_method'],
        'autodetect': True
    }

    if sensor_conf.get('delimiter') and sensor_conf['file_type'].upper() == 'CSV':
        operator_kwargs['field_delimiter'] = sensor_conf['delimiter']
        logger.info(f"Using delimiter for CSV: {sensor_conf['delimiter']}")

    return gcs_task_id, GCSToBigQueryOperator(**operator_kwargs)

# Create a BigQuery task that runs a SQL query and writes to a target table
def create_bigquery_task(conf, dag_file_path):
    logger.info(f"Creating BigQuery task: {conf['task_id']}")

    return BigQueryInsertJobOperator(
        task_id = conf['task_id'],
        configuration = {
            'query': {
                'query': load_sql(dag_file_path, conf['file']),
                'useLegacySql': False,
                'destinationTable': {
                    'projectId': '<project_id>',
                    'datasetId': conf['dataset_name'],
                    'tableId': conf['target_table']
                },
                'writeDisposition': conf['load_method']
            }
        },
        location = 'asia-southeast2'
    )

# Create a Python job that builds and runs a Docker image using Cloud Build
def create_python_task(conf):
    job_name = conf['task_id']
    logger.info(f"Creating Python operator chain for: {job_name}")
    service_account = os.environ.get("AIRFLOW_SERVICE_ACCOUNT")

    with TaskGroup(group_id = f"job_{job_name}") as python_group:
        def prepare_callable(**context):
            logger.info(f"Running prepare step for: {job_name}")
            result = prepare_job(job_name)
            context['ti'].xcom_push(key = 'result', value = result)

        prepare_task = PythonOperator(
            task_id = f"prepare_env_{job_name}",
            python_callable = prepare_callable
        )

        def cloud_build_callable(**context):
            logger.info(f"Running cloud build for: {job_name}")
            result = context['ti'].xcom_pull(key = 'result')
            if result['build_required']:
                logger.info(f"Build required for {job_name}, executing build")
                build_config = result['build_config']
                build_op = CloudBuildCreateBuildOperator(
                    task_id = f"cloud_build_{job_name}",
                    build = build_config,
                    project_id = 'maybank-analytics-production',
                    wait = True
                )
                build_op.execute(context)
            else:
                logger.info(f"Skipping build for {job_name}, image already exists")

        build_task = PythonOperator(
            task_id = f"build_env_{job_name}",
            python_callable = cloud_build_callable
        )

        def cloud_run_callable(**context):
            result = context['ti'].xcom_pull(key = 'result')
            image_uri = result['image_uri']
            logger.info(f"Running container for: {job_name}")
            build_config = {
                "service_account": f"projects/maybank-analytics-production/serviceAccounts/{service_account}",
                "options": {"logging": "CLOUD_LOGGING_ONLY"},
                "steps": [
                    {
                        "name": image_uri,
                        "entrypoint": "python",
                        "args": [f"/app/{conf['file']}"]
                    }
                ]
            }
            run_op = CloudBuildCreateBuildOperator(
                task_id = f"run_job_{job_name}",
                build = build_config,
                project_id = 'maybank-analytics-production',
                wait = True
            )
            run_op.execute(context)

        run_task = PythonOperator(
            task_id = f"run_{job_name}",
            python_callable = cloud_run_callable
        )

        prepare_task >> build_task >> run_task

    return python_group

# Create token to be sent to VDI
def create_token_generation_task(conf, dag_name, **kwargs):
    job_name = conf['task_id']
    starter_file = conf['starter_file']
    custom_variable = conf.get('custom_variable', 'null')

    return PythonOperator(
        task_id = job_name,
        python_callable = create_token,
        op_kwargs = {
            "task_id": job_name,
            "execution_date": "{{ ds }}",
            "dag_name": dag_name,
            "starter_file": starter_file,
            "custom_variable": custom_variable
        }
    )

# Create vdi job sensor through log
def create_log_validation_task(sensor_conf):
    task_id = f"validate_job_status_{sensor_conf['task_id'].replace('_log_sensor', '')}"
    bucket   = sensor_conf['bucket']
    obj_tmpl = sensor_conf['object']

    def log_validator(bucket, object_path, **context):
        logger.info(f"Validating VDI log gs://{bucket}/{object_path}*")
        gcs = GCSHook()
        client = gcs.get_conn()
        bucket_ref = client.bucket(bucket)

        matched_files = gcs.list(bucket_name=bucket, prefix=object_path)
        if not matched_files:
            raise AirflowFailException(f"No log file found in gs://{bucket}/{object_path}*")

        blobs = [bucket_ref.get_blob(f) for f in matched_files if bucket_ref.get_blob(f)]
        latest = max(blobs, key=lambda b: b.updated)
        name   = latest.name
        logger.info(f"Selected log file: {name}")

        with client.bucket(bucket).blob(name).open("r") as f:
            for line in f:
                logger.info(line.strip())

        up = name.upper()
        if "FAIL" in up or "ERROR" in up:
            raise AirflowFailException(f"VDI Job {name} FAILED")
        elif "SUCCESS" in up or "DONE" in up:
            logger.info(f"VDI Job {name} SUCCEEDED")
        else:
            raise AirflowFailException(f"Cannot determine status from {name!r}")

    return PythonOperator(
        task_id=task_id,
        python_callable=log_validator,
        op_kwargs={
            "bucket": bucket,
            "object_path": obj_tmpl,
        },
        provide_context=True,
    )

# Build all enabled tasks (sensor, loader, bigquery, python) and manage dependencies
def build_tasks(dag_file_path, enabled_operators):
    config = load_config(dag_file_path)
    task_dict = {}
    sensor_to_loader_map = {}

    if 'vdi_token_generator' in enabled_operators:
        for conf in config.get('tasks', []):
            if conf.get('type') == 'token':
                task = create_token_generation_task(conf, config.get('dag_name', []))
                task_dict[conf['task_id']] = task

    if 'gcs_sensor_operator' in enabled_operators:
        for sensor_conf in config.get('sensors', []):
            sensor_task = create_sensor_task(sensor_conf)
            task_dict[sensor_conf['task_id']] = sensor_task

            last_task = sensor_task

            if sensor_conf.get('target_table'):
                gcs_task_id, loader_task = create_loader_task(sensor_conf)
                task_dict[gcs_task_id] = loader_task
                
                sensor_task >> loader_task
                sensor_to_loader_map[sensor_conf['task_id']] = gcs_task_id

                last_task = loader_task

            if sensor_conf.get('vdi_job_sensor', False):
                validator_task = create_log_validation_task(sensor_conf)
                task_dict[validator_task.task_id] = validator_task
                
                last_task >> validator_task

    if 'bigquery_load_operator' in enabled_operators:
        for conf in config.get('tasks', []):
            if conf.get('type') == 'bigquery':
                bq_task = create_bigquery_task(conf, dag_file_path)
                task_dict[conf['task_id']] = bq_task

    if 'python_operator' in enabled_operators:
        for conf in config.get('tasks', []):
            if conf.get('type') == 'python':
                python_task_group = create_python_task(conf)
                task_dict[conf['task_id']] = python_task_group

    # Generate task dependencies
    for conf in config.get('tasks', []) + config.get('sensors', []):
        task = task_dict.get(conf['task_id'])
        if task:
            for dep in conf.get('dependencies', []):
                if dep:
                    upstream_task_id = sensor_to_loader_map.get(dep, dep)
                    upstream_task = task_dict.get(upstream_task_id)
                    if upstream_task:
                        logger.info(f"Setting dependency: {upstream_task.task_id} -> {task.task_id}")
                        upstream_task >> task

    logger.info("All tasks built successfully")
    return list(task_dict.values())

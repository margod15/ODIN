from google.cloud import bigquery
 
# Config
PROJECT_ID = "<project_id>"
DATASET = "playground"
TABLE_NAME = "testing_python_job_create" 

def create_bq_table():
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE_NAME}"

    schema = [
        bigquery.SchemaField("id", "STRING", description="Primary ID"),
        bigquery.SchemaField("customer_name", "STRING", description="Customer full name"),
        bigquery.SchemaField("transaction_date", "DATE", description="Transaction date"),
        bigquery.SchemaField("amount", "FLOAT", description="Transaction amount"),
    ]

    table = bigquery.Table(table_ref, schema=schema)

    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="transaction_date"
    )

    try:
        client.get_table(table_ref)
        print(f"Table already exists: {table_ref}")
    except Exception:
        client.create_table(table)
        print(f"✅ Table created: {table_ref}")

    return table_ref

def insert_dummy_data(table_ref):
    client = bigquery.Client(project=PROJECT_ID)

    rows_to_insert = [
        {"id": "C001", "customer_name": "John Doe", "transaction_date": "2024-01-10", "amount": 150.75},
        {"id": "C002", "customer_name": "Jane Smith", "transaction_date": "2024-02-15", "amount": 275.50},
        {"id": "C003", "customer_name": "Alice Johnson", "transaction_date": "2024-03-20", "amount": 410.00}
    ]

    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        print(f"❌ Insert errors: {errors}")
    else:
        print(f"✅ Inserted dummy data into {table_ref}")

schema_table_ref = create_bq_table()
insert_dummy_data(schema_table_ref)

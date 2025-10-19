import os
import argparse
import yaml

def load_yaml(file_path):
    with open(file_path, 'r') as f:
        return yaml.safe_load(f)

def write_column_yaml(path, name, dtype):
    class IndentDumper(yaml.SafeDumper):
        def increase_indent(self, flow=False, indentless=False):
            return super(IndentDumper, self).increase_indent(flow, False)

    content = {
        'name': name,
        'data_type': dtype,
        'description': [
            {'default': ""}
        ]
    }

    with open(path, 'w') as f:
        yaml.dump(content, f, default_flow_style=False, sort_keys=False, indent=2, Dumper=IndentDumper)
    print(f"Created: {name}")

def main(table_name, dag_name):
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    dicts_dir = os.path.join(project_root, "dictionaries")

    table_yaml_path = os.path.join(project_root, "dags", f"{dag_name}", "tables", f"{table_name}.yaml")
    if not os.path.isfile(table_yaml_path):
        print(f"Table YAML not found: {table_yaml_path}")
        return

    table_data = load_yaml(table_yaml_path)
    schema = table_data.get('schema', [])
    missing_columns = []

    for col in schema:
        col_name = col['name']
        col_type = col.get('type', '')
        dict_path = os.path.join(dicts_dir, f"{col_name}.yaml")
        if not os.path.exists(dict_path):
            write_column_yaml(dict_path, col_name, col_type)
            missing_columns.append(col_name)

    if not missing_columns:
        print("All columns are already registered.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Register missing columns from table schema into dictionaries folder.")
    parser.add_argument('--table_name', required=True, help="Table name (without .yaml)")
    parser.add_argument('--dag_name', required=True, help="Name of the DAG")
    args = parser.parse_args()
    main(args.table_name, args.dag_name)

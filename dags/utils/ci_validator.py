import sys
import os
import re
import yaml
from pathlib import Path

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def load_yaml(path):
    try:
        with open(path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"ERROR: Cannot read or parse YAML: {path}: {e}")
        return None

def error(msg, errors=None, error_key=None):
    print(f"##vso[task.logissue type=error]{msg}")
    if errors is not None and error_key is not None:
        errors.append(error_key)

def warn(msg):
    print(f"##vso[task.logissue type=warning]{msg}")

def validate_table_schema_file(table_yaml_path, dictionaries_dir, all_schema_paths, errors):
    table_yaml = load_yaml(table_yaml_path) or {}
    if not table_yaml:
        error(f"YAML cannot be loaded: {table_yaml_path}", errors, table_yaml_path)
        return

    table_name = table_yaml.get("name")
    schema = table_yaml.get("schema", [])
    
    table_match_files = []
    changed_file = Path(table_yaml_path).stem

    for p in all_schema_paths:
        # Check: table name inside yaml file conflict
        if p != os.path.relpath(table_yaml_path):
            existing_table_yaml = load_yaml(os.path.join(project_root,p))
            existing_table_name = existing_table_yaml.get("name", "") if isinstance(existing_table_yaml, dict) else ""

            if table_name == existing_table_name:
                error(
                    f"Table name in yaml {table_name} conflict with the used table name inside {p}", 
                    errors, 
                    p
                )

        if changed_file == Path(p).stem and p != os.path.relpath(table_yaml_path):
            table_match_files.append(p)

    for table_match_file in table_match_files:
        error(
            f"Table file name {table_yaml_path} conflict with {table_match_file}", 
            errors, 
            table_match_file
        )

    # Check: table yaml file name == name in content
    expected_table_name = os.path.splitext(os.path.basename(table_yaml_path))[0]
    if table_name and expected_table_name != table_name:
        error(
            f"Table YAML file '{table_yaml_path}' does not match its 'name:' value ('{table_name}')",
            errors,
            table_yaml_path,
        )

    # Check: all columns are registered in dictionaries
    missing_cols = []
    dtype_mismatch = []
    blank_desc = []
    for col in schema:
        col_name = col["name"]
        col_type = col.get("type")
        dict_path = os.path.join(dictionaries_dir, f"{col_name}.yaml")
        if not os.path.isfile(dict_path):
            missing_cols.append(col_name)
            continue
        col_dict = load_yaml(dict_path)
        if not col_dict:
            error(f"Cannot load dictionary YAML: {dict_path}", errors, dict_path)
            continue
        # Check datatype match
        dict_dtype = col_dict.get("data_type")
        if dict_dtype and dict_dtype.upper() != col_type.upper():
            dtype_mismatch.append((col_name, col_type, dict_dtype))
        # Check blank description
        descriptions = col_dict.get("description", [])
        has_nonblank = False
        for desc in descriptions:
            if isinstance(desc, dict) and desc.get("default", "").strip():
                has_nonblank = True
        if not has_nonblank:
            blank_desc.append(col_name)

    if missing_cols:
        error(
            f"Columns in {table_yaml_path} missing in dictionaries: {', '.join(missing_cols)}",
            errors,
            table_yaml_path,
        )
    if dtype_mismatch:
        for name, t, dt in dtype_mismatch:
            error(
                f"Type mismatch for column {name} in {table_yaml_path}: Table='{t}' vs Dictionaries='{dt}'",
                errors,
                table_yaml_path,
            )
    if blank_desc:
        for name in blank_desc:
            error(
                f"Column {name} in dictionaries/{name}.yaml has a blank default description.",
                errors,
                os.path.join(dictionaries_dir, f"{name}.yaml"),
            )

def validate_dag_file(dag_py_path, all_dag_paths, errors):
    changed_file = Path(dag_py_path).stem
    match_file = False
    for p in all_dag_paths:
        if changed_file == Path(p).stem:
            match_file = p

    if match_file:
        error(
            f"DAG filename conflict: {changed_file}.py already exist at {match_file}", 
            errors, 
            dag_py_path
        )

    try:
        with open(dag_py_path, "r", encoding="utf-8") as f:
            content = f.read()
        dag_id_match = re.search(r"dag_id\s*=\s*['\"]([^'\"]+)['\"]", content)
        if dag_id_match:
            dag_id = dag_id_match.group(1)
            expected_file = os.path.splitext(os.path.basename(dag_py_path))[0]
            if dag_id != expected_file:
                error(
                    f"DAG file {dag_py_path} filename does not match dag_id ('{dag_id}') in content",
                    errors,
                    dag_py_path,
                )
        
        conflicting_id_paths=[]
        
        for existing_dag_path in all_dag_paths:
            with open(existing_dag_path, "r", encoding="utf-8") as fl:
                content_dag = fl.read()

            existing_dag_id_match = re.search(r"dag_id\s*=\s*['\"]([^'\"]+)['\"]", content_dag)

            if existing_dag_id_match and existing_dag_id_match.group(1) == dag_id:
                conflicting_id_paths.append(existing_dag_path)

        if conflicting_id_paths:
            error(
                "DAG id '{0}' conflicts:\n - {1}".format(dag_id, "\n - ".join(map(str, conflicting_id_paths))),
                errors,
                dag_py_path,
            )

    except Exception as e:
        warn(f"Could not parse {dag_py_path}: {e}")
        errors.append(dag_py_path)

def main():
    changed_files = sys.argv[1:]
    if not changed_files:
        print("No files provided.")
        sys.exit(0)
    
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    dags_root = os.path.join(repo_root, "dags")
    dictionaries_dir = os.path.join(repo_root, "dictionaries")

    all_dag_paths = []
    all_schema_paths = []
    for root, _, files in os.walk(dags_root):
        for fn in files:
            if fn.endswith(".py"):
                all_dag_paths.append(os.path.join(root, fn))

            file_path = os.path.join(root, fn)
            if "tables" in file_path and ".yaml" in file_path:
                all_schema_paths.append(os.path.relpath(file_path))
    
    errors = []
    table_yaml_files = [f for f in changed_files if re.match(r"dags/.+/tables/.+\.yaml", f)]
    dag_py_files = [f for f in changed_files if re.match(r"^dags/(?!utils/).+/.+\.py$", f)]
    dict_yaml_files = [f for f in changed_files if re.match(r"dictionaries/.+\.yaml", f)]

    for table_yaml in table_yaml_files:
        validate_table_schema_file(table_yaml, dictionaries_dir, all_schema_paths, errors)

    for dag_py in dag_py_files:
        validate_dag_file(dag_py, all_dag_paths, errors)

    dict_file_name = [d['name'] for p in Path(dictionaries_dir).glob('*.y*ml')
                  if p.name not in {Path(x).name for x in dict_yaml_files}
                  and (d := load_yaml(p)) and isinstance(d, dict) and 'name' in d]

    for dict_yaml in dict_yaml_files:
        col_dict = load_yaml(dict_yaml)
        if not col_dict:
            error(f"Cannot load {dict_yaml}", errors, dict_yaml)
            continue
        descriptions = col_dict.get("description", [])
        has_nonblank = False
        for desc in descriptions:
            if isinstance(desc, dict) and desc.get("default", "").strip():
                has_nonblank = True
        if not has_nonblank:
            error(
                f"Dictionary file {dict_yaml} has blank default description.",
                errors,
                dict_yaml
            )

        match_col_name = col_dict['name'] in dict_file_name 
        if match_col_name:
            error(
                f"Column name in yaml file conflict {col_dict['name']}",
                errors,
                match_col_name
            )

    if errors:
        print("Validation failed.")
        sys.exit(1)
    else:
        print("All validations passed.")

if __name__ == "__main__":
    main()

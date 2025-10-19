
# ODIN — Optimized Data IngestioN

**ODIN** is a modular and scalable framework designed to manage data ingestion and pipeline orchestration efficiently across diverse sources and destinations. The system supports robust metadata-driven configurations and enforces schema governance to ensure data reliability, consistency, and enrichment.

## ✨ Overview

ODIN provides a unified structure to define and operate data pipelines that support:

- ✅ **Source Flexibility**:
  - On-premise and cloud-hosted databases (e.g., Oracle)
  - Cloud storage buckets (e.g., GCS)

- ✅ **Target Compatibility**:
  - Google BigQuery
  - Oracle Data Warehouse

- ✅ **Metadata-Driven Pipelines**:
  - YAML-based schema definition and enrichment
  - Centralized dictionary for column data type and description
  - Schema and naming validation against authoritative metadata

- ✅ **CI-Integrated Validation**:
  - Auto-checks for missing dictionary columns
  - Type and naming mismatch detection
  - Ensures consistency between DAG, YAML, and metadata files

## 📁 Project Structure

### 🔹 `dags/`
- **Purpose**: Stores all Airflow DAGs and pipeline-specific configuration.
- **Subfolders**:
  - `dags/<dag_name>/`
    - `dag_<dag_name>.py` — Main Airflow DAG definition.
    - `scripts/<query>.sql` — SQL scripts to extract or transform data.
    - `tables/<table>.yaml` — Table schema definition for the pipeline.

### 🔹 `dags/utils/`
- **Purpose**: Contains shared Python automation utilities.

### 🔹 `dictionaries/`
- **Purpose**: Central metadata store for **column-level definitions**.

### 🔹 `README.md`
- **Purpose**: Documentation for ODIN architecture, usage, and contribution.

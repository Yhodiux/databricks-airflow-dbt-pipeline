# Databricks + dbt + Airflow Data Engineering Pipeline

End-to-end data engineering project using Databricks, Delta Lake, Auto Loader and Apache Airflow.

## Architecture

CSV Files
   ↓
Databricks Auto Loader
   ↓
Bronze Layer
   ↓
Silver Layer
   ↓
Gold Layer

Apache Airflow orchestrates the Databricks workflow using the
DatabricksRunNowOperator.

## Pipeline

### Bronze
- Incremental CSV ingestion using Databricks Auto Loader.
- Schema tracking with `cloudFiles.schemaLocation`.
- Streaming checkpoint management.
- Data stored as Delta tables.

### Silver
- Data type transformations and cleansing.
- Deduplication using window functions.
- Incremental upserts using Delta Lake `MERGE`.

### Gold
- Business-level aggregations.
- Customer order and sales metrics.

## Orchestration

Apache Airflow runs locally using Docker Compose.

The Airflow DAG triggers the existing Databricks Job through:

`DatabricksRunNowOperator`

Workflow:

Airflow → Databricks Job → Bronze → Silver → Gold

## Technologies

- Databricks
- Apache Spark / PySpark
- Delta Lake
- Databricks Auto Loader
- Apache Airflow
- Docker
- Python
- SQL

## Project Structure

```text
databricks-airflow-pipeline/
├── airflow/
│   ├── dags/
│   │   └── primer_dag.py
│   ├── Dockerfile
│   └── docker-compose.yaml
├── databricks/
│   ├── 01_bronze_orders.py
│   ├── 02_silver_orders.py
│   └── 03_gold_customer_sales.py
├── data/
│   ├── orders_20260902.csv
│   ├── orders_20260903.csv
│   ├── orders_20260904.csv
│   └── orders_20260905.csv
└── README.md
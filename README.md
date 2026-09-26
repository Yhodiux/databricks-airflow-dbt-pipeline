# Databricks + dbt + Airflow Data Engineering Pipeline

End-to-end data engineering project using Databricks, Delta Lake, Auto Loader, dbt and Apache Airflow.

## Architecture

![Databricks + dbt + Airflow Architecture](docs/architecture.png)

## Pipeline Overview

This project implements an end-to-end data engineering pipeline with Databricks, Delta Lake, dbt and Apache Airflow.

Two complementary transformation flows are currently implemented:

### Databricks / PySpark pipeline

- **Bronze:** Incremental CSV ingestion using Databricks Auto Loader with schema tracking and checkpoints.
- **Silver:** Data cleansing, type casting, deduplication and incremental upserts using Delta Lake `MERGE`.
- **Gold:** Business-level aggregations providing customer order and sales metrics.
- **Orchestration:** Apache Airflow running in Docker triggers the Databricks Job through `DatabricksRunNowOperator`.

Workflow:

```text
CSV files
   ↓
Databricks Auto Loader
   ↓
Bronze
   ↓
Silver (PySpark + Delta MERGE)
   ↓
Gold
```

### dbt transformation pipeline

dbt connects to Databricks and uses the existing Bronze orders table as its source.

The model dependencies are defined with `source()` and `ref()`, allowing dbt to build and execute the transformation DAG automatically.

```text
Databricks Bronze
       ↓
   stg_orders
     (view)
       ↓
  silver_orders
     (table)
       ↓
 Data Quality Tests
       ↓
 customer_sales
     (table)
```

The dbt pipeline was validated successfully with `dbt build`.

## Bronze

The Databricks ingestion layer:

- Incremental CSV ingestion using Databricks Auto Loader.
- Schema tracking with `cloudFiles.schemaLocation`.
- Streaming checkpoint management.
- Delta table storage.
- File-arrival processing using `availableNow=True`.

## Silver

### PySpark / Delta implementation

- Data type transformations and cleansing.
- Deduplication using window functions.
- Incremental and idempotent upserts using Delta Lake `MERGE`.

### dbt implementation

The `silver_orders` model:

- Reads from the dbt staging model through `ref('stg_orders')`.
- Deduplicates orders using `row_number()`.
- Keeps the latest record based on `updated_at`.
- Is materialized as a Databricks table.

## Gold

### PySpark implementation

- Business-level aggregations.
- Customer order and sales metrics.

### dbt implementation

The `customer_sales` model:

- Reads from `silver_orders` using `ref()`.
- Filters completed orders.
- Aggregates order count and total sales by customer.
- Is materialized as a Databricks table.

## dbt

The dbt project is located under:

```text
dbt/orders_dbt/
```

The staging model `stg_orders` declares the Databricks Bronze table as a dbt source:

```text
workspace.bronze.orders
```

The current dbt DAG is:

```text
source('bronze', 'orders')
          ↓
      stg_orders
          ↓
     silver_orders
          ↓
     customer_sales
```

### Data Quality Tests

Generic dbt tests are defined for `silver_orders`:

- `order_id` must not be null.
- `order_id` must be unique.
- `customer_id` must not be null.

The complete dbt build currently executes:

- 1 staging view.
- 2 table models.
- 3 data quality tests.

Final validation:

```text
PASS=6
WARN=0
ERROR=0
SKIP=0
TOTAL=6
```

Useful dbt commands used in the project:

```bash
dbt debug
dbt run
dbt test
dbt build
```

## Orchestration

Apache Airflow runs locally using Docker Compose.

The Airflow DAG triggers the existing Databricks Job through:

```text
DatabricksRunNowOperator
```

Current Airflow workflow:

```text
Airflow
   ↓
Databricks Job
   ↓
Bronze → Silver → Gold
```

Airflow-to-dbt orchestration is not yet implemented. The dbt pipeline currently connects directly to Databricks and is executed independently.

## Technologies

- Databricks
- Apache Spark / PySpark
- Delta Lake
- Databricks Auto Loader
- dbt
- Apache Airflow
- Docker
- Python
- SQL
- Git / GitHub

## Project Structure

```text
databricks-airflow-dbt-pipeline/
├── airflow/
│   ├── dags/
│   │   └── primer_dag.py
│   ├── Dockerfile
│   └── docker-compose.yaml
├── databricks/
│   ├── 01_bronze_orders.py
│   ├── 02_silver_orders.py
│   └── 03_gold_customer_sales.py
├── dbt/
│   └── orders_dbt/
│       ├── models/
│       │   ├── staging/
│       │   │   ├── sources.yml
│       │   │   └── stg_orders.sql
│       │   ├── silver/
│       │   │   ├── schema.yml
│       │   │   └── silver_orders.sql
│       │   └── gold/
│       │       └── customer_sales.sql
│       └── dbt_project.yml
├── data/
│   ├── orders_20260902.csv
│   ├── orders_20260903.csv
│   ├── orders_20260904.csv
│   └── orders_20260905.csv
├── docs/
│   └── architecture.png
├── .gitignore
└── README.md
```

## Security

Local credentials and generated runtime artifacts are excluded from version control.

The repository does not include:

- Databricks access tokens.
- dbt `profiles.yml`.
- Python virtual environments.
- dbt `target/`, `logs/` or `dbt_packages/` directories.

## Current Status

Implemented and validated:

- Databricks Auto Loader ingestion.
- Bronze, Silver and Gold Delta pipeline.
- Incremental Silver processing with Delta `MERGE`.
- Databricks Job orchestration.
- Apache Airflow to Databricks Job integration.
- dbt connection to Databricks.
- dbt staging, Silver and Gold models.
- dbt model dependencies using `source()` and `ref()`.
- dbt generic data quality tests.
- Successful end-to-end `dbt build`.

Next step:

- Integrate dbt execution into the Apache Airflow orchestration workflow.

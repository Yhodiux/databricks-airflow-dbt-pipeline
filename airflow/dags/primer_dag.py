from airflow.sdk import DAG, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG(
    dag_id="primer_dag",
    start_date=datetime(2026, 9, 25),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def inicio():
        print("Iniciando pipeline desde Airflow")

    ejecutar_databricks = DatabricksRunNowOperator(
        task_id="ejecutar_databricks",
        databricks_conn_id="databricks_default",
        job_id=219467251318228,
    )

    @task
    def fin():
        print("Pipeline de Databricks terminado correctamente")

    inicio() >> ejecutar_databricks >> fin()
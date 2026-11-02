from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

DBT_DIR = "/opt/airflow/dbt"
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"

with DAG(
    dag_id="velib_iceberg_maintenance",
    schedule="7 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["velib", "iceberg", "maintenance"],
) as dag:
    maintenance = BashOperator(
        task_id="iceberg_maintenance",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} run-operation iceberg_maintenance",
    )
FROM apache/airflow:3.3.2

RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
    "dbt-core==1.11.16" \
    "dbt-spark[PyHive]==1.11.0"
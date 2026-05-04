"""
End-to-end Airflow DAG for the Multi-Cloud Retail Analytics Data Platform.

Why this DAG exists:
- Orchestrates the full local analytics pipeline.
- Replaces manual script execution with a repeatable Airflow workflow.
- Demonstrates production-style dependency management and observability.

Pipeline flow:
    generate retail data
        ↓
    initialize local warehouse
        ↓
    ingest generated JSON files to MinIO Bronze storage
        ↓
    load Bronze files into PostgreSQL Bronze raw tables
        ↓
    run Silver transformations
        ↓
    verify Silver tables
        ↓
    run Gold transformations
        ↓
    verify Gold tables
        ↓
    verify API query layer
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

PROJECT_DIR = "/opt/airflow/project"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="retail_analytics_e2e_pipeline",
    description="End-to-end retail analytics pipeline from source generation to Gold API verification.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["retail", "analytics", "bronze-silver-gold", "local"],
) as dag:
    initialize_warehouse = BashOperator(
        task_id="initialize_local_warehouse",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python scripts/init_local_warehouse.py"
        ),
    )

    generate_source_data = BashOperator(
        task_id="generate_source_data",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python ingestion/generate_retail_data.py "
            "--products 100 "
            "--customers 500 "
            "--stores 20 "
            "--campaigns 12 "
            "--sales 2000 "
            "--inventory 500 "
            "--return-rate 0.08"
        ),
    )

    ingest_to_bronze_storage = BashOperator(
        task_id="ingest_to_bronze_storage",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python ingestion/ingest_to_object_storage.py "
            "--run-id {{ ts_nodash }}"
        ),
    )

    load_bronze_warehouse = BashOperator(
        task_id="load_bronze_warehouse",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python scripts/load_bronze_warehouse.py"
        ),
    )

    run_silver_transformations = BashOperator(
        task_id="run_silver_transformations",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python scripts/run_silver_transformations.py"
        ),
    )

    verify_silver_tables = BashOperator(
        task_id="verify_silver_tables",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python scripts/verify_silver_tables.py"
        ),
    )

    run_gold_transformations = BashOperator(
        task_id="run_gold_transformations",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python scripts/run_gold_transformations.py"
        ),
    )

    verify_gold_tables = BashOperator(
        task_id="verify_gold_tables",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python scripts/verify_gold_tables.py"
        ),
    )

    verify_api_queries = BashOperator(
        task_id="verify_api_queries",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python scripts/verify_api_queries.py"
        ),
    )

    (
        initialize_warehouse
        >> generate_source_data
        >> ingest_to_bronze_storage
        >> load_bronze_warehouse
        >> run_silver_transformations
        >> verify_silver_tables
        >> run_gold_transformations
        >> verify_gold_tables
        >> verify_api_queries
    )
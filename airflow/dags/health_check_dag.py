"""
Airflow health check DAG.

Why this DAG exists:
- Confirms that Airflow can discover DAG files from our local repository.
- Confirms that the scheduler and webserver are working.
- Gives us a safe test before we build the real dynamic retail pipeline.

This DAG does not process business data.
It is only for infrastructure validation.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="health_check_dag",
    description="Validates that the local Airflow environment is running correctly.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["health-check", "local"],
)
def health_check_dag() -> None:
    """
    Defines a minimal Airflow DAG.

    We use the TaskFlow API because it is clean and readable.
    Later, our production-style DAGs will use a similar structure.
    """

    @task
    def print_health_message() -> str:
        """
        Simple task used to verify task execution.

        Returns:
            A status message that appears in Airflow task logs.
        """

        message = "Airflow is running correctly for the retail analytics platform."
        print(message)
        return message

    print_health_message()


health_check_dag()
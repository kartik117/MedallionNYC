from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task

BRONZE_DATASET = Dataset("file:///opt/airflow/data/bronze")
SILVER_DATASET = Dataset("file:///opt/airflow/data/silver")


@dag(
    dag_id="medallion_nyc_transform",
    schedule=[BRONZE_DATASET],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["medallion-nyc", "silver"],
)
def transform_dag():
    @task(outlets=[SILVER_DATASET])
    def clean() -> dict:
        from medallion_nyc.config import DEFAULT_MONTHS
        from medallion_nyc.silver.clean import run

        return run(DEFAULT_MONTHS)

    clean()


transform_dag()

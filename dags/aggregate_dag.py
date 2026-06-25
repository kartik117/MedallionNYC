from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task

SILVER_DATASET = Dataset("file:///opt/airflow/data/silver")
GOLD_DATASET = Dataset("file:///opt/airflow/data/gold")


@dag(
    dag_id="medallion_nyc_aggregate",
    schedule=[SILVER_DATASET],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["medallion-nyc", "gold"],
)
def aggregate_dag():
    @task(outlets=[GOLD_DATASET])
    def aggregate() -> dict:
        from medallion_nyc.gold.aggregate import run

        return run()

    aggregate()


aggregate_dag()

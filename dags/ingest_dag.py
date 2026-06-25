from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task

BRONZE_DATASET = Dataset("file:///opt/airflow/data/bronze")


@dag(
    dag_id="medallion_nyc_ingest",
    schedule="@monthly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["medallion-nyc", "bronze"],
)
def ingest_dag():
    @task(outlets=[BRONZE_DATASET])
    def ingest() -> list[str]:
        # Imported here, not at module level: the scheduler re-parses every
        # DAG file in this folder every few seconds, and importing pyspark
        # at parse time would start a JVM on every parse cycle.
        from medallion_nyc.bronze.ingest import ingest_months
        from medallion_nyc.config import DEFAULT_MONTHS

        paths = ingest_months(DEFAULT_MONTHS)
        return [str(path) for path in paths]

    ingest()


ingest_dag()

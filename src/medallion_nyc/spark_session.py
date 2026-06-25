import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# PySpark launches worker subprocesses via the `python3` on PATH by default,
# which may not be the venv's interpreter (e.g. a system Python with a
# different minor version) -- Spark refuses to run if driver and worker
# Python versions don't match. Pin both to the interpreter actually running
# this process so the venv is always what gets used.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def get_spark(app_name: str = "medallion-nyc") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from medallion_nyc.config import BRONZE_DIR, CONGESTION_PRICING_START, DEFAULT_MONTHS, SILVER_DIR
from medallion_nyc.spark_session import get_spark

logger = logging.getLogger(__name__)

# TLC's raw column names are inconsistently cased (VendorID, tpep_pickup_datetime,
# PULocationID, Airport_fee, ...). Normalize to snake_case once, here, so every
# downstream layer can rely on one consistent naming convention.
_RENAME_MAP = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "RatecodeID": "rate_code_id",
    "PULocationID": "pickup_zone_id",
    "DOLocationID": "dropoff_zone_id",
    "Airport_fee": "airport_fee",
}

# 264 ("Unknown") and 265 ("Outside of NYC") are explicit placeholder zone IDs
# in TLC's own taxi_zone_lookup.csv, not real zones -- trips can't be attributed
# to a zone-level demand bucket, so they're excluded rather than miscounted.
_VALID_ZONE_IDS = list(range(1, 264))


def load_bronze_month(spark: SparkSession, month: str, bronze_dir: Path = BRONZE_DIR) -> DataFrame:
    path = bronze_dir / f"yellow_tripdata_{month}.parquet"
    df = spark.read.parquet(str(path))
    for original, renamed in _RENAME_MAP.items():
        if original in df.columns:
            df = df.withColumnRenamed(original, renamed)
    if "cbd_congestion_fee" not in df.columns:
        # Column doesn't exist before the Jan 2025 congestion pricing rollout.
        df = df.withColumn("cbd_congestion_fee", F.lit(None).cast("double"))
    return df.withColumn("source_month", F.lit(month))


def load_bronze_months(spark: SparkSession, months: list[str], bronze_dir: Path = BRONZE_DIR) -> DataFrame:
    dfs = [load_bronze_month(spark, month, bronze_dir) for month in months]
    combined = dfs[0]
    for df in dfs[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)
    return combined


def clean(df: DataFrame) -> DataFrame:
    cleaned = (
        df.filter(F.col("fare_amount") > 0)
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("passenger_count") > 0)
        .filter(F.col("dropoff_datetime") > F.col("pickup_datetime"))
        .filter(F.col("pickup_zone_id").isin(_VALID_ZONE_IDS))
        .filter(F.col("dropoff_zone_id").isin(_VALID_ZONE_IDS))
        # A small number of trips in every TLC monthly file have corrupted
        # pickup timestamps -- some off by a single adjacent month, a few
        # off by decades (years like 2002 and 2008 show up in real data).
        # source_month is which file the row actually came from, so this
        # catches both kinds directly rather than guessing at a date range.
        .filter(F.date_format(F.col("pickup_datetime"), "yyyy-MM") == F.col("source_month"))
    )

    duration_minutes = (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0

    return cleaned.select(
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        F.round(duration_minutes, 2).alias("trip_duration_minutes"),
        "passenger_count",
        "trip_distance",
        "pickup_zone_id",
        "dropoff_zone_id",
        "payment_type",
        "fare_amount",
        "tip_amount",
        "tolls_amount",
        "total_amount",
        "congestion_surcharge",
        "cbd_congestion_fee",
        F.to_date("pickup_datetime").alias("pickup_date"),
        F.hour("pickup_datetime").alias("pickup_hour"),
        F.date_format("pickup_datetime", "yyyy-MM").alias("pickup_year_month"),
        (F.to_date("pickup_datetime") >= F.lit(str(CONGESTION_PRICING_START))).alias(
            "is_post_congestion_pricing"
        ),
    )


def run(
    months: list[str] | None = None,
    bronze_dir: Path = BRONZE_DIR,
    silver_dir: Path = SILVER_DIR,
) -> dict:
    months = months or DEFAULT_MONTHS
    spark = get_spark("medallion-nyc-silver")
    try:
        raw = load_bronze_months(spark, months, bronze_dir)
        raw_count = raw.count()
        cleaned = clean(raw)
        clean_count = cleaned.count()

        cleaned.write.format("delta").mode("overwrite").partitionBy("pickup_year_month").save(
            str(silver_dir)
        )

        rejected = raw_count - clean_count
        logger.info(
            "Silver: %d raw rows -> %d clean rows (%d rejected, %.1f%%)",
            raw_count,
            clean_count,
            rejected,
            100 * rejected / raw_count,
        )
        return {"raw_count": raw_count, "clean_count": clean_count, "rejected_count": rejected}
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(run())

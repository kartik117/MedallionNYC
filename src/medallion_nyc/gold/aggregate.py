import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from medallion_nyc.config import BRONZE_DIR, GOLD_DIR, SILVER_DIR
from medallion_nyc.spark_session import get_spark

logger = logging.getLogger(__name__)


def load_zone_lookup(spark: SparkSession, bronze_dir: Path = BRONZE_DIR) -> DataFrame:
    return spark.read.option("header", True).csv(str(bronze_dir / "taxi_zone_lookup.csv"))


def build_zone_hourly_metrics(silver_df: DataFrame) -> DataFrame:
    """Hourly demand, revenue, and fare-per-mile by zone -- the three headline
    metrics from the project brief, combined into one table since they share
    the same grouping keys."""
    return silver_df.groupBy(
        "pickup_zone_id", "pickup_date", "pickup_hour", "pickup_year_month", "is_post_congestion_pricing"
    ).agg(
        F.count("*").alias("trip_count"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg(F.col("fare_amount") / F.col("trip_distance")).alias("avg_fare_per_mile"),
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.avg("trip_duration_minutes").alias("avg_duration_minutes"),
    )


def build_demand_shift_by_zone(silver_df: DataFrame, zone_lookup_df: DataFrame) -> DataFrame:
    """Average daily pickups per zone before vs. after congestion pricing,
    answering the project's central question: which zones gained demand,
    which lost it."""
    daily_counts = silver_df.groupBy("pickup_zone_id", "pickup_date", "is_post_congestion_pricing").agg(
        F.count("*").alias("daily_trip_count")
    )
    period_avg = daily_counts.groupBy("pickup_zone_id", "is_post_congestion_pricing").agg(
        F.avg("daily_trip_count").alias("avg_daily_trips")
    )

    before = period_avg.filter(~F.col("is_post_congestion_pricing")).select(
        "pickup_zone_id", F.col("avg_daily_trips").alias("avg_daily_trips_before")
    )
    after = period_avg.filter(F.col("is_post_congestion_pricing")).select(
        "pickup_zone_id", F.col("avg_daily_trips").alias("avg_daily_trips_after")
    )

    comparison = before.join(after, "pickup_zone_id", "outer").fillna(
        0.0, subset=["avg_daily_trips_before", "avg_daily_trips_after"]
    )
    comparison = comparison.withColumn(
        "pct_change",
        F.when(
            F.col("avg_daily_trips_before") > 0,
            F.round(
                100
                * (F.col("avg_daily_trips_after") - F.col("avg_daily_trips_before"))
                / F.col("avg_daily_trips_before"),
                1,
            ),
        ),
    )

    return comparison.join(
        zone_lookup_df, comparison.pickup_zone_id == zone_lookup_df.LocationID, "left"
    ).select(
        comparison["pickup_zone_id"],
        zone_lookup_df["Borough"].alias("borough"),
        zone_lookup_df["Zone"].alias("zone_name"),
        "avg_daily_trips_before",
        "avg_daily_trips_after",
        "pct_change",
    )


def build_monthly_cbd_fee_summary(silver_df: DataFrame) -> DataFrame:
    """How the new CBD congestion toll actually shows up in the trip data,
    month by month, since the policy took effect."""
    return (
        silver_df.filter(F.col("is_post_congestion_pricing"))
        .groupBy("pickup_year_month")
        .agg(
            F.count("*").alias("total_trips"),
            F.sum(F.when(F.col("cbd_congestion_fee") > 0, 1).otherwise(0)).alias("trips_charged_cbd_fee"),
            F.sum("cbd_congestion_fee").alias("total_cbd_fee_revenue"),
        )
        .withColumn(
            "pct_trips_charged_cbd_fee",
            F.round(100 * F.col("trips_charged_cbd_fee") / F.col("total_trips"), 1),
        )
    )


def run(silver_dir: Path = SILVER_DIR, bronze_dir: Path = BRONZE_DIR, gold_dir: Path = GOLD_DIR) -> dict:
    spark = get_spark("medallion-nyc-gold")
    try:
        silver_df = spark.read.format("delta").load(str(silver_dir))
        zone_lookup_df = load_zone_lookup(spark, bronze_dir)

        tables = {
            "zone_hourly_metrics": build_zone_hourly_metrics(silver_df),
            "demand_shift_by_zone": build_demand_shift_by_zone(silver_df, zone_lookup_df),
            "monthly_cbd_fee_summary": build_monthly_cbd_fee_summary(silver_df),
        }

        counts = {}
        for name, df in tables.items():
            df.write.format("delta").mode("overwrite").save(str(gold_dir / name))
            counts[name] = df.count()
            logger.info("Gold table %s: %d rows", name, counts[name])

        return counts
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(run())

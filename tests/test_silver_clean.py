from datetime import datetime

from medallion_nyc.silver.clean import clean, load_bronze_month

_VALID_ROW = {
    "vendor_id": 1,
    "pickup_datetime": datetime(2025, 1, 10, 8, 0, 0),
    "dropoff_datetime": datetime(2025, 1, 10, 8, 10, 0),
    "passenger_count": 1,
    "trip_distance": 2.5,
    "pickup_zone_id": 100,
    "dropoff_zone_id": 150,
    "payment_type": 1,
    "fare_amount": 10.0,
    "tip_amount": 2.0,
    "tolls_amount": 0.0,
    "total_amount": 12.0,
    "congestion_surcharge": 0.0,
    "cbd_congestion_fee": 0.75,
    "source_month": "2025-01",
}


def make_row(**overrides):
    return {**_VALID_ROW, **overrides}


def test_clean_keeps_valid_rows(spark):
    df = spark.createDataFrame([make_row()])

    result = clean(df).collect()

    assert len(result) == 1
    assert result[0].trip_duration_minutes == 10.0
    assert result[0].pickup_hour == 8
    assert result[0].pickup_year_month == "2025-01"
    assert result[0].is_post_congestion_pricing is True


def test_clean_drops_non_positive_fare(spark):
    df = spark.createDataFrame([make_row(fare_amount=0.0), make_row(fare_amount=-5.0)])

    assert clean(df).count() == 0


def test_clean_drops_zero_distance(spark):
    df = spark.createDataFrame([make_row(trip_distance=0.0)])

    assert clean(df).count() == 0


def test_clean_drops_zero_passengers(spark):
    df = spark.createDataFrame([make_row(passenger_count=0)])

    assert clean(df).count() == 0


def test_clean_drops_dropoff_before_pickup(spark):
    df = spark.createDataFrame(
        [make_row(dropoff_datetime=datetime(2025, 1, 10, 7, 59, 0))]
    )

    assert clean(df).count() == 0


def test_clean_drops_unknown_and_outside_nyc_zones(spark):
    df = spark.createDataFrame(
        [make_row(pickup_zone_id=264), make_row(dropoff_zone_id=265)]
    )

    assert clean(df).count() == 0


def test_clean_marks_pre_congestion_pricing_rows(spark):
    df = spark.createDataFrame(
        [
            make_row(
                pickup_datetime=datetime(2024, 12, 15, 8, 0, 0),
                dropoff_datetime=datetime(2024, 12, 15, 8, 10, 0),
                source_month="2024-12",
            )
        ]
    )

    result = clean(df).collect()

    assert result[0].is_post_congestion_pricing is False


def test_clean_drops_rows_with_corrupted_timestamp_outside_source_month(spark):
    # Real TLC data quirk: a small number of rows in every monthly file have
    # a pickup_datetime that doesn't actually fall in that file's month --
    # sometimes off by one month, sometimes off by decades (e.g. year 2002).
    df = spark.createDataFrame(
        [
            make_row(source_month="2025-01", pickup_datetime=datetime(2002, 12, 5, 8, 0, 0), dropoff_datetime=datetime(2002, 12, 5, 8, 10, 0)),
            make_row(source_month="2025-01", pickup_datetime=datetime(2024, 12, 31, 23, 50, 0), dropoff_datetime=datetime(2025, 1, 1, 0, 5, 0)),
        ]
    )

    assert clean(df).count() == 0


def test_load_bronze_month_renames_columns_and_fills_missing_cbd_fee(spark, tmp_path):
    raw = spark.createDataFrame(
        [
            {
                "VendorID": 1,
                "tpep_pickup_datetime": datetime(2024, 11, 1, 9, 0, 0),
                "tpep_dropoff_datetime": datetime(2024, 11, 1, 9, 15, 0),
                "PULocationID": 100,
                "DOLocationID": 150,
                "fare_amount": 15.0,
            }
        ]
    )
    bronze_dir = tmp_path
    raw.write.parquet(str(bronze_dir / "yellow_tripdata_2024-11.parquet"))

    df = load_bronze_month(spark, "2024-11", bronze_dir=bronze_dir)

    assert "cbd_congestion_fee" in df.columns
    row = df.collect()[0]
    assert row.vendor_id == 1
    assert row.pickup_zone_id == 100
    assert row.dropoff_zone_id == 150
    assert row.cbd_congestion_fee is None
    assert row.source_month == "2024-11"

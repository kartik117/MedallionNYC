from datetime import date

from medallion_nyc.gold.aggregate import (
    build_demand_shift_by_zone,
    build_monthly_cbd_fee_summary,
    build_zone_hourly_metrics,
)

_SILVER_ROW = {
    "pickup_zone_id": 100,
    "dropoff_zone_id": 150,
    "pickup_date": date(2025, 1, 10),
    "pickup_hour": 8,
    "pickup_year_month": "2025-01",
    "is_post_congestion_pricing": True,
    "fare_amount": 10.0,
    "trip_distance": 5.0,
    "total_amount": 12.0,
    "trip_duration_minutes": 15.0,
    "cbd_congestion_fee": 0.75,
}


def make_row(**overrides):
    return {**_SILVER_ROW, **overrides}


def test_zone_hourly_metrics_aggregates_correctly(spark):
    df = spark.createDataFrame(
        [
            make_row(fare_amount=10.0, trip_distance=5.0, total_amount=12.0),
            make_row(fare_amount=20.0, trip_distance=10.0, total_amount=22.0),
        ]
    )

    result = build_zone_hourly_metrics(df).collect()

    assert len(result) == 1
    row = result[0]
    assert row.trip_count == 2
    assert row.total_revenue == 34.0
    assert row.avg_fare_per_mile == 2.0  # both rows are $10/5mi = $2/mi and $20/10mi = $2/mi


def test_demand_shift_by_zone_computes_pct_change(spark):
    rows = (
        # Zone 100: 1 trip/day before (2 days), 2 trips/day after (2 days) -> +100%
        [make_row(pickup_zone_id=100, pickup_date=date(2024, 12, 1), is_post_congestion_pricing=False)]
        + [make_row(pickup_zone_id=100, pickup_date=date(2024, 12, 2), is_post_congestion_pricing=False)]
        + [make_row(pickup_zone_id=100, pickup_date=date(2025, 1, 10), is_post_congestion_pricing=True)] * 2
        + [make_row(pickup_zone_id=100, pickup_date=date(2025, 1, 11), is_post_congestion_pricing=True)] * 2
    )
    silver_df = spark.createDataFrame(rows)
    zone_lookup_df = spark.createDataFrame(
        [{"LocationID": "100", "Borough": "Manhattan", "Zone": "Test Zone"}]
    )

    result = build_demand_shift_by_zone(silver_df, zone_lookup_df).collect()

    assert len(result) == 1
    row = result[0]
    assert row.borough == "Manhattan"
    assert row.zone_name == "Test Zone"
    assert row.avg_daily_trips_before == 1.0
    assert row.avg_daily_trips_after == 2.0
    assert row.pct_change == 100.0


def test_monthly_cbd_fee_summary_only_counts_post_pricing_trips(spark):
    rows = [
        make_row(is_post_congestion_pricing=True, cbd_congestion_fee=0.75),
        make_row(is_post_congestion_pricing=True, cbd_congestion_fee=0.75),
        make_row(is_post_congestion_pricing=True, cbd_congestion_fee=0.0),
        make_row(is_post_congestion_pricing=False, cbd_congestion_fee=None, pickup_year_month="2024-12"),
    ]
    df = spark.createDataFrame(rows)

    result = build_monthly_cbd_fee_summary(df).collect()

    assert len(result) == 1
    row = result[0]
    assert row.pickup_year_month == "2025-01"
    assert row.total_trips == 3
    assert row.trips_charged_cbd_fee == 2
    assert row.total_cbd_fee_revenue == 1.5
    assert row.pct_trips_charged_cbd_fee == 66.7

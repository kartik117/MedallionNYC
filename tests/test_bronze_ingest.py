import responses

from medallion_nyc.bronze.ingest import download_month, download_zone_lookup, ingest_months
from medallion_nyc.config import TAXI_ZONE_LOOKUP_URL, TLC_BASE_URL


@responses.activate
def test_download_month_writes_file(tmp_path):
    responses.add(
        responses.GET,
        f"{TLC_BASE_URL}/yellow_tripdata_2025-01.parquet",
        body=b"fake parquet bytes",
        status=200,
    )

    dest = download_month("2025-01", dest_dir=tmp_path)

    assert dest == tmp_path / "yellow_tripdata_2025-01.parquet"
    assert dest.read_bytes() == b"fake parquet bytes"


@responses.activate
def test_download_month_skips_existing_file(tmp_path):
    dest_path = tmp_path / "yellow_tripdata_2025-01.parquet"
    dest_path.write_bytes(b"already here")

    dest = download_month("2025-01", dest_dir=tmp_path)

    assert dest.read_bytes() == b"already here"
    assert len(responses.calls) == 0


@responses.activate
def test_download_zone_lookup_writes_file(tmp_path):
    responses.add(responses.GET, TAXI_ZONE_LOOKUP_URL, body=b"LocationID,Borough\n1,EWR\n", status=200)

    dest = download_zone_lookup(dest_dir=tmp_path)

    assert dest.name == "taxi_zone_lookup.csv"
    assert b"LocationID" in dest.read_bytes()


@responses.activate
def test_ingest_months_downloads_lookup_and_each_month(tmp_path):
    responses.add(responses.GET, TAXI_ZONE_LOOKUP_URL, body=b"lookup", status=200)
    responses.add(responses.GET, f"{TLC_BASE_URL}/yellow_tripdata_2024-12.parquet", body=b"dec", status=200)
    responses.add(responses.GET, f"{TLC_BASE_URL}/yellow_tripdata_2025-01.parquet", body=b"jan", status=200)

    paths = ingest_months(["2024-12", "2025-01"], dest_dir=tmp_path)

    assert [p.name for p in paths] == ["yellow_tripdata_2024-12.parquet", "yellow_tripdata_2025-01.parquet"]
    assert (tmp_path / "taxi_zone_lookup.csv").exists()

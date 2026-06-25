import logging
from pathlib import Path

import requests

from medallion_nyc.config import BRONZE_DIR, TAXI_ZONE_LOOKUP_URL, TLC_BASE_URL

logger = logging.getLogger(__name__)


def download_month(month: str, dest_dir: Path = BRONZE_DIR) -> Path:
    """Download one month of raw Yellow Taxi trip data, e.g. month='2025-01'.

    Idempotent: skips the download if the file already exists.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / f"yellow_tripdata_{month}.parquet"
    if dest_path.exists():
        logger.info("Already downloaded: %s", dest_path)
        return dest_path

    _download(f"{TLC_BASE_URL}/yellow_tripdata_{month}.parquet", dest_path)
    return dest_path


def download_zone_lookup(dest_dir: Path = BRONZE_DIR) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / "taxi_zone_lookup.csv"
    if dest_path.exists():
        return dest_path

    _download(TAXI_ZONE_LOOKUP_URL, dest_path)
    return dest_path


def ingest_months(months: list[str], dest_dir: Path = BRONZE_DIR) -> list[Path]:
    download_zone_lookup(dest_dir)
    return [download_month(month, dest_dir) for month in months]


def _download(url: str, dest_path: Path) -> None:
    """Streamed, atomic download: writes to a .tmp file then renames, so a
    crash mid-download can never leave a corrupt file at dest_path."""
    logger.info("Downloading %s -> %s", url, dest_path)
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")
    with requests.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        with open(tmp_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
    tmp_path.rename(dest_path)


if __name__ == "__main__":
    import sys

    from medallion_nyc.config import DEFAULT_MONTHS

    logging.basicConfig(level=logging.INFO)
    months = sys.argv[1:] or DEFAULT_MONTHS
    ingest_months(months)

from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# NYC's Congestion Relief Zone tolling program (Manhattan south of 60th St)
# started at midnight on this date. TLC trip data only has a cbd_congestion_fee
# column from this month onward -- verified directly against the source files,
# not just documentation.
CONGESTION_PRICING_START = date(2025, 1, 5)

# Three months on either side of the policy change. Deliberately not just one
# month each: a single month is too noisy (holidays, weather) to attribute a
# demand shift to the policy with any confidence.
DEFAULT_MONTHS = [
    "2024-10",
    "2024-11",
    "2024-12",
    "2025-01",
    "2025-02",
    "2025-03",
]

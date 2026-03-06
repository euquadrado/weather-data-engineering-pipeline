from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import json
import requests

from .config import DATA_DIR, days_back
from .logging import get_logger

logger = get_logger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

def extract_open_meteo_daily(lat: float, lon: float, city: str) -> Path:
    """Fetch daily climate data from Open-Meteo and write raw JSON to Bronze."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    bronze_dir = DATA_DIR / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    end = date.today()
    start = end - timedelta(days=days_back())

    params = {
        "latitude": lat,
        "longitude": lon,
        "timezone": "auto",
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "temperature_2m_mean",
            "precipitation_sum",
            "rain_sum",
            "wind_speed_10m_max",
        ],
    }

    logger.info("Extracting Open-Meteo daily for %s (%s, %s) [%s → %s]", city, lat, lon, start, end)
    r = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()

    out = bronze_dir / f"open_meteo_{city.replace(' ', '_').replace('/', '-')}.json"
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out

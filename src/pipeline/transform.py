from __future__ import annotations

from pathlib import Path
import json

import polars as pl

from .config import DATA_DIR
from .logging import get_logger

logger = get_logger(__name__)

def normalize_bronze(json_files: list[Path]) -> pl.DataFrame:
    """Normalize Open-Meteo raw JSON files into a single Bronze dataframe."""
    rows = []
    for jf in json_files:
        payload = json.loads(jf.read_text(encoding="utf-8"))
        city = jf.stem.replace("open_meteo_", "").replace("_", " ")

        daily = payload.get("daily", {})
        times = daily.get("time", [])
        for i, dt in enumerate(times):
            rows.append({
                "city": city,
                "date": dt,
                "temperature_2m_max_c": daily.get("temperature_2m_max", [None])[i],
                "temperature_2m_min_c": daily.get("temperature_2m_min", [None])[i],
                "temperature_2m_mean_c": daily.get("temperature_2m_mean", [None])[i],
                "precipitation_sum_mm": daily.get("precipitation_sum", [None])[i],
                "rain_sum_mm": daily.get("rain_sum", [None])[i],
                "wind_speed_10m_max_kmh": daily.get("wind_speed_10m_max", [None])[i],
            })

    df = pl.DataFrame(rows)
    df = df.with_columns(
        pl.col("date").str.strptime(pl.Date, strict=False),
        pl.col("city").cast(pl.Utf8),
    )
    return df

def write_bronze_parquet(df: pl.DataFrame) -> Path:
    bronze_dir = DATA_DIR / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    out = bronze_dir / "weather_daily.parquet"
    df.write_parquet(out)
    logger.info("Wrote bronze parquet: %s (rows=%s)", out, df.height)
    return out

def transform_to_silver(bronze_df: pl.DataFrame) -> pl.DataFrame:
    """Clean and standardize types; remove impossible values."""
    df = bronze_df.clone()

    # Basic cleaning rules
    df = df.filter(pl.col("temperature_2m_mean_c").is_not_null())
    df = df.filter((pl.col("temperature_2m_mean_c") > -60) & (pl.col("temperature_2m_mean_c") < 60))

    # Add month (YYYY-MM) for Gold aggregation
    df = df.with_columns(
        pl.col("date").dt.strftime("%Y-%m").alias("month")
    )

    # Enforce numeric types
    num_cols = [
        "temperature_2m_max_c",
        "temperature_2m_min_c",
        "temperature_2m_mean_c",
        "precipitation_sum_mm",
        "rain_sum_mm",
        "wind_speed_10m_max_kmh",
    ]
    df = df.with_columns([pl.col(c).cast(pl.Float64) for c in num_cols])

    return df

def write_silver_parquet(df: pl.DataFrame) -> Path:
    silver_dir = DATA_DIR / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)
    out = silver_dir / "weather_daily.parquet"
    df.write_parquet(out)
    logger.info("Wrote silver parquet: %s (rows=%s)", out, df.height)
    return out

def build_gold_city_monthly(silver_df: pl.DataFrame) -> pl.DataFrame:
    """Aggregate to monthly metrics per city."""
    gold = (
        silver_df
        .group_by(["city", "month"])
        .agg([
            pl.mean("temperature_2m_mean_c").alias("avg_temperature_2m_mean_c"),
            pl.mean("wind_speed_10m_max_kmh").alias("avg_wind_speed_10m_max_kmh"),
            pl.sum("precipitation_sum_mm").alias("total_precip_mm"),
            pl.sum("rain_sum_mm").alias("total_rain_mm"),
            pl.count().alias("days_count"),
        ])
        .sort(["city", "month"])
    )
    return gold

def write_gold_parquet(df: pl.DataFrame) -> Path:
    gold_dir = DATA_DIR / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)
    out = gold_dir / "city_monthly.parquet"
    df.write_parquet(out)
    logger.info("Wrote gold parquet: %s (rows=%s)", out, df.height)
    return out

import polars as pl
from src.pipeline.quality import validate_silver

def test_quality_passes():
    df = pl.DataFrame([
        {"city": "A", "date": "2026-01-01", "month": "2026-01",
         "temperature_2m_max_c": 25.0, "temperature_2m_min_c": 15.0, "temperature_2m_mean_c": 20.0,
         "precipitation_sum_mm": 0.0, "rain_sum_mm": 0.0, "wind_speed_10m_max_kmh": 10.0},
    ]).with_columns(pl.col("date").str.strptime(pl.Date, strict=False))
    validate_silver(df)

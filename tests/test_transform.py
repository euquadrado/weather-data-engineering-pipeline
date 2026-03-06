import polars as pl
from src.pipeline.transform import transform_to_silver, build_gold_city_monthly

def test_transform_filters_invalid_temperatures():
    df = pl.DataFrame([
        {"city": "X", "date": "2026-01-01", "temperature_2m_mean_c": 20, "temperature_2m_max_c": 25, "temperature_2m_min_c": 15,
         "precipitation_sum_mm": 0, "rain_sum_mm": 0, "wind_speed_10m_max_kmh": 10},
        {"city": "X", "date": "2026-01-02", "temperature_2m_mean_c": 200, "temperature_2m_max_c": 210, "temperature_2m_min_c": 190,
         "precipitation_sum_mm": 0, "rain_sum_mm": 0, "wind_speed_10m_max_kmh": 10},
    ]).with_columns(pl.col("date").str.strptime(pl.Date, strict=False))

    silver = transform_to_silver(df)
    assert silver.height == 1

def test_gold_aggregation():
    df = pl.DataFrame([
        {"city": "A", "date": "2026-01-01", "month": "2026-01", "temperature_2m_mean_c": 10.0, "wind_speed_10m_max_kmh": 5.0,
         "precipitation_sum_mm": 1.0, "rain_sum_mm": 1.0,
         "temperature_2m_max_c": 12.0, "temperature_2m_min_c": 8.0},
        {"city": "A", "date": "2026-01-02", "month": "2026-01", "temperature_2m_mean_c": 14.0, "wind_speed_10m_max_kmh": 7.0,
         "precipitation_sum_mm": 2.0, "rain_sum_mm": 2.0,
         "temperature_2m_max_c": 16.0, "temperature_2m_min_c": 12.0},
    ]).with_columns(pl.col("date").str.strptime(pl.Date, strict=False))

    gold = build_gold_city_monthly(df)
    assert gold.height == 1
    row = gold.row(0, named=True)
    assert row["total_precip_mm"] == 3.0
    assert row["days_count"] == 2

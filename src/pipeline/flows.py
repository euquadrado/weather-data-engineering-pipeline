from __future__ import annotations

from datetime import datetime
from pathlib import Path
from uuid import uuid4

from prefect import flow, task

from .config import CITIES
from .extract import extract_open_meteo_daily
from .transform import (
    normalize_bronze,
    write_bronze_parquet,
    transform_to_silver,
    write_silver_parquet,
    build_gold_city_monthly,
    write_gold_parquet,
)
from .quality import validate_silver
from .load import load_layers_to_postgres, log_run

@task(retries=2, retry_delay_seconds=10)
def t_extract_city(lat: float, lon: float, city: str) -> Path:
    return extract_open_meteo_daily(lat=lat, lon=lon, city=city)

@task
def t_bronze(json_files: list[Path]):
    bronze_df = normalize_bronze(json_files)
    write_bronze_parquet(bronze_df)
    return bronze_df

@task
def t_silver(bronze_df):
    silver_df = transform_to_silver(bronze_df)
    validate_silver(silver_df)
    write_silver_parquet(silver_df)
    return silver_df

@task
def t_gold(silver_df):
    gold_df = build_gold_city_monthly(silver_df)
    write_gold_parquet(gold_df)
    return gold_df

@task
def t_load(bronze_df, silver_df, gold_df):
    load_layers_to_postgres(bronze_df, silver_df, gold_df)

@flow(name="bronze-silver-gold-weather")
def main():
    run_id = str(uuid4())
    started = datetime.utcnow()

    status = "success"
    rows_bronze = rows_silver = rows_gold = 0
    finished = None

    try:
        json_files = []
        for c in CITIES:
            json_files.append(t_extract_city.submit(c["lat"], c["lon"], c["city"]).result())

        bronze_df = t_bronze(json_files)
        silver_df = t_silver(bronze_df)
        gold_df = t_gold(silver_df)
        t_load(bronze_df, silver_df, gold_df)

        rows_bronze = bronze_df.height
        rows_silver = silver_df.height
        rows_gold = gold_df.height

    except Exception:
        status = "failed"
        raise
    finally:
        finished = datetime.utcnow()
        # log metadata to Postgres (best-effort)
        try:
            log_run(run_id, started, finished, status, rows_bronze, rows_silver, rows_gold)
        except Exception:
            pass

if __name__ == "__main__":
    main()

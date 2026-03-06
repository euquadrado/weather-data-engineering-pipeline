from __future__ import annotations

import polars as pl
from sqlalchemy import create_engine, text

from .config import get_postgres_config
from .logging import get_logger

logger = get_logger(__name__)

def _to_sql(df: pl.DataFrame, table: str) -> None:
    """Load a Polars DataFrame into Postgres (replace table)."""
    cfg = get_postgres_config()
    engine = create_engine(cfg.sqlalchemy_url)

    # Use pandas for SQL write (most compatible)
    pdf = df.to_pandas()
    with engine.begin() as conn:
        # simple replace strategy
        pdf.to_sql(table, conn, if_exists="replace", index=False)
    logger.info("Loaded table: %s (rows=%s)", table, len(pdf))

def ensure_metadata_table() -> None:
    cfg = get_postgres_config()
    engine = create_engine(cfg.sqlalchemy_url)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id TEXT PRIMARY KEY,
                started_at TIMESTAMP NOT NULL,
                finished_at TIMESTAMP,
                status TEXT NOT NULL,
                rows_bronze INT,
                rows_silver INT,
                rows_gold INT
            );
        """))
    logger.info("Ensured metadata table pipeline_runs exists")

def log_run(run_id: str, started_at, finished_at, status: str, rows_bronze: int, rows_silver: int, rows_gold: int) -> None:
    cfg = get_postgres_config()
    engine = create_engine(cfg.sqlalchemy_url)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO pipeline_runs (run_id, started_at, finished_at, status, rows_bronze, rows_silver, rows_gold)
            VALUES (:run_id, :started_at, :finished_at, :status, :rows_bronze, :rows_silver, :rows_gold)
            ON CONFLICT (run_id) DO UPDATE SET
              finished_at = EXCLUDED.finished_at,
              status = EXCLUDED.status,
              rows_bronze = EXCLUDED.rows_bronze,
              rows_silver = EXCLUDED.rows_silver,
              rows_gold = EXCLUDED.rows_gold;
        """), {
            "run_id": run_id,
            "started_at": started_at,
            "finished_at": finished_at,
            "status": status,
            "rows_bronze": rows_bronze,
            "rows_silver": rows_silver,
            "rows_gold": rows_gold,
        })
    logger.info("Logged run metadata: %s (%s)", run_id, status)

def load_layers_to_postgres(bronze_df: pl.DataFrame, silver_df: pl.DataFrame, gold_df: pl.DataFrame) -> None:
    ensure_metadata_table()
    _to_sql(bronze_df, "bronze_weather_daily")
    _to_sql(silver_df, "silver_weather_daily")
    _to_sql(gold_df, "gold_city_monthly")

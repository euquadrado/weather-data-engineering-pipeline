from __future__ import annotations

import polars as pl
from .logging import get_logger

logger = get_logger(__name__)

def validate_silver(silver_df: pl.DataFrame) -> None:
    # Regras básicas de qualidade (exemplos)
    if silver_df.is_empty():
        raise ValueError("Silver está vazio")

    # temperatura média não pode ser nula
    if silver_df.filter(pl.col("temperature_2m_mean_c").is_null()).height > 0:
        raise ValueError("temperature_2m_mean_c contém NULL")

    # range plausível
    bad = silver_df.filter(
        (pl.col("temperature_2m_mean_c") <= -60) | (pl.col("temperature_2m_mean_c") >= 60)
    )
    if bad.height > 0:
        raise ValueError("temperature_2m_mean_c fora do range [-60, 60]")

    # precipitação e chuva não podem ser negativas
    bad_precip = silver_df.filter(
        (pl.col("precipitation_sum_mm") < 0) | (pl.col("rain_sum_mm") < 0)
    )
    if bad_precip.height > 0:
        raise ValueError("precipitation_sum_mm/rain_sum_mm negativos")

    logger.info("Silver data quality checks passed ✅")
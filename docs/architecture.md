## Componentes

- **Extract**: baixa dados diários de clima (Open-Meteo) para cidades pré-definidas.
- **Bronze**:
  - JSON raw por cidade
  - Parquet “raw normalized” (com campos básicos)
- **Transform + Quality**:
  - Normaliza tipos, garante ranges e nullability
  - Regras de qualidade (Pandera)
- **Silver**:
  - Parquet limpo/normalizado
  - Carga em Postgres (`silver_weather_daily`)
- **Gold**:
  - Agregações por cidade e mês (média de temperatura, total de precipitação)
  - Carga em Postgres (`gold_city_monthly`)

## Fluxo

1. `extract_open_meteo_daily()` → `data/bronze/open_meteo_<city>.json`
2. `normalize_to_bronze_parquet()` → `data/bronze/weather_daily.parquet`
3. `transform_to_silver()` + `validate_silver()` → `data/silver/weather_daily.parquet`
4. `build_gold_city_monthly()` → `data/gold/city_monthly.parquet`
5. `load_postgres_tables()` → bronze/silver/gold em Postgres
